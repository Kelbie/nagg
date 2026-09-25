package socialgraph

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/vertex-lab/nagg/internal/relayquery"
)

// StatsSource is nagg's own rollup of stored kind-3 events (pubkey_stats via
// clickhouse.Store.BatchPubkeyStats). Exact and free, and present only where
// the nostr module ingests kind 3 — a mint deployment stores kinds 0 and
// 38000, so its table is empty and this source is nil there.
type StatsSource interface {
	Followers(ctx context.Context, pubkeys []string) (map[string]Reach, error)
}

// VertexSource is the local Vertex DVM profile cache
// (clickhouse.Store.CachedVertexProfiles). Exact. It is declared in EVERY
// deployment — the plugin registry creates the three cache tables regardless
// of module set — so it is always worth asking, even where it is empty today.
type VertexSource interface {
	Followers(ctx context.Context, pubkeys []string) (map[string]Reach, error)
}

// RelayScanner counts, over the relays nagg already dials, the distinct
// authors whose kind-3 contact list names the target. Satisfied by
// relayquery.Client through NewRelayScanner.
type RelayScanner interface {
	ScanFollowers(ctx context.Context, pubkey string) (uint64, error)
}

// Config parameterizes the resolver.
type Config struct {
	// TTL is how long a resolved Reach stands before it is re-resolved.
	TTL time.Duration
	// Interval is how often the worker drains the pending queue.
	Interval time.Duration
	// Concurrency bounds simultaneous relay scans.
	Concurrency int
	// ScanTimeout bounds one relay scan.
	ScanTimeout time.Duration
	// MaxTracked caps how many pubkeys the resolver will ever hold. Callers
	// hand it whatever pubkeys their data contains, and announcements are free
	// to publish, so this is the bound that stops a spammer turning the
	// resolver into an unbounded relay crawler.
	MaxTracked int
}

func (c Config) withDefaults() Config {
	if c.TTL <= 0 {
		c.TTL = 6 * time.Hour
	}
	if c.Interval <= 0 {
		c.Interval = time.Minute
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 4
	}
	if c.ScanTimeout <= 0 {
		c.ScanTimeout = 20 * time.Second
	}
	if c.MaxTracked <= 0 {
		c.MaxTracked = 500
	}
	return c
}

type entry struct {
	reach      Reach
	resolvedAt time.Time
}

// Service is the one enrichment path /nostr/mint/discover and
// /app/ai-providers share.
//
// Reads never block: Reach answers from cache and queues what it does not
// know, and a background worker resolves the queue. That split is forced by
// the cheapest available source — a relay scan takes seconds per pubkey, which
// no request may pay, and which is exactly why the previous code reached for a
// table that happened to be empty and published the emptiness as zero.
type Service struct {
	cfg    Config
	stats  StatsSource
	vertex VertexSource
	relays RelayScanner
	logger *slog.Logger
	now    func() time.Time

	passMu  sync.Mutex
	mu      sync.Mutex
	entries map[string]entry
	pending map[string]struct{}
}

func NewService(cfg Config, stats StatsSource, vertexCache VertexSource, relays RelayScanner, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		cfg:     cfg.withDefaults(),
		stats:   stats,
		vertex:  vertexCache,
		relays:  relays,
		logger:  logger,
		now:     time.Now,
		entries: map[string]entry{},
		pending: map[string]struct{}{},
	}
}

// Reach answers for the given pubkeys without blocking on any source. A pubkey
// with no fresh answer comes back Unknown and is queued for the worker; an
// unusable pubkey comes back Unknown and is not queued, because no amount of
// scanning will resolve a string that is not a key.
func (s *Service) Reach(_ context.Context, pubkeys []string) (map[string]Reach, error) {
	out := make(map[string]Reach, len(pubkeys))
	if s == nil {
		return out, nil
	}
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, raw := range pubkeys {
		pubkey := NormalizePubkey(raw)
		if pubkey == "" {
			continue
		}
		if e, ok := s.entries[pubkey]; ok && now.Sub(e.resolvedAt) < s.cfg.TTL {
			out[raw] = e.reach
			continue
		}
		out[raw] = Unknown()
		if len(s.entries)+len(s.pending) < s.cfg.MaxTracked {
			s.pending[pubkey] = struct{}{}
		}
	}
	return out, nil
}

// Run drains the queue every Interval until the context ends.
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		s.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunOnce resolves everything currently queued, plus anything whose answer has
// aged past the TTL.
func (s *Service) RunOnce(ctx context.Context) {
	s.passMu.Lock()
	defer s.passMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	targets := s.drain()
	if len(targets) == 0 {
		return
	}

	// The exact sources are batched table reads, so ask them for everything at
	// once before spending a single relay scan.
	resolved := make(map[string]Reach, len(targets))
	for _, source := range []struct {
		name string
		from func(context.Context, []string) (map[string]Reach, error)
	}{
		{SourceGraph, s.statsFollowers},
		{SourceVertex, s.vertexFollowers},
	} {
		remaining := missing(targets, resolved)
		if len(remaining) == 0 {
			break
		}
		found, err := source.from(ctx, remaining)
		if err != nil {
			// A deployment without the nostr module has no pubkey_stats to
			// read. That is a missing signal, not a broken resolver: fall
			// through to the next source rather than publishing a zero.
			s.logger.Warn("socialgraph.source_failed", "source", source.name, "pubkeys", len(remaining))
			continue
		}
		for pubkey, reach := range found {
			if reach.Known {
				resolved[pubkey] = reach
			}
		}
	}

	scanned := s.scanRelays(ctx, missing(targets, resolved), resolved)

	now := s.now()
	s.mu.Lock()
	for pubkey, reach := range resolved {
		s.entries[pubkey] = entry{reach: reach, resolvedAt: now}
	}
	s.mu.Unlock()
	s.logger.Info("socialgraph.resolved",
		"targets", len(targets), "resolved", len(resolved), "relay_scans", scanned)
}

// drain takes the queued pubkeys plus every tracked pubkey whose answer has
// expired, in a stable order so a truncated pass always makes the same
// progress rather than shuffling.
func (s *Service) drain() []string {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	targets := make([]string, 0, len(s.pending)+len(s.entries))
	for pubkey := range s.pending {
		targets = append(targets, pubkey)
		delete(s.pending, pubkey)
	}
	for pubkey, e := range s.entries {
		if now.Sub(e.resolvedAt) >= s.cfg.TTL {
			targets = append(targets, pubkey)
		}
	}
	sort.Strings(targets)
	return uniqueSorted(targets)
}

func (s *Service) statsFollowers(ctx context.Context, pubkeys []string) (map[string]Reach, error) {
	if s.stats == nil {
		return nil, nil
	}
	return s.stats.Followers(ctx, pubkeys)
}

func (s *Service) vertexFollowers(ctx context.Context, pubkeys []string) (map[string]Reach, error) {
	if s.vertex == nil {
		return nil, nil
	}
	return s.vertex.Followers(ctx, pubkeys)
}

// scanRelays resolves the leftovers the exact sources could not answer, with
// bounded concurrency. A scan that fails leaves the pubkey unresolved — it
// stays Unknown and is retried next pass, rather than being recorded as zero.
func (s *Service) scanRelays(ctx context.Context, targets []string, into map[string]Reach) int {
	if s.relays == nil || len(targets) == 0 {
		return 0
	}
	sem := make(chan struct{}, s.cfg.Concurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	scanned := 0
	for _, pubkey := range targets {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(pubkey string) {
			defer wg.Done()
			defer func() { <-sem }()
			scanCtx, cancel := context.WithTimeout(ctx, s.cfg.ScanTimeout)
			defer cancel()
			followers, err := s.relays.ScanFollowers(scanCtx, pubkey)
			mu.Lock()
			defer mu.Unlock()
			scanned++
			if err != nil {
				return
			}
			into[pubkey] = FromRelays(followers)
		}(pubkey)
	}
	wg.Wait()
	return scanned
}

func missing(targets []string, resolved map[string]Reach) []string {
	out := make([]string, 0, len(targets))
	for _, pubkey := range targets {
		if _, done := resolved[pubkey]; !done {
			out = append(out, pubkey)
		}
	}
	return out
}

func uniqueSorted(sorted []string) []string {
	out := sorted[:0]
	var last string
	for i, v := range sorted {
		if i == 0 || v != last {
			out = append(out, v)
		}
		last = v
	}
	return out
}

// --- relay scanning -----------------------------------------------------------

// followerScanLimit bounds one kind-3 scan. Relays cap results of their own
// accord too, which is the other half of why SourceRelays is a lower bound.
const followerScanLimit = 2000

// relayQuerier is the one relay call the scan makes, narrowed so the counting
// rule can be tested without a relay. Satisfied by relayquery.Client.
//
// It is the PER-RELAY call on purpose. The fan-out Query folds every relay
// into one result and reports an error only when it ends up with no events at
// all (finishQuery), which makes two very different outcomes identical: "one
// relay answered and nobody follows this pubkey" and "every relay we asked
// fell over". Against a real relay set, where something is almost always
// failing, that collapsed every genuinely unfollowed operator into a
// permanent unresolved — 31 of 38 AI providers stuck at null in production,
// rescanned every pass and never resolving. Asking each relay separately is
// what makes "at least one relay answered" observable, which is the whole
// difference between a measured zero and a failed lookup.
type relayQuerier interface {
	QueryOne(ctx context.Context, relay string, filter map[string]any, timeout time.Duration) ([]relayquery.Event, error)
}

// relayScanner counts followers off the relays nagg already dials.
type relayScanner struct {
	client  relayQuerier
	relays  []string
	timeout time.Duration
}

// NewRelayScanner wraps the shared relay client. The scan asks every relay for
// kind-3 events carrying a `p` tag naming the target and counts DISTINCT
// AUTHORS, so the same contact list served by three relays counts once.
//
// Measured 2026-09-25 against nagg's own relay set: mint.minibits.cash's
// operator resolves to 174 distinct authors in about five seconds,
// mint.mountainlake.io to 77, mint.cubabitcoin.org to 39. Real numbers, no
// credentials, no Vertex credits — but floors, not counts, which is why the
// result is marked Approximate. (The Vertex cache, where it has an entry, puts
// minibits at 2982, which is the measure of how much of a floor this is.)
func NewRelayScanner(client relayquery.Client, timeout time.Duration) RelayScanner {
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	return relayScanner{client: client, relays: client.Relays, timeout: timeout}
}

// ScanFollowers asks every relay in parallel and counts the distinct authors
// whose contact list names the target.
//
// It returns an error only when NO relay answered. A relay that answers with
// nothing is evidence — the pubkey has no followers there — and a partial
// failure still yields a floor built from the relays that did answer.
func (r relayScanner) ScanFollowers(ctx context.Context, pubkey string) (uint64, error) {
	if len(r.relays) == 0 {
		return 0, errors.New("socialgraph: no relays configured")
	}
	filter := map[string]any{
		"kinds": []int{3},
		"#p":    []string{pubkey},
		"limit": followerScanLimit,
	}
	type outcome struct {
		events []relayquery.Event
		err    error
	}
	results := make(chan outcome, len(r.relays))
	for _, relay := range r.relays {
		go func(relay string) {
			events, err := r.client.QueryOne(ctx, relay, filter, r.timeout)
			results <- outcome{events: events, err: err}
		}(relay)
	}

	authors := make(map[string]struct{})
	answered := 0
	var lastErr error
	for range r.relays {
		res := <-results
		if res.err != nil {
			lastErr = res.err
			continue
		}
		answered++
		for _, event := range res.events {
			if event.Event != nil && event.Event.PubKey != "" {
				authors[event.Event.PubKey] = struct{}{}
			}
		}
	}
	if answered == 0 {
		if lastErr == nil {
			lastErr = errors.New("socialgraph: no relay answered")
		}
		return 0, lastErr
	}
	return uint64(len(authors)), nil
}
