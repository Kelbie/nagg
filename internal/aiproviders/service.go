package aiproviders

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/vertex-lab/nagg/internal/relayquery"
	"github.com/vertex-lab/nagg/internal/routstr"
)

// RelayFetcher is the Nostr read seam, satisfied by relayquery.Client — the
// same one the wallpapers and rates workers use. Discovery goes through nagg's
// existing relay layer rather than opening a second Nostr stack.
type RelayFetcher interface {
	Query(context.Context, map[string]any, time.Duration) ([]relayquery.Event, error)
}

// FollowerCounter resolves operator follower counts from nagg's own social
// graph (clickhouse.Store.BatchPubkeyStats, the same source
// /nostr/mint/discover ranks mint operators by). A deployment without the
// nostr module has no such graph; it passes nil and every provider reports 0
// followers rather than the service inventing a second source.
type FollowerCounter interface {
	Followers(ctx context.Context, pubkeys []string) (map[string]uint64, error)
}

// Config parameterizes discovery and the health sweep.
type Config struct {
	// Relays are queried for kind-38421 announcements.
	Relays []string
	// Seeds are node base URLs nagg already trusts (the configured Routstr
	// node and its fallbacks). They are providers in their own right AND the
	// first directories asked, so discovery still works with every relay down.
	Seeds []string
	// Interval is the sweep period. It is also the ttlSeconds the app is told
	// to respect, because a shorter client TTL only re-fetches the same answer.
	Interval time.Duration
	// Timeout bounds one HTTP probe request.
	Timeout time.Duration
	// Concurrency bounds simultaneous probes. Providers are third-party nodes
	// on other people's hardware; a sweep must not look like a burst.
	Concurrency int
	// MaxAge is how long a probe result stands for. Past it a provider reports
	// status unknown rather than continuing to assert a stale online/offline.
	// It must exceed CatalogMinAge or nodes without /v1/info flap to unknown
	// between catalog reads.
	MaxAge time.Duration
	// CatalogMinAge is the minimum gap between full /v1/models reads of one
	// provider. The catalog is the only source of the model counts and the only
	// source of the sealed-model count, but it is also the expensive request
	// (three quarters of a megabyte on the largest live node), so the cheap
	// /v1/info probe carries status between reads.
	CatalogMinAge time.Duration
	// MaxProviders caps the directory. Announcements are unauthenticated and
	// free to publish; the cap is what stops a spammer turning the picker into
	// a phone book.
	MaxProviders int
	// MaxDirectorySources caps how many nodes are asked for their
	// /v1/providers/ list in one sweep: the seeds first, then providers already
	// known to be online.
	MaxDirectorySources int
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = 5 * time.Minute
	}
	if c.Timeout <= 0 {
		c.Timeout = 10 * time.Second
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 6
	}
	if c.CatalogMinAge <= 0 {
		c.CatalogMinAge = 30 * time.Minute
	}
	if c.MaxAge <= 0 {
		c.MaxAge = 2 * time.Hour
	}
	if c.MaxAge <= c.CatalogMinAge {
		c.MaxAge = 2 * c.CatalogMinAge
	}
	if c.MaxProviders <= 0 {
		c.MaxProviders = 100
	}
	if c.MaxDirectorySources <= 0 {
		c.MaxDirectorySources = 8
	}
	return c
}

// record is one provider's accumulated knowledge: what discovery said about it,
// and the outcome of its last probe. A failed probe never deletes a record —
// dropping a provider from the list would read to the app as "it does not
// exist", when what nagg knows is "it did not answer".
type record struct {
	baseURL string
	name    string
	pubkey  string
	mints   []string

	// probedAt is zero until the first probe completes; that zero is what makes
	// a freshly discovered provider unknown rather than offline.
	probedAt  time.Time
	reachable bool
	latency   time.Duration

	// modelCount/encrypted are carried forward from the last SUCCESSFUL catalog
	// read, including across probe failures: what a provider serves when it is
	// up is still the best answer to "how big is it" while it is down.
	modelCount int
	encrypted  int
	countsAt   time.Time

	// infoUnsupported records that this node answered /v1/info with a non-2xx
	// status (older nodes 404 it). Its status probe is the catalog read from
	// then on, so the sweep stops paying for a request that can only fail.
	infoUnsupported bool
	followers       uint64
}

// Service discovers Routstr providers, sweeps their health, and serves the
// resulting directory. It mirrors wallpapers.Service: Run loops, RunOnce does
// one pass, every failure is logged and none is fatal.
type Service struct {
	cfg       Config
	relays    RelayFetcher
	followers FollowerCounter
	http      *http.Client
	logger    *slog.Logger
	now       func() time.Time

	passMu  sync.Mutex
	mu      sync.RWMutex
	records map[string]*record
	sweptAt time.Time
}

func NewService(cfg Config, relays RelayFetcher, followers FollowerCounter, logger *slog.Logger) *Service {
	cfg = cfg.withDefaults()
	if logger == nil {
		logger = slog.Default()
	}
	s := &Service{
		cfg:       cfg,
		relays:    relays,
		followers: followers,
		http:      &http.Client{Timeout: cfg.Timeout},
		logger:    logger,
		now:       time.Now,
		records:   map[string]*record{},
	}
	// The seeds are providers, not just directory sources: with every relay
	// unreachable the route still answers with the nodes nagg is configured to
	// pay, instead of an empty picker.
	for _, seed := range cfg.Seeds {
		if url := normalizeBaseURL(seed); url != "" {
			s.absorb(found{baseURL: url})
		}
	}
	return s
}

// Run sweeps immediately, then every Interval until the context ends.
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

// RunOnce is one full pass: discover, probe, count followers.
func (s *Service) RunOnce(ctx context.Context) {
	s.passMu.Lock()
	defer s.passMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	discovered := s.discover(ctx)
	s.probeAll(ctx)
	s.refreshFollowers(ctx)
	if ctx.Err() != nil {
		// A cancelled pass is a partial pass; do not stamp it as a sweep.
		return
	}
	s.mu.Lock()
	s.sweptAt = s.now()
	total, online := len(s.records), 0
	for _, rec := range s.records {
		if rec.reachable {
			online++
		}
	}
	s.mu.Unlock()
	s.logger.Info("ai_providers.sweep", "discovered", discovered, "providers", total, "online", online)
}

// Directory renders the current snapshot, sorted best-first. ok is false only
// while nothing at all is known, which is the 503 "warming" window; once any
// provider is known the route answers, with statuses that say how much of it
// has actually been established.
func (s *Service) Directory() (Directory, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.records) == 0 {
		return Directory{}, false
	}
	now := s.now()
	providers := make([]Provider, 0, len(s.records))
	for _, rec := range s.records {
		providers = append(providers, rec.render(now, s.cfg.MaxAge))
	}
	Sort(providers)
	if len(providers) > s.cfg.MaxProviders {
		providers = providers[:s.cfg.MaxProviders]
	}
	checkedAt := s.sweptAt
	if checkedAt.IsZero() {
		checkedAt = now
	}
	return Directory{
		Providers:  providers,
		CheckedAt:  checkedAt.UTC().Truncate(time.Second),
		TTLSeconds: int(s.cfg.Interval / time.Second),
	}, true
}

// render turns one record into a response row. Status is decided HERE, against
// the serving clock rather than the sweep clock, so a worker that dies takes
// the directory to unknown instead of leaving a stale "online" standing
// forever.
func (rec *record) render(now time.Time, maxAge time.Duration) Provider {
	p := Provider{
		BaseURL:             rec.baseURL,
		Name:                displayName(rec.name, rec.baseURL),
		Pubkey:              rec.pubkey,
		Followers:           rec.followers,
		ModelCount:          rec.modelCount,
		EncryptedModelCount: rec.encrypted,
		Mints:               rec.mints,
		Status:              StatusUnknown,
	}
	if p.Mints == nil {
		// An empty list, never null: the payment path reads "no mints
		// published" as "any mint", and a null would make the app guess which.
		p.Mints = []string{}
	}
	if rec.probedAt.IsZero() || now.Sub(rec.probedAt) > maxAge {
		return p
	}
	checkedAt := rec.probedAt.UTC().Truncate(time.Second)
	p.CheckedAt = &checkedAt
	if rec.reachable {
		p.Status = StatusOnline
		p.LatencyMs = int(rec.latency.Milliseconds())
	} else {
		p.Status = StatusOffline
	}
	return p
}

// --- discovery ---------------------------------------------------------------

// discover merges the Nostr registry with the HTTP directories and returns how
// many rows were seen. Neither source is authoritative and both are optional:
// relays down leaves the directories, directories disabled leaves the relays,
// and both silent leaves the seeds already in the map.
func (s *Service) discover(ctx context.Context) int {
	var hits []found
	hits = append(hits, s.fromRelays(ctx)...)
	for _, source := range s.directorySources() {
		hits = append(hits, s.fromDirectory(ctx, source)...)
	}
	for _, hit := range hits {
		s.absorb(hit)
	}
	return len(hits)
}

func (s *Service) fromRelays(ctx context.Context) []found {
	if s.relays == nil || len(s.cfg.Relays) == 0 {
		return nil
	}
	events, err := s.relays.Query(ctx, map[string]any{
		"kinds": []int{AnnouncementKind},
		"limit": announcementLimit,
	}, s.cfg.Timeout)
	if err != nil {
		s.logger.Warn("ai_providers.discovery.relays_failed", "relays", len(s.cfg.Relays))
		return nil
	}
	out := make([]found, 0, len(events))
	for _, event := range events {
		out = append(out, parseAnnouncement(event.Event)...)
	}
	return out
}

// directorySources is the seeds first, then providers already known to be
// online — a node's directory holds only what it has seen, so asking several
// is how coverage grows. Bounded and deterministically ordered so a sweep
// cannot fan out with the directory it just discovered.
func (s *Service) directorySources() []string {
	seen := make(map[string]struct{}, s.cfg.MaxDirectorySources)
	out := make([]string, 0, s.cfg.MaxDirectorySources)
	add := func(url string) bool {
		if _, dup := seen[url]; dup || url == "" {
			return len(out) < s.cfg.MaxDirectorySources
		}
		seen[url] = struct{}{}
		out = append(out, url)
		return len(out) < s.cfg.MaxDirectorySources
	}
	for _, seed := range s.cfg.Seeds {
		if !add(normalizeBaseURL(seed)) {
			return out
		}
	}
	s.mu.RLock()
	online := make([]string, 0, len(s.records))
	for url, rec := range s.records {
		if rec.reachable {
			online = append(online, url)
		}
	}
	s.mu.RUnlock()
	sort.Strings(online)
	for _, url := range online {
		if !add(url) {
			return out
		}
	}
	return out
}

func (s *Service) fromDirectory(ctx context.Context, baseURL string) []found {
	body, _, ok := s.get(ctx, baseURL+"/v1/providers/")
	if !ok {
		return nil
	}
	return parseDirectory(body)
}

// absorb merges one discovery hit into the record set, filling gaps rather than
// overwriting: an announcement usually has the name, a directory usually has
// the mints, and /v1/info has the operator's own key. Whichever arrives first
// wins its field; a later source only supplies what is still missing.
func (s *Service) absorb(hit found) {
	if hit.baseURL == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := s.records[hit.baseURL]
	if rec == nil {
		if len(s.records) >= s.cfg.MaxProviders {
			return
		}
		rec = &record{baseURL: hit.baseURL}
		s.records[hit.baseURL] = rec
	}
	if rec.name == "" {
		rec.name = hit.name
	}
	if rec.pubkey == "" {
		rec.pubkey = hit.pubkey
	}
	if len(rec.mints) == 0 {
		rec.mints = hit.mints
	}
}

// --- health sweep -------------------------------------------------------------

// probeAll probes every known provider with bounded concurrency.
func (s *Service) probeAll(ctx context.Context) {
	s.mu.RLock()
	targets := make([]*record, 0, len(s.records))
	for _, rec := range s.records {
		targets = append(targets, rec)
	}
	s.mu.RUnlock()

	sem := make(chan struct{}, s.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, rec := range targets {
		if ctx.Err() != nil {
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(rec *record) {
			defer wg.Done()
			defer func() { <-sem }()
			s.probe(ctx, rec)
		}(rec)
	}
	wg.Wait()
}

// probe establishes one provider's status, and refreshes its catalog counts
// when they are due.
//
// The cheap request is /v1/info: a few hundred bytes that also carry the node's
// own name, npub and accepted mints. The expensive one is /v1/models, which is
// the only place the model counts exist. So /v1/info answers "is it up" on most
// sweeps and /v1/models runs at most once per CatalogMinAge — except on the
// older nodes that 404 /v1/info, where the catalog read is the only probe there
// is.
func (s *Service) probe(ctx context.Context, rec *record) {
	s.mu.RLock()
	infoUnsupported, countsAt := rec.infoUnsupported, rec.countsAt
	s.mu.RUnlock()

	var reachable bool
	var latency time.Duration
	var info *nodeInfo
	var nowUnsupported bool

	if !infoUnsupported {
		body, took, ok := s.get(ctx, rec.baseURL+"/v1/info")
		if ok {
			var parsed nodeInfo
			if err := json.Unmarshal(body, &parsed); err == nil {
				info = &parsed
			}
			reachable, latency = true, took
		} else if body != nil {
			// A response arrived, just not a usable one: this node does not
			// serve /v1/info. Stop asking and let the catalog read decide.
			nowUnsupported = true
		}
	}

	catalogDue := countsAt.IsZero() || s.now().Sub(countsAt) >= s.cfg.CatalogMinAge
	models := -1
	encrypted := 0
	if catalogDue || !reachable {
		body, took, ok := s.get(ctx, rec.baseURL+"/v1/models")
		if ok {
			// One parser for one catalog: routstr.ParseModels is what
			// /app/ai-lineup reads the same bodies with, so both endpoints
			// agree on which rows are models and which upstream serves them.
			if parsed, err := routstr.ParseModels(body); err == nil {
				models, encrypted = len(parsed), countEncrypted(parsed)
			}
			if !reachable {
				reachable, latency = true, took
			}
		}
	}

	probedAt := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if nowUnsupported {
		rec.infoUnsupported = true
	}
	rec.probedAt = probedAt
	rec.reachable = reachable
	if reachable {
		rec.latency = latency
	}
	if models >= 0 {
		rec.modelCount, rec.encrypted, rec.countsAt = models, encrypted, probedAt
	}
	if info != nil {
		applyInfo(rec, info)
	}
}

// countEncrypted counts the models a node serves through a Tinfoil enclave.
//
// It counts MODELS, not providers, on purpose — see Provider.EncryptedModelCount.
func countEncrypted(models []routstr.Model) int {
	n := 0
	for _, m := range models {
		if m.Encrypted() {
			n++
		}
	}
	return n
}

// applyInfo lets a node's own /v1/info override what third parties said about
// it. This is the one source where the subject is the author: an announcement
// re-served by another node can be stale or simply wrong about the name, the
// operator key, or — the field that decides whether a user can pay at all —
// which mints it redeems.
func applyInfo(rec *record, info *nodeInfo) {
	if name := strings.TrimSpace(info.Name); name != "" {
		rec.name = name
	}
	if mints := cleanMints(info.Mints); len(mints) > 0 {
		rec.mints = mints
	}
	if pubkey := decodeNpub(info.Npub); pubkey != "" {
		rec.pubkey = pubkey
	}
}

// decodeNpub turns a node's published npub into the hex every social-graph read
// in nagg takes. A bare hex key is accepted too, since some nodes publish one.
func decodeNpub(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if hex := normalizeHexPubkey(raw); hex != "" {
		return hex
	}
	prefix, value, err := nip19.Decode(raw)
	if err != nil || prefix != "npub" {
		return ""
	}
	hex, _ := value.(string)
	return normalizeHexPubkey(hex)
}

// get performs one bounded GET. It returns the body, the round trip, and
// whether the response was usable. A non-2xx response returns a non-nil
// (possibly empty) body so the caller can tell "answered badly" — which is
// information about the route — from "did not answer", which is information
// about the node.
func (s *Service) get(ctx context.Context, url string) ([]byte, time.Duration, bool) {
	reqCtx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, false
	}
	start := s.now()
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return []byte{}, 0, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	took := s.now().Sub(start)
	if err != nil {
		return nil, 0, false
	}
	return body, took, true
}

// --- social ------------------------------------------------------------------

// refreshFollowers resolves operator reach from nagg's own graph — the same
// pubkey_stats read that ranks mint operators on /nostr/mint/discover — in one
// batched query rather than a lookup per provider.
func (s *Service) refreshFollowers(ctx context.Context) {
	if s.followers == nil || ctx.Err() != nil {
		return
	}
	s.mu.RLock()
	pubkeys := make([]string, 0, len(s.records))
	seen := make(map[string]struct{}, len(s.records))
	for _, rec := range s.records {
		if rec.pubkey == "" {
			continue
		}
		if _, dup := seen[rec.pubkey]; dup {
			continue
		}
		seen[rec.pubkey] = struct{}{}
		pubkeys = append(pubkeys, rec.pubkey)
	}
	s.mu.RUnlock()
	if len(pubkeys) == 0 {
		return
	}
	counts, err := s.followers.Followers(ctx, pubkeys)
	if err != nil {
		// A deployment without the nostr module has no graph to read. That is a
		// missing signal, not a broken directory: every row keeps its last
		// known count and the sweep goes on.
		s.logger.Warn("ai_providers.followers_failed", "pubkeys", len(pubkeys))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rec := range s.records {
		if n, ok := counts[rec.pubkey]; ok {
			rec.followers = n
		}
	}
}
