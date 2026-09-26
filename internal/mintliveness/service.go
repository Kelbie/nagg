// Package mintliveness is nagg's in-memory liveness sweep over the mint
// work-list: is each mint answering right now, since when, and how fast.
//
// It is the mint twin of internal/aiproviders' health sweep and publishes the
// same three facts with the same semantics — status decided at serve time,
// unknown until probed and again once the probe is older than MaxAge,
// checkedAt omitted while unknown, latencyMs only from the last successful
// probe — so a client reads a mint's `status` exactly as it reads a
// provider's. It is NOT the snapshotter: internal/mintinfo polls /v1/info once
// a day to record what a mint SAYS and keeps that history in ClickHouse; this
// package asks every few minutes whether the mint ANSWERS and keeps nothing.
package mintliveness

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/vertex-lab/nagg/internal/mintinfo"
)

// Status values, spelled exactly as aiproviders spells them so the app's one
// status vocabulary covers mints and providers alike.
//
// StatusUnknown is deliberately distinct from StatusOffline: it means nagg has
// not established the mint's state, either because it joined the work-list
// after the last sweep or because its last probe is older than MaxAge.
const (
	StatusOnline  = "online"
	StatusOffline = "offline"
	StatusUnknown = "unknown"
)

// Status is one mint's liveness as of CheckedAt.
type Status struct {
	Status string `json:"status"`
	// CheckedAt is when this status was last established; nil while unknown,
	// because nothing has been established.
	CheckedAt *time.Time `json:"checkedAt,omitempty"`
	// LatencyMs is the round trip of the last SUCCESSFUL probe; 0 when the mint
	// has never answered one.
	LatencyMs int `json:"latencyMs,omitempty"`
}

// Unknown is the status of a mint nagg has not established: the answer for
// every key when no sweep is wired, and for a key the sweep has not reached.
func Unknown() Status { return Status{Status: StatusUnknown} }

// Config parameterizes the sweep.
type Config struct {
	// Interval is the sweep period. Default 5m.
	Interval time.Duration
	// Timeout bounds one /v1/info request. Default 8s, matching the
	// snapshotter's fetch budget so the two agree on what "slow" is.
	Timeout time.Duration
	// Concurrency bounds simultaneous probes. Mints are other people's
	// servers; a sweep must not look like a burst. Default 6.
	Concurrency int
	// MaxAge is how long a probe result stands. Past it the mint reports
	// unknown rather than continuing to assert a stale online/offline, which
	// is what takes the whole feed to unknown if the worker dies. Default 2h.
	MaxAge time.Duration
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = 5 * time.Minute
	}
	if c.Timeout <= 0 {
		c.Timeout = 8 * time.Second
	}
	if c.Concurrency <= 0 {
		c.Concurrency = 6
	}
	if c.MaxAge <= 0 {
		c.MaxAge = 2 * time.Hour
	}
	return c
}

// maxInfoBody caps the /v1/info read, as the snapshotter does: NUT-06 bodies
// are a few KB and a misbehaving endpoint must not stream forever.
const maxInfoBody = 1 << 20

// record is one mint's accumulated probe knowledge. A failed probe never
// deletes a record: the difference between "did not answer" and "does not
// exist" is the whole point of publishing a status.
type record struct {
	// target is the fetch URL, in mintinfo's path-preserving form — HTTP paths
	// are case-sensitive, and https://mint.minibits.cash/Bitcoin/v1/info is
	// not https://mint.minibits.cash/bitcoin/v1/info.
	target string
	// probedAt is zero until the first probe completes; that zero is what
	// makes a freshly listed mint unknown rather than offline.
	probedAt  time.Time
	reachable bool
	latency   time.Duration
}

// Service sweeps the mint work-list and serves the resulting statuses. It
// mirrors aiproviders.Service: Run loops, RunOnce does one pass, every failure
// is logged and none is fatal.
type Service struct {
	cfg     Config
	targets mintinfo.MintLister
	http    *http.Client
	logger  *slog.Logger
	now     func() time.Time

	passMu  sync.Mutex
	mu      sync.RWMutex
	records map[string]*record // keyed by Key(target)
}

// New builds a sweep over targets. A nil client gets one bounded by
// cfg.Timeout; the request context is bounded too, so either is enough.
func New(cfg Config, client *http.Client, targets mintinfo.MintLister, logger *slog.Logger) *Service {
	cfg = cfg.withDefaults()
	if client == nil {
		client = &http.Client{Timeout: cfg.Timeout}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		cfg:     cfg,
		targets: targets,
		http:    client,
		logger:  logger,
		now:     time.Now,
		records: map[string]*record{},
	}
}

// Key collapses every spelling of one mint onto the lookup key: mintinfo's
// normalization (scheme and host lowercased, path preserved, trailing slash
// trimmed) lowercased in full. That makes it equal to appview's dedup key for
// the same URL, so the handler may look up with either its own key or the raw
// URL and land on the same record.
func Key(raw string) string {
	return strings.ToLower(mintinfo.NormalizeMintURL(raw))
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

// RunOnce is one full pass: refresh the work-list, then probe every known mint.
func (s *Service) RunOnce(ctx context.Context) {
	s.passMu.Lock()
	defer s.passMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	s.refreshTargets(ctx)
	s.probeAll(ctx)
	if ctx.Err() != nil {
		return
	}
	s.mu.RLock()
	total, online := len(s.records), 0
	for _, rec := range s.records {
		if rec.reachable {
			online++
		}
	}
	s.mu.RUnlock()
	s.logger.Info("mint_liveness.sweep", "mints", total, "online", online)
}

// refreshTargets adds every work-list mint not yet known. Records are never
// removed here: the work-list is a union of two sources and either may fail
// for one pass, and a partial list must not wipe what nagg has established
// about the mints it did not name this time.
func (s *Service) refreshTargets(ctx context.Context) {
	if s.targets == nil {
		return
	}
	urls, err := s.targets.MintURLs(ctx)
	if err != nil {
		s.logger.Warn("mint_liveness.worklist_failed", "error", err)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, raw := range urls {
		target := mintinfo.NormalizeMintURL(raw)
		if target == "" {
			continue
		}
		key := Key(target)
		if _, known := s.records[key]; !known {
			s.records[key] = &record{target: target}
		}
	}
}

// probeAll probes every known mint with bounded concurrency.
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

// probe is one timed GET of <mint>/v1/info. A 2xx carrying a JSON object is
// reachable; a non-2xx, a network error, a timeout or an unparseable body is
// unreachable. There is no fallback to another endpoint: /v1/info is the one
// NUT-06 makes mandatory, and a mint that cannot serve it is not serving.
func (s *Service) probe(ctx context.Context, rec *record) {
	if ctx.Err() != nil {
		return
	}
	reachable, latency := s.get(ctx, rec.target+mintinfo.CashuNUT06.InfoPath)
	probedAt := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	rec.probedAt = probedAt
	rec.reachable = reachable
	if reachable {
		rec.latency = latency
	}
}

func (s *Service) get(ctx context.Context, url string) (bool, time.Duration) {
	reqCtx, cancel := context.WithTimeout(ctx, s.cfg.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return false, 0
	}
	start := s.now()
	resp, err := s.http.Do(req)
	if err != nil {
		return false, 0
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		return false, 0
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxInfoBody))
	took := s.now().Sub(start)
	if err != nil || !isJSONObject(body) {
		return false, 0
	}
	return true, took
}

// isJSONObject accepts exactly what a NUT-06 info document is: one JSON
// object. A 200 serving HTML (a parked domain, a reverse proxy's error page)
// is a server answering, not a mint answering.
func isJSONObject(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	return len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(trimmed)
}

// Liveness answers for every key given, under the caller's own spelling of
// it, so a handler indexes the result with the strings it passed. A key nagg
// has no record for — or whose record is older than MaxAge — reads unknown.
// Status is decided HERE, against the serving clock rather than the sweep
// clock, so a worker that dies takes the feed to unknown instead of leaving a
// stale online standing forever.
func (s *Service) Liveness(keys []string) map[string]Status {
	out := make(map[string]Status, len(keys))
	now := s.now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, key := range keys {
		rec := s.records[Key(key)]
		if rec == nil {
			out[key] = Unknown()
			continue
		}
		out[key] = rec.render(now, s.cfg.MaxAge)
	}
	return out
}

func (rec *record) render(now time.Time, maxAge time.Duration) Status {
	if rec.probedAt.IsZero() || now.Sub(rec.probedAt) > maxAge {
		return Unknown()
	}
	checkedAt := rec.probedAt.UTC().Truncate(time.Second)
	st := Status{Status: StatusOffline, CheckedAt: &checkedAt}
	if rec.reachable {
		st.Status = StatusOnline
		st.LatencyMs = int(rec.latency.Milliseconds())
	}
	return st
}
