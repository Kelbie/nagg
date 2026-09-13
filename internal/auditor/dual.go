package auditor

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"
)

var ErrUnavailable = errors.New("auditor: no snapshot within 24 hours")

// Dual owns the snapshot and its age. Only Run/RunOnce perform network I/O.
// Mints takes a short read lock, even while a refresh is blocked upstream.
// The snapshot is the UNION of both auditors (see RunOnce).

type Dual struct {
	primary, fallback Client
	refresh           time.Duration
	refreshMu         sync.Mutex
	mu                sync.RWMutex
	cached            []Mint
	fetchedAt         time.Time
	source            string
}

func NewDual(primary, fallback Client, refresh time.Duration) *Dual {
	if refresh <= 0 {
		refresh = time.Hour
	}
	return &Dual{primary: primary, fallback: fallback, refresh: refresh}
}

func (d *Dual) Run(ctx context.Context) {
	for ctx.Err() == nil {
		if err := d.RunOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("auditor.refresh.failed", "error", err)
		}
		if !pause(ctx, d.refresh) {
			return
		}
	}
}

func (d *Dual) Mints(context.Context) ([]Mint, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.cached == nil || time.Since(d.fetchedAt) > 24*time.Hour {
		return nil, ErrUnavailable
	}
	return cloneMints(d.cached), nil
}

func fetchMints(ctx context.Context, client Client) ([]Mint, error) {
	if client == nil {
		return nil, ErrUnavailable
	}
	if probe, ok := client.(interface{ Probe(context.Context) error }); ok {
		if err := probe.Probe(ctx); err != nil {
			return nil, err
		}
	}
	// Do not let the legacy stale cache masquerade as a fresh upstream fetch.
	if legacy, ok := client.(*LegacyClient); ok {
		return legacy.fetch(ctx)
	}
	return client.Mints(ctx)
}

// RunOnce refreshes the roster, then enriches ucash data at a bounded pace.
// A good roster is published before enrichment so boot warming is prompt.
//
// Both auditors are fetched on every pass and UNIONED: ucash tracks a small
// curated set (nine mints at the time of writing) while the legacy 8333
// auditor still lists ~65, so treating ucash as a replacement collapsed the
// discovery roster to a fraction of what the app used to show. Rows are
// deduped by normalized URL; when both auditors know a mint the ucash row wins
// (it carries measured uptime and an upstream update time), otherwise the
// legacy row rides through with Source "8333". One auditor failing degrades to
// the other; only both failing is an error.
func (d *Dual) RunOnce(ctx context.Context) error {
	d.refreshMu.Lock()
	defer d.refreshMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	primary, perr := fetchMints(ctx, d.primary)
	if err := ctx.Err(); err != nil {
		return err
	}
	fallback, ferr := fetchMints(ctx, d.fallback)
	if err := ctx.Err(); err != nil {
		return err
	}
	if perr != nil && len(primary) == 0 {
		primary = nil
	}
	if ferr != nil && len(fallback) == 0 {
		fallback = nil
	}
	if len(primary) == 0 && len(fallback) == 0 {
		if perr != nil {
			return perr
		}
		if ferr != nil {
			return ferr
		}
		return ErrUnavailable
	}
	mints := mergeRosters(primary, fallback)
	source := rosterSource(len(primary) > 0, len(fallback) > 0)
	at := time.Now()
	d.mu.Lock()
	oldSource := d.source
	d.cached, d.fetchedAt, d.source = cloneMints(mints), at, source
	d.mu.Unlock()
	if oldSource != source {
		slog.Info("auditor.source.changed", "from", oldSource, "source", source)
	}
	enriched := 0
	if len(primary) > 0 {
		if enricher, ok := d.primary.(interface {
			enrich(context.Context, []Mint) int
		}); ok {
			enriched = enricher.enrich(ctx, mints)
			d.mu.Lock()
			d.cached = mints
			d.mu.Unlock()
		}
	}
	slog.Info("auditor.refresh", "source", source, "mints", len(mints), "ucash", len(primary), "legacy", len(fallback), "uptimeEnriched", enriched)
	return nil
}

// rosterSource labels which auditors contributed to the current snapshot.
func rosterSource(ucash, legacy bool) string {
	switch {
	case ucash && legacy:
		return "ucash+8333"
	case ucash:
		return "ucash"
	default:
		return "8333"
	}
}

// rosterKey is the dedup key shared with appview's discovery merge: lowercase,
// trailing slash trimmed. It is a KEY, never a fetch target, so lowercasing the
// path is fine here (unlike mintinfo.NormalizeMintURL).
func rosterKey(url string) string {
	return strings.ToLower(strings.TrimRight(strings.TrimSpace(url), "/"))
}

// mergeRosters unions the ucash and legacy rosters by rosterKey. ucash rows are
// kept verbatim (Source "ucash"); legacy rows fill only the keys ucash lacks
// (Source "8333"). Order: ucash rows first in their upstream order, then the
// legacy-only rows in theirs, so the result is deterministic for a given input.
func mergeRosters(primary, fallback []Mint) []Mint {
	out := make([]Mint, 0, len(primary)+len(fallback))
	seen := make(map[string]struct{}, len(primary)+len(fallback))
	for _, m := range cloneMints(primary) {
		key := rosterKey(m.URL)
		if key == "" {
			continue
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		m.Source = "ucash"
		out = append(out, m)
	}
	for _, m := range cloneMints(fallback) {
		key := rosterKey(m.URL)
		if key == "" {
			continue
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		m.Source = "8333"
		out = append(out, m)
	}
	return out
}

func cloneMints(mints []Mint) []Mint {
	out := append([]Mint(nil), mints...)
	for i := range out {
		out[i].Units = append([]string(nil), out[i].Units...)
		out[i].Nuts = append([]byte(nil), out[i].Nuts...)
		if out[i].Uptime24h != nil {
			v := *out[i].Uptime24h
			out[i].Uptime24h = &v
		}
		if out[i].AvgLatencyMs != nil {
			v := *out[i].AvgLatencyMs
			out[i].AvgLatencyMs = &v
		}
	}
	return out
}
