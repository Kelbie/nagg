package auditor

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

var ErrUnavailable = errors.New("auditor: no snapshot within 24 hours")

// Dual owns the snapshot and its age. Only Run/RunOnce perform network I/O.
// Mints takes a short read lock, even while a refresh is blocked upstream.
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
func (d *Dual) RunOnce(ctx context.Context) error {
	d.refreshMu.Lock()
	defer d.refreshMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	mints, err := fetchMints(ctx, d.primary)
	source := "ucash"
	if err != nil || len(mints) == 0 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		mints, err = fetchMints(ctx, d.fallback)
		source = "8333"
	}
	if err != nil {
		return err
	}
	if len(mints) == 0 {
		return ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	mints = cloneMints(mints)
	for i := range mints {
		mints[i].Source = source
	}
	at := time.Now()
	d.mu.Lock()
	oldSource := d.source
	d.cached, d.fetchedAt, d.source = cloneMints(mints), at, source
	d.mu.Unlock()
	if oldSource != source {
		slog.Info("auditor.source.changed", "from", oldSource, "source", source)
	}
	enriched := 0
	if source == "ucash" {
		if enricher, ok := d.primary.(interface {
			enrich(context.Context, []Mint) int
		}); ok {
			enriched = enricher.enrich(ctx, mints)
			d.mu.Lock()
			d.cached = mints
			d.mu.Unlock()
		}
	}
	slog.Info("auditor.refresh", "source", source, "mints", len(mints), "uptimeEnriched", enriched)
	return nil
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
