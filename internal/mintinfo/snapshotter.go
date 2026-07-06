package mintinfo

import (
	"context"
	"log/slog"
	"time"
)

// SnapshotStore is the write + poller-state seam (implemented by
// *clickhouse.Store).
type SnapshotStore interface {
	PutSnapshot(ctx context.Context, mintURL, hash string, document []byte, at time.Time) error
	PutObservation(ctx context.Context, o Observation) error
	LastMintObservations(ctx context.Context) (map[string]LastObservation, error)
}

// Config parameterizes the snapshotter.
type Config struct {
	// Interval is how often RunOnce re-checks for due mints. Actual per-mint
	// cadence is bounded by MinAge, so this can be short (hourly) and still poll
	// each mint at most once per MinAge.
	Interval time.Duration
	// MinAge is the minimum time between polls of the same mint — the anti-spam
	// gate. Default 24h.
	MinAge time.Duration
	// Throttle is the delay between consecutive mint fetches in one pass, so a
	// large work-list trickles out instead of bursting. Default 1.5s.
	Throttle time.Duration
	// Timeout is the per-fetch HTTP budget. Default 8s.
	Timeout time.Duration
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = time.Hour
	}
	if c.MinAge <= 0 {
		c.MinAge = 24 * time.Hour
	}
	if c.Throttle < 0 {
		c.Throttle = 0
	}
	if c.Throttle == 0 {
		c.Throttle = 1500 * time.Millisecond
	}
	if c.Timeout <= 0 {
		c.Timeout = 8 * time.Second
	}
	return c
}

// Stats summarizes one pass.
type Stats struct {
	Due         int // mints polled this pass (were past MinAge)
	Skipped     int // not yet due
	Changed     int // polls whose canonical doc differed → new snapshot
	Unreachable int // polls that failed to yield a usable document
}

// Snapshotter walks the mint work-list, polling each due mint once per MinAge,
// canonicalizing NUT-06, and storing a full snapshot only when the content hash
// moves. It mirrors vertex.Syncer: Run loops, RunOnce does one pass. Errors are
// logged, never fatal — the observation log makes every pass independent.
type Snapshotter struct {
	store  SnapshotStore
	mints  MintLister
	fetch  InfoFetcher
	source Source
	cfg    Config
	now    func() time.Time // test seam; nil means time.Now
	logger *slog.Logger
}

func NewSnapshotter(store SnapshotStore, mints MintLister, fetch InfoFetcher, source Source, cfg Config, logger *slog.Logger) *Snapshotter {
	if logger == nil {
		logger = slog.Default()
	}
	return &Snapshotter{
		store:  store,
		mints:  mints,
		fetch:  fetch,
		source: source,
		cfg:    cfg.withDefaults(),
		now:    time.Now,
		logger: logger,
	}
}

// Run executes a pass immediately, then every Interval until the context ends.
func (s *Snapshotter) Run(ctx context.Context) {
	if s == nil || s.store == nil || s.mints == nil || s.fetch == nil {
		return
	}
	for {
		stats, err := s.RunOnce(ctx)
		if err != nil {
			s.logger.Error("mintinfo: snapshot pass failed", "error", err)
		} else {
			s.logger.Info("mintinfo: snapshot pass complete",
				"source", s.source.Name, "due", stats.Due, "skipped", stats.Skipped,
				"changed", stats.Changed, "unreachable", stats.Unreachable)
		}
		if !sleep(ctx, s.cfg.Interval) {
			return
		}
	}
}

// RunOnce polls every mint that is past MinAge, records an observation for each,
// and stores a new snapshot whenever the canonical document changed.
func (s *Snapshotter) RunOnce(ctx context.Context) (Stats, error) {
	targets, err := s.mints.MintURLs(ctx)
	if err != nil {
		return Stats{}, err
	}
	state, err := s.store.LastMintObservations(ctx)
	if err != nil {
		return Stats{}, err
	}

	var stats Stats
	for _, url := range targets {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		last := state[url]
		if !last.CheckedAt.IsZero() && s.now().Sub(last.CheckedAt) < s.cfg.MinAge {
			stats.Skipped++
			continue
		}
		if stats.Due > 0 && s.cfg.Throttle > 0 {
			if !sleep(ctx, s.cfg.Throttle) {
				return stats, ctx.Err()
			}
		}
		stats.Due++
		s.pollOne(ctx, url, last, &stats)
	}
	return stats, nil
}

// pollOne fetches, canonicalizes, stores-if-changed, and always records an
// observation. "reachable" means we obtained a USABLE canonical document — a
// fetch that returns unparseable JSON is recorded as unreachable so it never
// becomes the change basis (LastMintObservations reads reachable rows only).
func (s *Snapshotter) pollOne(ctx context.Context, url string, last LastObservation, stats *Stats) {
	raw, fetched := s.fetch.Info(ctx, url)

	obs := Observation{MintURL: url, CheckedAt: s.now().UTC()}
	if fetched {
		canonical, err := s.source.Canonicalize(raw)
		if err != nil {
			s.logger.Warn("mintinfo: canonicalize failed", "mint", url, "error", err)
		} else {
			obs.Hash = Hash(canonical)
			obs.Reachable = true
			if obs.Hash != last.Hash {
				obs.Changed = true
				stats.Changed++
				if perr := s.store.PutSnapshot(ctx, url, obs.Hash, canonical, s.now().UTC()); perr != nil {
					s.logger.Warn("mintinfo: snapshot store failed", "mint", url, "error", perr)
				}
			}
		}
	}
	if !obs.Reachable {
		stats.Unreachable++
	}
	if err := s.store.PutObservation(ctx, obs); err != nil {
		s.logger.Warn("mintinfo: observation store failed", "mint", url, "error", err)
	}
}

// sleep waits d or until ctx ends; false means the context ended.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
