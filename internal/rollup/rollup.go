// Package rollup hosts the periodic database-first aggregation job. Each tick it
// recomputes, over a bounded recent/engaged window, the direct-reply edges, the
// vertex-real engagement counts, the per-user stats, and the per-event rank
// features that the For-You / trending hot path reads. It mirrors the shape of
// internal/vertex.Syncer (Run / RunOnce / sleep).
package rollup

import (
	"context"
	"log/slog"
	"time"

	"github.com/vertex-lab/nagg/internal/chgate"
	"github.com/vertex-lab/nagg/internal/clickhouse"
)

// Store is the subset of the ClickHouse store the rollup needs. Defined as an
// interface so the runner can be unit-tested with a mock.
type Store interface {
	RecomputeReplyEdges(ctx context.Context, since time.Time, limit int) error
	RecomputeEngagementReal(ctx context.Context, since time.Time, limit int, th clickhouse.Thresholds, computedAt time.Time) error
	RecomputeUserStats(ctx context.Context, since time.Time, limit int, computedAt time.Time) error
	RecomputeRankFeatures(ctx context.Context, since time.Time, limit int, th clickhouse.Thresholds, computedAt time.Time) error
	LoadRollupState(ctx context.Context, task string) (clickhouse.RollupState, error)
	SaveRollupState(ctx context.Context, st clickhouse.RollupState) error
}

const rollupTask = "engagement"

type Config struct {
	Interval     time.Duration
	RecentWindow time.Duration
	MaxTargets   int
	Thresholds   clickhouse.Thresholds
}

type Runner struct {
	store  Store
	config Config
	logger *slog.Logger
	now    func() time.Time
	// gate is the process-wide heavy-ClickHouse gate. The rollup's periodic
	// multi-way aggregations previously ran ungated alongside user reads on
	// the same capacity-limited instance; each tick now holds one slot so a
	// rollup can't stack on top of a full request burst. nil = ungated.
	gate *chgate.Gate
}

// WithGate installs the shared heavy-query gate (see chgate).
func (r *Runner) WithGate(gate *chgate.Gate) *Runner {
	if r != nil {
		r.gate = gate
	}
	return r
}

func NewRunner(store Store, cfg Config, logger *slog.Logger) *Runner {
	if cfg.Interval <= 0 {
		cfg.Interval = 15 * time.Minute
	}
	if cfg.RecentWindow <= 0 {
		cfg.RecentWindow = 48 * time.Hour
	}
	if cfg.MaxTargets <= 0 {
		cfg.MaxTargets = 50000
	}
	if cfg.Thresholds.Version == "" {
		cfg.Thresholds.Version = "v1"
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{store: store, config: cfg, logger: logger, now: time.Now}
}

func (r *Runner) Run(ctx context.Context) {
	if r == nil || r.store == nil {
		return
	}
	for {
		if err := r.RunOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Error("rollup failed", "error", err)
		}
		if err := sleep(ctx, r.config.Interval); err != nil {
			return
		}
	}
}

// RunOnce recomputes one bounded batch in dependency order: reply edges (feed the
// real-reply count and rank features), then real engagement, then user stats, then
// rank features. A stage error aborts the tick so the next tick retries cleanly.
func (r *Runner) RunOnce(ctx context.Context) error {
	release, err := r.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()

	now := r.now()
	since := now.Add(-r.config.RecentWindow)
	limit := r.config.MaxTargets
	th := r.config.Thresholds

	if err := r.store.RecomputeReplyEdges(ctx, since, limit); err != nil {
		return err
	}
	if err := r.store.RecomputeEngagementReal(ctx, since, limit, th, now); err != nil {
		return err
	}
	if err := r.store.RecomputeUserStats(ctx, since, limit, now); err != nil {
		return err
	}
	if err := r.store.RecomputeRankFeatures(ctx, since, limit, th, now); err != nil {
		return err
	}

	state := clickhouse.RollupState{Task: rollupTask, CursorCreatedAt: since, LastRunAt: now}
	if err := r.store.SaveRollupState(ctx, state); err != nil {
		return err
	}
	r.logger.Info("rollup batch metrics",
		"since", since,
		"window", r.config.RecentWindow.String(),
		"max_targets", limit,
		"threshold_version", th.Version,
		"min_actor_score", th.MinActorScore,
	)
	return nil
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
