// Package rollup hosts the periodic database-first aggregation job. Each tick it
// recomputes, over a bounded recent/engaged window, the direct-reply edges, the
// vertex-real engagement counts, the per-user stats, and the per-event rank
// features that the For-You / trending hot path reads. It mirrors the shape of
// internal/vertex.Syncer (Run / RunOnce / sleep).
package rollup

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/vertex-lab/nagg/internal/chgate"
	"github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/safego"
)

// Store is the subset of the ClickHouse store the rollup needs. Defined as an
// interface so the runner can be unit-tested with a mock.
type Store interface {
	RecomputeReplyEdges(ctx context.Context, since time.Time, limit int) error
	RecomputeEngagementReal(ctx context.Context, since time.Time, limit int, th clickhouse.Thresholds, computedAt time.Time) error
	RecomputeUserStats(ctx context.Context, since time.Time, limit int, computedAt time.Time) error
	RecomputeRankFeatures(ctx context.Context, since time.Time, limit int, th clickhouse.Thresholds, computedAt time.Time) error
	RecomputeNotificationsFeed(ctx context.Context, now time.Time) (bool, error)
	RunRetention(ctx context.Context, dryRun bool) ([]clickhouse.RetentionRunResult, error)
	LoadRollupState(ctx context.Context, task string) (clickhouse.RollupState, error)
	SaveRollupState(ctx context.Context, st clickhouse.RollupState) error
}

const rollupTask = "engagement"

type Config struct {
	Interval     time.Duration
	RecentWindow time.Duration
	MaxTargets   int
	Thresholds   clickhouse.Thresholds
	// NotificationsInterval paces the notifications_feed incremental tick —
	// much faster than the main rollup (notifications must be seconds-to-a-
	// minute fresh, not 15m). During historical catch-up the loop ticks
	// near-continuously until the watermark reaches now.
	NotificationsInterval time.Duration
	// RetentionInterval paces the declarative retention pass
	// (clickhouse.RetentionRules). <= 0 disables retention entirely.
	RetentionInterval time.Duration
	// RetentionDryRun logs what each rule WOULD delete without deleting.
	RetentionDryRun bool
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
	if cfg.NotificationsInterval <= 0 {
		cfg.NotificationsInterval = time.Minute
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
	safego.Go("rollup.notifications_feed", func() { r.runNotificationsLoop(ctx) })
	safego.Go("rollup.retention", func() { r.runRetentionLoop(ctx) })
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

// runNotificationsLoop maintains the notifications_feed read-model: fast
// incremental ticks in steady state, near-continuous slices while the
// historical catch-up is still behind. Each slice holds one gate slot so the
// catch-up can never crowd out user reads.
func (r *Runner) runNotificationsLoop(ctx context.Context) {
	for ctx.Err() == nil {
		release, err := r.gate.Acquire(ctx)
		if err != nil {
			return
		}
		caughtUp, err := r.store.RecomputeNotificationsFeed(ctx, r.now())
		release()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			r.logger.Error("notifications feed rollup failed", "error", err)
			if sleep(ctx, r.config.NotificationsInterval) != nil {
				return
			}
			continue
		}
		wait := r.config.NotificationsInterval
		if !caughtUp {
			// Historical catch-up: keep chewing with just enough pause to let
			// queued user reads take the gate first.
			wait = 2 * time.Second
		}
		if sleep(ctx, wait) != nil {
			return
		}
	}
}

// retentionInitialDelay keeps the first retention pass off the boot path so a
// deploy settles (migrations, catch-up, warm caches) before any deletes are
// submitted.
const retentionInitialDelay = 10 * time.Minute

// retentionBusyInterval is the re-tick cadence while retention has work in
// flight or queued: after submitting a mutation (there is probably another
// partition waiting) and while a mutation is still executing. The idle cadence
// — everything converged, nothing matched — is Config.RetentionInterval.
const retentionBusyInterval = 5 * time.Minute

// runRetentionLoop applies the declarative retention rules, one gate slot per
// pass so a retention count/delete can never crowd out user reads. Each pass
// submits AT MOST one partition-scoped mutation (see Store.RunRetention) —
// concurrent multi-part mutations starve this instance's thread pool and take
// user reads down with error 439.
func (r *Runner) runRetentionLoop(ctx context.Context) {
	if r.config.RetentionInterval <= 0 {
		return
	}
	if sleep(ctx, retentionInitialDelay) != nil {
		return
	}
	for ctx.Err() == nil {
		release, err := r.gate.Acquire(ctx)
		if err != nil {
			return
		}
		results, err := r.store.RunRetention(ctx, r.config.RetentionDryRun)
		release()

		wait := r.config.RetentionInterval
		switch {
		case errors.Is(err, clickhouse.ErrRetentionBusy):
			r.logger.Info("retention waiting on in-flight mutation")
			wait = retentionBusyInterval
		case errors.Is(err, clickhouse.ErrRetentionNoHeadroom):
			// Background merges free space over time; retry on the busy cadence.
			r.logger.Info("retention waiting for disk headroom", "detail", err.Error())
			wait = retentionBusyInterval
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			r.logger.Error("retention run failed", "error", err, "dry_run", r.config.RetentionDryRun)
			wait = retentionBusyInterval
		}
		for _, result := range results {
			r.logger.Info("retention rule",
				"rule", result.Rule,
				"table", result.Table,
				"matched_rows", result.MatchedRows,
				"deleted", result.Deleted,
				"dry_run", r.config.RetentionDryRun,
			)
			if result.Deleted {
				// A mutation went out; more work likely remains.
				wait = retentionBusyInterval
			}
		}
		if sleep(ctx, wait) != nil {
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
