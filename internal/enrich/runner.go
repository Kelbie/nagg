package enrich

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
)

type RunnerConfig struct {
	BatchSize    int
	PollInterval time.Duration
}

type Runner struct {
	store      Store
	processors []Processor
	config     RunnerConfig
	logger     *slog.Logger
	now        func() time.Time
}

func NewRunner(store Store, processors []Processor, cfg RunnerConfig, logger *slog.Logger) *Runner {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{
		store:      store,
		processors: processors,
		config:     cfg,
		logger:     logger,
		now:        func() time.Time { return time.Now().UTC() },
	}
}

func (r *Runner) Run(ctx context.Context) error {
	if len(r.processors) == 0 {
		r.logger.Info("enricher has no enabled tasks")
		<-ctx.Done()
		return nil
	}

	group, ctx := errgroup.WithContext(ctx)
	for _, processor := range r.processors {
		processor := processor
		group.Go(func() error {
			return r.runTask(ctx, processor)
		})
	}
	return group.Wait()
}

func (r *Runner) runTask(ctx context.Context, processor Processor) error {
	for {
		processed, err := r.RunOnce(ctx, processor)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			r.logger.Error("enrichment task failed", "task", processor.Task(), "error", err)
		}
		if processed == 0 || err != nil {
			if sleepErr := sleepContext(ctx, r.config.PollInterval); sleepErr != nil {
				return nil
			}
		}
	}
}

func (r *Runner) RunOnce(ctx context.Context, processor Processor) (int, error) {
	state, ok, err := r.store.LoadEnrichmentState(ctx, processor.Task())
	if err != nil {
		return 0, fmt.Errorf("load enrichment state: %w", err)
	}
	if !ok {
		state = State{Task: processor.Task()}
	}

	events, err := r.store.FetchEnrichmentEvents(ctx, state.Cursor, r.config.BatchSize)
	if err != nil {
		return 0, fmt.Errorf("fetch enrichment events: %w", err)
	}
	if len(events) == 0 {
		return 0, nil
	}

	results, err := processor.ProcessBatch(ctx, events)
	if err != nil {
		return 0, fmt.Errorf("process %s batch: %w", processor.Task(), err)
	}
	if len(results) != len(events) {
		return 0, fmt.Errorf("process %s batch returned %d results for %d events", processor.Task(), len(results), len(events))
	}

	computedAt := r.now()
	annotations := make([]Annotation, 0, len(events))
	var processed uint64
	var failed uint64
	var skipped uint64
	for i, event := range events {
		state.Cursor = Cursor{CreatedAt: event.CreatedAt, EventID: event.ID}
		result := results[i]
		if result.Err != nil {
			failed++
			r.logger.Warn("enrichment event skipped", "task", processor.Task(), "event_id", event.ID, "error", result.Err)
			continue
		}

		annotation := result.Annotation
		annotation.Event = event
		if annotation.ComputedAt.IsZero() {
			annotation.ComputedAt = computedAt
		}
		if !annotation.Empty() {
			annotations = append(annotations, annotation)
		} else {
			skipped++
		}
		processed++
	}

	if len(annotations) > 0 {
		if err := r.store.WriteEnrichmentAnnotations(ctx, annotations); err != nil {
			return 0, fmt.Errorf("write enrichment annotations: %w", err)
		}
	}

	state.Processed += processed
	state.Failed += failed
	state.UpdatedAt = computedAt
	if err := r.store.SaveEnrichmentState(ctx, state); err != nil {
		return 0, fmt.Errorf("save enrichment state: %w", err)
	}

	r.logger.Info("enrichment batch processed",
		"task", processor.Task(),
		"events", len(events),
		"processed", processed,
		"failed", failed,
		"skipped", skipped,
		"annotations", len(annotations),
		"watermark_lag_seconds", watermarkLagSeconds(computedAt, state.Cursor.CreatedAt),
		"cursor_created_at", state.Cursor.CreatedAt,
		"cursor_event_id", state.Cursor.EventID,
	)
	return len(events), nil
}

func watermarkLagSeconds(now time.Time, cursorCreatedAt time.Time) int64 {
	if cursorCreatedAt.IsZero() || now.Before(cursorCreatedAt) {
		return 0
	}
	return int64(now.Sub(cursorCreatedAt).Seconds())
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
