package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/config"
	"github.com/vertex-lab/nagg/internal/firehose"
	"github.com/vertex-lab/nagg/internal/ingest"
	"github.com/vertex-lab/nagg/internal/relevance"
	"github.com/vertex-lab/nagg/internal/runtimelimits"
	"github.com/vertex-lab/nagg/internal/safego"
)

func main() {
	runtimelimits.Apply()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := chstore.OpenWithRetry(ctx, cfg.ClickHouse, logger)
	if err != nil {
		slog.Error("clickhouse connection failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		slog.Error("clickhouse migration failed", "error", err)
		os.Exit(1)
	}
	if result, err := store.PruneRemovedEventKinds(ctx, cfg.StoredKinds); err != nil {
		slog.Warn("clickhouse event kind retention failed; continuing", "error", err)
	} else if result.Skipped {
		slog.Warn("clickhouse event kind retention skipped: no configured NAGG_KINDS")
	} else if result.RemovedEvents > 0 {
		slog.Info(
			"clickhouse pruned removed event kinds",
			"events", result.RemovedEvents,
			"kinds", result.RemovedCounts,
			"rebuilt_appview", result.RebuiltAppView,
		)
	}

	events := make(chan firehose.RelayEvent, cfg.Ingest.QueueSize)
	client, err := firehose.New(cfg.Firehose)
	if err != nil {
		slog.Error("firehose setup failed", "error", err)
		os.Exit(1)
	}

	// The exemption set for the post cap is read from ClickHouse (known_viewers
	// is written by the API process's viewer-touch seam), so the standalone
	// ingester only needs the refresher, not the touch side.
	relevanceTracker := relevance.NewTracker(store, logger)
	safego.Go("ingester.relevance", func() { relevanceTracker.Run(ctx) })

	pipeline := ingest.New(store, cfg.Ingest, ingest.WithExemption(relevanceTracker.Exempt))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer safego.Recover("ingester.worker")
		defer wg.Done()
		client.Run(ctx, events)
		close(events)
	}()
	go func() {
		defer safego.Recover("ingester.worker")
		defer wg.Done()
		// Restart consumption in-process instead of exiting on the first
		// insert-failure burst: exiting hands control to Railway's restart
		// policy, which re-pays boot (connect + migrate check) and — on a
		// still-degraded ClickHouse — becomes a restart loop. A nil return
		// means the firehose closed the channel.
		backoff := time.Second
		for {
			err := pipeline.Run(ctx, events)
			if err == nil || ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			slog.Error("ingestion stopped with error; restarting", "error", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}()

	<-ctx.Done()
	wg.Wait()
}
