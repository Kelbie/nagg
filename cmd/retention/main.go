package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/config"
)

func main() {
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

	result, err := store.PruneRemovedEventKinds(ctx, cfg.Firehose.Kinds)
	if err != nil {
		slog.Error("clickhouse event kind retention failed", "error", err)
		os.Exit(1)
	}
	if result.Skipped {
		level := slog.LevelWarn
		message := "clickhouse event kind retention skipped"
		if result.SkipReason == "active_mutations" || result.SkipReason == "low_disk_headroom" {
			level = slog.LevelError
			message = "clickhouse event kind retention blocked"
		}
		slog.Log(
			ctx,
			level,
			message,
			"reason", result.SkipReason,
			"active_mutations", result.ActiveMutations,
			"disk_free_ratio", result.DiskFreeRatio,
			"min_disk_free_ratio", result.MinDiskFreeRatio,
			"configured_kinds", result.ConfiguredKinds,
		)
		if result.SkipReason == "active_mutations" || result.SkipReason == "low_disk_headroom" {
			os.Exit(1)
		}
		return
	}
	slog.Info(
		"clickhouse event kind retention complete",
		"events", result.RemovedEvents,
		"kinds", result.RemovedCounts,
		"rebuilt_appview", result.RebuiltAppView,
		"configured_kinds", result.ConfiguredKinds,
	)
}
