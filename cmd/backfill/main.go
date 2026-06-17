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
	if err := store.Backfill(ctx); err != nil {
		slog.Error("clickhouse appview backfill failed", "error", err)
		os.Exit(1)
	}
	slog.Info("clickhouse appview backfill complete")
}
