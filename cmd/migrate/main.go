package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/config"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// Hard safety guard for read-only deploys (e.g. the devnagg staging service)
	// that share a production ClickHouse: skip migrate entirely so the pre-deploy
	// command can never run CREATE/reconcile against prod data. Belt-and-suspenders
	// with NAGG_SCHEMA_RECONCILE=off.
	if skipMigrate() {
		slog.Info("clickhouse migration skipped (NAGG_SKIP_MIGRATE)")
		return
	}

	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration failed", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := chstore.Open(ctx, cfg.ClickHouse)
	if err != nil {
		slog.Error("clickhouse connection failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		slog.Error("clickhouse migration failed", "error", err)
		os.Exit(1)
	}
	slog.Info("clickhouse migration complete")
}

func skipMigrate() bool {
	v, err := strconv.ParseBool(strings.TrimSpace(os.Getenv("NAGG_SKIP_MIGRATE")))
	return err == nil && v
}
