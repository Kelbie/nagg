package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/config"
	"github.com/vertex-lab/nagg/internal/firehose"
	"github.com/vertex-lab/nagg/internal/ingest"
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

	events := make(chan firehose.RelayEvent, cfg.Ingest.QueueSize)
	client, err := firehose.New(cfg.Firehose)
	if err != nil {
		slog.Error("firehose setup failed", "error", err)
		os.Exit(1)
	}

	pipeline := ingest.New(store, cfg.Ingest)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		client.Run(ctx, events)
		close(events)
	}()
	go func() {
		defer wg.Done()
		if err := pipeline.Run(ctx, events); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("ingestion stopped with error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	wg.Wait()
}
