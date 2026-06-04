package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/config"
	"github.com/vertex-lab/nagg/internal/enrich"
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
	healthServer := startHealthServer(logger)
	if healthServer != nil {
		defer shutdownHealthServer(healthServer)
	}

	processors, err := enrich.NewProcessors(cfg.Enrich.Tasks, enrich.ProcessorConfig{
		ModelDir:                 cfg.Enrich.ModelDir,
		ModelVersion:             cfg.Enrich.ModelVersion,
		ModelBackend:             cfg.Enrich.ModelBackend,
		OnnxLibraryPath:          cfg.Enrich.OnnxLibraryPath,
		TrendingDedupeSimilarity: cfg.Enrich.TrendingDedupeSimilarity,
	})
	if err != nil {
		slog.Error("enricher setup failed", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := enrich.CloseProcessors(processors); err != nil {
			slog.Warn("enricher model cleanup failed", "error", err)
		}
	}()

	slog.Info("enricher starting",
		"tasks", enrich.NormalizeTasks(cfg.Enrich.Tasks),
		"batch_size", cfg.Enrich.BatchSize,
		"poll_interval", cfg.Enrich.PollInterval,
		"model_dir", cfg.Enrich.ModelDir,
		"model_version", cfg.Enrich.ModelVersion,
		"model_backend", cfg.Enrich.ModelBackend,
		"trending_dedupe_similarity", cfg.Enrich.TrendingDedupeSimilarity,
	)
	runner := enrich.NewRunner(store, processors, enrich.RunnerConfig{
		BatchSize:    cfg.Enrich.BatchSize,
		PollInterval: cfg.Enrich.PollInterval,
	}, logger)
	if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("enricher stopped with error", "error", err)
		os.Exit(1)
	}
}

func startHealthServer(logger *slog.Logger) *http.Server {
	addr := strings.TrimSpace(os.Getenv("NAGG_ENRICH_HEALTH_ADDR"))
	if addr == "" {
		port := strings.TrimSpace(os.Getenv("PORT"))
		if port == "" {
			return nil
		}
		addr = ":" + port
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		logger.Info("enricher health server listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("enricher health server stopped with error", "error", err)
		}
	}()
	return server
}

func shutdownHealthServer(server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
