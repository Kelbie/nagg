package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/vertex-lab/nagg/internal/appview"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/config"
	"github.com/vertex-lab/nagg/internal/graphqlapi"
	"github.com/vertex-lab/nagg/internal/vertex"
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

	var vertexClient *vertex.Client
	if cfg.Vertex.PrivateKey != "" {
		vertexClient, err = vertex.New(vertex.Config{
			PrivateKey: cfg.Vertex.PrivateKey,
			Relay:      cfg.Vertex.Relay,
		})
		if err != nil {
			slog.Error("vertex client failed", "error", err)
			os.Exit(1)
		}
	}

	var userFeedBackfiller *appview.RelayUserFeedBackfiller
	if cfg.OnDemand.UserFeed {
		userFeedBackfiller = appview.NewRelayUserFeedBackfiller(store, appview.UserFeedBackfillConfig{
			Relays:          cfg.Firehose.Relays,
			ReadLimit:       cfg.Firehose.ReadLimit,
			Cooldown:        cfg.OnDemand.Cooldown,
			Timeout:         cfg.OnDemand.Timeout,
			Wait:            cfg.OnDemand.Wait,
			AuthorLimit:     cfg.OnDemand.AuthorLimit,
			EngagementLimit: cfg.OnDemand.EngagementLimit,
			ThreadLimit:     cfg.OnDemand.ThreadLimit,
			FollowLimit:     cfg.OnDemand.FollowLimit,
		})
	}

	schemaOpts := []graphqlapi.Option{}
	if userFeedBackfiller != nil {
		schemaOpts = append(schemaOpts, graphqlapi.WithUserFeedBackfill(userFeedBackfiller))
	}
	schema, err := graphqlapi.NewSchema(store, schemaOpts...)
	if err != nil {
		slog.Error("graphql schema failed", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", graphqlapi.Handler(schema))
	appviewOpts := []appview.Option{
		appview.WithNIP05Validation(cfg.Vertex.ValidateNIP05),
		appview.WithVertexProfileMinFollowers(cfg.Vertex.ProfileMinFollowers),
	}
	if vertexClient != nil {
		appviewOpts = append(appviewOpts, appview.WithVertex(vertexClient))
	}
	if userFeedBackfiller != nil {
		appviewOpts = append(appviewOpts, appview.WithUserFeedBackfill(userFeedBackfiller))
	}
	appview.New(store, appviewOpts...).Register(mux)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
	})

	addr := listenAddr(os.Getenv)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("graphql api listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("graphql api failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

func listenAddr(getenv func(string) string) string {
	if addr := strings.TrimSpace(getenv("NAGG_API_ADDR")); addr != "" {
		return addr
	}
	port := strings.TrimSpace(getenv("PORT"))
	if port == "" {
		return ":8080"
	}
	if strings.HasPrefix(port, ":") || strings.Contains(port, ":") {
		return port
	}
	return ":" + port
}
