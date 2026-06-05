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
	"github.com/vertex-lab/nagg/internal/cache"
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
		vertexSyncer := vertex.NewSyncer(store, vertexClient, vertex.SyncConfig{
			MinFollowers: uint64(cfg.Vertex.RankMinFollowers),
			BatchSize:    cfg.Vertex.SyncBatch,
		}, logger)
		go vertexSyncer.Run(ctx)
	}

	var userFeedBackfiller *appview.RelayUserFeedBackfiller
	if cfg.OnDemand.UserFeed || cfg.OnDemand.GraphQLHydration {
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
			DMLimit:         cfg.OnDemand.DMLimit,
			DMBackfillPages: cfg.OnDemand.DMBackfillPages,
			GraphQLLimit:    cfg.OnDemand.GraphQLLimit,
		})
	}

	schemaOpts := []graphqlapi.Option{
		graphqlapi.WithPubkeyScoreMinFollowers(cfg.Vertex.RankMinFollowers),
		graphqlapi.WithProfileSearch(vertex.NewSearchProvider(store, vertexClient, vertex.SearchProviderConfig{
			MaxAge: 7 * 24 * time.Hour,
		}, logger)),
	}
	if userFeedBackfiller != nil {
		schemaOpts = append(schemaOpts, graphqlapi.WithUserFeedBackfill(userFeedBackfiller))
	}
	schema, err := graphqlapi.NewSchema(store, schemaOpts...)
	if err != nil {
		slog.Error("graphql schema failed", "error", err)
		os.Exit(1)
	}

	responseCache := cache.New(cfg.Cache.URL, logger)
	if responseCache.Enabled() {
		slog.Info("response cache enabled", "default_ttl", cfg.Cache.DefaultTTL)
	}

	mux := http.NewServeMux()
	gqlHandler := graphqlapi.Handler(
		schema,
		graphqlapi.WithRequestTimeout(cfg.API.GraphQLTimeout),
		graphqlapi.WithRelayHydrationMaxJobs(cfg.OnDemand.GraphQLMaxJobsPerRequest),
	)
	mux.HandleFunc("/graphql", cache.WrapGraphQL(gqlHandler, responseCache, cfg.Cache.DefaultTTL))
	mux.HandleFunc("/v1/graphql", cache.WrapGraphQL(gqlHandler, responseCache, cfg.Cache.DefaultTTL))
	mux.HandleFunc("/graphiql", graphqlapi.GraphiQLHandler("/graphql"))
	appviewOpts := []appview.Option{
		appview.WithNIP05Validation(cfg.Vertex.ValidateNIP05),
		appview.WithVertexProfileMinFollowers(cfg.Vertex.ProfileMinFollowers),
		appview.WithViewerPubkey(cfg.Viewer.PubKey),
		appview.WithResponseCache(responseCache, cfg.Cache.DefaultTTL),
	}
	if vertexClient != nil {
		appviewOpts = append(appviewOpts, appview.WithVertex(vertexClient))
	}
	if userFeedBackfiller != nil && cfg.OnDemand.UserFeed {
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
