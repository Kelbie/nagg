package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
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
	"github.com/vertex-lab/nagg/internal/enrich"
	"github.com/vertex-lab/nagg/internal/firehose"
	"github.com/vertex-lab/nagg/internal/graphqlapi"
	"github.com/vertex-lab/nagg/internal/ingest"
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

	store, err := chstore.OpenWithRetry(ctx, cfg.ClickHouse, logger)
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

	// Optionally host the firehose ingester and the enrichment runner in-process
	// so a single `nagg` service does everything (HTTP + Vertex sync + ingest +
	// enrich) against one ClickHouse + Redis. Set NAGG_RUN_INGESTER=false /
	// NAGG_RUN_ENRICHER=false to split them back into cmd/ingester / cmd/enricher.
	// Worker setup failures are logged but NON-fatal: serving the API always
	// takes priority over a background worker that can't start.
	workerSchemaReady := true
	if cfg.RunIngester || cfg.RunEnricher {
		// In-process workers need the schema present. Migrations are idempotent
		// (CREATE ... IF NOT EXISTS), so this is safe alongside the deploy-time
		// migrate step; if it fails the API still serves (reads surface errors).
		if err := store.Migrate(ctx); err != nil {
			slog.Error("in-process worker migration failed; serving continues", "error", err)
			workerSchemaReady = false
		}
	}

	if cfg.RunEnricher && workerSchemaReady {
		processors, err := enrich.NewProcessors(cfg.Enrich.Tasks, enrich.ProcessorConfig{
			ModelVersion: cfg.Enrich.ModelVersion,
		})
		if err != nil {
			slog.Error("in-process enricher disabled: setup failed", "error", err)
		} else {
			runner := enrich.NewRunner(store, processors, enrich.RunnerConfig{
				BatchSize:    cfg.Enrich.BatchSize,
				PollInterval: cfg.Enrich.PollInterval,
			}, logger)
			slog.Info("in-process enricher starting", "tasks", enrich.NormalizeTasks(cfg.Enrich.Tasks))
			go func() {
				if err := runner.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
					slog.Error("enricher stopped with error", "error", err)
				}
			}()
		}
	}

	if cfg.RunIngester && !workerSchemaReady {
		slog.Error("in-process ingester disabled: schema setup failed")
	}
	if cfg.RunEnricher && !workerSchemaReady {
		slog.Error("in-process enricher disabled: schema setup failed")
	}
	if cfg.RunIngester && workerSchemaReady {
		startInProcessIngester(ctx, store, cfg)
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
		slog.Info("response cache enabled", "default_ttl", cfg.Cache.DefaultTTL, "stale_for", cfg.Cache.StaleFor)
	}

	mux := http.NewServeMux()
	gqlHandler := graphqlapi.Handler(
		schema,
		graphqlapi.WithRequestTimeout(cfg.API.GraphQLTimeout),
		graphqlapi.WithRelayHydrationMaxJobs(cfg.OnDemand.GraphQLMaxJobsPerRequest),
	)
	mux.HandleFunc("/graphql", cache.WrapGraphQL(gqlHandler, responseCache, cfg.Cache.DefaultTTL, cfg.Cache.StaleFor))
	mux.HandleFunc("/v1/graphql", cache.WrapGraphQL(gqlHandler, responseCache, cfg.Cache.DefaultTTL, cfg.Cache.StaleFor))
	mux.HandleFunc("/graphiql", graphqlapi.GraphiQLHandler("/graphql"))
	// Reuse the GraphQL schema options so the REST ranked-feed route runs the
	// exact same ranking pipeline (scoring + on-demand hydration) as the
	// GraphQL rankedEvents resolver.
	ranker := graphqlapi.NewRanker(store, schemaOpts...)
	appviewOpts := []appview.Option{
		appview.WithNIP05Validation(cfg.Vertex.ValidateNIP05),
		appview.WithVertexProfileMinFollowers(cfg.Vertex.ProfileMinFollowers),
		appview.WithViewerPubkey(cfg.Viewer.PubKey),
		appview.WithResponseCache(responseCache, cfg.Cache.DefaultTTL, cfg.Cache.StaleFor),
		appview.WithRankedFeed(ranker),
		appview.WithMaxConcurrentRequests(cfg.API.MaxConcurrentRequests),
	}
	if vertexClient != nil {
		appviewOpts = append(appviewOpts, appview.WithVertex(vertexClient))
	}
	if userFeedBackfiller != nil && cfg.OnDemand.UserFeed {
		appviewOpts = append(appviewOpts, appview.WithUserFeedBackfill(userFeedBackfiller))
	}
	appview.New(store, appviewOpts...).Register(mux)
	mux.HandleFunc("/healthz", healthHandler(store, cfg.Firehose.Kinds))

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

func startInProcessIngester(ctx context.Context, store *chstore.Store, cfg config.Config) {
	if result, err := store.PruneRemovedEventKinds(ctx, cfg.Firehose.Kinds); err != nil {
		slog.Error("in-process ingester disabled: event kind retention failed", "error", err)
		return
	} else if result.Skipped {
		slog.Warn("in-process ingester event kind retention skipped: no configured NAGG_KINDS")
	} else if result.RemovedEvents > 0 {
		slog.Info(
			"clickhouse pruned removed event kinds",
			"events", result.RemovedEvents,
			"kinds", result.RemovedCounts,
			"rebuilt_appview", result.RebuiltAppView,
		)
	}

	firehoseClient, err := firehose.New(cfg.Firehose)
	if err != nil {
		slog.Error("in-process ingester disabled: firehose setup failed", "error", err)
		return
	}

	pipeline := ingest.New(store, cfg.Ingest)
	events := make(chan firehose.RelayEvent, cfg.Ingest.QueueSize)
	slog.Info("in-process ingester starting", "relays", len(cfg.Firehose.Relays), "kinds", cfg.Firehose.Kinds)
	go func() {
		firehoseClient.Run(ctx, events)
		close(events)
	}()
	go func() {
		if err := pipeline.Run(ctx, events); err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("ingestion stopped with error", "error", err)
		}
	}()
}

type healthStore interface {
	EventCount(context.Context) (uint64, error)
	EventKindStats(context.Context, []int) (map[int]chstore.EventKindStats, error)
}

type healthResponse struct {
	OK         string               `json:"ok"`
	EventCount uint64               `json:"eventCount,omitempty"`
	EventKinds []eventKindBreakdown `json:"eventKinds,omitempty"`
	Error      string               `json:"error,omitempty"`
}

type eventKindInfo struct {
	Kind        int
	Description string
	Source      string
}

type eventKindBreakdown struct {
	Kind        int     `json:"kind"`
	Description string  `json:"description"`
	Source      string  `json:"source"`
	Count       uint64  `json:"count"`
	StoredBytes uint64  `json:"storedBytes"`
	StoredGB    float64 `json:"storedGB"`
}

var healthEventKinds = []eventKindInfo{
	{Kind: 0, Description: "User Metadata", Source: "NIP-01"},
	{Kind: 1, Description: "Short Text Note", Source: "NIP-10"},
	{Kind: 3, Description: "Follows", Source: "NIP-02"},
	{Kind: 4, Description: "Encrypted Direct Messages", Source: "NIP-04"},
	{Kind: 6, Description: "Repost", Source: "NIP-18"},
	{Kind: 7, Description: "Reaction", Source: "NIP-25"},
	{Kind: 16, Description: "Generic Repost", Source: "NIP-18"},
	{Kind: 443, Description: "KeyPackage", Source: "Marmot"},
	{Kind: 444, Description: "Welcome Message", Source: "Marmot"},
	{Kind: 445, Description: "Group Event", Source: "Marmot"},
	{Kind: 1059, Description: "Gift Wrap", Source: "NIP-59"},
	{Kind: 1063, Description: "File Metadata", Source: "NIP-94"},
	{Kind: 9735, Description: "Zap", Source: "NIP-57"},
	{Kind: 10051, Description: "KeyPackage Relays List", Source: "Marmot"},
	{Kind: 30078, Description: "Application-specific Data", Source: "NIP-78"},
	{Kind: 38000, Description: "Ecash Mint Recommendation", Source: "NIP-87"},
}

func healthEventKindNumbers(configuredKinds []int) []int {
	kinds := make([]int, 0, len(configuredKinds))
	seen := make(map[int]struct{}, len(configuredKinds))
	for _, kind := range configuredKinds {
		if kind < 0 {
			continue
		}
		if _, ok := seen[kind]; ok {
			continue
		}
		seen[kind] = struct{}{}
		kinds = append(kinds, kind)
	}
	return kinds
}

func healthEventKindInfo(kind int) eventKindInfo {
	for _, info := range healthEventKinds {
		if info.Kind == kind {
			return info
		}
	}
	return eventKindInfo{
		Kind:        kind,
		Description: "Unknown Nostr event kind",
		Source:      "",
	}
}

func healthEventKindBreakdown(kinds []int, stats map[int]chstore.EventKindStats) []eventKindBreakdown {
	breakdown := make([]eventKindBreakdown, 0, len(kinds))
	for _, kind := range kinds {
		info := healthEventKindInfo(kind)
		stat := stats[info.Kind]
		breakdown = append(breakdown, eventKindBreakdown{
			Kind:        info.Kind,
			Description: info.Description,
			Source:      info.Source,
			Count:       stat.Count,
			StoredBytes: stat.StoredBytesRaw,
			StoredGB:    bytesToDecimalGB(stat.StoredBytesRaw),
		})
	}
	return breakdown
}

func bytesToDecimalGB(bytes uint64) float64 {
	return math.Round(float64(bytes)/1_000_000_000*1_000_000) / 1_000_000
}

func healthHandler(store healthStore, configuredKinds []int) http.HandlerFunc {
	kindNumbers := healthEventKindNumbers(configuredKinds)
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		eventCount, err := store.EventCount(ctx)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(healthResponse{OK: "false", Error: "clickhouse event count failed"})
			return
		}

		eventKindStats, err := store.EventKindStats(ctx, kindNumbers)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(healthResponse{OK: "false", Error: "clickhouse event kind stats failed"})
			return
		}

		_ = json.NewEncoder(w).Encode(healthResponse{
			OK:         "true",
			EventCount: eventCount,
			EventKinds: healthEventKindBreakdown(kindNumbers, eventKindStats),
		})
	}
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
