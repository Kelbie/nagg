package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/vertex-lab/nagg/internal/appview"
	"github.com/vertex-lab/nagg/internal/auditor"
	"github.com/vertex-lab/nagg/internal/cache"
	"github.com/vertex-lab/nagg/internal/chgate"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/config"
	"github.com/vertex-lab/nagg/internal/enrich"
	"github.com/vertex-lab/nagg/internal/firehose"
	"github.com/vertex-lab/nagg/internal/graphqlapi"
	"github.com/vertex-lab/nagg/internal/ingest"
	"github.com/vertex-lab/nagg/internal/rollup"
	"github.com/vertex-lab/nagg/internal/runtimelimits"
	"github.com/vertex-lab/nagg/internal/safego"
	"github.com/vertex-lab/nagg/internal/vertex"
)

const apiInitializationRetryDelay = 10 * time.Second

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

	runtime := &apiRuntime{}
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", liveHandler)
	mux.Handle("/", runtime)

	addr := listenAddr(os.Getenv)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		defer safego.Recover("api.worker")
		slog.Info("graphql api listening", "addr", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("graphql api failed", "error", err)
			stop()
		}
	}()

	go initializeAPI(ctx, cfg, logger, runtime, stop)

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	runtime.Close()
}

func initializeAPI(ctx context.Context, cfg config.Config, logger *slog.Logger, runtime *apiRuntime, stop context.CancelFunc) {
	for {
		store, err := chstore.OpenWithRetry(ctx, cfg.ClickHouse, logger)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("clickhouse connection failed; retrying api initialization", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(apiInitializationRetryDelay):
				continue
			}
		}

		handler, err := buildReadyAPI(ctx, store, cfg, logger)
		if err != nil {
			store.Close()
			slog.Error("api initialization failed", "error", err)
			stop()
			return
		}

		runtime.SetReady(store, handler)
		slog.Info("api ready")
		return
	}
}

func buildReadyAPI(ctx context.Context, store *chstore.Store, cfg config.Config, logger *slog.Logger) (http.Handler, error) {
	// One process-wide gate for every heavy ClickHouse path. Previously only
	// heavy REST routes were semaphored — GraphQL (its own mux) and background
	// workers (rollup) piled onto CH past the measured concurrency ceiling,
	// producing the shed/5xx/reset behavior the semaphore existed to prevent.
	gate := chgate.New(cfg.API.MaxConcurrentRequests)

	var vertexClient *vertex.Client
	if cfg.Vertex.PrivateKey != "" {
		client, err := vertex.New(vertex.Config{
			PrivateKey: cfg.Vertex.PrivateKey,
			Relay:      cfg.Vertex.Relay,
		})
		if err != nil {
			return nil, fmt.Errorf("vertex client failed: %w", err)
		}
		vertexClient = client
		vertexSyncer := vertex.NewSyncer(store, vertexClient, vertex.SyncConfig{
			MinFollowers: uint64(cfg.Vertex.RankMinFollowers),
			BatchSize:    cfg.Vertex.SyncBatch,
			Throttle:     cfg.Vertex.SyncThrottle,
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
	if cfg.RunIngester || cfg.RunEnricher || cfg.RunRollup {
		// In-process workers need the schema present. Migrations are idempotent
		// (CREATE ... IF NOT EXISTS), so this is safe alongside the deploy-time
		// migrate step; if it fails the API still serves (reads surface errors).
		if err := store.Migrate(ctx); err != nil {
			slog.Error("in-process worker migration failed; serving continues", "error", err)
			workerSchemaReady = false
		}
	}

	// Database-first aggregation: the rollup job maintains the direct-reply edges,
	// vertex-real engagement counts, per-user stats, and the per-event rank-feature
	// table the For-You / trending hot path reads. Started AFTER the in-process
	// migrate (and gated on it) so its first tick can't race table creation on a
	// fresh database. Set NAGG_RUN_ROLLUP=false to split it out.
	if cfg.RunRollup && workerSchemaReady {
		rollupRunner := rollup.NewRunner(store, rollup.Config{
			Interval:     cfg.Rollup.Interval,
			RecentWindow: cfg.Rollup.RecentWindow,
			MaxTargets:   cfg.Rollup.MaxTargets,
			Thresholds: chstore.Thresholds{
				MinActorScore: cfg.Rollup.MinActorScore,
				Version:       cfg.Rollup.Version,
			},
		}, logger).WithGate(gate)
		go rollupRunner.Run(ctx)
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
				defer safego.Recover("api.worker")
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
			UserFeedWait:    cfg.OnDemand.UserFeedWait,
			AuthorLimit:     cfg.OnDemand.AuthorLimit,
			EngagementLimit: cfg.OnDemand.EngagementLimit,
			ThreadLimit:     cfg.OnDemand.ThreadLimit,
			FollowLimit:     cfg.OnDemand.FollowLimit,
			DMLimit:         cfg.OnDemand.DMLimit,
			DMBackfillPages: cfg.OnDemand.DMBackfillPages,
			GraphQLLimit:    cfg.OnDemand.GraphQLLimit,
		})
	}

	// One cache-backed profile-search provider, shared by the GraphQL resolver and
	// the REST /nostr/search handler so both serve identical Vertex-pagerank
	// results from the same ClickHouse cache (and dedup live refreshes via the
	// provider's singleflight). vertexClient is a *vertex.Client, which is a typed
	// nil when no Vertex key is configured; assign through the interface so the
	// provider sees a true nil and returns ErrUnavailable (callers fall back to the
	// local index) instead of panicking on a nil-pointer method call.
	var searchRefresh vertex.SearchRefreshClient
	if vertexClient != nil {
		searchRefresh = vertexClient
	}
	searchProvider := vertex.NewSearchProvider(store, searchRefresh, vertex.SearchProviderConfig{
		MaxAge: 7 * 24 * time.Hour,
	}, logger)
	schemaOpts := []graphqlapi.Option{
		graphqlapi.WithPubkeyScoreMinFollowers(cfg.Vertex.RankMinFollowers),
		graphqlapi.WithProfileSearch(searchProvider),
	}
	// Attach on-demand relay hydration to the GraphQL schema (and, via schemaOpts
	// reuse below, the REST ranked-feed) only when explicitly enabled. This is
	// decoupled from the user-feed/profile backfill (WithUserFeedBackfill on the
	// appview, below): GraphQL hydration fans relay+ClickHouse work out across
	// every GraphQL query app-wide, which can exhaust the ClickHouse connection
	// pool, so it stays opt-in independent of the profile feed default.
	if userFeedBackfiller != nil && cfg.OnDemand.GraphQLHydration {
		schemaOpts = append(schemaOpts, graphqlapi.WithUserFeedBackfill(userFeedBackfiller))
	}
	schema, err := graphqlapi.NewSchema(store, schemaOpts...)
	if err != nil {
		return nil, fmt.Errorf("graphql schema failed: %w", err)
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
	// Gate INSIDE the response cache so cache hits never queue.
	gatedGQL := gate.Middleware(gqlHandler)
	mux.HandleFunc("/graphql", cache.WrapGraphQL(gatedGQL, responseCache, cfg.Cache.DefaultTTL, cfg.Cache.StaleFor))
	mux.HandleFunc("/v1/graphql", cache.WrapGraphQL(gatedGQL, responseCache, cfg.Cache.DefaultTTL, cfg.Cache.StaleFor))
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
		appview.WithConcurrencyGate(gate),
	}
	// Route REST profile search through the same cache-backed provider as GraphQL.
	// Injected unconditionally: with no Vertex key the provider returns
	// ErrUnavailable on a cache miss and the search handler falls back to the local
	// index, so it is safe even when vertexClient is nil.
	appviewOpts = append(appviewOpts, appview.WithProfileSearch(searchProvider))
	if vertexClient != nil {
		appviewOpts = append(appviewOpts, appview.WithVertex(vertexClient))
	}
	if cfg.Auditor.Enabled && cfg.Auditor.URL != "" {
		auditorClient := auditor.NewHTTPClient(cfg.Auditor.URL, auditor.WithLimit(cfg.Auditor.Limit))
		appviewOpts = append(appviewOpts, appview.WithAuditor(auditorClient))
		slog.Info("mint auditor enabled", "url", cfg.Auditor.URL, "limit", cfg.Auditor.Limit)
		// Warm the auditor cache in the background so the first /nostr/mint/discover
		// request doesn't pay the cold upstream fetch.
		go func() {
			defer safego.Recover("api.worker")
			warmCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if _, err := auditorClient.Mints(warmCtx); err != nil {
				slog.Warn("mint auditor warm-up failed", "error", err)
			}
		}()
	}
	appviewOpts = append(appviewOpts, appview.WithAppVersion(cfg.AppVersion.LatestVersion, cfg.AppVersion.UpdateMessage))
	if userFeedBackfiller != nil && cfg.OnDemand.UserFeed {
		appviewOpts = append(appviewOpts, appview.WithUserFeedBackfill(userFeedBackfiller))
	}
	appview.New(store, appviewOpts...).Register(mux)
	healthStorageStats := newHealthStorageStatsCache(store, cfg.Firehose.Kinds, logger)
	go healthStorageStats.Run(ctx)
	mux.HandleFunc("/healthz", healthHandler(store, cfg.Firehose.Kinds, healthStorageStats.Snapshot))

	return mux, nil
}

type apiRuntime struct {
	mu      sync.RWMutex
	handler http.Handler
	store   *chstore.Store
}

func (r *apiRuntime) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	handler := r.handler
	r.mu.RUnlock()
	if handler == nil {
		apiUnavailable(w)
		return
	}
	handler.ServeHTTP(w, req)
}

func (r *apiRuntime) SetReady(store *chstore.Store, handler http.Handler) {
	r.mu.Lock()
	r.store = store
	r.handler = handler
	r.mu.Unlock()
}

func (r *apiRuntime) Close() {
	r.mu.Lock()
	store := r.store
	r.store = nil
	r.handler = nil
	r.mu.Unlock()
	if store != nil {
		store.Close()
	}
}

func liveHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
}

func apiUnavailable(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", "5")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"ok":    "false",
		"error": "api initializing",
	})
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
		defer safego.Recover("api.worker")
		firehoseClient.Run(ctx, events)
		close(events)
	}()
	go func() {
		defer safego.Recover("api.ingest_pipeline")
		// The pipeline must outlive failure bursts: a returned error (e.g. CH
		// inserts failing past their retries while the database restarts)
		// previously ended this goroutine SILENTLY — ingestion stopped while
		// the API kept serving, and the firehose eventually blocked on the
		// full channel (observed as a lone "ingestion stopped with error" at
		// the end of a session's logs). Resume consuming with backoff until
		// shutdown; a nil return means the firehose closed the channel.
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
}

type healthStore interface {
	EventCount(context.Context) (uint64, error)
	EventKindCounts(context.Context, []int) (map[int]uint64, error)
}

type healthResponse struct {
	OK                string               `json:"ok"`
	EventCount        uint64               `json:"eventCount,omitempty"`
	StorageStatsReady bool                 `json:"storageStatsReady"`
	EventKinds        []eventKindBreakdown `json:"eventKinds,omitempty"`
	Error             string               `json:"error,omitempty"`
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
	{Kind: 10050, Description: "Relay list to receive DMs", Source: "NIP-51/NIP-17"},
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

func healthEventKindBreakdown(kinds []int, counts map[int]uint64, storage healthStorageSnapshot) []eventKindBreakdown {
	breakdown := make([]eventKindBreakdown, 0, len(kinds))
	for _, kind := range kinds {
		info := healthEventKindInfo(kind)
		storedBytes := storage.StoredBytes[info.Kind]
		breakdown = append(breakdown, eventKindBreakdown{
			Kind:        info.Kind,
			Description: info.Description,
			Source:      info.Source,
			Count:       counts[info.Kind],
			StoredBytes: storedBytes,
			StoredGB:    bytesToDecimalGB(storedBytes),
		})
	}
	return breakdown
}

func bytesToDecimalGB(bytes uint64) float64 {
	return math.Round(float64(bytes)/1_000_000_000*1_000_000) / 1_000_000
}

type healthStorageSnapshot struct {
	Ready       bool
	StoredBytes map[int]uint64
}

type healthStorageStatsCache struct {
	store  *chstore.Store
	kinds  []int
	logger *slog.Logger

	mu       sync.RWMutex
	snapshot healthStorageSnapshot
}

const (
	healthStorageStatsInitialDelay    = 30 * time.Second
	healthStorageStatsRefreshInterval = 10 * time.Minute
	healthStorageStatsRefreshTimeout  = 45 * time.Second
)

func newHealthStorageStatsCache(store *chstore.Store, configuredKinds []int, logger *slog.Logger) *healthStorageStatsCache {
	return &healthStorageStatsCache{
		store:  store,
		kinds:  healthEventKindNumbers(configuredKinds),
		logger: logger,
	}
}

func (c *healthStorageStatsCache) Run(ctx context.Context) {
	timer := time.NewTimer(healthStorageStatsInitialDelay)
	select {
	case <-ctx.Done():
		timer.Stop()
		return
	case <-timer.C:
	}

	c.refresh(ctx)
	ticker := time.NewTicker(healthStorageStatsRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.refresh(ctx)
		}
	}
}

func (c *healthStorageStatsCache) refresh(ctx context.Context) {
	refreshCtx, cancel := context.WithTimeout(ctx, healthStorageStatsRefreshTimeout)
	defer cancel()

	stats, err := c.store.EventKindStats(refreshCtx, c.kinds)
	if err != nil {
		if c.logger != nil {
			c.logger.Warn("health storage stats refresh failed", "error", err)
		}
		return
	}

	storedBytes := make(map[int]uint64, len(stats))
	for kind, stat := range stats {
		storedBytes[kind] = stat.StoredBytesEstimated
	}

	c.mu.Lock()
	c.snapshot = healthStorageSnapshot{
		Ready:       true,
		StoredBytes: storedBytes,
	}
	c.mu.Unlock()
}

func (c *healthStorageStatsCache) Snapshot() healthStorageSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()

	storedBytes := make(map[int]uint64, len(c.snapshot.StoredBytes))
	for kind, bytes := range c.snapshot.StoredBytes {
		storedBytes[kind] = bytes
	}
	return healthStorageSnapshot{
		Ready:       c.snapshot.Ready,
		StoredBytes: storedBytes,
	}
}

func healthHandler(store healthStore, configuredKinds []int, storageSnapshot func() healthStorageSnapshot) http.HandlerFunc {
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

		eventKindCounts, err := store.EventKindCounts(ctx, kindNumbers)
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(healthResponse{OK: "false", Error: "clickhouse event kind count failed"})
			return
		}
		storage := healthStorageSnapshot{}
		if storageSnapshot != nil {
			storage = storageSnapshot()
		}

		_ = json.NewEncoder(w).Encode(healthResponse{
			OK:                "true",
			EventCount:        eventCount,
			StorageStatsReady: storage.Ready,
			EventKinds:        healthEventKindBreakdown(kindNumbers, eventKindCounts, storage),
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
