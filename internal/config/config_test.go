package config

import (
	"strings"
	"testing"
)

func TestLoadDefaultRelaysExcludeExternalCacheHost(t *testing.T) {
	t.Setenv("NAGG_RELAYS", "")
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Firehose.Relays) == 0 {
		t.Fatal("expected default relays")
	}
	if cfg.OnDemand.UserFeed {
		t.Fatal("on-demand user feed backfill should be opt-in by default")
	}
	if cfg.OnDemand.GraphQLHydration {
		t.Fatal("graphql relay hydration should follow the disabled user-feed default")
	}
	if cfg.OnDemand.Wait != 0 {
		t.Fatalf("on-demand wait = %s, want instant default", cfg.OnDemand.Wait)
	}
	if cfg.ClickHouse.MaxOpenConns != 30 || cfg.ClickHouse.MaxIdleConns != 10 {
		t.Fatalf("clickhouse pool = open %d idle %d", cfg.ClickHouse.MaxOpenConns, cfg.ClickHouse.MaxIdleConns)
	}
	if !containsKind(cfg.Firehose.Kinds, 38000) {
		t.Fatalf("default kinds = %v, want mint review kind 38000", cfg.Firehose.Kinds)
	}
	if !containsKind(cfg.Firehose.Kinds, 10050) {
		t.Fatalf("default kinds = %v, want NIP-17 DM inbox relay kind 10050", cfg.Firehose.Kinds)
	}
	if cfg.OnDemand.DMLimit != 200 {
		t.Fatalf("on-demand DM limit = %d, want 200", cfg.OnDemand.DMLimit)
	}
	if cfg.OnDemand.DMBackfillPages != 2 {
		t.Fatalf("on-demand DM backfill pages = %d, want 2", cfg.OnDemand.DMBackfillPages)
	}
	if cfg.OnDemand.GraphQLLimit != 100 {
		t.Fatalf("graphql hydration limit = %d, want 100", cfg.OnDemand.GraphQLLimit)
	}
	if cfg.OnDemand.GraphQLMaxJobsPerRequest != 4 {
		t.Fatalf("graphql hydration max jobs = %d, want 4", cfg.OnDemand.GraphQLMaxJobsPerRequest)
	}
	if cfg.Vertex.ProfileMinFollowers != 500 {
		t.Fatalf("profile min followers = %d, want 500", cfg.Vertex.ProfileMinFollowers)
	}
	if cfg.Vertex.RankMinFollowers != 500 {
		t.Fatalf("rank min followers = %d, want 500", cfg.Vertex.RankMinFollowers)
	}
	if cfg.Vertex.SyncBatch != 200 {
		t.Fatalf("vertex sync batch = %d, want 200", cfg.Vertex.SyncBatch)
	}
	if got := strings.Join(cfg.Enrich.Tasks, ","); got != "topics,embeddings,trending,stance,sentiment,quality,controversy,nsfw" {
		t.Fatalf("enrich tasks = %q, want default signal tasks", got)
	}
	if cfg.Enrich.TrendingDedupeSimilarity != 0.82 {
		t.Fatalf("trending dedupe similarity = %f, want 0.82", cfg.Enrich.TrendingDedupeSimilarity)
	}
	for _, relay := range cfg.Firehose.Relays {
		if strings.Contains(strings.ToLower(relay), "pri"+"mal") {
			t.Fatalf("default relay set includes external cache host %q", relay)
		}
	}
}

func TestLoadGraphQLHydrationFollowsUserFeedDefault(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_ON_DEMAND_USER_FEED", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.OnDemand.UserFeed || !cfg.OnDemand.GraphQLHydration {
		t.Fatalf("on-demand flags = userFeed %v graphql %v", cfg.OnDemand.UserFeed, cfg.OnDemand.GraphQLHydration)
	}
}

func TestLoadGraphQLHydrationOverride(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_ON_DEMAND_USER_FEED", "false")
	t.Setenv("NAGG_ON_DEMAND_GRAPHQL_HYDRATION", "true")
	t.Setenv("NAGG_ON_DEMAND_GRAPHQL_LIMIT", "75")
	t.Setenv("NAGG_ON_DEMAND_GRAPHQL_MAX_JOBS_PER_REQUEST", "2")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.OnDemand.UserFeed {
		t.Fatal("user feed should remain disabled")
	}
	if !cfg.OnDemand.GraphQLHydration {
		t.Fatal("graphql relay hydration should be enabled by explicit override")
	}
	if cfg.OnDemand.GraphQLLimit != 75 || cfg.OnDemand.GraphQLMaxJobsPerRequest != 2 {
		t.Fatalf("graphql hydration config = limit %d jobs %d", cfg.OnDemand.GraphQLLimit, cfg.OnDemand.GraphQLMaxJobsPerRequest)
	}
}

func TestLoadVertexRankMinFollowersOverride(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_VERTEX_RANK_MIN_FOLLOWERS", "1250")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Vertex.RankMinFollowers != 1250 {
		t.Fatalf("rank min followers = %d, want 1250", cfg.Vertex.RankMinFollowers)
	}
}

func TestLoadRejectsNegativeVertexRankMinFollowers(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_VERTEX_RANK_MIN_FOLLOWERS", "-1")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "NAGG_VERTEX_RANK_MIN_FOLLOWERS") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadVertexSyncBatchOverride(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_VERTEX_SYNC_BATCH", "75")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Vertex.SyncBatch != 75 {
		t.Fatalf("vertex sync batch = %d, want 75", cfg.Vertex.SyncBatch)
	}
}

func TestLoadRejectsInvalidVertexSyncBatch(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_VERTEX_SYNC_BATCH", "0")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "NAGG_VERTEX_SYNC_BATCH") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadEnrichConfigOverride(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_ENRICH_TASKS", "topics,embeddings,topics")
	t.Setenv("NAGG_ENRICH_BATCH_SIZE", "32")
	t.Setenv("NAGG_ENRICH_POLL_INTERVAL", "5s")
	t.Setenv("NAGG_ENRICH_MODEL_DIR", "/models")
	t.Setenv("NAGG_ENRICH_MODEL_VERSION", "test-model-v1")
	t.Setenv("NAGG_ENRICH_MODEL_BACKEND", "ort")
	t.Setenv("NAGG_ENRICH_ONNX_LIBRARY_PATH", "/opt/onnxruntime/libonnxruntime.so")
	t.Setenv("NAGG_TRENDING_DEDUPE_SIM", "0.7")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(cfg.Enrich.Tasks, ","); got != "topics,embeddings" {
		t.Fatalf("enrich tasks = %q", got)
	}
	if cfg.Enrich.BatchSize != 32 {
		t.Fatalf("enrich batch size = %d, want 32", cfg.Enrich.BatchSize)
	}
	if cfg.Enrich.PollInterval.String() != "5s" {
		t.Fatalf("enrich poll interval = %s, want 5s", cfg.Enrich.PollInterval)
	}
	if cfg.Enrich.ModelDir != "/models" {
		t.Fatalf("enrich model dir = %q", cfg.Enrich.ModelDir)
	}
	if cfg.Enrich.ModelVersion != "test-model-v1" {
		t.Fatalf("enrich model version = %q", cfg.Enrich.ModelVersion)
	}
	if cfg.Enrich.ModelBackend != "ort" {
		t.Fatalf("enrich model backend = %q, want ort", cfg.Enrich.ModelBackend)
	}
	if cfg.Enrich.OnnxLibraryPath != "/opt/onnxruntime/libonnxruntime.so" {
		t.Fatalf("enrich onnx library path = %q", cfg.Enrich.OnnxLibraryPath)
	}
	if cfg.Enrich.TrendingDedupeSimilarity != 0.7 {
		t.Fatalf("trending dedupe similarity = %f, want 0.7", cfg.Enrich.TrendingDedupeSimilarity)
	}
}

func TestLoadRejectsUnknownEnrichTask(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_ENRICH_TASKS", "topics,unknown")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "NAGG_ENRICH_TASKS") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRejectsUnknownEnrichBackend(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_ENRICH_MODEL_BACKEND", "cuda")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "NAGG_ENRICH_MODEL_BACKEND") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRejectsInvalidTrendingDedupeSimilarity(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_TRENDING_DEDUPE_SIM", "1.5")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "NAGG_TRENDING_DEDUPE_SIM") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadViewerPubkey(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_VIEWER_PUBKEY", strings.ToUpper(testViewerPubkey))

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Viewer.PubKey != testViewerPubkey {
		t.Fatalf("viewer pubkey = %q, want %q", cfg.Viewer.PubKey, testViewerPubkey)
	}
}

func TestLoadRejectsInvalidViewerPubkey(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_VIEWER_PUBKEY", "not-a-pubkey")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "NAGG_VIEWER_PUBKEY") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadVertexProfileMinFollowersOverride(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_VERTEX_PROFILE_MIN_FOLLOWERS", "750")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Vertex.ProfileMinFollowers != 750 {
		t.Fatalf("profile min followers = %d, want 750", cfg.Vertex.ProfileMinFollowers)
	}
}

func TestLoadRejectsNegativeVertexProfileMinFollowers(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_VERTEX_PROFILE_MIN_FOLLOWERS", "-1")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "NAGG_VERTEX_PROFILE_MIN_FOLLOWERS") {
		t.Fatalf("error = %v", err)
	}
}

func containsKind(kinds []int, want int) bool {
	for _, kind := range kinds {
		if kind == want {
			return true
		}
	}
	return false
}

const testViewerPubkey = "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"
