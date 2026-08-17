package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/dvm"
	"github.com/vertex-lab/nagg/internal/enrich"
	"github.com/vertex-lab/nagg/internal/firehose"
	"github.com/vertex-lab/nagg/internal/ingest"
	"github.com/vertex-lab/nagg/internal/modules"
	"github.com/vertex-lab/nagg/internal/relayquery"
	"github.com/vertex-lab/nagg/internal/rules"
	"github.com/vertex-lab/nagg/internal/vertex"
)

type Config struct {
	ClickHouse chstore.Config
	// Modules is the deployment's enabled slice set (NAGG_MODULES). It is the
	// single declaration every other default follows from: the rule registry,
	// the relay kinds, the mounted HTTP routes, and which background workers
	// start. Unset means every module — production's behavior, unchanged.
	Modules modules.Set
	// DVM is the plugin registry shared by the store (cache-table schema) and
	// the API wiring (provider attachment, score-source validation).
	DVM      *dvm.Registry
	API      APIConfig
	Firehose firehose.Config
	// StoredKinds is the set of event kinds this app-view KEEPS
	// (NAGG_KINDS). It drives the retention prune — anything stored outside
	// this set is deleted — plus /healthz's per-kind stats and the
	// NAGG_HISTORY_FLOOR walk.
	//
	// It is deliberately separate from Firehose.Kinds (NAGG_FIREHOSE_KINDS),
	// the kinds we SUBSCRIBE to live. A mint deployment stores kind 0 —
	// reviewer and operator profiles, fetched on demand for the few pubkeys
	// that appear — while subscribing only to kind 38000; with one shared knob
	// the prune would delete those profiles on every restart.
	StoredKinds []int
	Ingest     ingest.Config
	Vertex     VertexConfig
	OnDemand   OnDemandConfig
	Viewer     ViewerConfig
	Enrich     EnrichConfig
	Cache      CacheConfig
	Auditor    AuditorConfig
	AppVersion AppVersionConfig
	Routstr    RoutstrConfig
	MintInfo   MintInfoConfig

	// RunIngester / RunEnricher let the API process host the firehose ingester
	// and the enrichment runner in-process (alongside the HTTP server + Vertex
	// syncer), so a single `nagg` service can do everything against one
	// ClickHouse + Redis. Defaults follow NAGG_MODULES — the ingester runs for
	// the nostr and mint modules (both need relay data), the enricher only for
	// nostr. Set false to split those workers back out into the standalone
	// cmd/ingester / cmd/enricher binaries (e.g. to scale the API horizontally
	// without N duplicate firehose consumers).
	RunIngester bool
	RunEnricher bool

	// Rollup configures the database-first aggregation job (direct-reply edges,
	// vertex-real engagement counts, per-user stats, per-event rank features).
	Rollup RollupConfig
	// RunRollup hosts the rollup job in the API process (alongside the Vertex
	// syncer + enricher). Defaults on for the nostr module — it maintains
	// nostr-owned tables and has nothing to do in a mint deployment.
	RunRollup bool

	// RunMintInfo hosts the mint-info snapshotter (internal/mintinfo) in the API
	// process. Defaults on for the mint module. The read side
	// (/nostr/mint/history) is served whenever that module is enabled; this only
	// gates the background poller.
	RunMintInfo bool
}

// MintInfoConfig parameterizes the mint-info snapshotter (internal/mintinfo):
// how often it re-checks for due mints, the per-mint minimum between polls (the
// anti-spam gate), the delay between fetches in one pass, and the per-fetch HTTP
// budget.
type MintInfoConfig struct {
	Interval time.Duration
	MinAge   time.Duration
	Throttle time.Duration
	Timeout  time.Duration
}

// RollupConfig parameterizes the periodic rollup job. MinActorScore is the Vertex
// score an engagement actor must clear to count toward a "real" (bot-resistant)
// count; Version is stamped into the threshold_version column so a threshold change
// is a new logical row.
type RollupConfig struct {
	Interval      time.Duration
	RecentWindow  time.Duration
	MaxTargets    int
	MinActorScore float64
	Version       string
	// RetentionInterval paces the declarative retention rules
	// (clickhouse.RetentionRules, docs/retention.md). <= 0 disables.
	RetentionInterval time.Duration
	// RetentionDryRun logs per-rule matched counts without deleting anything —
	// the retention rollback lever.
	RetentionDryRun bool
}

// AppVersionConfig backs POST /app/latest-version so the Sovran app's update
// check reads through nagg instead of api.sovran.money. LatestVersion empty
// means "no update advertised" (the client treats an empty/older-or-equal
// version as up to date).
type AppVersionConfig struct {
	LatestVersion string
	UpdateMessage string
}

// AuditorConfig configures the upstream cashu mint auditor client that powers
// /nostr/mint/discover. URL empty (or Enabled false) leaves discovery
// Nostr-only (reviews + operator social, no audit state / supported units).
type AuditorConfig struct {
	URL     string
	Enabled bool
	Limit   int
}

// RoutstrConfig configures the Routstr node catalog client behind
// GET /app/ai-lineup. URL empty (or Enabled false) leaves the route 503 and
// the app derives its AI lineup client-side. Vendors is the ordered
// comma-separated vendor-slug list to curate tabs for; Pins is a JSON blob of
// per-vendor tier overrides ({"anthropic":{"max":"claude-opus-4.7"}}), the
// OTA lever for hardcoding a model onto old app builds.
type RoutstrConfig struct {
	URL     string
	Enabled bool
	Vendors []string
	Pins    string
}

type APIConfig struct {
	GraphQLTimeout time.Duration
	// MaxConcurrentRequests bounds concurrent CH-heavy app-view requests (cache
	// misses). 0 = unlimited. Protects a capacity-limited ClickHouse from being
	// overwhelmed by a burst of concurrent heavy queries.
	MaxConcurrentRequests int
}

// CacheConfig configures the response cache. URL selects a shared Redis (so
// replicas share hits); with it empty the cache falls back to an in-process
// cache of MemoryBytes rather than being disabled.
type CacheConfig struct {
	URL string
	// MemoryBytes bounds the in-process cache. It is response bodies only, and
	// it lives inside nagg's container memory limit, so keep it well under it.
	MemoryBytes int64
	DefaultTTL  time.Duration
	// StaleFor is how long past a key's fresh TTL a cached response may still be
	// served while it is revalidated in the background (stale-while-revalidate).
	// A non-positive value disables stale serving, making every expiry a full
	// recompute.
	StaleFor time.Duration
}

type VertexConfig struct {
	PrivateKey    string
	Relay         string
	ValidateNIP05 bool
	SyncBatch     int
	SyncInterval  time.Duration
	SyncThrottle  time.Duration
}

type ViewerConfig struct {
	PubKey string
}

type EnrichConfig struct {
	Tasks        []string
	BatchSize    int
	PollInterval time.Duration
	ModelVersion string
}

type OnDemandConfig struct {
	UserFeed                 bool
	GraphQLHydration         bool
	Cooldown                 time.Duration
	Timeout                  time.Duration
	Wait                     time.Duration
	UserFeedWait             time.Duration
	AuthorLimit              int
	EngagementLimit          int
	ThreadLimit              int
	FollowLimit              int
	DMLimit                  int
	DMBackfillPages          int
	GraphQLLimit             int
	GraphQLMaxJobsPerRequest int
}

func Load() (Config, error) {
	// NAGG_MODULES is the one declaration every default below follows from:
	// which rule registry drives the schema, which kinds are stored and
	// subscribed, and which background workers start. Unset means every module,
	// so a deployment that has never heard of modules is unaffected.
	mods, err := modules.Parse(os.Getenv("NAGG_MODULES"))
	if err != nil {
		return Config{}, fmt.Errorf("NAGG_MODULES: %w", err)
	}
	nostrModule := mods.Has(modules.Nostr)
	mintModule := mods.Has(modules.Mint)

	onDemandUserFeed := parseBool(env("NAGG_ON_DEMAND_USER_FEED", "true"))

	// The declarative rule registry (internal/rules) drives the generated
	// aggregate schema, the ingest event_refs fan-out, retention, and the
	// per-author ingest caps. NAGG_POST_CAP_PER_DAY parameterizes the default
	// cap rule; 0 disables capping. Without the nostr module there is nothing
	// to aggregate or cap, so the far smaller mint rule set applies instead.
	ruleRegistry := rules.MustMint()
	if nostrModule {
		ruleRegistry = rules.MustDefault(parseInt(env("NAGG_POST_CAP_PER_DAY", "20")))
	}

	// The DVM plugin registry: static identity (name, kinds, cache DDL) is
	// declared here so every process — api, ingester, migrate, backfill —
	// derives the same schema; runtime providers are attached by cmd/api.
	dvmRegistry := dvm.MustRegistry(vertex.NewPlugin())

	// Two kind sets, deliberately: what we KEEP and what we SUBSCRIBE to.
	// See Config.StoredKinds — a mint deployment keeps on-demand-fetched kind-0
	// profiles it never subscribes to, and one shared knob would prune them.
	storedKinds := parseKinds(env("NAGG_KINDS", defaultStoredKinds(mods)))
	firehoseKinds := parseKinds(env("NAGG_FIREHOSE_KINDS", defaultFirehoseKinds(mods, storedKinds)))

	// NAGG_HISTORY_FLOOR: absolute date; when set, relay history for the FULL
	// firehose kind set is walked down to it (see rules.HistoryFloorBackfill).
	historyFloor, err := parseDate(os.Getenv("NAGG_HISTORY_FLOOR"))
	if err != nil {
		return Config{}, fmt.Errorf("NAGG_HISTORY_FLOOR: %w", err)
	}

	cfg := Config{
		Modules:     mods,
		StoredKinds: storedKinds,
		DVM:         dvmRegistry,
		ClickHouse: chstore.Config{
			Addr:         env("NAGG_CLICKHOUSE_ADDR", "127.0.0.1:9000"),
			Database:     env("NAGG_CLICKHOUSE_DATABASE", "default"),
			Username:     env("NAGG_CLICKHOUSE_USERNAME", "default"),
			Password:     os.Getenv("NAGG_CLICKHOUSE_PASSWORD"),
			MaxOpenConns: parseInt(env("NAGG_CLICKHOUSE_MAX_OPEN_CONNS", "30")),
			MaxIdleConns: parseInt(env("NAGG_CLICKHOUSE_MAX_IDLE_CONNS", "10")),
			Rules:        ruleRegistry,
			DVM:          dvmRegistry,
			Modules:      mods,
			// Per-query memory ceiling (bytes) applied to every app-view read, as a
			// runaway guard: an over-budget query is rejected by ClickHouse with a
			// clean MEMORY_LIMIT_EXCEEDED (which the read-retry surfaces as one failed
			// request) instead of consuming the whole instance and cgroup-OOMing the
			// container (the source of the 502/restart flaps). The footprint is now
			// measured: the worst legitimate read (pre-fix contact-list lookups)
			// peaked at 7.31 GiB and its replacement runs in single-digit MiB, so
			// 4 GiB is far above every sanctioned query while still shielding the
			// instance. 0 = explicitly uncapped.
			MaxQueryMemoryBytes: parseInt64(env("NAGG_CLICKHOUSE_MAX_QUERY_MEMORY_BYTES", "4294967296")),
			// Emergency escape hatch for the notifications read-model rollout;
			// remove once the model has served production for a while.
			NotificationsLegacyRead: parseBool(env("NAGG_NOTIFICATIONS_LEGACY_READ", "false")),
		},
		API: APIConfig{
			GraphQLTimeout: parseDuration(env("NAGG_GRAPHQL_TIMEOUT", "30s")),
			// Slots on the PROCESS-WIDE heavy-query gate (chgate): heavy REST,
			// GraphQL, and the rollup all share it. The old ceiling of 2 matched a
			// world where a single read could eat multiple GiB (an engaged account's
			// notifications; the 7 GiB contact-list lookups); with those reads fixed
			// and every query capped at 4 GiB, the worst sanctioned read is tens of
			// MiB and 4 concurrent queries fit comfortably. Excess queues on the gate
			// (bounded by the request context) and succeeds slower instead of 5xx-ing.
			MaxConcurrentRequests: parseInt(env("NAGG_MAX_CONCURRENT_REQUESTS", "4")),
		},
		Firehose: firehose.Config{
			// Sanitized: the prod env value has been observed with hard line-wraps
			// INSIDE urls ("wss://relay.h\n  odl.ar"), which dial-fail forever.
			Relays:        relayquery.SanitizeRelays(splitCSV(env("NAGG_RELAYS", "wss://relay.damus.io,wss://nos.lol,wss://relay.snort.social"))),
			Kinds:         firehoseKinds,
			Since:         parseDurationPtr(env("NAGG_SINCE", "24h")),
			RelayRetry:    parseDuration(env("NAGG_RELAY_RETRY", "30s")),
			SeenCacheSize: parseInt(env("NAGG_SEEN_CACHE_SIZE", "200000")),
			ReadLimit:     parseInt64(env("NAGG_RELAY_READ_LIMIT_BYTES", "2097152")),
			SubID:         env("NAGG_SUB_ID", "nagg-firehose"),
		},
		Ingest: ingest.Config{
			BatchSize:     parseInt(env("NAGG_BATCH_SIZE", "1000")),
			FlushInterval: parseDuration(env("NAGG_FLUSH_INTERVAL", "5s")),
			QueueSize:     parseInt(env("NAGG_QUEUE_SIZE", "10000")),
			VerifyEvents:  parseBool(env("NAGG_VERIFY_EVENTS", "true")),
			// Declarative relay-history backfills: kinds walked out of relay
			// history and kept topped up, because a live firehose never
			// surfaces old, rarely-republished events. NAGG_HISTORY_FLOOR
			// additionally walks the full firehose kind set down to an
			// absolute date, through the same caps/gates as the live firehose.
			Backfills: historyFloorBackfills(ruleRegistry.Backfills(), firehoseKinds, historyFloor),
			// Declarative per-author firehose caps for authors NOT relevant
			// to any Sovran user (see internal/relevance). Measured 2026-07:
			// 20/day removes ~90% of monthly post volume, all of it from
			// firehose bridge/bot accounts.
			Caps: ruleRegistry.Caps(),
			// Declarative recipient-relevance gates: recipient-only kinds
			// (gift wraps) ingest only when p-tagged to the exemption
			// universe. Measured 2026-07: 99% of stored wraps were addressed
			// to pubkeys no Sovran viewer maps to.
			AddresseeGates: ruleRegistry.AddresseeGates(),
		},
		Vertex: VertexConfig{
			PrivateKey:    os.Getenv("NAGG_VERTEX_PRIVATE_KEY"),
			Relay:         env("NAGG_VERTEX_RELAY", "wss://relay.vertexlab.io"),
			ValidateNIP05: parseBool(env("NAGG_NIP05_VALIDATE", "true")),
			SyncBatch:     parseInt(env("NAGG_VERTEX_SYNC_BATCH", "200")),
			SyncInterval:  parseDuration(env("NAGG_VERTEX_SYNC_INTERVAL", "30m")),
			SyncThrottle:  parseDuration(env("NAGG_VERTEX_SYNC_THROTTLE", "0s")),
		},
		Auditor: AuditorConfig{
			URL:     env("NAGG_AUDITOR_URL", "https://api.audit.8333.space"),
			Enabled: parseBool(env("NAGG_AUDITOR_ENABLED", boolText(mintModule))),
			Limit:   parseInt(env("NAGG_AUDITOR_LIMIT", "200")),
		},
		AppVersion: AppVersionConfig{
			LatestVersion: os.Getenv("NAGG_APP_LATEST_VERSION"),
			UpdateMessage: os.Getenv("NAGG_APP_UPDATE_MESSAGE"),
		},
		Routstr: RoutstrConfig{
			URL:     env("NAGG_ROUTSTR_URL", "https://api.routstr.com"),
			Enabled: parseBool(env("NAGG_ROUTSTR_ENABLED", boolText(mods.Has(modules.App)))),
			Vendors: splitCSV(env("NAGG_AI_LINEUP_VENDORS", "openai,anthropic,x-ai,google")),
			Pins:    os.Getenv("NAGG_AI_LINEUP_PINS"),
		},
		OnDemand: OnDemandConfig{
			UserFeed:                 onDemandUserFeed,
			GraphQLHydration:         parseBool(env("NAGG_ON_DEMAND_GRAPHQL_HYDRATION", "false")),
			Cooldown:                 parseDuration(env("NAGG_ON_DEMAND_COOLDOWN", "5m")),
			Timeout:                  parseDuration(env("NAGG_ON_DEMAND_TIMEOUT", "5s")),
			Wait:                     parseDuration(env("NAGG_ON_DEMAND_WAIT", "0s")),
			UserFeedWait:             parseDuration(env("NAGG_ON_DEMAND_USER_FEED_WAIT", "3s")),
			AuthorLimit:              parseInt(env("NAGG_ON_DEMAND_AUTHOR_LIMIT", "100")),
			EngagementLimit:          parseInt(env("NAGG_ON_DEMAND_ENGAGEMENT_LIMIT", "1000")),
			ThreadLimit:              parseInt(env("NAGG_ON_DEMAND_THREAD_LIMIT", "1000")),
			FollowLimit:              parseInt(env("NAGG_ON_DEMAND_FOLLOW_LIMIT", "1000")),
			DMLimit:                  parseInt(env("NAGG_ON_DEMAND_DM_LIMIT", "200")),
			DMBackfillPages:          parseInt(env("NAGG_ON_DEMAND_DM_BACKFILL_PAGES", "2")),
			GraphQLLimit:             parseInt(env("NAGG_ON_DEMAND_GRAPHQL_LIMIT", "100")),
			GraphQLMaxJobsPerRequest: parseInt(env("NAGG_ON_DEMAND_GRAPHQL_MAX_JOBS_PER_REQUEST", "4")),
		},
		Viewer: ViewerConfig{
			PubKey: strings.ToLower(strings.TrimSpace(os.Getenv("NAGG_VIEWER_PUBKEY"))),
		},
		Enrich: EnrichConfig{
			Tasks:        supportedEnrichTasks(splitCSV(env("NAGG_ENRICH_TASKS", "quality"))),
			BatchSize:    parseInt(env("NAGG_ENRICH_BATCH_SIZE", "256")),
			PollInterval: parseDuration(env("NAGG_ENRICH_POLL_INTERVAL", "30s")),
			ModelVersion: env("NAGG_ENRICH_MODEL_VERSION", "local-skeleton-v1"),
		},
		Cache: CacheConfig{
			URL:         strings.TrimSpace(os.Getenv("NAGG_REDIS_URL")),
			MemoryBytes: parseInt64(env("NAGG_CACHE_MEMORY_BYTES", "134217728")), // 128 MiB
			DefaultTTL:  parseDuration(env("NAGG_CACHE_DEFAULT_TTL", "30s")),
			StaleFor:    parseDuration(env("NAGG_CACHE_STALE_FOR", "5m")),
		},
		// Worker defaults follow the enabled modules. The ingester serves both
		// nostr (the full firehose) and mint (the kind-38000 slice plus its
		// relay-history walk); the enricher and rollup only maintain
		// nostr-owned tables; the snapshotter is the mint module's whole point.
		RunIngester: parseBool(env("NAGG_RUN_INGESTER", boolText(nostrModule || mintModule))),
		RunEnricher: parseBool(env("NAGG_RUN_ENRICHER", boolText(nostrModule))),
		RunRollup:   parseBool(env("NAGG_RUN_ROLLUP", boolText(nostrModule))),
		RunMintInfo: parseBool(env("NAGG_RUN_MINT_INFO", boolText(mintModule))),
		MintInfo: MintInfoConfig{
			Interval: parseDuration(env("NAGG_MINT_INFO_INTERVAL", "1h")),
			MinAge:   parseDuration(env("NAGG_MINT_INFO_MIN_AGE", "24h")),
			Throttle: parseDuration(env("NAGG_MINT_INFO_THROTTLE", "1.5s")),
			Timeout:  parseDuration(env("NAGG_MINT_INFO_TIMEOUT", "8s")),
		},
		Rollup: RollupConfig{
			Interval:          parseDuration(env("NAGG_ROLLUP_INTERVAL", "15m")),
			RecentWindow:      parseDuration(env("NAGG_ROLLUP_RECENT_WINDOW", "48h")),
			MaxTargets:        parseInt(env("NAGG_ROLLUP_MAX_TARGETS", "50000")),
			MinActorScore:     parseFloat(env("NAGG_ROLLUP_MIN_ACTOR_SCORE", "0")),
			Version:           env("NAGG_ROLLUP_THRESHOLD_VERSION", "v1"),
			RetentionInterval: parseDuration(env("NAGG_RETENTION_INTERVAL", "24h")),
			RetentionDryRun:   parseBool(env("NAGG_RETENTION_DRY_RUN", "false")),
		},
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if _, _, err := net.SplitHostPort(c.ClickHouse.Addr); err != nil {
		return fmt.Errorf("NAGG_CLICKHOUSE_ADDR: %w", err)
	}
	if len(c.Firehose.Relays) == 0 {
		return errors.New("NAGG_RELAYS must contain at least one relay URL")
	}
	if c.Ingest.BatchSize < 1 {
		return errors.New("NAGG_BATCH_SIZE must be positive")
	}
	if c.Ingest.FlushInterval <= 0 {
		return errors.New("NAGG_FLUSH_INTERVAL must be positive")
	}
	if c.API.GraphQLTimeout <= 0 {
		return errors.New("NAGG_GRAPHQL_TIMEOUT must be positive")
	}
	if c.Vertex.PrivateKey != "" {
		if len(c.Vertex.PrivateKey) != 64 {
			return errors.New("NAGG_VERTEX_PRIVATE_KEY must be 64 hex characters")
		}
		if _, err := hex.DecodeString(c.Vertex.PrivateKey); err != nil {
			return fmt.Errorf("NAGG_VERTEX_PRIVATE_KEY: %w", err)
		}
		relayURL, err := url.Parse(c.Vertex.Relay)
		if err != nil {
			return fmt.Errorf("NAGG_VERTEX_RELAY: %w", err)
		}
		if relayURL.Scheme != "wss" && relayURL.Scheme != "ws" {
			return errors.New("NAGG_VERTEX_RELAY must use ws or wss")
		}
	}
	if c.Vertex.SyncBatch < 1 {
		return errors.New("NAGG_VERTEX_SYNC_BATCH must be positive")
	}
	if c.Viewer.PubKey != "" {
		if len(c.Viewer.PubKey) != 64 {
			return errors.New("NAGG_VIEWER_PUBKEY must be 64 hex characters")
		}
		if _, err := hex.DecodeString(c.Viewer.PubKey); err != nil {
			return fmt.Errorf("NAGG_VIEWER_PUBKEY: %w", err)
		}
	}
	if c.Enrich.BatchSize < 1 {
		return errors.New("NAGG_ENRICH_BATCH_SIZE must be positive")
	}
	if c.Enrich.PollInterval <= 0 {
		return errors.New("NAGG_ENRICH_POLL_INTERVAL must be positive")
	}
	if strings.TrimSpace(c.Enrich.ModelVersion) == "" {
		return errors.New("NAGG_ENRICH_MODEL_VERSION must be non-empty")
	}
	// NAGG_ENRICH_TASKS is NOT rejected here: unsupported tasks are dropped at
	// load (supportedEnrichTasks). Now that the API hosts the enricher in-process,
	// a stale env value must not crash the whole service — it just runs the
	// supported subset.
	// NAGG_REDIS_URL is intentionally not validated here: the cache is
	// best-effort, so an empty or malformed URL just disables it (see cache.New)
	// rather than failing the process.
	return nil
}

func validEnrichTask(task string) bool {
	return enrich.SupportedTask(task)
}

// supportedEnrichTasks drops any tasks this build no longer supports (e.g. a
// stale NAGG_ENRICH_TASKS env left over from a previous version with trending /
// topics / ML tasks). Dropping instead of erroring keeps the API — which now
// hosts the enricher in-process — from crashing on a stale value; unsupported
// tasks are simply ignored.
func supportedEnrichTasks(tasks []string) []string {
	out := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if validEnrichTask(task) {
			out = append(out, task)
		}
	}
	return out
}

// nostrKinds is the full app-view's kind set: everything the social surfaces
// read. It is spelled out rather than derived because it is a product decision
// (which Nostr surfaces this app-view serves), not a consequence of the rules.
const nostrKinds = "0,1,3,4,6,7,16,443,444,445,1059,1063,9735,10050,10051,30078,38000"

// mintStoredKinds is what a mint-only deployment KEEPS: NIP-87 mint
// recommendations and reviews (38000), plus the kind-0 profiles of the
// reviewers and operators those events name. The profiles arrive through
// on-demand relay fetches for the handful of pubkeys that actually appear —
// which is why they must be stored but must NOT be subscribed.
const mintStoredKinds = "0,38000"

// mintFirehoseKinds is what a mint-only deployment SUBSCRIBES to. Kind 38000 is
// a trickle; a global kind-0 subscription is hundreds of thousands of events a
// day for information the on-demand path fetches precisely.
const mintFirehoseKinds = "38000"

// defaultStoredKinds is the NAGG_KINDS default for a module set: the union of
// what each enabled module needs kept.
func defaultStoredKinds(mods modules.Set) string {
	if mods.Has(modules.Nostr) {
		// The nostr set already contains 38000 and 0, so it covers mint too.
		return nostrKinds
	}
	if mods.Has(modules.Mint) {
		return mintStoredKinds
	}
	return ""
}

// defaultFirehoseKinds is the NAGG_FIREHOSE_KINDS default. Every module except
// mint subscribes to exactly what it stores; mint deliberately subscribes to
// less, leaving kind 0 to the on-demand path.
func defaultFirehoseKinds(mods modules.Set, storedKinds []int) string {
	if !mods.Has(modules.Nostr) && mods.Has(modules.Mint) {
		return mintFirehoseKinds
	}
	return joinKinds(storedKinds)
}

func joinKinds(kinds []int) string {
	parts := make([]string, 0, len(kinds))
	for _, k := range kinds {
		parts = append(parts, strconv.Itoa(k))
	}
	return strings.Join(parts, ",")
}

// boolText renders a module-derived default for an env-var lookup, so the
// override path (parseBool(env(KEY, default))) stays identical whether the
// default is a literal or computed.
func boolText(v bool) string { return strconv.FormatBool(v) }

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func parseKinds(value string) []int {
	var kinds []int
	for _, part := range splitCSV(value) {
		if start, end, ok := strings.Cut(part, "-"); ok {
			a, errA := strconv.Atoi(strings.TrimSpace(start))
			b, errB := strconv.Atoi(strings.TrimSpace(end))
			if errA == nil && errB == nil && a <= b {
				for k := a; k <= b; k++ {
					kinds = append(kinds, k)
				}
			}
			continue
		}
		if k, err := strconv.Atoi(part); err == nil {
			kinds = append(kinds, k)
		}
	}
	return kinds
}

func parseDuration(value string) time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return d
}

func parseDurationPtr(value string) *time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return nil
	}
	return &d
}

func parseInt(value string) int {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return n
}

func parseInt64(value string) int64 {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseBool(value string) bool {
	v, err := strconv.ParseBool(value)
	return err == nil && v
}

func parseFloat(value string) float64 {
	v, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return v
}

// parseDate parses an absolute date — "2006-01-02" (midnight UTC) or full
// RFC3339 — into unix seconds. Empty means unset (0, nil error). Unlike the
// other forgiving parse helpers this one errors on malformed input: a typo'd
// history floor that silently parsed to "unset" would silently skip the walk.
func parseDate(value string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, nil
	}
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t.Unix(), nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return 0, fmt.Errorf("want YYYY-MM-DD or RFC3339, got %q", value)
	}
	return t.Unix(), nil
}

// historyFloorBackfills appends the NAGG_HISTORY_FLOOR deep-history rule to
// the registry's declared backfills; floor 0 (unset) leaves them untouched.
func historyFloorBackfills(declared []rules.Backfill, kinds []int, floor int64) []rules.Backfill {
	rule, ok := rules.HistoryFloorBackfill(kinds, floor, declared)
	if !ok {
		return declared
	}
	return append(append([]rules.Backfill(nil), declared...), rule)
}
