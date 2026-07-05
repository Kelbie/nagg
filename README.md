# Nagg

Nagg is a terminology-agnostic Nostr app-view server. It ingests events from
relays, stores them raw in ClickHouse, and understands exactly three things
about them: **kinds**, **tags**, and the **references** between events and
pubkeys. It refuses to know what a post, like, repost, profile, thread, or
notification is — those are names clients give to kinds and references, and
they live in clients.

What the server does instead is declarative: relationships, aggregations,
projections, lifetimes, and ingest caps are **declared as data** in a rule
registry, and the ClickHouse schema, the materialized views, the historical
backfills, the retention predicates, and both read surfaces (REST app-view +
GraphQL) are all derived from those declarations.

## Principles

These exist so future changes can be checked against them. A change that
violates one needs a better reason than convenience.

1. **The server speaks events, kinds, tags, and targets.** App concepts
   (posts, likes, profiles, threads) never appear in table names, column
   names, identifiers, or API payloads. Clients reconstruct concepts from
   kinds and references.
2. **Behavior is declared, not hand-rolled.** Every aggregation, projection,
   lifetime, supersession, and cap is a rule in `internal/rules`. If you are
   writing a bespoke count table, a hand-written materialized view, or a
   one-off retention query, you are doing it wrong — declare it.
3. **Prototype in GraphQL, promote to a declared rule.** GraphQL can count
   any kind/tag relationship live with zero server changes; a declared rule
   serves the same answer as a precomputed lookup. The measured gap
   (`docs/bench-appview-vs-graphql.md`): the registry-backed aggregates read
   returns the full rule vocabulary ~5× faster than the GraphQL prototype
   returns a subset of it — and the gap grows with the corpus.
4. **Raw events are the source of truth.** `nostr_events`, `event_tags`, and
   `event_seen_relays` are protected; everything else must be rebuildable
   from them. A derived table you cannot drop and regenerate is a bug.
5. **One envelope for every route.** Responses are
   `{order, orderBy, events, aggregates, cursor}`: hydration is just more
   events (a profile is a kind-0 event), counts are rule names. No
   route-specific response vocabulary.
6. **External services are plugins.** A DVM integration declares its name,
   kinds, cache tables, and providers through `internal/dvm` — never ad-hoc
   wiring or hardcoded provider strings.
7. **Route paths may be use-case-named** (`/nostr/feed`, `/nostr/thread`) —
   that is the one deliberate concession — but nothing beneath a route may
   be: the shapes, tables, and code stay generic.

## The declarative registry

Everything below lives in `internal/rules` and is validated at startup; see
`docs/rules-registry.md` for the full developer guide and
`docs/appview-api.md` for the envelope contract clients parse.

**Relationships** — kind-to-kind reference aggregations:

```go
{
    Name:    rules.CanonicalName([]int{7}, "e"),   // "k7_e"
    Kinds:   []int{7},
    Ref:     rules.Ref{TagKey: "e", Target: rules.TargetEventID},
    Metrics: []rules.Metric{{Name: "actors", Agg: rules.AggUniqActors}},
    Refresh: rules.RefreshIngest,
}
```

A `Ref` is exactly one of: a **tag key** (optionally with a NIP-10 marker
filter), a named **extractor** (for references a tag match cannot express —
the first is `zap_target`, resolving kind-9735 receipts to their target and
sat amount from nested JSON/bolt11), or **author** (aggregate events against
their own author's pubkey). `RefreshIngest` rules become an
AggregatingMergeTree fed by a generated materialized view — maintained at
insert time, reads are trusted lookups. `RefreshPeriodic` rules get the table
only and a rollup pass owns the writes (used when resolution needs machinery
an MV can't run, like the NIP-10 direct-parent walk).

**Projections** — latest-event-per-author extractions:

```go
{Name: "k0", Kinds: []int{0}, Fields: []rules.ProjField{
    {Name: "name", JSONPath: "name"},
    {Name: "raw_json", RawContent: true},
}}
```

generates `latest_k0` (ReplacingMergeTree keyed by pubkey) plus its MV and
backfill. Field sources are a closed set: content JSON paths, raw content, or
a tag key's 64-hex values.

**Supersessions** — opt-in pruning of replaced versions:

```go
{Name: "replaceable_latest", Kinds: []int{0, 3, 10050, 10051}}
```

**The default is keep**: a kind with no supersession rule retains every
version forever. Declaring one prunes older versions once an author publishes
a newer event (per author, or per `(author, d-tag)` with `PerDTag`).

**Lifetimes** — when events stop being stored (absence = forever):

```go
{Name: "k1_1111_unreferenced_1y", Kinds: []int{1, 1111},
 Policy: rules.MaxAgeUnlessReferenced{Age: 365 * 24 * time.Hour,
     ByRules: []string{"k7_e", "k6_16_e", "k1_q", "k9735_e", "k1_1111_e_reply"}}}
```

`MaxAgeUnlessReferenced` builds its protection ledger from the named
relationships' aggregate tables, which outlive the referencing events.

**Caps** — per-author ingest limits:

```go
{Name: "k1_1111_6_16_daily", Kinds: []int{1, 1111, 6, 16},
 Max: 20, Window: 24 * time.Hour, ExemptKnownViewers: true}
```

`Window == 0` declares a lifetime cap. Exemption is the relevance model
(known viewers and everyone they follow).

**How it all derives.** At migrate time the store applies the
registry-generated DDL after the static SQL; a table created for the first
time is backfilled from raw history automatically (so declaring a rule
retroactively covers everything already ingested). The schema reconciler
treats static SQL + generated DDL as the single desired schema and drops
anything undeclared — deleting a rule retires its table and view. Readers
resolve `(rule, metric)` through `ReadSpec` to `{table, column, merge
function}`; the REST envelope's `aggregates` map and GraphQL's rank metric
names are the rules' names (`k7_e.actors`, `k9735_e.value_total`), produced
by `CanonicalName` so the client-visible vocabulary stays kind-derived.

**DVM plugins** (`internal/dvm`): an external data-vending-machine
integration implements

```go
type Plugin interface {
    Name() string        // provider namespace clients see ("vertex")
    Kinds() []KindPair   // request/response kinds it speaks
    CacheDDL() []string  // its cache tables — applied and reconciled
                         // exactly like rule tables
    Policy() Policy      // usage rules: cache TTL + inbound-ref gate
    ScoreProvider() any
    SearchProvider() any
    RecommendProvider() any
}
```

A plugin also declares its **usage policy** — `Policy{CacheTTL,
MinInboundRefs}`. `CacheTTL` is how long cached provider values are trusted
before the score sync refetches them (best-effort values that sharpen in
place as the provider's own dataset matures); `MinInboundRefs` gates which
pubkeys are worth consulting the provider for, measured as latest-list
kind-3 inbound refs (the `latest_k3` fan-in) — the declarative form of the
historical "more than 500 followers" requirement. Vertex currently declares BOOTSTRAP values (1-minute TTL =
always-refetch, gate 100) while the self-hosted graph converges; the
steady-state targets are 7 days and 500 — see the declaration comment.

Vertex is the first plugin (kinds 5312/6312, 5313/6313, 5315/6315; cache
tables `vertex_scores`, `vertex_profile_cache`, `vertex_search_cache`).
GraphQL's score-source default and the app-view `providers` namespace resolve
through the plugin registry — a future DVM plugs in by adding one entry.

## API surfaces

Every REST app-view route returns the generic envelope (`docs/appview-api.md`
is the contract): `order` (anchor event ids), `events` (raw Nostr events —
ordered items plus hydration, including each author's kind-0), `aggregates`
(`target → rule name → metric → value`), `cursor`. The GraphQL endpoint
(`POST /graphql`, GraphiQL at `/graphiql`) exposes raw events plus
constrained generic aggregation over `EVENTS`, `TAGS`, and `RELAYS` — app
concepts are client query recipes over `kinds` and tag filters, and
server-side joins stay generic through primitive relations like
`pubkeyEvents(kinds: [0], limit: 1)`.

## Prerequisites

### ClickHouse

Run a ClickHouse instance using the [official Docker image](https://hub.docker.com/_/clickhouse):

```sh
docker run -d \
  --name nagg-db \
  -e CLICKHOUSE_USER=nagg \
  -e CLICKHOUSE_PASSWORD=nagg_secret \
  -e CLICKHOUSE_DB=default \
  --ulimit nofile=262144:262144 \
  -p 8123:8123 \
  -p 9000:9000 \
  clickhouse/clickhouse-server:latest
```

## Run The Ingester

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_CLICKHOUSE_USERNAME=nagg \
NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
go run ./cmd/ingester
```

Useful configuration:

```sh
NAGG_RELAYS=wss://relay.damus.io,wss://nos.lol,wss://relay.snort.social
NAGG_KINDS=0,1,3,4,6,7,16,443,444,445,1059,1063,9735,10050,10051,30078,38000
NAGG_SINCE=24h
NAGG_BATCH_SIZE=1000
NAGG_FLUSH_INTERVAL=5s
NAGG_VERIFY_EVENTS=true
NAGG_ON_DEMAND_USER_FEED=false
NAGG_ON_DEMAND_WAIT=0s
```

The default `NAGG_KINDS` is `0,1,3,4,6,7,16,443,444,445,1059,1063,9735,10050,10051,30078,38000`. Set `NAGG_KINDS` explicitly when you need a different relay subscription. The ingester treats this list as the retained kind allowlist: when it starts, any raw, tag, relay-provenance, derived, or viewer-ref rows for kinds outside the configured list are pruned before new relay subscriptions open. Set `NAGG_SINCE=0` to omit the `since` filter.

## Backfill The Derived Tables

Rule-derived tables (aggregates and projections) backfill themselves from raw
history the first time a declaration creates them (see
`docs/rules-registry.md`). The manual backfill below is only needed to force
a full rebuild of every derived table:

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_CLICKHOUSE_USERNAME=nagg \
NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
go run ./cmd/backfill
```

The command is idempotent: it truncates and rebuilds the rule-derived tables from `nostr_events` and `event_tags`.

## Run The API

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_CLICKHOUSE_USERNAME=nagg \
NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
go run ./cmd/api
```

The API listens on `:8080` by default and serves `POST /graphql`, `GET /graphiql`, `GET /healthz`, and the app-view REST routes under `/nostr/*` (also mounted at `/v1/nostr/*`). Set `NAGG_API_ADDR=:9090` to change the bind address. If `NAGG_API_ADDR` is unset and Railway provides `PORT`, the API listens on that port.

Set `NAGG_VIEWER_PUBKEY` to a 64-hex pubkey when you want app-view viewer routes to work without an explicit viewer parameter. It is used as the fallback for `/nostr/feed`, `/nostr/feed/user`, `/nostr/follows`, and `/nostr/profile`; explicit invalid pubkeys still return `400`.

The Vertex DVM proxy routes (`/nostr/search`, `/nostr/recommended`) require a funded/authorized 64-hex `NAGG_VERTEX_PRIVATE_KEY`. `/nostr/profile` always returns local data when available; it only calls Vertex for pubkeys with at least `NAGG_VERTEX_PROFILE_MIN_FOLLOWERS` local followers, default `500`, and falls back to the permanent ClickHouse Vertex profile cache when live Vertex fails. GraphQL ranking reads the columnar `vertex_scores` cache only; when the Vertex client is configured, the API service warms recent high-follower authors in the background using `NAGG_VERTEX_RANK_MIN_FOLLOWERS` and `NAGG_VERTEX_SYNC_BATCH`.

```sh
NAGG_VERTEX_PRIVATE_KEY=<64-hex-secret> \
NAGG_VERTEX_RELAY=wss://relay.vertexlab.io \
NAGG_VERTEX_PROFILE_MIN_FOLLOWERS=500 \
NAGG_VERTEX_RANK_MIN_FOLLOWERS=500 \
NAGG_VERTEX_SYNC_BATCH=200 \
NAGG_VIEWER_PUBKEY=<64-hex-pubkey> \
NAGG_NIP05_VALIDATE=true \
go run ./cmd/api
```

App-view smoke checks:

```sh
curl http://127.0.0.1:8080/healthz
open http://127.0.0.1:8080/graphiql
curl 'http://127.0.0.1:8080/nostr/feed?pubkeys=<64-hex-pubkey>&limit=20'
curl -X POST http://127.0.0.1:8080/nostr/events/aggregates \
  -H 'content-type: application/json' \
  -d '{"ids":[]}'
```

`/healthz` returns the total raw event count plus a per-configured-kind breakdown with the event count, estimated `nostr_events` bytes, and decimal GB. The GB value is refreshed asynchronously from ClickHouse part metadata and per-kind counts, so health checks do not scan raw event payloads; use `storageStatsReady` to tell whether the latest response includes a refreshed estimate. The GB value is useful for comparing kind pressure; it is not exact per-kind ClickHouse part size.

## Deploy On Railway

This repo includes a `Dockerfile` and `railway.toml` for the API service. Railway builds the Dockerfile, runs `./nagg-migrate` as the pre-deploy command, starts `./nagg-api`, and checks `GET /healthz` before making the deployment active.

Required service variables:

```sh
NAGG_CLICKHOUSE_ADDR=<clickhouse-private-host>:9000
NAGG_CLICKHOUSE_DATABASE=default
NAGG_CLICKHOUSE_USERNAME=<clickhouse-user>
NAGG_CLICKHOUSE_PASSWORD=<clickhouse-password>
```

Optional service variables:

```sh
NAGG_RELAYS=wss://relay.damus.io,wss://nos.lol,wss://relay.snort.social
NAGG_KINDS=0,1,3,4,6,7,16,443,444,445,1059,1063,9735,10050,10051,30078,38000
NAGG_VERTEX_PRIVATE_KEY=<64-hex-secret>
NAGG_VERTEX_RELAY=wss://relay.vertexlab.io
NAGG_VERTEX_PROFILE_MIN_FOLLOWERS=500
NAGG_VERTEX_RANK_MIN_FOLLOWERS=500
NAGG_VERTEX_SYNC_BATCH=200
NAGG_VIEWER_PUBKEY=<64-hex-pubkey>
NAGG_NIP05_VALIDATE=true
NAGG_GRAPHQL_TIMEOUT=30s
NAGG_POST_CAP_PER_DAY=20
NAGG_ON_DEMAND_USER_FEED=false
NAGG_ON_DEMAND_GRAPHQL_HYDRATION=false
NAGG_ON_DEMAND_COOLDOWN=5m
NAGG_ON_DEMAND_TIMEOUT=5s
NAGG_ON_DEMAND_WAIT=0s
NAGG_CLICKHOUSE_MAX_OPEN_CONNS=30
NAGG_CLICKHOUSE_MAX_IDLE_CONNS=10
NAGG_ON_DEMAND_AUTHOR_LIMIT=100
NAGG_ON_DEMAND_ENGAGEMENT_LIMIT=1000
NAGG_ON_DEMAND_THREAD_LIMIT=1000
NAGG_ON_DEMAND_FOLLOW_LIMIT=1000
NAGG_ON_DEMAND_DM_LIMIT=200
NAGG_ON_DEMAND_DM_BACKFILL_PAGES=2
NAGG_ON_DEMAND_GRAPHQL_LIMIT=100
NAGG_ON_DEMAND_GRAPHQL_MAX_JOBS_PER_REQUEST=4
NAGG_ENRICH_TASKS=quality
NAGG_ENRICH_BATCH_SIZE=256
NAGG_ENRICH_POLL_INTERVAL=30s
NAGG_ENRICH_MODEL_VERSION=local-skeleton-v1
```

Do not set `PORT` yourself on Railway; Railway injects it for the web service. Set `NAGG_API_ADDR` only when you intentionally want to override the bind address outside Railway.

Set `NAGG_ON_DEMAND_USER_FEED=true` on the API service to opportunistically hydrate app-view reads from `NAGG_RELAYS`. `NAGG_ON_DEMAND_GRAPHQL_HYDRATION` defaults to the same value and can be enabled separately for GraphQL-only relay hydration. The API inserts fetched author events, matching originals, referencing events, kind-0/kind-3 events, DM envelopes, and relay-safe GraphQL event-query matches into ClickHouse. GraphQL hydration maps relay-safe filters such as ids, pubkeys/authors, kinds, tag values, `since`, `until`, and the requested pagination window to Nostr relay filters, then returns ClickHouse-indexed results as usual; search, ranking, shuffle, exclusions, and derived tags remain local-only filters. `NAGG_ON_DEMAND_GRAPHQL_LIMIT` caps each relay query and `NAGG_ON_DEMAND_GRAPHQL_MAX_JOBS_PER_REQUEST` caps the number of background relay jobs a single GraphQL request can schedule. For NIP-17, Nagg fetches the relay-facing `kind:1059` gift wraps p-tagged to the viewer plus the viewer's `kind:10050` DM inbox relay list; it does not decrypt inner `kind:14` chat messages, `kind:15` file messages, or wrapped reactions. `NAGG_ON_DEMAND_DM_LIMIT` controls the per-page relay query size and `NAGG_ON_DEMAND_DM_BACKFILL_PAGES` controls how many older pages each DM request hydrates. By default `NAGG_ON_DEMAND_WAIT=0s`, so reads return the indexed data already available while targeted hydration continues in the background for the next matching request. Set `NAGG_ON_DEMAND_WAIT=500ms` or similar only if you want a request to briefly wait and re-read when hydration finishes quickly. This covers `/graphql` event, aggregate, viewer-feed, reference, ranking, and DM queries plus `/nostr/feed`, `/nostr/feed/user`, `/nostr/events`, `/nostr/dm/envelopes`, `/nostr/profiles`, `/nostr/profile`, `/nostr/follows`, `/nostr/events/aggregates`, and `/nostr/thread`. Keep the cooldown enabled in production so repeated requests for the same missing data do not fan out to relays every time.

The pre-deploy command only migrates schemas; rule-derived tables backfill themselves on first creation. Run the full rebuild only after changing derivation logic for existing tables, from the Railway shell or a one-off command:

```sh
./nagg-backfill
```

If you want the ingester as a separate Railway service, deploy the same image and override the start command to:

```sh
./nagg-ingester
```

If you want the enrichment worker as a separate Railway service, deploy the same image with `railway.enricher.toml` or override the start command to:

```sh
./nagg-enricher
```

The enrichment worker scans `nostr_events` by a persisted per-task watermark, writes `derived_tags`/`derived_metrics`, and resumes idempotently through `enrichment_state`. The only supported task in this build is `quality` (the `contribution_quality` derived metric, read by ranking); `NAGG_ENRICH_TASKS=none` disables all tasks for a no-op service, and unsupported task names left over from older builds are dropped rather than erroring.

Run the example GraphQL client:

```sh
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql go run ./cmd/graphql-client
```

If the ClickHouse container is not published to localhost, run API/client containers in the `nagg-db` network namespace:

```sh
docker run --rm --network container:nagg-db \
  -v "$PWD/bin/nagg-api-linux-arm64:/nagg-api:ro" \
  -e NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
  -e NAGG_CLICKHOUSE_USERNAME=nagg \
  -e NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
  alpine:3.20 /nagg-api
```

## Ask In Plain Language

The `nagg-graphql` skill (in `.claude/skills/nagg-graphql/`) turns plain-language questions into GraphQL queries and runs them against `$NAGG_GRAPHQL_ENDPOINT`. For example:

- "How many kind-7 reactions and kind-6 reposts does `nevent1…` have?"
- "How many followers does `npub1…` have, and what's their nip05?"
- "Show the 10 newest replies in the tree rooted at `nevent1…`."
- "What are the top event kinds on the relay?"

## Thread Exploration Tools (retired)

The v1 exploration tools (`thread-crawler`, `thread-demo`, `thread-cli`) are
gone: on-demand thread backfill now happens inside the app-view — `GET
/nostr/thread?id=<hex>` fetches missing events from relays automatically and
serves the ranked tree in the generic envelope.
