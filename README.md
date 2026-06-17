# Nagg

Nagg is a Nostr AppView aggregator. The first milestone is a Go ingester that subscribes to configured Nostr relays, validates events, and stores raw events, relay provenance, and flattened tags in ClickHouse.

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

The default `NAGG_KINDS` is `0,1,3,4,6,7,16,443,444,445,1059,1063,9735,10050,10051,30078,38000`, which covers profiles, notes, contact lists, legacy encrypted DMs, reposts, reactions, generic reposts, NIP-17 gift wraps, file metadata, zaps, DM relay lists, Marmot key-package relay lists, app data, and ecash mint recommendations for the app-view API. Set `NAGG_KINDS` explicitly when you need a different relay subscription. Removed kinds stop being requested the next time the ingester or API-hosted ingester starts. Stored ClickHouse rows are not pruned during startup; run the retention command deliberately when you are ready to delete stale content. Set `NAGG_SINCE=0` to omit the `since` filter.

## Backfill The App-View Tables

ClickHouse materialized views only populate from inserts after migration. Run the app-view backfill once after creating the schema or after changing app-view aggregates:

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_CLICKHOUSE_USERNAME=nagg \
NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
go run ./cmd/backfill
```

The command is idempotent: it truncates and rebuilds app-view counter/profile tables from `nostr_events` and `event_tags`.

## Prune Removed Event Kinds

After removing a kind from `NAGG_KINDS`, run retention once to delete stored rows whose kind is outside the current allowlist:

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_CLICKHOUSE_USERNAME=nagg \
NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
go run ./cmd/retention
```

The command deletes matching relay-provenance, derived, notification, tag, and raw event rows, then rebuilds app-view tables if an app-view source kind was pruned. It skips when `NAGG_KINDS` is empty and refuses to run while ClickHouse already has active mutations or less than 10% disk headroom. Run it as an explicit maintenance action, not as deploy startup, because DELETE mutations rewrite table parts.

## Run The API

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_CLICKHOUSE_USERNAME=nagg \
NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
go run ./cmd/api
```

The API listens on `:8080` by default and serves `POST /graphql`, `GET /graphiql`, `GET /healthz`, and the app-view REST routes under `/nostr/*`. Set `NAGG_API_ADDR=:9090` to change the bind address. If `NAGG_API_ADDR` is unset and Railway provides `PORT`, the API listens on that port.

Set `NAGG_VIEWER_PUBKEY` to a 64-hex pubkey when you want app-view viewer routes to work without an explicit viewer parameter. It is used as the fallback for `/nostr/feed`, `/nostr/feed/user`, `/nostr/follows`, and `/nostr/profile`; explicit invalid pubkeys still return `400`.

The Vertex DVM proxy routes (`/nostr/search`, `/nostr/recommended`) require a funded/authorized 64-hex `NAGG_VERTEX_PRIVATE_KEY`. `/nostr/profile` always returns local Nagg profile data when available; it only calls Vertex for profiles with at least `NAGG_VERTEX_PROFILE_MIN_FOLLOWERS` local followers, default `500`, and falls back to the permanent ClickHouse Vertex profile cache when live Vertex fails. GraphQL ranking reads the columnar `vertex_scores` cache only; when the Vertex client is configured, the API service warms recent high-follower authors in the background using `NAGG_VERTEX_RANK_MIN_FOLLOWERS` and `NAGG_VERTEX_SYNC_BATCH`.

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
curl 'http://127.0.0.1:8080/nostr/feed?kind=trending&limit=20'
curl -X POST http://127.0.0.1:8080/nostr/notes/stats \
  -H 'content-type: application/json' \
  -d '{"ids":[]}'
```

`/healthz` returns the total raw event count plus a per-configured-kind breakdown with the event count, estimated raw `nostr_events` bytes, and decimal GB. The GB value is useful for comparing kind pressure; it is not exact compressed ClickHouse part size.

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
NAGG_ENRICH_TASKS=topics,embeddings,trending,stance,sentiment,quality,controversy,nsfw
NAGG_ENRICH_BATCH_SIZE=256
NAGG_ENRICH_POLL_INTERVAL=30s
NAGG_ENRICH_MODEL_DIR=/models
NAGG_ENRICH_MODEL_VERSION=local-skeleton-v1
NAGG_ENRICH_MODEL_BACKEND=go
NAGG_ENRICH_ONNX_LIBRARY_PATH=
NAGG_TRENDING_DEDUPE_SIM=0.82
```

Do not set `PORT` yourself on Railway; Railway injects it for the web service. Set `NAGG_API_ADDR` only when you intentionally want to override the bind address outside Railway.

Set `NAGG_ON_DEMAND_USER_FEED=true` on the API service to opportunistically hydrate app-view reads from `NAGG_RELAYS`. `NAGG_ON_DEMAND_GRAPHQL_HYDRATION` defaults to the same value and can be enabled separately for GraphQL-only relay hydration. The API inserts fetched author notes/reposts, matching originals, engagement events, replies, profiles, follow/contact-list events, DM envelopes, and relay-safe GraphQL event-query matches into ClickHouse. GraphQL hydration maps relay-safe filters such as ids, pubkeys/authors, kinds, tag values, `since`, `until`, and the requested pagination window to Nostr relay filters, then returns ClickHouse-indexed results as usual; search, ranking, shuffle, exclusions, and derived tags remain local-only filters. `NAGG_ON_DEMAND_GRAPHQL_LIMIT` caps each relay query and `NAGG_ON_DEMAND_GRAPHQL_MAX_JOBS_PER_REQUEST` caps the number of background relay jobs a single GraphQL request can schedule. For NIP-17, Nagg fetches the relay-facing `kind:1059` gift wraps p-tagged to the viewer plus the viewer's `kind:10050` DM inbox relay list; it does not decrypt inner `kind:14` chat messages, `kind:15` file messages, or wrapped reactions. `NAGG_ON_DEMAND_DM_LIMIT` controls the per-page relay query size and `NAGG_ON_DEMAND_DM_BACKFILL_PAGES` controls how many older pages each DM request hydrates. By default `NAGG_ON_DEMAND_WAIT=0s`, so reads return the indexed data already available while targeted hydration continues in the background for the next matching request. Set `NAGG_ON_DEMAND_WAIT=500ms` or similar only if you want a request to briefly wait and re-read when hydration finishes quickly. This covers `/graphql` event, aggregate, notification, reference, ranking, and DM queries plus `/nostr/feed`, `/nostr/feed/user`, `/nostr/events`, `/nostr/dm/envelopes`, `/nostr/profiles`, `/nostr/profile`, `/nostr/follows`, `/nostr/notes/stats`, and `/nostr/thread`. Keep the cooldown enabled in production so repeated requests for the same missing data do not fan out to relays every time.

The pre-deploy command only migrates schemas. After first deploy, or after changing app-view aggregate logic, run the backfill command once from the Railway shell or a one-off command:

```sh
./nagg-backfill
```

After changing `NAGG_KINDS`, deploy normally first so removed kinds are no longer requested from relays, then run retention from the Railway shell or a one-off command when ClickHouse is not already processing mutations:

```sh
./nagg-retention
```

Do not run `./nagg-retention` from `preDeployCommand` or normal API/ingester startup.

If you want the ingester as a separate Railway service, deploy the same image and override the start command to:

```sh
./nagg-ingester
```

If you want the enrichment worker as a separate Railway service, deploy the same image with `railway.enricher.toml` or override the start command to:

```sh
./nagg-enricher
```

The enrichment worker scans `nostr_events` by a persisted per-task watermark, writes `derived_tags`, `derived_metrics`, `event_embeddings`, and `trending_clusters`, and resumes idempotently through `enrichment_state`. `NAGG_ENRICH_TASKS=none` disables all tasks for a no-op service. The trending task emits H8, H24, and D7 clusters, and `NAGG_TRENDING_DEDUPE_SIM` controls the centroid-cosine merge threshold. The fallback processors are local and deterministic; Hugot model-backed processors load ONNX model artifacts from `NAGG_ENRICH_MODEL_DIR` when matching model folders are present. The default Docker image supports `NAGG_ENRICH_MODEL_BACKEND=go`, Hugot's pure-Go backend. Use `ort` only with a custom image that includes ONNX Runtime and Hugot's native tokenizer library; set `NAGG_ENRICH_ONNX_LIBRARY_PATH` if the ORT library is not in the default location. The Phase 8 signal tasks write stance and `nsfw` derived tags plus `sentiment`, `contribution_quality`, contribution sub-scores, and `controversy` derived metrics.

Expected model folder names under `NAGG_ENRICH_MODEL_DIR`:

- `embeddings` for Hugot feature extraction.
- `sentiment` for text classification.
- `stance` for zero-shot NLI classification.
- `nsfw-text` for text classification of media posts.
- `nsfw-image` for local image-path classification.

Use `scripts/export-models/` to export Hugging Face models into this folder shape offline.

Run the example GraphQL client:

```sh
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql go run ./cmd/graphql-client
```

The read layer exposes raw Nostr events plus constrained generic aggregations over `EVENTS`, `TAGS`, and `RELAYS`. App concepts such as reactions, replies, profiles, follows, and zaps are expressed as client query recipes using `kinds` and tag filters rather than hard-coded GraphQL fields. When you need a server-side join, the API stays generic by exposing more raw events through primitive relations like `pubkeyEvents(kinds: [0], limit: 1)`.

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

- "How many likes and reposts does `nevent1…` have?"
- "How many followers does `npub1…` have, and what's their nip05?"
- "Show the 10 newest comments in the thread rooted at `nevent1…`."
- "What are the top event kinds on the relay?"
- "Which events got the most reactions?"

## Backfill One Thread

Fetch a root event from configured Nostr relays, recursively crawl associated `e`-tagged events, and backfill kind-0 profiles for all discovered pubkeys:

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_THREAD_RELAY=wss://relay.damus.io \
NAGG_THREAD_EXTRA_RELAYS=wss://nos.lol,wss://relay.nostr.band \
go run ./cmd/thread-crawler nevent1...
```

Then demonstrate that GraphQL can resolve a display-ready thread summary using only raw events plus same-`pubkey` lookups for kind `0` metadata:

```sh
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql \
go run ./cmd/thread-demo <root-event-id>
```

For a cleaner text UI that renders the thread tree with usernames, profile image URLs, comment counts, and like counts, use:

```sh
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql \
go run ./cmd/thread-cli <root-event-id>
```
