# Modules (`NAGG_MODULES`)

nagg ships as one binary that can serve two very different things: a
network-wide Nostr app-view, and a cashu mint observatory. They cost wildly
different amounts to run — the archive is ~17 event kinds of the whole network
plus ranking, enrichment and rollups; the observatory is a daily HTTP poll of
~200 mints plus a trickle of NIP-87 events.

`NAGG_MODULES` says which of them **this** deployment is. Everything else
follows from that one declaration: the migrations that run, the rule registry
that generates the schema, the relay kinds subscribed and stored, the HTTP
routes mounted, and the background workers started.

Unset means every module — production's behavior, unchanged.

## The modules

| module | owns |
| --- | --- |
| `core` | always on, never named: the ingestion tables (`nostr_events`, `event_tags`, `event_seen_relays`), the migration ledger, `relay_backfill_state`, the system-log bounds, `/nostr/capabilities` |
| `nostr` | the social app-view — feed, thread, notifications, DMs, profiles, search, follows, social graph, ranking; the enricher, the rollup, retention, the relevance tracker; GraphQL |
| `mint` | the cashu mint observatory — `/nostr/mint/{reviews,discover,history,changes}`, the `/mint-changes` page, the NUT-06 snapshotter, the auditor client |
| `vertex` | client-signed Vertex DVM relay; shared profile/search/recommended reads (also owned by `nostr`); optional trickle sync; existing plugin caches |
| `app` | the client-config surface — `/app/latest-version`, `/app/ai-lineup` (Routstr), `/app/rates` (BTC fiat), `/app/wallpapers`, `/app/btcmap/places` and `/app/btcmap/places/{id}` |

## What each module changes

**Schema.** Every file in `internal/clickhouse/migrations/` declares its owner on
its first line:

```sql
-- +module nostr
```

The tag is mandatory — an untagged file is a hard error at startup, because a
new migration silently landing in every deployment is exactly the drift this
mechanism prevents. `migrationNames` applies only the enabled modules' files.

**Rules.** `rules.Default` (six relationships, two projections, the ingest post
cap, the gift-wrap gates) drives a `nostr` deployment. Without that module,
`rules.Mint` applies instead: the kind-0 projection, replaceable pruning for
kinds 0 and 38000, and the NIP-87 history walk. No relationships means no
aggregate tables, no materialized views, and — because `InsertEvents` opens its
`event_refs` batch only when extractor rules exist — no `event_refs` at all.

**Reconciler.** The drop pass is bounded by what *any* module declares, not just
the active one. A `mint` process cannot mistake the Nostr app-view for dead
weight and strip it; a table is only retired when no module claims it.

**Routes.** Each entry in `appview.Handler.routes()` names its owning module.
`Register` skips the rest, and `/nostr/capabilities` advertises exactly what was
mounted, so a client feature-gating against a mint-only host sees the truth.

**Workers.** Defaults, each still individually overridable:

| flag | default |
| --- | --- |
| `NAGG_RUN_INGESTER` | `nostr` or `mint` |
| `NAGG_RUN_ENRICHER` | `nostr` |
| `NAGG_RUN_ROLLUP` | `nostr` |
| `NAGG_RUN_MINT_INFO` | `mint` |
| `NAGG_AUDITOR_ENABLED` | `mint` |
| `NAGG_ROUTSTR_ENABLED` | `app` |
| `NAGG_RATES_ENABLED` | `app` |
| `NAGG_VERTEX_RELAY_ENABLED` | `vertex` or `nostr` |
| `NAGG_WALLPAPERS_ENABLED` | `app` |
| `NAGG_BTCMAP_ENABLED` | `app` (request-time HTTP client, no periodic job) |

## Mint auditor refresh

The `mint` module enables `NAGG_AUDITOR_ENABLED` by default. Its background
worker tries `auditor.ucash.space` first and falls back to `api.audit.8333.space`
when the availability probe or roster fetch fails, returns HTML/non-JSON, or
returns no mints. It retries the primary on every pass, so recovery switches
back automatically. No schema changes or extra worker services are needed.

The worker warms at boot and waits `NAGG_AUDITOR_REFRESH` (default `1h`) between
passes. Discovery and the mint-info work-list read the last successful in-memory
snapshot without auditor network requests. Before warming, or after 24h without
a successful roster fetch, discovery uses NIP-87 data only. Optional Redis
response caching still follows the app-view cache policy.

`NAGG_AUDITOR_UCASH_ENABLED=false` uses only the legacy fallback;
`NAGG_AUDITOR_UCASH_UPTIME_ENABLED=false` skips the optional per-mint uptime and
latency calls. With enrichment enabled, requests are paced 200ms apart and the
roster is available before enrichment finishes. The Leptos function suffix is
configured through `NAGG_AUDITOR_UCASH_FN_SUFFIX`; an outdated suffix fails over
instead of accepting the app's HTML as audit data. See the
[README env table](../README.md#deploy-on-railway) for URLs and defaults.

Watch `auditor.source.changed` (`from`, `source`) when the selected source
changes, including the initial selection. Each successful pass emits
`auditor.refresh` with `source`, `mints`, and `uptimeEnriched` (number of rows
with a measured uptime). Individual enrichment failures leave those optional
fields absent; `auditor.refresh.failed` means neither roster was usable.

## App configuration and ops

`NAGG_MODULES=mint,app` mounts both `/app/latest-version` (GET/POST) and
`/app/ai-lineup`, `/app/rates`, `/app/wallpapers`, and `/app/btcmap/places`
(list and individual place GETs), including their `/v1/app/*` aliases. Adding `app` adds
no ClickHouse migrations, tables, or social workers: the mint rule registry,
stored kinds, and firehose kinds stay the same. The version endpoint reads only
configuration. AI lineup uses the Routstr HTTP client, enabled by default for
`app` with `NAGG_ROUTSTR_URL` as the primary and ordered
`NAGG_ROUTSTR_FALLBACK_URLS` as failovers; no extra credentials or database schema
are needed. Every refresh re-probes the primary; the response names the active
node and reports `node.fallbackUsed`. `NAGG_ROUTSTR_AUTH_MODE` optionally
advertises `bearer` or `x-cashu` for all configured nodes. Vendor/tier overrides
use `NAGG_AI_LINEUP_PINS`; absent enabled IDs are reported in `pinsMissing` and
warned once per successful catalog refresh. Catalogs are fresh for 15 minutes;
if every node fails, the last catalog remains available regardless of age.
See the [env defaults](../README.md#deploy-on-railway) and
[operator checks](appview-api.md#ai-lineup-operator-checks).
If Routstr is explicitly disabled, AI lineup returns 503.

The rates worker fetches signed price-bot notes directly from
`NAGG_RATES_RELAYS` (default `NAGG_RELAYS`) and cross-checks with mempool.space
HTTP prices. It runs immediately and hourly by default, stores only in memory,
and never inserts relay notes into ClickHouse. There is no reason to change
`NAGG_KINDS` or `NAGG_FIREHOSE_KINDS` for rates. GBP uses HTTP until a bot is
added through the source registry or `NAGG_RATES_EXTRA_SOURCES`. The endpoint
returns 503 before warming or when all retained prices expire; disabling the
worker also leaves it at 503. `rates.pass` logs per-source health every pass.

The wallpaper worker queries signed kind-30078 (`d=wallpaper-catalog`) and
kind-1063 (`t=wallpaper`) events from the configured admin directly through
`relayquery`. It warms at boot and refreshes hourly; the in-memory catalog is
usable for 24h after its last successful refresh. Empty or failed passes do not
reset that deadline. The route returns 503 before warmup, when disabled, or
when expired, and sends `Cache-Control: public, max-age=300` on success. It
bypasses the response cache to preserve that deadline. `wallpapers.refresh`
logs catalog counts; `wallpapers.refresh.failed` logs a fixed failure category.
No `nostr` module, event-query route, schema change, or `NAGG_KINDS` change is
needed.

BTC Map uses a bounded HTTP client to `/v4/places` with app-compatible default
fields and `include_deleted=false`. The existing response cache gives it 1h
fresh / 24h stale, including versioned aliases. It needs no worker or Redis;
the normal memory cache works when Redis is absent. See the
[API details and size limit](appview-api.md#wallpapers-and-btc-map).

`NAGG_APP_LATEST_VERSION`, `NAGG_APP_UPDATE_MESSAGE`, and `NAGG_APP_MIN_VERSION`
configure the version response (all default empty). Other GET `/app/*` responses use
60 seconds fresh / 24 hours stale in the response cache. The latest-version
response also sends `Cache-Control: public, max-age=60` for GET and POST.

`NAGG_LOG_LEVEL` defaults to `info` in API, ingester, enricher, migrate, and
backfill; use `debug` for per-request diagnostics or `warn`/`error` to reduce
logs. `NAGG_RATE_LIMIT_PER_MIN` defaults to 120 REST requests per client IP.
Both knobs apply independently of modules; see the [README env table](../README.md#deploy-on-railway).

## Stored kinds vs firehose kinds

`NAGG_KINDS` and `NAGG_FIREHOSE_KINDS` are two different questions, and
conflating them breaks the mint deployment.

- **`NAGG_KINDS` — what we KEEP.** It drives `PruneRemovedEventKinds`, which
  **deletes** every stored event outside the set, plus `/healthz`'s per-kind
  stats and the `NAGG_HISTORY_FLOOR` walk.
- **`NAGG_FIREHOSE_KINDS` — what we SUBSCRIBE to.** Defaults to the stored set,
  so a deployment that only narrows `NAGG_KINDS` behaves exactly as before.

A mint deployment stores kind 0 but does not subscribe to it: reviewer and
operator profiles arrive through the on-demand relay fetch in
`Handler.profileInfos`, for the handful of pubkeys that actually appear in a
kind-38000 event. A global kind-0 subscription would be hundreds of thousands of
events a day for information we can fetch precisely. With one shared knob, the
prune would delete those profiles on every restart.

```
NAGG_MODULES=mint
NAGG_KINDS=0,38000            # keep recommendations + the profiles they name
NAGG_FIREHOSE_KINDS=38000     # subscribe to the trickle only
```

## The mint deployment's whole ClickHouse

Eight tables and one materialized view, pinned by
`TestMintModuleDeclaresOnlyMintSchema`:

```
schema_migrations   nostr_events   event_tags   event_seen_relays
relay_backfill_state
mint_info_snapshots   mint_info_observations
latest_k0   (+ mv_latest_k0)
```

Plus the three Vertex DVM cache tables (`vertex_scores`,
`vertex_profile_cache`, `vertex_search_cache`), which every deployment creates:
the plugin registry declares them statically so all four binaries derive the
same schema, and `buildReadyAPI` reads the plugin's policy unconditionally.
Client-signed requests can populate them without `NAGG_VERTEX_PRIVATE_KEY`; keeping them means
`/nostr/mint/discover` can read cached operator reputation the moment social
enrichment is switched on.

`k38000_history` (a `rules.Backfill` with a 24h resync) walks the relays for
NIP-87 events, because a live firehose alone captures almost none of them:
measured 2026-07, months of live listening had 23 kind-38000 events against
~1.5k already sitting on the configured relay set.

## Vertex on the mint deployment

`NAGG_MODULES=mint,app,vertex` mounts `POST /nostr/vertex/relay`,
`GET|POST /nostr/search`, `GET /nostr/profile`, and `GET /nostr/recommended`
(and `/v1` aliases). `nostr` also mounts them, once even when both modules are
named. Without `nostr`, profile and ranking envelopes skip social stats and
retain ranked pubkeys even when kind-0 rows are absent.

The vertex plugin owns `vertex_scores`, `vertex_profile_cache`, and
`vertex_search_cache`. Its static registry is shared with mint and registered
once for every binary. Adding `vertex` introduces no DDL, migrations, columns,
or social workers: schema reconcile has the same desired schema and the same
all-module drop boundary as the mint slice. A regression test pins the exact
DDL and empty reconcile plan.

Client relay is on by default for `vertex`/`nostr`, with
`NAGG_VERTEX_CLIENT_MAX_PER_MIN=10` and
`NAGG_VERTEX_ALLOW_PERSONALIZED=false`. `NAGG_VERTEX_PRIVATE_KEY` is optional.
When set, the syncer runs every `NAGG_VERTEX_SYNC_INTERVAL` (30m), with
`NAGG_VERTEX_SYNC_BATCH=20` and `NAGG_VERTEX_SYNC_THROTTLE=2s` by default for
vertex without nostr (200/0s with nostr). Mint-mode candidates come from the
existing score cache, not social tables. A credit-exhausted tick stops after
one `vertex.sync.credits_exhausted` warning. Policy constants are seven days
and 500 inbound refs. See [client relay](vertex-client-relay.md) for the credit
model, protocol, privacy, caching limits, and exact operator settings.

## Adding a module

1. Add the constant to `internal/modules` and to `known`.
2. Tag its migrations `-- +module <name>`.
3. Tag the routes it owns in `appview.Handler.routes()`.
4. If it needs its own rule set, add it next to `rules.Mint` **and** to
   `allModuleDDL` — otherwise the reconciler will treat its tables as dead.
5. Give its workers a module-derived default in `config.Load`.

## Deploying

`railway.mint.toml` is the mint deployment's Railway config. One deployment, one
database: a mint-mode process pointed at a database holding Nostr data will
prune it away.

### Sizing it

There are two ClickHouse profiles, and they are not variations of each other:

| profile | for | why |
| --- | --- | --- |
| `deploy/clickhouse` | the full app-view (~26 GB) | direct-I/O merges, week-long forensic logs. Its page cache is load-bearing — capping this instance's memory has broken production twice. |
| `deploy/clickhouse-small` | a mint deployment (~1 MB) | absolute cache and thread-pool sizes instead of ratios of the host, self-logging switched off, `max_server_memory_usage` under the container cap. |

The problem the small profile solves: ClickHouse sizes its caches and pools from
the **host**, and Railway's host reports 24 cores and ~120 GB of RAM. Measured
on first boot, ClickHouse was billing 1.33 GB — about 97% of the deployment's
cost — to hold roughly a megabyte, while the nagg API beside it used 35 MB.

Pair it with Railway resource limits (Settings → Resource Limits, or
`serviceInstanceLimitsUpdate`). Those are not just a ceiling — both processes
size themselves from the cgroup, so the cap changes actual usage:
`max_server_memory_usage_to_ram_ratio` resolves against the container, and the
API's `automemlimit` sets `GOMEMLIMIT` from it (uncapped, it read ~20 GB).

Measured on the mint deployment, before → after:

| | ClickHouse | nagg-mint |
| --- | --- | --- |
| memory | 1.33 GB → **0.73 GB** | 0.035 GB → **0.015 GB** |
| CPU | 0.054 → **0.031** vCPU | 0.002 → **0.000** vCPU |
| threads | 857 → **256** | — |
| `max_server_memory_usage` | 21.6 GB → **787 MB** | — |
| Railway limit | 1 GB / 1 vCPU | 0.5 GB / 1 vCPU |

The remaining ~0.7 GB is mostly floor: `MemoryCode` is 475 MB of ClickHouse
binary. Anonymous heap is ~336 MB, so the 1 GB cap has real headroom.
