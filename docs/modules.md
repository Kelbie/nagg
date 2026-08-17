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
| `app` | the client-config surface — `/app/latest-version`, `/app/ai-lineup` (Routstr) |

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
They stay empty without `NAGG_VERTEX_PRIVATE_KEY`, and keeping them means
`/nostr/mint/discover` can read cached operator reputation the moment social
enrichment is switched on.

`k38000_history` (a `rules.Backfill` with a 24h resync) walks the relays for
NIP-87 events, because a live firehose alone captures almost none of them:
measured 2026-07, months of live listening had 23 kind-38000 events against
~1.5k already sitting on the configured relay set.

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
