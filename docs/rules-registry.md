# The rules registry (`internal/rules`)

The registry is the declarative core of nagg's data layer. Kind-to-kind
aggregations, event lifetimes, and per-author ingest caps are **data** — Go
literals validated at startup — and everything else derives from them:

- the ClickHouse schema (aggregate tables + the materialized views feeding
  them) is generated from the declarations at migrate time;
- the schema reconciler treats static SQL + generated DDL as the single
  desired schema, so deleting a rule retires its table and view;
- the ingest pipeline fans extractor-based references into `event_refs`;
- retention predicates and cap enforcement come from the same lists;
- the REST envelope's `aggregates` map and GraphQL's rank metric names are
  the rules' names — declare a relationship and both surfaces serve it.

The vocabulary is deliberately unopinionated: rules speak in event kinds, tag
keys, and targets. What an app calls a "like count" is the unique-actor metric
of the rule counting kind-7 events that `e`-reference an event.

## Declaring a relationship

```go
{
    Name:    rules.CanonicalName([]int{7}, "e"),   // "k7_e"
    Kinds:   []int{7},                              // source kinds
    Ref:     rules.Ref{TagKey: "e", Target: rules.TargetEventID},
    Metrics: []rules.Metric{{Name: "actors", Agg: rules.AggUniqActors}},
    Refresh: rules.RefreshIngest,
}
```

- **Ref** is exactly one of:
  - `TagKey` — the tag whose value is the target (`e`, `p`, `q`, `a`); add
    `Marker` to require a NIP-10 marker at tag position 3;
  - `Extractor` — a named entry in the extractor registry, for references a
    tag match cannot express;
  - `Author: true` — aggregate events against their own author's pubkey
    ("how many events of these kinds has each pubkey created").
- **Metrics**: `AggUniqActors` (distinct referencing pubkeys),
  `AggUniqSources` (distinct referencing events), `AggSumValue` (sum of the
  extractor-provided value; extractor refs only).
- **Refresh**: `RefreshIngest` rules get an `AggregatingMergeTree` plus a
  generated materialized view — maintained at insert time, reads are trusted
  lookups. `RefreshPeriodic` rules get the table only; a rollup pass owns the
  writes (used when the resolution needs machinery an MV can't run, e.g. the
  NIP-10 direct-parent walk behind `k1_1111_e_reply`).

**Naming**: use `CanonicalName(kinds, ref)` — mechanical, kind-derived names
(`k7_e`, `k6_16_e`, `k1_1111_author`) keep the client-visible vocabulary
neutral. The name is the aggregate identifier clients see in envelopes and
rank inputs, and the basis of the table name (`agg_<name>`, view
`mv_agg_<name>`).

## The prototype-then-declare workflow

Measure a question first — GraphQL's `events`/`aggregateEvents` can count any
kind/tag relationship live. If the query is worth trusting at scale, declare
the rule. On the next deploy, `Store.applyGeneratedSchema` creates the table
and **backfills it from raw history automatically** (tag/author rules via a
generated `INSERT … SELECT` over `event_tags`/`nostr_events`; extractor rules
via a Go replay into `event_refs`). Backfills run only when the table is newly
created, so uniq states stay exact and sums never double-count. From then on
the count is maintained automatically and served from the envelope.

## Extractors

The closed registry in `extractors.go` holds named parsing primitives for
references a tag match cannot express. Rules reference them by name, so the
rule set stays inspectable data while irregular parsing lives in one place.
First entry: `zap_target` — resolves a kind-9735 receipt to the zapped event
(receipt `e` tag, falling back to the `e` tag inside the `description`
zap-request JSON) and the paid sats (`amount` tag msats, falling back to
parsing the bolt11 invoice).

**Add an extractor only when a real rule needs it — never speculatively.**

## Projections

`Projection{Name, Kinds, Fields}` declares a latest-event-per-author
extraction: "for each pubkey, keep the newest event of `Kinds`, with these
columns pulled out of it". The generated table is `latest_<name>`
(ReplacingMergeTree(created_at) keyed by pubkey, implicit
pubkey/event_id/created_at columns), fed by a generated MV over
`nostr_events`, with the same first-creation backfill as relationships. Field
sources are a closed set, like extractors: `JSONPath` (a
`JSONExtractString(content, …)` string), `RawContent` (the whole content),
`TagKey` (the event's 64-hex values of that tag, as an array). The defaults
declare `latest_k0` (kind-0 metadata fields + raw_json) and `latest_k3`
(`refs` = p-tag values) — the tables that used to be hand-written SQL.

## Supersessions

`Supersession{Name, Kinds, PerDTag}` declares that once an author publishes a
newer event of one of `Kinds`, the replaced versions get pruned (per author,
or per `(author, d-tag)` with `PerDTag`). **The default is keep**: a kind with
no supersession rule retains every version forever — deleting replaced events
is an explicit per-kind opt-in, exactly like any other lifetime decision.
Supersessions compile into lifetime rules and run through the same retention
machinery.

## Lifetimes

Absence of a rule means events live forever. Policies:

- `MaxAgeUnlessReferenced{Age, ByRules}` — expire after `Age` unless any of
  the named relationships recorded a reference; the protection ledger is the
  rules' aggregate tables, which outlive the referencing events;
- `MaxAge{Age}` — unconditional;
- `KeepLatestPerAuthor` / `KeepLatestPerAuthorDTag` — the policies
  supersession rules compile to; declare a `Supersession` instead of using
  these directly.

Execution mechanics (async lightweight DELETEs, one mutation at a time,
headroom guard) are unchanged — see `docs/retention.md`.

## Caps

`Cap{Kinds, Max, Window, ExemptKnownViewers}`: a non-exempt author gets at
most `Max` events of `Kinds` ingested per `Window` (a 24h window buckets on
the ingestion-time UTC day, so backdated `created_at` can't dodge it).
`Window == 0` declares a lifetime cap — currently approximated by an
in-process counter that resets on restart (under-enforces, never over-drops);
a durable counter is a follow-up. The default set's exemption is the
relevance model (known viewers ∪ their follows, `docs/retention.md`).

## Backfills

`Backfill{Kinds, Resync}`: events of `Kinds` are pulled out of relay history
systematically instead of only observed live. The firehose only ever sees new
publications, so long-lived, rarely-republished kinds (parameterized-
replaceable app data — kind 38000 was the motivating case: months of live
listening captured 23 events while the configured relays held ~1.5k) never
accumulate without one.

The executor is `ingest.Backfiller`. Per `(rule, kind, relay)` it walks
NIP-01 `until`/`limit` pagination newest-first down to relay exhaustion, with
**per-relay cursors** — a merged multi-relay cursor advances at the pace of
whichever relay's page reached oldest and silently skips the deeper relays'
middle ranges. Every page checkpoints watermarks to `relay_backfill_state`
(`oldest_synced`, `newest_synced`, `completed`), so an interrupted walk
resumes where it stopped and a partial walk is never mistaken for a complete
one — completeness is explicit state, not inferred from "some events exist".
Once complete, the walk re-runs every `Resync` from the top down to overlap
with `newest_synced`, catching events published while the service was down
(the firehose subscription looks back at most `NAGG_SINCE`). `Resync == 0`
means initial walk only.

Backfilled inserts go through `Store.InsertEvents` directly: like every
on-demand backfill they bypass the per-author caps (which target firehose
flooding, not history), and transport-level id + signature verification
already happened in `relayquery`. Declare a rule only for kinds served as a
browsable network-wide corpus; high-volume social kinds stay live-plus-on-
demand, their history deliberately bounded by cap and lifetime rules.

## The DVM plugin seam (`internal/dvm`)

External data-vending-machine integrations are plugins, not bespoke wiring:

```go
type Plugin interface {
    Name() string        // provider namespace clients see ("vertex")
    Kinds() []KindPair   // request/response kinds it speaks
    CacheDDL() []string  // its ClickHouse cache tables — applied at Migrate
                         // and reconciled exactly like rule tables
    Policy() Policy      // usage rules: cache TTL + inbound-ref gate
    ScoreProvider() any  // nil when unsupported
    SearchProvider() any
    RecommendProvider() any
}
```

Vertex is the first plugin (`internal/vertex/plugin.go`): kinds 5312/6312
(profile), 5313/6313 (recommend), 5315/6315 (search); cache tables
`vertex_scores`, `vertex_profile_cache`, `vertex_search_cache` declared by the
plugin rather than static SQL. `config.Load` builds the registry once for
every process, so no process can reconcile-drop another's cache tables.
GraphQL's score-source default and the appview `providers` namespace resolve
through the registry — a future DVM plugs in by adding one entry.

A plugin also declares its usage policy: `Policy{CacheTTL, MinInboundRefs}`.
`CacheTTL` bounds how long cached provider values are trusted before the
score sync refetches them; `MinInboundRefs` gates which pubkeys the provider
is consulted for, measured as latest-list kind-3 inbound refs (`latest_k3`
fan-in) — the declarative form of the historical >500-followers requirement.
Vertex currently declares bootstrap values (`time.Minute` = always-refetch, `100`) while the self-hosted graph converges; steady-state targets are `7 * 24h` and `500`. The old NAGG_VERTEX_*_MIN_FOLLOWERS env
vars are gone; change the declaration instead.

## Deliberately outside the registry

- **Rank features** (`rank_features`, engagement-real): multi-column,
  threshold-versioned rank plumbing computed by the rollup — consumed via
  weighted rank terms that *reference* rule names, but not themselves
  kind-to-kind counts.
- **The notifications feed** (`viewer_feed`): a viewer-indexed read
  model with its own incremental tick (`docs/notifications-flow.md`).
- **The mint auditor** (`internal/auditor`): a plain cached HTTP client — not
  a DVM, so not a plugin.
- **The enricher** (`internal/enrich`): model-computed derived metrics
  (`contribution_quality`), a different kind of provider.
- **Mint-info snapshots** (`internal/mintinfo`): a daily poller that snapshots
  each Cashu mint's NUT-06 `/v1/info` (stored full only on change, every poll
  logged) and serves the history as an initial document + RFC 6902 diffs. Polled
  HTTP config, not a Nostr primitive — so it sits here with the auditor, with its
  own tables (`mint_info_snapshots`, `mint_info_observations`) and a declarative
  `mintinfo.Source` (info path + volatile keys) rather than a registry rule.
