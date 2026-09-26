# nagg app-view API (v2)

The app-view is nagg's REST surface: precomputed, aggregated Nostr reads served
over HTTP/JSON. It is the **only** transport `sovran-app` uses (via the
`@sovranbitcoin/nagg-ts` client) — there is no client-side GraphQL. nagg's
GraphQL endpoint exists server-side as the prototyping surface over the same
rule registry, but is not consumed by clients.

**v2 is terminology-agnostic.** The server speaks Nostr primitives — events,
kinds, tags, pubkeys — and declared aggregation rules. It never says post,
like, repost, reply, zap, or mention. Clients reconstruct concepts: a repost is
a kind-6/16 event whose `e` reference is embedded alongside; a profile is a
kind-0 event; counts are whatever aggregation rules the registry declares
(see `docs/rules-registry.md`). `appViewVersion` in `/nostr/capabilities` is
`"v2"`; the capability token is `appview.v2`.

Nostr routes mount at both `/nostr/*` and `/v1/nostr/*`; app configuration
routes mount at `/app/*` and `/v1/app/*` (identical handler,
cache, and middleware). Heavy routes run multi-query ClickHouse aggregations and
pass through a concurrency limiter; light routes do not. All responses are JSON.

## 1. The envelope

Every route returns ONE shape (route-specific extensions ride alongside, §4):

```jsonc
{
  "order":   ["<event-id>", "…"],        // server-authoritative render order
  "orderBy": "created_at",               // or "rank"
  "events":  [Event],                    // everything the response references
  "aggregates": {                        // target → rule → metric → value
    "<event-id>": { "k7_e": { "actors": 3 }, "k9735_e": { "value_total": 21, "sources": 1 } }
  },
  "cursor":  "…"                         // opaque pagination token; absent on last/only page
}
```

- **`order`** — anchor event ids in render order. For kind-6/16 entries the
  anchor is the **referenced (original) event's id**, so multiple reposts of
  one event collapse to one stable anchor. `orderBy: "rank"` means the client
  must not prepend live items above the fold; `"created_at"` means it may.
- **`events`** — raw Nostr shape (`{id, kind, pubkey, content, tags,
  created_at}`), deduplicated by id. Includes the ordered items **plus all
  hydration**: repost originals, resolved roots, quoted (`q`-tag) events, and
  each author's latest kind-0 profile event. Hydration is just more events —
  there are no side-maps. Parse profile fields from the kind-0 `content` JSON.
- **`aggregates`** — declared rule values keyed by target (event id, or pubkey
  on profile routes). **Zero values are omitted entirely**: a missing rule or
  metric means 0. Rule vocabulary in §2.
- **`cursor`** — echo it back to continue. Feeds encode
  `"<oldest created_at unix>|<page length>"` (pass the components back as the
  `until`/`offset` request params); list routes (DM, events/query, own
  history, notifications) encode `"<RFC3339Nano>|<id>"` of the oldest row.

## 2. Aggregation rule names

The values clients render come from the rule registry. Event-keyed:

| Rule.metric | Meaning |
| --- | --- |
| `k7_e.actors` | unique pubkeys that published a kind-7 referencing the event |
| `k6_16_e.actors` | unique pubkeys that published a kind-6/16 referencing it |
| `k1_q.sources` | unique kind-1 events `q`-referencing it |
| `k1_1111_e_reply.sources` | unique direct NIP-10/22 replies (periodic tier — minutes-stale) |
| `k9735_e.value_total` | sats total across kind-9735 receipts referencing it |
| `k9735_e.sources` | unique kind-9735 receipts referencing it |
| `vertex_k7_e.actors`, `vertex_k6_16_e.actors`, `vertex_k1_q.sources`, `vertex_k1_1111_e_reply.sources`, `vertex_k9735_e.sources`, `vertex_k9735_e.value_total` | the same signals counted only from Vertex-score-gated engagers (spam-resistant); computed by the rollup, zero until it has run for the event |
| `vertex_actors.actors` | distinct score-gated engagers across all reference types |

Pubkey-keyed (profile-family routes):

| Rule.metric | Meaning |
| --- | --- |
| `k3_p_latest.actors` | followers — latest kind-3 lists containing the pubkey |
| `k3_author_latest.sources` | following — size of the pubkey's own latest kind-3 |
| `k1_1111_author.sources` | events of kind 1/1111 the pubkey created |

## 3. Endpoint inventory

| Method | Path | Heavy | Response | Module |
| --- | --- | --- | --- | --- |
| GET | `/nostr/capabilities` | no | service info; `appViewVersion: "v2"` | core |
| GET,POST | `/nostr/feed` | yes | envelope + `hasMore` (§4) | nostr |
| GET | `/nostr/feed/user` | yes | envelope + `hasMore` (§4) | nostr |
| POST | `/nostr/feed/ranked` | yes | envelope + `hasMore` (§4; `orderBy: "rank"`) | nostr |
| GET,POST | `/nostr/notifications` | yes | envelope + `entries` + `hasNext` (§4) | nostr |
| GET | `/nostr/notifications/seen` | no | envelope holding the viewer's kind-30078 read-marker event; client parses `seenUntil` from its content | nostr |
| POST | `/nostr/events/aggregates` | no | envelope, aggregates only (`order`/`events` empty). Body `{"ids": ["<id>", …]}`, ≤ 100. **Replaces `/nostr/notes/stats`.** | nostr |
| GET | `/nostr/thread` | yes | envelope + `total` (§4); `order[0]` is the root id on every page, the rest is the server-ranked reply order | nostr |
| GET | `/nostr/follows` | no | envelope; pubkey-keyed aggregates | nostr |
| GET | `/nostr/events` | no | envelope; `order` = requested ids that resolved | nostr |
| POST | `/nostr/events/query` | yes | envelope (bare when the queried kinds include 1059 — §5) | nostr |
| GET,POST | `/nostr/dm/envelopes` | yes | bare envelope (§5) | nostr |
| GET,POST | `/nostr/dm/conversation` | yes | bare envelope (§5) | nostr |
| GET | `/nostr/follow-status` | no | envelope + `edges` (§4) | nostr |
| GET | `/nostr/mint/reviews` | yes | **not an envelope** (mint objects, not events) | mint |
| GET | `/nostr/mint/discover` | yes | **not an envelope** | mint |
| GET | `/nostr/mint/info` | yes | **not an envelope**: per-mint metadata + testnut verdict for the caller's own mint list (§6) | mint |
| GET | `/nostr/mint/history` | yes | **not an envelope**: NUT-06 info snapshot history (§7) | mint |
| GET | `/nostr/mint/changes` | yes | **not an envelope**: ecosystem changes and roster stats; optional `limit` (§7) | mint |
| GET | `/nostr/social-graph` | yes | envelope: the viewer's latest kind-3 / 10002 / 10000 events; derive follows, relays, mutes from their tags | nostr |
| GET | `/nostr/own/profiles` | no | envelope: kind-0 events + pubkey-keyed aggregates | nostr |
| GET | `/nostr/own/{type}` | yes | envelope of the viewer's own action history | nostr |
| GET | `/nostr/profiles` | no | envelope: kind-0 events for the requested pubkeys | nostr |
| GET | `/nostr/profile` | no | envelope + `pubkeys`/`providers`/`fromCache` (§4) | vertex or nostr |
| GET,POST | `/nostr/search` | no | envelope + `pubkeys`/`providers`/`fromCache` (§4) | vertex or nostr |
| GET | `/nostr/recommended` | no | envelope + `pubkeys`/`providers` (§4) | vertex or nostr |
| POST | `/nostr/vertex/relay` | no | signed event relay, write-through cache, `{ok, kind, result, fetchedAt, cached}` | vertex or nostr |
| GET,POST | `/app/latest-version` | no | app version, optional message and `minVersion`; no required params (§8) | app |
| GET | `/app/ai-lineup` | no | curated AI lineup, active node/auth mode, missing pins; no params (§8) | app |
| GET | `/app/ai-providers` | no | server-curated AI provider directory with health, model counts and operator reach; no params (§8) | app |
| GET | `/app/rates` | no | in-memory BTC fiat prices and source health; no params (§8) | app |
| GET | `/app/wallpapers` | no | `{wallpapers, albums, lastUpdated}`; signed admin catalog (§8) | app |
| GET | `/app/btcmap/places` | no | BTC Map v4 place array; sync query params (§8) | app |
| GET | `/app/btcmap/places/{id}` | no | BTC Map v4 place object, including requested `osm:*` fields (§8) | app |

Request parameters are unchanged from v1 (feed `spec`/`limit`/`until`/`offset`,
thread `id`/`sort`/`viewer`/…, notifications `viewer`/`tab`/`policy`/…).
Feed routes additionally accept `maxContentLength` (GET param or body field on
`/nostr/feed`; `target.maxContentLength` inside the `/nostr/feed/ranked` input,
capability `graphql.events.maxContentLength`): when >0, text events (kinds
1/1111) whose content exceeds that many UTF-8 code points are excluded
server-side, so the page stays full of skimmable posts. Non-text kinds pass
untouched (a kind-6/16 repost's content is the reposted event's JSON —
NIP-18). `/nostr/feed/user` (an author's own page) never applies it.
Thread `sort` accepts `new` (default), `ranked`, `relevant` — `ranked` orders
by the declared `k7_e.actors` aggregation. `relevant` is the product default:
ALL of the root author's direct replies lead the order (chronological), then
one followed reply to the root (viewer-scoped), then ranked direct replies,
then the remaining direct replies. Explicit sorts are literal — no OP pin
on `new`/`ranked`. Every sort is deterministic and honors `offset`/`replyLimit`
(`replyLimit=0` = everything from `offset`).

The ordered reply list carries ONLY the root's DIRECT replies — events whose
NIP-10/NIP-22 resolved parent (`reply` marker > last unmarked `e` tag > `root`
marker; kind-1111's lowercase `e`) is the root. Nested descendants and
mention-tagged events remain hydrated in `events` (for tap-through and client
caches) but are never ordered as replies of the root, and `total` counts
direct replies only.

## 4. Route extensions

**Feeds** — `/nostr/feed`, `/nostr/feed/user`, and `/nostr/feed/ranked`
include a boolean `hasMore` (capability `appview.feed.hasMore`):

```jsonc
{
  "hasMore": true,
  "cursor": "1710000000|30" // present only when hasMore is true
}
```

`hasMore` is a page-saturation hint: the number of fetched feed rows is at
least the effective positive limit, before hydration or repost-anchor
deduplication. No COUNT query is issued. A short or empty page has
`hasMore: false` and no `cursor`; a full page may still be the last page and
require an empty follow-up to discover the end. Clients that use a missing
cursor as end-of-feed now stop one request earlier on short non-empty pages.

The default limit is 30 for feed and ranked feed, and 50 for user feed.
Explicit zero or oversized limits (>100) fall back to 30. Ranked feed uses
the effective top-level `limit` from the shared ranker, not `target.limit`.

**Notifications** — envelope plus:

```jsonc
{
  "entries": [
    { "id": "<event-id>", "kind": 7, "actor": "<pubkey>", "target": "<event-id>",
      "total": 12, "totalCapped": false,
      "actors": [{ "pubkey": "…", "eventId": "…", "createdAt": 1710000000, "actorVertexScore": 42.0 }] }
  ],
  "hasNext": true
}
```

`id` is the representative (newest) triggering event; it is also the `order`
anchor and is embedded in `events`, as is the target event. **No reason
strings** — the kind carries the semantics: 3 = a contact list now references
you; 6/16 = a repost of your event; 7 = a reaction to it; 9735 = a zap receipt
for it; 1 = a kind-1 references you or your event, and the client derives
which by reading the embedded event's tags (`q` tag naming your event →
quote; `e` tag whose target is your event → reply; otherwise mention).
Entries without `total` are singles; grouped entries collapse many
same-kind/same-target events (grouping semantics and the conservative
`hasNext` hint are unchanged — see `docs/notifications-flow.md`).

**Thread** — envelope plus the pre-paging ordered-reply count (capability
`appview.thread.total`):

```jsonc
{
  "order": ["<rootId>", "<reply1>", "…"],  // root leads on EVERY page
  "orderBy": "rank",
  "events": [ /* the full fetched descendant set + hydration, on every page */ ],
  "aggregates": { "…": {} },
  "total": 130,      // ordered replies after dedupe/availability filter,
                     // BEFORE offset/replyLimit slicing; excludes the root
  "cursor": "0|72"   // "<until>|<offset>" (until pinned to 0); present iff
                     // offset + (len(order) - 1) < total — echo the offset
                     // back as ?offset= for the next page
}
```

`hasMore` truth is `cursor != null`. Clients must not derive it from event
counts or from the `k1_1111_e_reply` aggregate: the aggregate counts direct
replies from never-pruned `ref_edges`, while `events` carries all fetched
descendants — the two legitimately disagree.

**Follow-status** — envelope plus directional reference edges (no verb labels;
mutual = `out && in`):

```jsonc
{ "edges": { "<candidate-pubkey>": { "out": true, "in": false } } }
```

**Profile / search / recommended** — envelope plus:

```jsonc
{
  "pubkeys": ["<pubkey>", "…"],       // complete ranked list, including pubkeys
                                       // with no locally indexed kind-0 (order can
                                       // only anchor locally known profile events)
  "providers": {                       // provider-namespaced non-count data
    "<pubkey>": {
      "vertex": { "vertexFetchedAt": 1710000000, "rank": 1, "score": 87.2, "nodes": 210433, "references": ["<pubkey>", "…"] },
      "nip05":  { "valid": true },
      "nagg":   { "firstEventAt": 1710000000 }
    }
  },
  "fromCache": false,
  "vertexFresh": true // search: Vertex cache age < seven-day Policy.CacheTTL
}
```

Provider payloads are float/context-shaped data from named providers (the DVM
plugin seam, `internal/dvm`); counts stay in `aggregates`.

These three routes also carry `identities` (see [Identities](#identities)) for
every pubkey in `pubkeys` — on `/nostr/profile`, for the subject and its
`vertex.references` too — with the same Vertex figures as `providers[pk]`.

**Client-signed Vertex refresh** — `POST /nostr/vertex/relay` accepts a signed
5312/5313/5315 event as the JSON body, forwards it unchanged, verifies the
response signature and correlation, and writes parsed profile/search results
to the existing cache. Profile `result` is `ProfileResult`; search/recommend
`result` is a `SearchResult` array. Recommendations are uncached (`cached:false`).
Profile payload JSON also retains the signed response; the search table retains
only parsed ranks because it has no raw-event column. Source-scoped/non-global
profile results are also uncached to preserve global reputation.

`GET /nostr/profile` and `GET|POST /nostr/search` accept optional
`signedVertexRequest` via the **`svr` query parameter**, containing unpadded
base64url-encoded signed event JSON. POST search's JSON body is
`{query, limit?, sort?, source?}`; `svr` stays in the URL. The signed target or
query/limit/sort/source must match the read. With `svr`, the refresh is
synchronous within the 15-second DVM deadline and the response uses the fresh
result directly. Without it, existing cache/fallback behavior applies.
`providers[pk].vertex.vertexFetchedAt` is Unix seconds or null;
`vertexFresh` on search is true for Vertex data younger than `Policy.CacheTTL`
(seven days), including a successful empty signed search. Local-only fallback
is false. Ordinary GET response caching still applies; signed reads bypass it.

Signature/ID, kind, timestamp (±300s), bounded content/tags, and parameter
validation failures return 400. Personalized Pagerank is rejected unless
`NAGG_VERTEX_ALLOW_PERSONALIZED=true`. Relay and piggyback requests share the
per-signing-pubkey limit (`NAGG_VERTEX_CLIENT_MAX_PER_MIN`, default 10/min);
exceeding it returns 429. Kind-7000 errors return HTTP 200
`{ok:false,reason:"insufficient_credits"|"rejected",message:"<sanitized>"}`;
timeout returns 504 `{ok:false,reason:"timeout"}`. Other upstream/cache failures
return 502 `{ok:false,reason:"unavailable"}`; relay disabled/unconfigured is 503.
The relay cache policy is zero, and both relay and signed read responses are
`Cache-Control: no-store` and bypass Redis/LRU entirely.

The `vertex` module mounts these routes without social tables; profiles return
cached Vertex plus available kind-0 data and omit social counts. No private
key is required for client relay or cached reads. Read the
[Vertex client relay guide](vertex-client-relay.md) for wire examples, credits,
privacy, async cache visibility, and operator settings.

## Identities

Every response that names a Nostr pubkey carries a top-level `identities`
map, keyed by lowercase hex pubkey, so the app renders score, reach, profile
and cross-links from any route out of one cache. It is additive: the
per-route fields it duplicates (`providers[pk].vertex`, a discover row's
`operatorPubkey`/`followers`/`vertexRank`, the `profiles` maps) are unchanged.

```jsonc
"identities": {
  "<hex>": {
    "pubkey": "<hex>",
    "npub": "npub1…",
    "profile": {                       // null when no kind-0 is known
      "name": "…", "displayName": "…", "picture": "…", "banner": "…", "about": "…",
      "nip05": "…", "nip05Valid": true, "website": "…", "lud16": "…"   // empty keys omitted
    },
    "reach": { "followers": 1234, "follows": 56, "source": "graph" },
    "vertex": { "rank": 0.0012, "score": 87.2, "fetchedAt": 1710000000 },
    "operates": { "mints": ["https://mint.example"], "aiProviders": ["https://ai.example"] },
    "firstEventAt": 1710000000
  }
}
```

Absence is `null`, never `0`. `reach.followers`/`follows` are null when nagg
could not resolve them (`source` is then omitted); `source` is `graph`,
`vertex` or `relays` as under [AI provider directory](#ai-provider-directory),
and a `relays` answer counts followers only, so `follows` is null. `vertex`
is always an object, each field null when unknown; it is the local cache,
never a live DVM call, except where the route already holds a fresh result
(`/nostr/profile` with `svr`, ranked search rows), which the identity then
repeats so the two cannot disagree. `operates` is always present with both
lists (empty, never null): the mints whose NUT-06 nostr contact is this
pubkey, from the auditor roster, and the AI providers the directory attributes
to it. `firstEventAt` is computed only on `/nostr/profile` and null elsewhere.
`nip05Valid` is present only on `/nostr/profile`, the one route that validates
the name; list routes carry the claimed `nip05` unverified.

Routes: `/nostr/profile`, `/nostr/search`, `/nostr/recommended` (every ranked
pubkey), `/nostr/mint/discover` (returned rows' operators),
`/nostr/mint/reviews` (reviewers and the mint's operator), `/nostr/mint/info`
(the rows' operators) and `/app/ai-providers` (the rows' operators). The
kind-0 read includes the on-demand relay backfill, so a mint-only deployment
still fills `profile`.

## 5. DM privacy: bare envelopes

`/nostr/dm/envelopes`, `/nostr/dm/conversation`, and any `/nostr/events/query`
whose kinds include 1059 return the envelope with **empty aggregates and no
profile hydration**. Gift-wrap authors are ephemeral pubkeys; enriching them
would be meaningless at best and correlating at worst. nagg never decrypts —
the client decrypts and buckets by counterparty. `/nostr/dm/conversation`
takes an optional `counterparty`: kind-4 is scoped to the pair; kind-1059
returns the full viewer inbox (nagg cannot see inside the wraps).

## 6. On-demand relay backfill

Most read endpoints trigger a bounded, non-blocking relay backfill on a cache
miss / incomplete result (feed, feed/user, thread, events/aggregates, events,
follows, dm, profiles, profile, social-graph, search), so a cold author or
thread is populated from relays and served. Backfill failures are logged and
the response proceeds with whatever is available. `feed/ranked` (precomputed)
and the mint/events-query paths do not backfill.

## Mint discovery

`GET /nostr/mint/discover?limit=200[&mint=<url>][&testnut=true|false]` returns
`{mints: [...], profiles: {...}}`, **not an envelope**. It unions NIP-87 kind-38000
reviews (scored reviews and score-less recommendations alike) with the most
recent auditor roster, itself the union of both auditors (see below). The review
aggregate scans the full stored review set (capped at 5000 events), not a page,
so per-mint counts match `/nostr/mint/reviews` and a mint whose only reviews are
old still appears. `mint` is an optional URL-encoded,
normalized exact-match filter (the existing mint URL normalization ignores host
case and trailing slashes). It applies before `limit` and returns at most one
row, or `mints: []` when unknown. It does not fetch a mint or auditor on demand.
`testnut=true` returns only testnut mints, `testnut=false` only the rest;
omitted returns both, and any other value is a 400. Like `mint`, it applies
before `limit`. The `/v1/nostr/mint/discover` alias supports the same query.

Each row includes `mintUrl`, optional `name`, `iconUrl`, `description`,
`supportedUnits` (union of NUT-04/05 method units), and the raw NUT-06 `nuts` map.
Review fields are `averageScore` (nullable), `reviewCount`, and `favouriteCount`.
Audit fields include `hasAudit`, `state`, `nMints`, `nMelts`, and `nErrors`, plus:

| Field | Type | Meaning |
| --- | --- | --- |
| `uptime24h` | number, optional | Measured 24h uptime percentage, 0–100. Zero is included; absent means unavailable or enrichment disabled. |
| `avgLatencyMs` | number, optional | Auditor's lifetime average operation latency in milliseconds (`avg_latency_ms`), not its 24h latency. Zero is included. |
| `auditSource` | string, optional | `ucash` or `8333` — which auditor supplied the row (ucash wins when both track the mint); absent for review-only rows. |
| `testnut` | boolean | The weekly unpaid-quote probe (`internal/mintprobe`) saw this mint mark a never-paid NUT-04 quote as paid — a fake payment backend. `false` also covers mints not yet probed (a new mint is probed on the next hourly pass). A week where the mint is down or refuses the quote leaves the previous verdict standing. |
| `auditUpdatedAt` | integer, optional | Upstream mint record's update time, Unix seconds; omitted when unknown (including legacy records). It is not nagg's refresh time. |

Operator reach comes from the shared resolver described under
[AI provider directory](#ai-provider-directory) — `followers`/`follows`, plus
`followersKnown` and `followersSource`. `followersKnown` is the field that
matters: false means nagg could not establish the operator's reach and the `0`
in `followers` is a placeholder, not a count. Every row on this endpoint
reported `followers: 0` in production because the two were spelled alike and
the only source consulted (`pubkey_stats`) is empty without the `nostr` module.
`followers` stays a plain number rather than becoming nullable, because clients
already read it; `followersKnown` is additive. A mint publishing no NUT-06
nostr contact reports a KNOWN zero — there is nobody to count.

`vertexRank`/`vertexScore` come from the local Vertex DVM profile cache, which
is read in every deployment. Those three cache tables are declared by the DVM
plugin registry regardless of module set and fill from client-signed profile
reads without a server key, so they are no longer gated behind
`WithSocialEnrichment` alongside `pubkey_stats` — that gating is what left them
empty on a deployment documented as supporting them. They stay `0`/`null` until
something populates the cache.

The `profiles` map retains its existing behavior, and `identities` (see
[Identities](#identities)) covers the returned rows' operators with the same
reach and Vertex figures as the flat fields. `/nostr/mint/reviews` likewise
adds `identities` for every reviewer and, when the roster names one, the
mint's operator, keeping its `profiles` map. Review-only mints use the latest reachable stored mint-info snapshot
for name, icon, description, nuts, and units when available. This does not set
`hasAudit` or invent audit measurements; a snapshot lookup failure leaves the
review row usable.

Auditor data refreshes at boot and in the background (default hourly) as the
deduped union of ucash and 8333 (normalized URL key; one auditor failing
degrades to the other). Requests read the in-memory snapshot immediately;
no snapshot or one older than 24h means NIP-87-only discovery. The roster is
published before optional uptime/metrics enrichment completes. Per-mint
measurement failures omit those fields without discarding the roster. Standard
REST response caching still applies when Redis is configured.

Ranking keeps auditor `OK` rows first. Within each tier, its uptime component
uses `uptime24h / 100` when available, otherwise the historical operation-success
ratio. Review score/count and operator followers retain their existing weights.
Unresolved reach contributes nothing to the blend, the same as a measured zero:
the blend is additive over evidence and absent evidence adds nothing, while
`followersKnown` publishes which of the two a `0` is.
The capability manifest and headers advertise `appview.mint.discover.uptime`;
individual rows can still lack measurements.

### Mint info (caller's own mints)

`GET /nostr/mint/info?u=<mintUrl>[&u=<mintUrl>...]` returns `{mints: [...]}`,
**not an envelope**: one row per distinct requested mint (URL-normalized like
`discover`), in request order, at most 50 per request (more, or none, is a 400).
It is the wallet-side twin of `discover`: a wallet classifies the mints it
already holds, including ones outside the discovery roster, without walking the
feed. It reads only what nagg has stored and never fetches a mint on demand.

| Field | Type | Meaning |
|---|---|---|
| `mintUrl` | string | The requested URL, echoed so the caller keys on its own form. |
| `known` | boolean | `false` when nagg has never seen the mint (no auditor row, stored info, or probe verdict); every other field is then empty. |
| `name`, `iconUrl`, `description`, `supportedUnits`, `nuts` | optional | Same distilled NUT-06 fields as a `discover` row: the auditor's when it tracks the mint, else the latest stored info snapshot. |
| `testnut` | boolean | Same verdict as `discover`. It is only a verdict when `probedAt` is present; `false` without `probedAt` means "not probed yet". |
| `probedAt` | integer, optional | Newest probe verdict's time, Unix seconds; omitted until the mint has one. |
| `operatorPubkey` | string, optional | The NUT-06 nostr contact as lowercase hex, from the same source as the metadata; omitted when the mint publishes none. |

The response also carries `identities` for every `operatorPubkey` the rows
name (see [Identities](#identities)).

Callers cache these verdicts, so a failed verdict lookup fails the whole
request instead of answering `testnut: false`. Only mints on the probe
work-list (auditor rosters + NIP-87 recommendations) ever get a verdict.

## 7. Mint info snapshot history

`GET /nostr/mint/history?u=<mintUrl>` returns a Cashu mint's NUT-06 `/v1/info`
drift over time. A background poller (`internal/mintinfo`, gated by
`NAGG_RUN_MINT_INFO`) walks the auditor ∪ kind-38000 work-list ~daily, stores a
full canonical document only when it changes (the volatile `time` field is
stripped so it isn't a phantom change), and records every poll.

The response is **not an envelope**. It leads with the initial full document,
then one `revisions[]` entry per change (newest first) as an
[RFC 6902](https://datatracker.ietf.org/doc/html/rfc6902) JSON Patch against the
previous document, plus a server-rendered human `summary` (e.g. `"version:
Nutshell/0.15.0 → Nutshell/0.16.0"`, `"NUT-17 enabled"`). "Checked, unchanged"
is conveyed by top-level `lastCheckedAt` / `checkCount` / `unchangedSince`, not
by empty rows; append `&observations=true` for the full per-poll `observations[]`
log. 404 when the mint has never been observed. The same data is served by the
GraphQL `mintInfoHistory(input: {mintUrl, includeObservations})` field.

```jsonc
{
  "mintUrl": "https://mint.host", "normalizedUrl": "https://mint.host",
  "currentHash": "9f2c…", "firstSeenAt": 1719792000,
  "lastCheckedAt": 1751760000, "checkCount": 142, "unchangedSince": 1751328000,
  "initial":   { "at": 1719792000, "hash": "3b7d…", "document": { /* full NUT-06 */ } },
  "revisions": [ {
    "at": 1751328000, "previousLastSeenAt": 1751241600, "hash": "9f2c…",
    "summary": ["version: Nutshell/0.15.0 → Nutshell/0.16.0", "NUT-17 enabled"],
    "patch": [ { "op": "test", "path": "/version", "value": "Nutshell/0.15.0" },
               { "op": "replace", "path": "/version", "value": "Nutshell/0.16.0" },
               { "op": "add", "path": "/nuts/17", "value": { "supported": true } } ]
  } ],
  "observations": null
}
```

`GET /nostr/mint/changes?limit=100` returns the ecosystem-wide changelog:
`{trackedMints, reachableMints, totalChanges, changes}`. `limit` accepts 1–500;
omitted, invalid, or out-of-range values use 100. `changes` is newest first;
each entry has `mintUrl`, `name`, `at`, `previousLastSeenAt`, `hash`, `summary`
and an RFC 6902 `patch`. `totalChanges` counts collected revisions before the
limit is applied (from up to the 500 most recently changed mints), not an
unbounded archive total. An empty feed has `changes: []`. Returns 503 when the
mint-history provider is not configured.

## 8. App configuration

These routes require `NAGG_MODULES` to include `app`. They return standalone
objects, not the Nostr envelope, and do not query ClickHouse.

`GET|POST /app/latest-version` returns:

```json
{"version":"0.1.3","message":"Update available","minVersion":"0.1.2"}
```

`version` comes from `NAGG_APP_LATEST_VERSION` (empty by default).
`message` and `minVersion` come from `NAGG_APP_UPDATE_MESSAGE` and
`NAGG_APP_MIN_VERSION`; each is omitted when empty. `minVersion` is the
minimum supported client version for clients implementing a blocking update
gate; capability `app.latestVersion.minVersion` advertises support for this
field. No parameters are required; a POST body (including the legacy
`{"storage":{"version":"…"}}`) is accepted and ignored. Both methods send
`Cache-Control: public, max-age=60`.

`GET /app/ai-lineup` takes no parameters and returns
`{version, updatedAt, node: {baseUrl, fallbackUsed, authMode?}, providers, pinsMissing}`. `version` is the lineup
schema version (currently 1); `updatedAt` is its build time in Unix seconds.
Each provider contains `id`, `vendor`, and `models`. Each model contains
`tier` (`auto`, `pro`, or `max`), `id`, `name`, `created`, `contextLength`,
`inputModalities`, optional `maxCompletionTokens`, optional `upstreamId`, and
`pricing` with `prompt`, `completion`, `request`, `maxCost`, `maxPromptCost`,
and `maxCompletionCost` in sats. Providers or tiers without eligible models are
omitted.

`upstreamId` is the node's own id for the account serving that model
(`openrouter`, `tinfoil`, `generic`, …); it is omitted when the node does not
report `upstream_provider_id`. One node commonly fronts several upstreams and
they fail independently, so models sharing an `upstreamId` are one failure
domain: when a node's credit with one upstream runs out, every model behind it
answers 402 while the node's catalog, wallet and other upstreams stay healthy.
A client retrying a sibling from the same `upstreamId` is paying to be refused
again. Vendor bucketing uses `canonical_slug` first and falls back to the
upstream id only for rows carrying neither a slug nor a qualified id — without
that fallback each such row forms its own single-model vendor and can never be
curated into an auto/pro/max ladder. Returns 503 when Routstr is disabled/unconfigured, or 502 only when no
catalog has ever been fetched successfully and all configured nodes fail.

`node.baseUrl` is the active node that supplied this catalog, never a separate
lookup that can drift from its models. `node.fallbackUsed` is always a boolean.
The client tries `NAGG_ROUTSTR_URL` first on every refresh, then
`NAGG_ROUTSTR_FALLBACK_URLS` in order, adopting the first valid non-empty enabled
catalog and logging `routstr.node.switched` once per change. The default fallback
order is `https://ai.redsh1ft.com`, `https://api.nonkycai.com`,
`https://routstr.otrta.me`, `https://llm.satsandsports.cash`,
`https://routstr.satoshisend.xyz`. The catalog is fresh for 15 minutes. Failed
refreshes retain the last good catalog and its node indefinitely (including
beyond the former 24-hour stale limit); subsequent requests retry. Primary
recovery switches back on the next successful refresh. Boot warm-up also uses
this failover sequence. Successful catalog discovery does not verify paid chat.

### AI provider directory

`GET /app/ai-providers` takes no parameters and returns
`{providers, checkedAt, ttlSeconds, identities}`; `identities` covers every
row's `pubkey` (see [Identities](#identities)) and the rows are unchanged. It is the SIBLING of `/app/ai-lineup`,
not a replacement: that route curates the *models* of one chosen node, this one
lists the *providers* to choose between. The app used to discover them itself
on every cold start — a relay round-trip plus a per-node probe fan-out before it
could draw a picker — which is work a server does once for every client.

Each provider carries `baseUrl`, `name`, `followers`, `modelCount`,
`encryptedModelCount`, `teeModelCount`, `mints`, `status`, and optionally
`pubkey`, `followersSource`, `checkedAt` and `latencyMs`. `baseUrl` is normalized (https only, no trailing slash, no
trailing `/v1`) so two spellings of one node cannot render as two rows.
`mints` is always a list — empty means the provider publishes none, which the
payment path reads as "any mint".

`status` is `online`, `offline` or `unknown`. `unknown` means nagg has not
established this provider's state: it was discovered after the last sweep, or
its last probe is older than `NAGG_AI_PROVIDERS_MAX_AGE`. It is deliberately
distinguishable from `offline` — an unprobed provider is worth showing above
one nagg has watched fail, and the aging rule is what takes the directory to
`unknown` if the sweep worker dies rather than leaving a stale `online`
standing. A provider that fails its probe is marked `offline` and KEPT, with
the model counts it last served: dropping the row would read to the app as
"this provider does not exist".

`checkedAt` on a provider is when THAT provider's status was last established,
omitted while the status is `unknown`; the top-level `checkedAt` is when the
sweep ran. `latencyMs` is the last successful probe's round trip, omitted when
there has never been one. `ttlSeconds` is `NAGG_AI_PROVIDERS_INTERVAL` in
seconds — a shorter client TTL only re-fetches the same answer.

`encryptedModelCount` is how many of the provider's models a CLIENT will seal
to an enclave, end to end, with the node unable to read the prompt. It is a
COUNT, never a boolean, because "this provider is E2EE" is not a true property:
a live node badged E2EE serves 564 priced models of which 9 are client-sealable,
and most of those have an identically named plaintext twin in the same catalog
(`tinfoil-glm-5-3` alongside `glm-5-3`). Encryption is per-model routing.

The count is the `tinfoil-` id prefix, matched byte for byte, because that
prefix is the whole of what a Routstr client checks before switching on sealed
transport — `@routstr/sdk`: `isTinfoilModel(modelId) = modelId.startsWith("tinfoil-")`,
with `getTinfoilUpstreamModelId` stripping exactly that prefix, so the prefixed
entry is the sealed route to the model its unprefixed twin serves in the clear.
A differently cased prefix does not count: the SDK's check is case-sensitive,
so such a model is sent in the clear.

`teeModelCount` is the SEPARATE and weaker claim: how many models the node
itself declares it forwards to a Tinfoil enclave (`upstream_provider_id`). It
is always `>= encryptedModelCount`, and the models in the gap are sent to the
node in the clear — the node decrypts, reads and forwards them, so only the
node's own hop to the enclave is protected. On the live redsh1ft catalog that
gap is 4 models, and three of the four are NAMED "Private (E2EE) …". The name
is the node's marketing; the prefix is what the client does.

The two must never be merged. Counting the upstream id into
`encryptedModelCount` would have this route claim 13 while the app's model
picker badges 9, and the four in the gap are exactly the ones where the
end-to-end promise is false. Only `encryptedModelCount` may be surfaced as
"end-to-end encrypted"; `teeModelCount` is published because where inference
runs is real information, and named so it cannot be mistaken for the stronger
claim.

Providers are sorted server-side, best first, so every client renders the same
picker: `online` before `unknown` before `offline`; then providers with
`encryptedModelCount > 0`; then operator reach — a known positive count, then
an unresolved one, then a known zero — and finally `baseUrl` ascending, a total
order so equal rows never swap places between two requests. Unresolved sits
above a measured zero on purpose: "nagg did not manage to ask" is not evidence
of nobody, and demoting it would punish a provider for nagg's own gap. The same
ladder (`socialgraph.CompareBest`) is what ranks operators everywhere.
The boost keys on `encryptedModelCount`, not `teeModelCount`: a declared
enclave upstream with nothing the client will seal earns no ranking credit.

Discovery and the sweep run in nagg, not in the app. Providers announce
themselves on Nostr as kind-38421 addressable events (both shapes are read: `u`
tags carrying endpoints, and JSON content holding a directory), and nagg merges
that registry with the `GET /v1/providers/` directories of the nodes it already
knows — the seeds first, then providers already known online, capped by
`NAGG_AI_PROVIDERS_DIRECTORY_SOURCES`. Later sources fill gaps rather than
overwrite, except a node's own `/v1/info`, which overrides third-party claims
about its name, operator key and accepted mints.

Each sweep probes every known provider with bounded concurrency
(`NAGG_AI_PROVIDERS_CONCURRENCY`). The cheap request is `/v1/info` — a few
hundred bytes that also carry the node's own name, npub and mints — and the
expensive one is `/v1/models`, which is the only source of the model counts
(three quarters of a megabyte on the largest live node). So `/v1/info` carries
status on most sweeps and the catalog is re-read at most once per
`NAGG_AI_PROVIDERS_CATALOG_MIN_AGE`. Older nodes answer `/v1/info` with 404;
that is a node saying nothing rather than a node being down, so it is asked
once and the catalog read becomes its status probe.

`followers` is the operator's Nostr reach, or **null** when nagg could not
establish it. Null and 0 are different facts — a provider whose operator nobody
follows is not a provider nagg failed to look up — and the sort keys on the
difference. A provider publishing no operator pubkey reports 0, not null: there
is nobody to count, which is itself an established fact. `followersSource` names
which source answered and is omitted when `followers` is null.

Reach is resolved by one shared path, [`internal/socialgraph`](../internal/socialgraph),
which `/nostr/mint/discover` reads too, so the two endpoints cannot answer the
same question differently. They used to: both derived it from `pubkey_stats`
alone, which a mint deployment never populates, and both published the
emptiness as the number 0 on every row. Sources are tried in order, and the
first that answers wins:

| `followersSource` | source | exact? | availability |
| --- | --- | --- | --- |
| `graph` | `pubkey_stats`, nagg's own kind-3 rollup | yes | needs the `nostr` module's firehose |
| `vertex` | the local Vertex DVM profile cache | yes | tables exist everywhere; fill from client-signed reads or a server-key syncer |
| `relays` | a live kind-3 scan of the relays nagg already dials | **no — a lower bound** | always |

A `relays` count is the number of distinct authors whose contact list names the
target, counted over `NAGG_SOCIAL_REACH_RELAYS`. Relays cap results, the relay
set is partial, and a slow relay is dropped, so it is a floor: render it as
"174+", never as an exact count. It needs no credentials and no Vertex credits.

Resolution is asynchronous by necessity — a relay scan takes seconds per pubkey,
which no request may pay. A read answers from cache and queues what it does not
know, so a newly discovered operator reports `null` until a later pass resolves
it. A failed lookup never overwrites a count already established.

Returns 503 when `NAGG_AI_PROVIDERS_ENABLED=false`, and also while the first
sweep is still warming — never an empty `providers` list, which the app would
read as "there are no providers". In both cases the app falls back to
discovering providers client-side.

`NAGG_ROUTSTR_AUTH_MODE=bearer` advertises Bearer authentication;
`NAGG_ROUTSTR_AUTH_MODE=x-cashu` advertises per-request `X-Cashu`. Empty omits
`node.authMode`; invalid values log `config.routstr_auth_mode.invalid` and omit
it. This is an operator declaration shared by all configured nodes, not
protocol autodetection or an auth header sent by nagg. Configure nodes that
support the advertised mode.

`pinsMissing` is always an array (empty `[]` when all pins resolve), containing
sorted `vendor/tier:id` strings for configured pins absent from the enabled,
priced, non-alias catalog. Missing pins leave the derived tier in place.
`ai_lineup.pin_missing` is a Warn event containing all missing pins, emitted at
most once per successful catalog refresh, deduplicated by the catalog's internal
`UpdatedAt` timestamp. Repeated reads or stale serves do not repeat the warning.
Capability `app.aiLineup.pinsMissing` advertises this field.

GET responses under `/app/*` (and `/v1/app/*`) normally use 60 seconds fresh /
24 hours stale in the server response cache. BTC Map uses 1h fresh / 24h stale
instead; wallpapers and rates bypass this cache and enforce their own snapshot
expiry. During the stale window, the previous payload
can be served while background revalidation runs. Successful cached app
responses send `Cache-Control: public, max-age=60`, or `max-age=3600` for BTC Map.
POST bypasses the server response cache.


### AI lineup operator checks

After deploy, this must show a live node (confirm its catalog with the second
command below):

```sh
curl -fsS https://nagg.up.railway.app/app/ai-lineup | jq '{node, pinsMissing}'
```

Run the following with the deployment's current `NAGG_AI_LINEUP_PINS` JSON in
your shell (unset means no pins). It fetches the advertised active node, compares
its enabled IDs with both lineup IDs and configured pin IDs, and prints a
machine-readable diff. This reads public HTTP catalogs only.

```sh
pins_json=${NAGG_AI_LINEUP_PINS:-'{}'}
lineup_json=$(curl -fsS https://nagg.up.railway.app/app/ai-lineup) &&
node_url=$(printf '%s' "$lineup_json" | jq -er '.node.baseUrl') &&
curl -fsS "${node_url%/}/v1/models" |
jq --argjson lineup "$lineup_json" --argjson pins "$pins_json" '
  [.data[] | select(.enabled != false) | .id] | unique as $enabled
  | [$lineup.providers[].models[].id] | unique as $selected
  | [$pins[][]] | unique as $pinned
  | {node: $lineup.node, pinsMissing: $lineup.pinsMissing,
     lineupNotEnabled: ($selected - $enabled),
     pinsNotEnabled: ($pinned - $enabled),
     enabledNotInLineup: ($enabled - $selected)}'
```

Start with pins unset and inspect the active node's `/v1/models` entries. Choose
exact enabled `id` values with `sats_pricing`, text output, and suitable context,
completion limits and cost; use `canonical_slug`'s vendor prefix (or the ID
prefix when absent) as the vendor key. Do not pin rolling `~vendor` aliases.
Pin only tiers that need a deliberate override, using the JSON shape
`{vendor: {tier: exactCatalogID}}` with real strings copied from the catalog;
`auto`, `pro`, and `max` are the only tier keys. Pins bypass automatic age/cost
selection. Check availability on your configured fallback catalogs too: their
inventories can differ. Set the resulting JSON as `NAGG_AI_LINEUP_PINS`, deploy,
and re-run the diff after response-cache revalidation. Expect
`lineupNotEnabled: []`, `pinsNotEnabled: []`, and `pinsMissing: []`.
`enabledNotInLineup` is normally non-empty because the lineup is curated.
Remove or replace a missing pin using current catalog IDs; never guess an ID.
The HTTP response cache may briefly show the previous node/lineup while
revalidating, so repeat a mismatched check after refresh.
### BTC fiat rates

`GET /app/rates` (also `/v1/app/rates`, capability `app.rates`) returns:

```json
{
  "version": 1,
  "base": "BTC",
  "updatedAt": 1800000000,
  "degraded": true,
  "rates": {
    "GBP": {"price": 57274, "at": 1800000000, "samples": 1, "sources": ["mempool"], "confidence": "single-source"}
  },
  "sources": [
    {"id": "mempool", "kind": "http-json", "currency": "GBP", "ok": true, "lastSuccessAt": 1800000000, "lastErrorAt": null, "consecutiveFailures": 0, "lastError": ""}
  ]
}
```

The normal registry covers USD, EUR, GBP and CHF; the abbreviated example
shows GBP alone. Prices are fiat units per BTC. `at` is the newest surviving
observation timestamp; `updatedAt` is the newest accepted rate timestamp.
Both are Unix seconds and are never advanced just because a failed refresh
ran. Missing/expired currencies are omitted. No available currency returns
503 `{"error":"rates warming"}`, including before the first successful pass
and when the worker is disabled. Successful responses send
`Cache-Control: public, max-age=60`.

The worker admits observations at most `NAGG_RATES_MAX_AGE` old (6h), taking
the latest usable signed kind-1 note per bot and one HTTP sample per
source/currency. Repeated notes from one publisher never add independent votes.
It checks the note's reciprocal sats price within 2%, drops
outliers beyond `max(3 × 1.4826 × MAD, 0.5% × median)`, and takes the median
of survivors. If only two sources survive and disagree by more than 20% of the
lower price, the refresh is rejected: neither source has independent support.
A movement over 20% from the previous accepted value is rejected
until that value is older than 24h, when re-anchoring is allowed.

`samples` counts survivors; `sources` contains sorted unique provider IDs.
Confidence is `high` for at least three providers, `medium` for two, and
`single-source` for one.
`degraded` is true if any currency has only one provider, uses a retained/stale
value, is unavailable, or any source has failed at least three passes in a row.
GBP is HTTP-only by default, so overall degradation is expected today.

Source health is keyed by `(id, currency)`; the four mempool entries share one
HTTP request per pass. `ok` means the last pass supplied fresh usable data,
before consensus filtering. A successful fetch clears the failure counter
and `lastError` but retains the historical `lastErrorAt`; timestamps not yet
recorded are null. Errors use fixed sanitized categories without upstream
URLs, payloads, or raw exception messages. Failed or implausible refreshes
retain the last good price up to `NAGG_RATES_STALE_FOR` (24h total observation
age). The server response cache is bypassed so it cannot extend that retention.
Clients retaining responses locally must inspect each rate's `at`.

Sources are literals in `internal/rates/source.go`; adding a supported bot is
one literal. `NAGG_RATES_EXTRA_SOURCES` appends declarations of the same shape:

```json
[{"id":"gbpbot","currency":"GBP","kind":"nostr-note","pubkey":"<hex or npub>","priority":0}]
```

HTTP declarations use `kind: "http-json"`, `url`, and `jsonKey` instead of
`pubkey`. JSON prices must be positive finite numbers. The optional `time`
field is Unix seconds; absent `time` uses fetch time. Future or expired
timestamps are rejected. Lower `priority` fetches first; it does not weight
consensus. IDs use 1–64 letters, digits, underscores or hyphens; currencies
are three uppercase letters. Duplicate `(id, currency)` entries are ignored
(first wins). Invalid JSON or invalid entries emit startup warnings without
their contents. Disabling `NAGG_RATES_HTTP_ENABLED` excludes all HTTP sources,
including extras. None of this feature reads or writes ClickHouse.


### Wallpapers and BTC Map

These `app` module routes also mount under `/v1/app/*`. They work on a
`NAGG_MODULES=mint,app` deployment without social event routes or new tables.
Add `vertex` (`NAGG_MODULES=mint,app,vertex`) to also enable client-signed
reputation lookups; see [Vertex client relay](vertex-client-relay.md).

`GET /app/wallpapers` (capability `app.wallpapers`) returns:

```json
{
  "wallpapers": [{
    "eventId": "<event-id>", "themeName": "sunset", "displayName": "Sunset",
    "blossomUrl": "https://example.com/sunset.jpg",
    "thumbUrl": "https://example.com/sunset-thumb.jpg",
    "sha256": "<sha256>", "fileSize": 12345, "dimensions": "1080x1920",
    "albumSlug": "nature", "palette": {}, "dominantColors": [],
    "gradientColors": [], "createdAt": 1789257600
  }],
  "albums": [{"slug": "nature", "displayName": "Nature", "description": "",
    "sortOrder": 0, "topic": "Other", "coverThemeName": "sunset"}],
  "lastUpdated": 1789257600000
}
```

`createdAt` is event time in Unix seconds; `lastUpdated` is successful refresh
time in Unix milliseconds. The default publisher is the app's Sovran support
public key, configurable as hex or npub. The worker queries the latest
kind-30078 `d=wallpaper-catalog` and up to 500 kind-1063 `t=wallpaper` events
per relay. Signatures are checked by `relayquery`; author, kind, tags and future
timestamps are checked again by the catalog builder. Files deduplicate by
`theme_name`, newest first (lowest event ID breaks timestamp ties).

File tags map as follows: `theme_name` → `themeName`, `title` → `displayName`
(fallback theme name), `url` → `blossomUrl`, `thumb` → `thumbUrl` (fallback full
URL), `x` → `sha256`, `size` → `fileSize`, `dim` → `dimensions`, and `l` with
namespace `money.sovran.wallpaper` → `albumSlug` (fallback `uncategorized`).
`palette`, `dominant_colors`, and `gradient_colors` contain JSON color metadata;
invalid colors are omitted. Unknown tags and undeclared nested fields are
never exposed. Invalid required file fields are skipped. Albums come from the
latest catalog event's `content.albums`; if none are supplied, they are derived
from wallpaper slugs. Optional `topic` defaults to `Other`. A malformed catalog
or an empty wallpaper pass preserves the previous snapshot without extending
its age.

The worker runs immediately and every `NAGG_WALLPAPERS_INTERVAL` (default 1h).
Cold, disabled, or 24h-expired catalogs return JSON 503. Successful responses
send `Cache-Control: public, max-age=300`. The snapshot bypasses Redis/response
caching so repeated reads cannot extend its stale deadline.

`GET /app/btcmap/places` and `GET /app/btcmap/places/{id}` (capability
`app.btcmap`) proxy the [BTC Map v4 places API](https://github.com/teambtcmap/btcmap-api/blob/master/docs/rest/v4/places.md).
IDs accept digits or `node:`, `way:`, `relation:` followed by digits. Query
parameters `fields`, `updated_since`, `include_deleted`, and `limit` pass
through; other parameters are ignored (`refresh` remains a nagg cache hint).
The app currently supplies no query parameters. The list therefore defaults
to `fields=id,lat,lon,icon,comments,boosted_until,deleted_at,updated_at` and
`include_deleted=false`. Detail defaults add contact, address, payment, OSM,
and verification fields consumed by the app. An explicit `fields` overrides
the defaults. Valid JSON arrays/objects pass through without an envelope,
including colon-keyed `osm:*` fields.

The HTTP client uses an 8s timeout, a 4 MiB **decoded response** limit, and no
redirects. Upstream 400/404/429 remain 400/404/429; timeouts become 504; other
upstream, malformed JSON, wrong envelope, and size-limit failures become 502.
Disabled routes return 503. Errors expose fixed categories, never upstream
bodies. The response cache uses 1h fresh plus 24h stale, only stores successful
responses, and sends `Cache-Control: public, max-age=3600` on misses and hits.
Both endpoints allow GET only (405 with `Allow: GET` otherwise).

**Current full-list limitation:** a public upstream check during implementation
returned 5,261,223 decoded bytes for the default list, exceeding the requested
4 MiB cap. Such requests return 502. Bounded `limit`/`updated_since` requests
work; moving the existing unpaginated app call requires increasing the cap or
coordinating client pagination. No list is silently truncated.

Operator configuration: no new variables are required when `app` is enabled.
`NAGG_WALLPAPERS_ENABLED=true` and `NAGG_BTCMAP_ENABLED=true` explicitly enable
the defaults. Keep `NAGG_WALLPAPERS_RELAYS` unset to inherit `NAGG_RELAYS`, or
set it to relays carrying the admin events. Do not change stored/firehose kinds.

After the orchestrator deploys:

```sh
curl -s https://nagg.up.railway.app/app/wallpapers | jq '.albums|length'
curl -s 'https://nagg.up.railway.app/app/btcmap/places?limit=100' | jq 'length'
curl -s 'https://nagg.up.railway.app/app/btcmap/places/23143' | jq '{id,lat,lon,icon}'
```
