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

Every route is mounted at both `/nostr/*` and `/v1/nostr/*` (identical handler,
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

| Method | Path | Heavy | Response |
| --- | --- | --- | --- |
| GET | `/nostr/capabilities` | no | service info; `appViewVersion: "v2"` |
| GET,POST | `/nostr/feed` | yes | envelope |
| GET | `/nostr/feed/user` | yes | envelope |
| POST | `/nostr/feed/ranked` | yes | envelope (`orderBy: "rank"`) |
| GET,POST | `/nostr/notifications` | yes | envelope + `entries` + `hasNext` (§4) |
| GET | `/nostr/notifications/seen` | no | envelope holding the viewer's kind-30078 read-marker event; client parses `seenUntil` from its content |
| POST | `/nostr/events/aggregates` | no | envelope, aggregates only (`order`/`events` empty). Body `{"ids": ["<id>", …]}`, ≤ 100. **Replaces `/nostr/notes/stats`.** |
| GET | `/nostr/thread` | yes | envelope + `total` (§4); `order[0]` is the root id on every page, the rest is the server-ranked reply order |
| GET | `/nostr/follows` | no | envelope; pubkey-keyed aggregates |
| GET | `/nostr/events` | no | envelope; `order` = requested ids that resolved |
| POST | `/nostr/events/query` | yes | envelope (bare when the queried kinds include 1059 — §5) |
| GET,POST | `/nostr/dm/envelopes` | yes | bare envelope (§5) |
| GET,POST | `/nostr/dm/conversation` | yes | bare envelope (§5) |
| GET | `/nostr/follow-status` | no | envelope + `edges` (§4) |
| GET | `/nostr/mint/reviews` | yes | **not an envelope** (mint objects, not events) |
| GET | `/nostr/mint/discover` | yes | **not an envelope** |
| GET | `/nostr/mint/history` | yes | **not an envelope**: NUT-06 info snapshot history (§7) |
| GET | `/nostr/social-graph` | yes | envelope: the viewer's latest kind-3 / 10002 / 10000 events; derive follows, relays, mutes from their tags |
| GET | `/nostr/own/profiles` | no | envelope: kind-0 events + pubkey-keyed aggregates |
| GET | `/nostr/own/{type}` | yes | envelope of the viewer's own action history |
| GET | `/nostr/profiles` | no | envelope: kind-0 events for the requested pubkeys |
| GET | `/nostr/profile` | no | envelope + `pubkeys`/`providers`/`fromCache` (§4) |
| GET | `/nostr/search` | no | envelope + `pubkeys`/`providers`/`fromCache` (§4) |
| GET | `/nostr/recommended` | no | envelope + `pubkeys`/`providers` (§4) |
| GET | `/app/latest-version` | no | static app-version payload |

Request parameters are unchanged from v1 (feed `spec`/`limit`/`until`/`offset`,
thread `id`/`sort`/`viewer`/…, notifications `viewer`/`tab`/`policy`/…).
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
      "vertex": { "rank": 1, "score": 87.2, "nodes": 210433, "references": ["<pubkey>", "…"] },
      "nip05":  { "valid": true },
      "nagg":   { "firstEventAt": 1710000000 }
    }
  },
  "fromCache": false
}
```

Provider payloads are float/context-shaped data from named providers (the DVM
plugin seam, `internal/dvm`); counts stay in `aggregates`.

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
