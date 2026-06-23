# nagg app-view API

The app-view is nagg's REST surface: precomputed, aggregated Nostr reads served
over HTTP/JSON. It is the **only** transport `sovran-app` uses (via the
`@sovranbitcoin/nagg-ts` client) — there is no client-side GraphQL. nagg's
GraphQL endpoint still exists server-side (the REST handlers reuse its
resolver/store internals) but is not consumed by clients.

Every route is mounted at both `/nostr/*` and `/v1/nostr/*` (identical handler,
cache, and middleware). Heavy routes run multi-query ClickHouse aggregations and
pass through a concurrency limiter; light routes do not. All responses are JSON.

## 1. Endpoint inventory + sovran-app usage

| Method | Path | Heavy | Used by sovran-app | nagg-ts binding / call site |
|---|---|---|---|---|
| GET | `/nostr/capabilities` | no | yes | service-info / capability probe |
| GET,POST | `/nostr/feed` | yes | yes | `followsFeedAppView` — following-recent, following-replies, posts-by-pubkeys (recent) |
| GET | `/nostr/feed/user` | yes | yes | `userFeedAppView` — profile feed (`getUserFeed`) |
| POST | `/nostr/feed/ranked` | yes | yes | `rankedFeedAppView` — For-You, following-popular, posts-by-pubkeys (popular) |
| GET,POST | `/nostr/notifications` | yes | yes | `notificationsAppView` — `getNotifications` |
| GET | `/nostr/notifications/seen` | no | yes | notifications read-marker |
| POST | `/nostr/notes/stats` | no | yes | `noteStatsAppView` — engagement counts by id |
| GET | `/nostr/thread` | yes | yes | `threadAppView` — `getThread` (sorts: `new` / `ranked` / `relevant`) |
| GET | `/nostr/follows` | no | yes | follow counts |
| GET | `/nostr/follow-status` | no | yes | `followStatusAppView` — batch viewer↔candidate relationship |
| GET | `/nostr/events` | no | yes | `eventsAppView` — enrich/quoted events by id |
| POST | `/nostr/events/query` | yes | yes | `eventsQueryAppView` — Whitenoise group msgs/invites, wallpaper catalog |
| GET,POST | `/nostr/dm/envelopes` | yes | yes | `dmEnvelopesAppView` — DM/contacts inbox |
| GET,POST | `/nostr/dm/conversation` | yes | yes | `dmConversationAppView` — scoped DM conversation |
| GET | `/nostr/own/profiles` | no | yes | `ownProfilesAppView` — own accounts + follow counts |
| GET | `/nostr/own/{type}` | yes | yes | own action history (authored/replies/likes/reposts/bookmarks/follows/mutes/relays) |
| GET | `/nostr/profiles` | no | yes | `profilesAppView` — batch profile info by pubkey |
| GET | `/nostr/profile` | no | yes | single profile summary (score + counts + top followers) |
| GET | `/nostr/social-graph` | yes | yes | contacts + relays + mutes bundle |
| GET | `/nostr/search` | no | yes | `profileSearchAppView` — profile search (Vertex DVM) |
| GET | `/nostr/recommended` | no | yes | recommended profiles |
| GET | `/nostr/mint/reviews` | yes | yes | NIP-87 cashu mint reviews |
| GET | `/nostr/mint/discover` | yes | yes | cashu mint discovery |

> Two reads are intentionally **not** served by nagg and fall through to other
> tiers (Primal / relays): batch kind-0 `Profiles` enrichment beyond what
> `/nostr/profiles` covers, and `ProfileStats`. The reputation score
> (`/nostr/profile`-adjacent score API) and NIP-11 relay metadata are separate
> services, not app-view endpoints.

## 2. Shared concepts

**Enrichment side-maps.** Feed/thread/notification responses carry the events
plus three keyed side-maps so the client never issues follow-up fetches:
`metrics` (event id → `NoteStats`), `profiles` (pubkey → `ProfileInfo`),
`quoted` (event id → full quoted `FeedEvent`).

**Ordering manifest.** `ordering` is the server-authoritative render order. The
client renders strictly by `ordering.elements`; `ordering.orderBy` carries the
semantic the ids alone don't:

- `rank` — algorithmic order; the client must **not** prepend live items above
  the fold.
- `created_at` — chronological; live items may prepend.

**Pagination.** Feeds use `paginationUntil` (oldest `created_at`) +
`paginationOffset`. List shapes (DM, notifications) use a `pageInfo` connection
cursor (`hasNextPage`, `endCursor` = `<RFC3339Nano>|<id>` of the oldest row).

## 3. Response structures

### FeedEvent

```jsonc
{
  "id": "<64-hex>", "kind": 1, "pubkey": "<64-hex>",
  "content": "…", "tags": [["e","…"],["p","…"]], "created_at": 1710000000
}
```

Raw-event connections (`/nostr/events/query`, `/nostr/dm/*`) instead use the
ClickHouse `EventView` JSON: `{id, pubkey, kind, createdAt (RFC3339), content,
tags, sig, updatedAt}` (the client schema accepts either `created_at` epoch or
`createdAt` string).

### NoteStats (metrics map value)

```jsonc
{
  "likeCount": 0, "repostCount": 0, "replyCount": 0, "satsZapped": 0,
  "realLikeCount": 0, "realRepostCount": 0, "realReplyCount": 0, "realSatsZapped": 0
}
```

`real*` counts engagement only from Vertex-scored distinct engagers (spam-resistant).

### ProfileInfo (profiles map value)

```jsonc
{ "name": "…", "picture": "…" }
```

### OrderingManifest

```jsonc
{ "orderBy": "rank" | "created_at", "elements": ["<id>", "…"] }
```

### FeedResponse — `/nostr/feed`, `/nostr/feed/user`, `/nostr/feed/ranked`

```jsonc
{
  "items": [
    { "type": "note",   "event": FeedEvent, "rootEvent": FeedEvent?, "rootEventId": "…"? },
    { "type": "repost", "repostEvent": FeedEvent, "originalEvent": FeedEvent?, "originalEventId": "…" }
  ],
  "ordering": OrderingManifest,
  "metrics":  { "<id>": NoteStats },
  "profiles": { "<pubkey>": ProfileInfo },
  "quoted":   { "<id>": FeedEvent },
  "paginationUntil": 1710000000,
  "paginationOffset": 30
}
```

### ThreadResponse — `/nostr/thread`

A flat list of descendant events under `root`; the client builds structure from
`events` and renders the reply order from `ordering`.

```jsonc
{
  "root": FeedEvent,
  "events": [FeedEvent],          // all descendants
  "ordering": OrderingManifest,    // reply render order
  "metrics":  { "<id>": NoteStats },
  "profiles": { "<pubkey>": ProfileInfo },
  "quoted":   { "<id>": FeedEvent }
}
```

Query params: `id` (required), `limit` (fetch cap, default 1000), `sort`
(`new` default | `ranked` | `relevant`), `viewer` (required for `relevant`),
`offset`, `replyLimit` (page size; 0 = all), `candidateLimit`, `rankedLimit`.

- `new` — descendant order as stored (backward-compatible default).
- `ranked` — direct replies by engagement (`likes`).
- `relevant` — viewer-specific merge, computed server-side from precomputed
  store primitives: **author self-reply chain → one followed-tail reply →
  ranked direct replies → remaining replies**, deduped, source excluded.

### EnrichmentResponse — `/nostr/events`, `/nostr/profiles`

```jsonc
{ "metrics": { "<id>": NoteStats }, "profiles": { "<pubkey>": ProfileInfo }, "quoted": { "<id>": FeedEvent } }
```

### Event connection — `/nostr/events/query`, `/nostr/dm/envelopes`, `/nostr/dm/conversation`

Wrapped under a transport-specific key (`events` / `dmEnvelopes` /
`dmConversation`) so the body byte-matches the canonical client shape:

```jsonc
{ "events": { "nodes": [EventView], "pageInfo": { "hasNextPage": false, "endCursor": "…|…" } } }
```

`POST /nostr/events/query` body (constrained filter; at least one of
`ids`/`authors`/`kinds`/`tags` required, `limit` ≤ 500):

```jsonc
{ "kinds": [1063], "authors": ["<hex>"], "tags": [{"key":"e","values":["<id>"]}],
  "since": 0, "until": 0, "limit": 100 }
```

`/nostr/dm/conversation` adds an optional `counterparty`: NIP-04 (kind 4) is
scoped to the pair; gift wraps (kind 1059) return the full viewer inbox (nagg
can't see inside them). nagg never decrypts — the client does.

### FollowStatusResponse — `/nostr/follow-status`

Params: `viewer`, `candidates` (csv, ≤ 500).

```jsonc
{ "followStatus": [
  { "pubkey": "<hex>", "following": true, "followsYou": false,
    "mutual": false, "relationship": "following" }   // following | follows_you | mutual | none
] }
```

### OwnProfilesResponse — `/nostr/own/profiles`

Params: `pubkeys` (csv, ≤ 10). Metadata + follow counts for the viewer's own
accounts.

```jsonc
{ "ownProfiles": [
  { "pubkey": "<hex>", "name": "…", "displayName": "…", "picture": "…",
    "about": "…", "nip05": "…", "lud16": "…", "banner": "…", "website": "…",
    "followers": 0, "follows": 0, "createdAt": 1710000000 }
] }
```

### On-demand relay backfill

Most read endpoints trigger a bounded, non-blocking relay backfill on a cache
miss / incomplete result (feed, feed/user, thread, notes/stats, events, follows,
dm, profiles, profile, social-graph, search), so a cold author/thread is
populated from relays and served. Backfill failures are logged and the response
proceeds with whatever is available. `feed/ranked` (precomputed) and the
mint/events-query paths do not backfill.
