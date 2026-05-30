# nagg GraphQL — schema reference

Authoritative vocabulary for the nagg GraphQL API. Source: `internal/graphqlapi/schema.go`
(types/resolvers) and `internal/clickhouse/read.go` (SQL semantics).

## Transport

`POST $NAGG_GRAPHQL_ENDPOINT` (default `http://127.0.0.1:8080/graphql`), body
`{"query": "...", "variables": {...}}`. **POST only** (GET returns 405). 10s
server timeout per request.

## Query roots

| Root | Args | Returns |
|---|---|---|
| `event` | `id: String!` | `Event` |
| `profile` | `pubkey: String!` | `Profile` |
| `thread` | `rootEventId: String!` | `Thread` |
| `aggregateEvents` | `input: EventAggregationInput!` | `AggregationResult!` |

`id` / `pubkey` / `rootEventId` MUST be lowercase 64-char hex (regex-validated;
bech32 `npub`/`note`/`nevent` are rejected — decode with `scripts/to-hex.py`).
A missing event/profile still returns an object with empty fields, not an error.

## Event

| Field | Type | Meaning |
|---|---|---|
| `id` | String! | event id (hex) |
| `pubkey` | String | author pubkey (null if event unknown) |
| `kind` | Int | nostr kind |
| `createdAt` | DateTime | event timestamp |
| `content` | String | raw content |
| `tags` | [[String]] | raw tag arrays |
| `sig` | String | signature |
| `author` | Profile | resolved from latest kind-0 |
| `likes` | Int! | kind-7 reactions with content `''` or `'+'` |
| `reposts` | Int! | kinds 6, 16 referencing this event |
| `commentCount` | Int! | kinds 1, 1111 replies referencing this event |
| `reactionsByContent(first)` | [ReactionTally!]! | `{content, count}` emoji tallies, desc |
| `likers(first)` | ReactionConnection! | edges `{node: Profile, content, reactedAt, cursor}` |
| `reposters(first)` | RepostConnection! | edges `{node, repostedAt, cursor}` |
| `thread` | Thread! | thread rooted at this event |
| `zaps` `zapSats` `uniqueZappers` | Int! | **always 0 — stubbed** (kind 9735 not wired) |
| `zappers(first)` | ZapConnection! | **always empty — stubbed** |
| `updatedAt` | DateTime! | last-seen time |

## Profile

| Field | Type | Meaning |
|---|---|---|
| `pubkey` | String! | hex pubkey |
| `metadata` | ProfileMetadata | `{name, displayName, picture, about, nip05, lud16}`; null if no kind-0 |
| `followers` | Int! | distinct pubkeys whose latest kind-3 `p`-tags this pubkey |
| `following` | Int! | distinct `p`-tags in this pubkey's kind-3 |
| `followerList(first)` | FollowConnection! | edges `{node, followedAt, cursor}` |
| `followingList(first)` | FollowConnection! | edges `{node, followedAt, cursor}` |
| `followedBy(viewerPubkey: String!)` | Boolean! | does `viewerPubkey` follow this profile |
| `updatedAt` | DateTime! | |

## Thread

| Field | Type | Meaning |
|---|---|---|
| `root` | Event! | the root event |
| `directReplies` | Int! | == root's `commentCount` |
| `participants` | Int! | distinct authors of replies |
| `comments(first, sort)` | CommentConnection! | `sort` = `NEWEST` (default) \| `OLDEST` |

**Comment** node: `{id, author: Profile!, content, createdAt, replyCount, likes,
reposts, zaps/zapSats/uniqueZappers (0)}`.

## Connections

Every `*Connection`: `{ edges [...], pageInfo { hasNextPage, endCursor }, totalCount }`.
`first`: default 50, **max 100**. `pageInfo.hasNextPage` is **always false** and
cursors are not honored as input — there is no working pagination yet.

`ReactionTally`: `{ content: String!, count: Int! }`.

## aggregateEvents(input)

```graphql
input EventAggregationInput {
  dataset: String!        # EVENTS | TAGS | REACTIONS | REPLIES
  groupBy: [String!]!     # >= 1 dimension (see matrix)
  metrics: [String!]      # default ["COUNT"]
  kinds: [Int!]           # kind filter — honored ONLY for EVENTS and TAGS
  limit: Int              # default 100, max 1000 (0 -> 100)
}
```

Returns `AggregationResult { rows: [AggregationRow!]! }`, each row
`{ dimensions: JSON, metrics: JSON }`. Rows are ordered by the **first metric**
DESC. Dimension values come back as **strings**; metric values as numbers.
**Result JSON keys are lower-cased**, not the SCREAMING_CASE you pass in:
`KIND`→`kind`, `UNIQUE_EVENTS`→`unique_events`, `TARGET_EVENT`→`target_event`.

### Datasets

| dataset | source | filter |
|---|---|---|
| `EVENTS` | `nostr_events` | none |
| `TAGS` | `event_tags` (one row per tag) | none |
| `REACTIONS` | kind-7 events e/E-tagging a target | `kind=7 AND tag_key IN ('e','E')` |
| `REPLIES` | kind 1/1111 events e/E-tagging a target | `kind IN (1,1111) AND tag_key IN ('e','E')` |

### Dimension validity (`groupBy`)

| dim | EVENTS | TAGS | REACTIONS | REPLIES | meaning |
|---|:-:|:-:|:-:|:-:|---|
| `DAY` | ✓ | ✓ | ✓ | ✓ | `toDate(created_at)` |
| `HOUR` | ✓ | ✓ | ✓ | ✓ | `toStartOfHour(created_at)` |
| `KIND` | ✓ | ✓ | ✓ (=7) | ✓ | event kind |
| `AUTHOR` | ✓ | ✓ | ✓ | ✓ | pubkey |
| `EVENT_ID` | ✓ | ✓ | ✓ | ✓ | event id |
| `TAG_KEY` | – | ✓ | – | – | tag name (e.g. `p`, `e`, `t`) |
| `TAG_VALUE` | – | ✓ | – | – | tag value |
| `TARGET_EVENT` | – | – | ✓ | ✓ | the e/E-tagged target id |
| `REACTION` | – | – | ✓ | – | reaction content (emoji) |

### Metric validity

| metric | EVENTS | TAGS | REACTIONS | REPLIES | meaning |
|---|:-:|:-:|:-:|:-:|---|
| `COUNT` | ✓ | ✓ | ✓ | ✓ | row count |
| `UNIQUE_EVENTS` | ✓ | ✓ | ✓ | ✓ | distinct event ids |
| `UNIQUE_AUTHORS` | ✓ | ✓ | ✓ | ✓ | distinct pubkeys |
| `UNIQUE_TARGETS` | – | ✓ | ✓ | ✓ | distinct tag_value (targets) |

A dim/metric in a `–` cell makes the API return an error.

> **`TARGET_EVENT` ids are usually NOT stored events.** They're whatever an `e`/`E`
> tag points at, so looking one up with `event(id:)` typically returns an object
> with `kind`/`pubkey` = null. Report engagement straight from the aggregate
> counts; don't expect the target to resolve as a full event.
>
> **`KIND` in `REACTIONS`/`REPLIES` is the reactor's/replier's kind** (so always 7
> for reactions), NOT the target's. There is no way to restrict a target to
> kind-1 — i.e. "most-reacted **note**" specifically is not expressible; only
> "most-reacted **target event**" is.

## Kind reference (common)

`0` profile · `1` short note · `3` contacts/follows · `5` deletion ·
`6`/`16` repost · `7` reaction · `1111` comment · `9735` zap receipt ·
`10002` relay list.

## Limits — what the schema can NOT do

Say so plainly and offer the closest query instead of inventing fields.

- **No time-range filter** (no since/until). Only `DAY`/`HOUR` grouping.
- **No content/text search.**
- **No zap data** — `zaps`/`zapSats`/`uniqueZappers`/`zappers` are stubbed to 0/empty.
- **No arbitrary WHERE** beyond `kinds` (and that only for `EVENTS`/`TAGS`).
- **No list-all root** — you must already have an id/pubkey for `event`/`profile`/`thread`.
  To enumerate events, use `aggregateEvents` grouped by `EVENT_ID`.
- **No real pagination** — `first` ≤ 100, `hasNextPage` always false.
