# GraphQL Schema Proposal: Generic Nostr Analytics Layer

**Status:** REALIZED (2026-07-05) — the rules registry (`internal/rules`,
`docs/rules-registry.md`) now enforces this proposal mechanically. GraphQL and
the REST app-view are two thin surfaces over one registry. The typed
domain-vocabulary surface that had drifted back in after this proposal
(`noteStats { likes, reposts, zapSats }`, `aggregateReferencedBy`,
`rankedReferencedBy`, `selectedReferences`, the `authoredReplyChain` wrapper)
was pruned — none of it had consumers; nagg-ts is pure REST. Rank metric
names are now registry rule names (`k7_e.actors`, `k6_16_e.actors`,
`k1_q.sources`, `k1_1111_e_reply.sources`, `k9735_e.value_total`, plus the
cross-rule `actors` signal). Remaining TODOs from this document: cursor-based
pagination (`after: Cursor`) and a real `pageInfo.hasNextPage` (still
hardcoded `false`); `Hex64`/`Cursor` remain plain strings with runtime
validation.

**Original status:** Revised — generic-first read layer for nagg  
**Date:** 2026-05-30  
**Context:** This replaces the earlier typed social-stats proposal. The previous shape baked in app-level concepts such as `likes`, `thread`, `participants`, `followers`, and `zaps`. Those are useful interpretations, but they should be client-side query recipes over Nostr primitives, not server-side schema concepts.

---

## TL;DR

nagg should expose raw Nostr events plus constrained generic filtering and aggregation over:

- event fields: `id`, `pubkey`, `kind`, `created_at`, `content`
- flattened tags: `tag_key`, `tag_value`, tag order/extra values
- relay provenance

The server should not decide that kind `7` means "likes", kind `3` means "followers", or an `e` tag means "thread". Clients can still ask those questions, but they do it by specifying the relevant `kind`, tag filters, dimensions, and metrics.

This keeps nagg useful for social apps, video apps, music apps, marketplaces, private conventions, and future Nostr NIPs without schema churn.

---

## What Was Accidentally Baked In

The old proposal included these app-level concepts:

- `likes`, `likers`, `reactionsByContent`: assumes kind `7` reaction semantics and assumes `content` is the reaction dimension.
- `reposts`, `reposters`: assumes kinds `6` and `16` deserve first-class fields.
- `thread`, `commentCount`, `directReplies`, `participants`, `Comment`, `ThreadParent`: assumes `e`/`E` tags imply a reply tree.
- `followers`, `following`, `followerList`, `followedBy`: assumes kind `3` is interpreted as current social graph state.
- `zaps`, `zapSats`, `uniqueZappers`, `Zap`: assumes kind `9735` should be interpreted as payment analytics.
- `Profile.metadata`: assumes kind `0` should be parsed into profile fields.
- `Engageable`, `ActorSort`, `CommentSort`, `ZapSort`: assumes an engagement model and app-specific pagination semantics.

Those concepts are not wrong. They are just not the right abstraction boundary for nagg's core read API.

---

## Design Principles

- **Nostr primitives first.** The API exposes events, tags, relays, filters, dimensions, and metrics.
- **No raw SQL proxy.** Clients choose from allowed datasets, filters, dimensions, and metrics.
- **Interpretations are recipes.** "Likes", "comments", "followers", and "zaps" are documented query examples, not GraphQL fields.
- **Kind-agnostic by default.** Any client can define meaning through `kinds` and `tags`.
- **Joins stay primitive.** Related data is requested as more raw events keyed by `pubkey`, `id`, or tags, not through app-specific types like `Profile` or `Author`.
- **Full event bodies remain available.** `event(id:)` and `events(input:)` return the full Nostr event so clients can render and verify it.
- **Semantic tables may exist internally later.** Materialized views and semantic indexes can optimize common recipes without changing the public API.

---

## Proposed Schema

```graphql
scalar Hex64
scalar DateTime
scalar JSON
scalar Cursor

type Query {
  "Fetch one full Nostr event by id."
  event(id: Hex64!): NostrEvent

  "Fetch full events using constrained filters."
  events(input: EventQueryInput!): EventConnection!

  "Aggregate over events, tags, or relay provenance using allowed dimensions and metrics."
  aggregateEvents(input: EventAggregationInput!): AggregationResult!
}

type NostrEvent {
  id: Hex64!
  pubkey: Hex64!
  kind: Int!
  createdAt: DateTime!
  content: String!
  tags: [[String!]!]!
  sig: String!
  updatedAt: DateTime!
  pubkeyEvents(kinds: [Int!], limit: Int = 1): [NostrEvent!]!
}

type EventConnection {
  nodes: [NostrEvent!]!
  pageInfo: PageInfo!
}

type PageInfo {
  hasNextPage: Boolean!
  endCursor: Cursor
}

input EventQueryInput {
  ids: [Hex64!]
  pubkeys: [Hex64!]
  kinds: [Int!]
  tags: [TagFilterInput!]
  since: DateTime
  until: DateTime
  limit: Int = 50
  after: Cursor
}

input TagFilterInput {
  "Tag key, for example e, p, d, t, bolt11, imeta."
  key: String!
  "Single tag value match. If omitted, only tag-key existence is required."
  value: String
  "Any-of tag value match."
  values: [String!]
}

input EventAggregationInput {
  dataset: String = "EVENTS"
  ids: [Hex64!]
  pubkeys: [Hex64!]
  kinds: [Int!]
  tags: [TagFilterInput!]
  since: DateTime
  until: DateTime
  groupBy: [String!]!
  metrics: [String!] = ["COUNT"]
  limit: Int = 100
}

type AggregationResult {
  rows: [AggregationRow!]!
}

type AggregationRow {
  dimensions: JSON!
  metrics: JSON!
}
```

Current implementation note: the first server accepts allowlisted strings rather than GraphQL enum types so the analytics surface can evolve quickly. Valid dataset strings are `EVENTS`, `TAGS`, and `RELAYS`. Valid dimensions are `DAY`, `HOUR`, `KIND`, `PUBKEY`, `AUTHOR`, `EVENT_ID`, `CONTENT`, `TAG_KEY`, `TAG_VALUE`, and `RELAY`, depending on dataset. Valid metrics are `COUNT`, `UNIQUE_EVENTS`, `UNIQUE_PUBKEYS`, `UNIQUE_AUTHORS`, `UNIQUE_TAG_VALUES`, and `UNIQUE_RELAYS`, depending on dataset.

---

## How The Old Use Cases Work Now

### 1. "Likes" / reactions for an event

Instead of a `likes` field, the client asks: "For kind `7` events that have an `e` tag pointing at this event, group by reaction content."

```graphql
query {
  aggregateEvents(input: {
    dataset: TAGS
    kinds: [7]
    tags: [{ key: "e", value: "TARGET_EVENT_ID" }]
    groupBy: [CONTENT]
    metrics: [UNIQUE_EVENTS, UNIQUE_PUBKEYS]
    limit: 20
  }) {
    rows { dimensions metrics }
  }
}
```

To list the reaction events themselves:

```graphql
query {
  events(input: {
    kinds: [7]
    tags: [{ key: "e", value: "TARGET_EVENT_ID" }]
    limit: 20
  }) {
    nodes { id pubkey kind createdAt content tags }
  }
}
```

### 2. "Comments" / replies for an event

Instead of `thread(rootEventId:)`, the client asks for events of whichever kinds it considers replies, with whichever tag convention it cares about.

```graphql
query {
  events(input: {
    kinds: [1, 1111]
    tags: [{ key: "e", value: "ROOT_OR_PARENT_EVENT_ID" }]
    limit: 20
  }) {
    nodes { id pubkey kind createdAt content tags }
  }
}
```

To count reply authors:

```graphql
query {
  aggregateEvents(input: {
    dataset: TAGS
    kinds: [1, 1111]
    tags: [{ key: "e", value: "ROOT_OR_PARENT_EVENT_ID" }]
    groupBy: [TAG_VALUE]
    metrics: [UNIQUE_EVENTS, UNIQUE_PUBKEYS]
  }) {
    rows { dimensions metrics }
  }
}
```

### 3. "Followers" for a pubkey

Instead of `profile(pubkey).followers`, the client asks: "Which kind `3` events have a `p` tag for this pubkey?"

```graphql
query {
  aggregateEvents(input: {
    dataset: TAGS
    kinds: [3]
    tags: [{ key: "p", value: "PUBKEY" }]
    groupBy: [TAG_VALUE]
    metrics: [UNIQUE_PUBKEYS, COUNT]
  }) {
    rows { dimensions metrics }
  }
}
```

To list follower contact-list events:

```graphql
query {
  events(input: {
    kinds: [3]
    tags: [{ key: "p", value: "PUBKEY" }]
    limit: 50
  }) {
    nodes { id pubkey createdAt tags }
  }
}
```

### 4. "Profile metadata"

Instead of parsing kind `0` into a `ProfileMetadata` object in the schema:

```graphql
query {
  events(input: {
    pubkeys: ["PUBKEY"]
    kinds: [0]
    limit: 1
  }) {
    nodes { id pubkey createdAt content }
  }
}
```

The client can parse `content` as profile JSON, or nagg can later expose a generic JSON helper without making profile semantics mandatory.

If the client wants that data inline in a single GraphQL request, it can still stay generic by asking for more raw events that share the same `pubkey`:

```graphql
query {
  events(input: {
    kinds: [1]
    limit: 20
  }) {
    nodes {
      id
      pubkey
      content
      pubkeyEvents(kinds: [0], limit: 1) {
        id
        pubkey
        kind
        createdAt
        content
      }
    }
  }
}
```

That keeps the public API generic: the server is only resolving a same-`pubkey` relation and returning more `NostrEvent` rows.

### 5. Zaps

Instead of `zapSats`, `uniqueZappers`, or a `Zap` type, clients can query the raw receipt events and tags:

```graphql
query {
  aggregateEvents(input: {
    dataset: TAGS
    kinds: [9735]
    tags: [{ key: "e", value: "TARGET_EVENT_ID" }]
    groupBy: [TAG_KEY]
    metrics: [COUNT, UNIQUE_EVENTS]
  }) {
    rows { dimensions metrics }
  }
}
```

If nagg later validates receipts and extracts msat amounts, that can be added as a generic derived dataset, for example `ZAP_RECEIPT_TAGS` or `DERIVED_EVENTS`, without changing the core event/tag model.

---

## Implementation Notes

- The public API should use generic store methods such as `QueryEvents(ctx, EventQueryInput)` and `AggregateEvents(ctx, AggregateInput)`.
- Avoid read-model methods named after interpretations, such as `LikeCount`, `Followers`, or `ThreadParticipants`.
- Internally, ClickHouse materialized views can optimize common recipes:
  - reaction tags by target event
  - latest kind-3 contact lists
  - kind-0 latest metadata
  - zap receipt validation
- Those optimizations should be hidden behind the same generic API. A query recipe should not care whether it is backed by raw tables or derived tables.

---

## Safety Rules

- `limit` is capped server-side.
- Dataset, dimensions, and metrics are allowlisted.
- Tag filters are structured; clients cannot inject SQL.
- Expensive dimensions can require a kind or time filter later.
- Pagination should become cursor-based before production-scale public use.
- `event(id:)` should eventually use an id-ordered projection or bloom filter index on `nostr_events`.
