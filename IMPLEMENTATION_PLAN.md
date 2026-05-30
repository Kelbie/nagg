````markdown
# Nostr AppView Aggregator Implementation Plan

## Goal

Build a Nostr “AppView” service that:

- ingests events from many relays using `vertex-lab/crawler_v2`
- stores raw Nostr events and flattened tags in ClickHouse
- computes aggregate stats in ClickHouse
- exposes a generic but constrained GraphQL API for client-side filtering and analytics

`crawler_v2` is a good fit because it is designed to continuously crawl the Nostr network, discover relays via `kind:10002` relay lists, and handle relay disconnects/retries/backoff. Source: https://github.com/vertex-lab/crawler_v2

---

# 1. High-level architecture

```text
Nostr relays
   ↓
crawler_v2 firehose / crawler
   ↓
Go ingestion adapter
   ↓
ClickHouse
   ├─ raw events
   ├─ flattened tags
   ├─ semantic tables
   └─ aggregate tables
   ↓
Go GraphQL API
   ↓
Clients / dashboards / apps
````

---

# 2. Components

## 2.1 Firehose / crawler

Use:

```text
https://github.com/vertex-lab/crawler_v2
```

Responsibilities:

* connect to many Nostr relays
* discover additional relays from `kind:10002`
* read relevant events continuously
* handle reconnects, subscription closures, retry, and backoff
* emit normalized Nostr events into our ingestion pipeline

Initial event kinds to ingest:

```text
kind 0      profile metadata
kind 1      text notes
kind 3      contact lists
kind 5      deletion events
kind 6      reposts
kind 7      reactions / likes
kind 9735   zap receipts
kind 10002  relay lists
kind 30000-39999 addressable events
```

Implementation options:

```text
Option A: fork crawler_v2 and add ClickHouse writer
Option B: run crawler_v2 as a separate service and consume its output
Option C: vendor its crawler logic into our Go ingestion service
```

Recommended:

```text
Start with Option A or C.
Keep ingestion in Go so validation, tag extraction, and batching are easy to control.
```

---

# 3. ClickHouse schema

## 3.1 Raw events table

Store every valid Nostr event once.

```sql
CREATE TABLE IF NOT EXISTS nostr_events
(
    id FixedString(64),
    pubkey FixedString(64),
    created_at DateTime,
    kind UInt32,
    tags_json String,
    content String,
    sig FixedString(128),

    first_seen_at DateTime DEFAULT now(),
    last_seen_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(last_seen_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (kind, created_at, pubkey, id);
```

Notes:

* `id` is the canonical event identifier.
* Use `ReplacingMergeTree` to dedupe repeated events seen from many relays.
* Keep `tags_json` raw so semantic extraction can be rebuilt later.
* Do not rely only on JSON queries for production analytics.

---

## 3.2 Relay provenance table

Track where events were observed.

```sql
CREATE TABLE IF NOT EXISTS event_seen_relays
(
    event_id FixedString(64),
    relay LowCardinality(String),
    first_seen_at DateTime,
    last_seen_at DateTime
)
ENGINE = ReplacingMergeTree(last_seen_at)
ORDER BY (event_id, relay);
```

Useful for:

* coverage reports
* relay quality scoring
* debugging missing data
* trust / provenance metadata

---

## 3.3 Flattened tags table

Flatten every tag during ingestion.

```sql
CREATE TABLE IF NOT EXISTS event_tags
(
    event_id FixedString(64),
    pubkey FixedString(64),
    kind UInt32,
    created_at DateTime,

    tag_index UInt16,
    tag_key LowCardinality(String),
    tag_value String,
    tag_extra Array(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (tag_key, tag_value, kind, created_at, event_id);
```

Why this matters:

* Fast lookup by `e` tag.
* Fast lookup by `p` tag.
* Supports replies, mentions, reactions, reposts, zaps, addressable events.
* Preserves tag order, which matters for reply/thread interpretation.

---

# 4. Semantic tables

Raw events and tags are not enough. Build semantic tables from them.

## 4.1 Reactions

```sql
CREATE TABLE IF NOT EXISTS event_reactions
(
    reaction_event_id FixedString(64),
    reactor_pubkey FixedString(64),
    target_event_id FixedString(64),
    target_pubkey String,
    reaction String,
    created_at DateTime
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (target_event_id, reaction, reactor_pubkey, reaction_event_id);
```

Interpretation:

```text
kind 7
e tag -> target event
p tag -> target pubkey
content "+" or "" -> like
content "-" -> dislike
other content -> emoji/custom reaction
```

---

## 4.2 Replies / comments

```sql
CREATE TABLE IF NOT EXISTS event_replies
(
    reply_event_id FixedString(64),
    author_pubkey FixedString(64),
    root_event_id String,
    parent_event_id String,
    created_at DateTime
)
ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (root_event_id, created_at, reply_event_id);
```

Used for:

```text
all comments to a post
thread trees
reply counts
conversation analytics
```

---

## 4.3 Latest profiles

```sql
CREATE TABLE IF NOT EXISTS profiles_latest
(
    pubkey FixedString(64),
    event_id FixedString(64),
    created_at DateTime,

    name String,
    display_name String,
    picture String,
    nip05 String,
    about String,

    raw_json String
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (pubkey);
```

Used for joining comments with author `kind 0` metadata.

---

## 4.4 Addressable events

```sql
CREATE TABLE IF NOT EXISTS addressable_events
(
    address String,
    event_id FixedString(64),
    pubkey FixedString(64),
    kind UInt32,
    d String,
    created_at DateTime
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (address);
```

Where:

```text
address = kind:pubkey:d
```

Used for `kind 30000-39999`.

---

# 5. Aggregation layer

Aggregation rules are still open-ended. The system should support adding rules over time.

## 5.1 Initial examples

### Post reaction counts

```sql
CREATE TABLE IF NOT EXISTS post_reaction_counts
(
    target_event_id FixedString(64),
    reaction String,
    count_state AggregateFunction(uniq, FixedString(64))
)
ENGINE = AggregatingMergeTree
ORDER BY (target_event_id, reaction);
```

Materialized view:

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_post_reaction_counts
TO post_reaction_counts
AS
SELECT
    target_event_id,
    reaction,
    uniqState(reaction_event_id) AS count_state
FROM event_reactions
GROUP BY
    target_event_id,
    reaction;
```

Query:

```sql
SELECT
    target_event_id,
    reaction,
    uniqMerge(count_state) AS count
FROM post_reaction_counts
WHERE target_event_id = {event_id:String}
GROUP BY
    target_event_id,
    reaction;
```

---

### Reply counts per post

```sql
CREATE TABLE IF NOT EXISTS post_reply_counts
(
    root_event_id String,
    count_state AggregateFunction(uniq, FixedString(64))
)
ENGINE = AggregatingMergeTree
ORDER BY (root_event_id);
```

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_post_reply_counts
TO post_reply_counts
AS
SELECT
    root_event_id,
    uniqState(reply_event_id) AS count_state
FROM event_replies
GROUP BY root_event_id;
```

---

### Events per kind per day

```sql
CREATE TABLE IF NOT EXISTS daily_kind_counts
(
    day Date,
    kind UInt32,
    count_state AggregateFunction(uniq, FixedString(64))
)
ENGINE = AggregatingMergeTree
ORDER BY (day, kind);
```

```sql
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_kind_counts
TO daily_kind_counts
AS
SELECT
    toDate(created_at) AS day,
    kind,
    uniqState(id) AS count_state
FROM nostr_events
GROUP BY
    day,
    kind;
```

---

### Author activity per day

```sql
CREATE TABLE IF NOT EXISTS daily_author_activity
(
    day Date,
    pubkey FixedString(64),
    kind UInt32,
    count_state AggregateFunction(uniq, FixedString(64))
)
ENGINE = AggregatingMergeTree
ORDER BY (pubkey, day, kind);
```

---

# 6. Go ingestion pipeline

## 6.1 Ingestion stages

```text
1. Receive event from crawler_v2
2. Validate event structure
3. Optionally verify id and signature
4. Normalize fields
5. Batch insert into nostr_events
6. Batch insert into event_seen_relays
7. Flatten tags into event_tags
8. Derive semantic rows:
   - reactions
   - replies
   - reposts
   - profiles_latest
   - addressable_events
9. Insert semantic rows
```

## 6.2 Batching

Recommended defaults:

```text
batch size: 1,000 to 10,000 events
flush interval: 1 to 5 seconds
retry: exponential backoff
```

Avoid single-event inserts into ClickHouse.

---

# 7. GraphQL API

## 7.1 Philosophy

GraphQL should be generic, but not arbitrary SQL.

Good:

```text
GraphQL input -> validated analytics AST -> safe ClickHouse SQL
```

Bad:

```text
GraphQL input -> raw SQL builder -> public ClickHouse query engine
```

---

## 7.2 Main API shape

```graphql
scalar Time
scalar JSON

type Query {
  events(input: EventQueryInput!): EventConnection!
  aggregateEvents(input: EventAggregationInput!): AggregationResult!
  commentsForPost(input: CommentsForPostInput!): CommentConnection!
  postStats(eventId: ID!): PostStats!
}
```

---

## 7.3 Generic event query

```graphql
input EventQueryInput {
  ids: [ID!]
  authors: [ID!]
  kinds: [Int!]

  since: Time
  until: Time

  tags: [TagFilterInput!]

  limit: Int = 50
  offset: Int = 0
  orderBy: EventOrderBy = CREATED_AT_DESC
}

input TagFilterInput {
  key: String!
  value: String!
}

enum EventOrderBy {
  CREATED_AT_ASC
  CREATED_AT_DESC
}
```

Example:

```graphql
query {
  events(input: {
    kinds: [1]
    authors: ["pubkey..."]
    since: "2026-01-01T00:00:00Z"
    limit: 20
  }) {
    nodes {
      id
      pubkey
      kind
      content
      createdAt
    }
  }
}
```

---

## 7.4 Generic aggregation query

```graphql
input EventAggregationInput {
  dataset: AnalyticsDataset!

  filters: AnalyticsFilterInput
  groupBy: [AnalyticsDimension!]!
  metrics: [AnalyticsMetric!]!

  since: Time
  until: Time

  orderBy: [AnalyticsOrderBy!]
  limit: Int = 100
}

enum AnalyticsDataset {
  EVENTS
  TAGS
  REACTIONS
  REPLIES
  PROFILES
  ADDRESSABLE_EVENTS
}

input AnalyticsFilterInput {
  ids: [ID!]
  authors: [ID!]
  kinds: [Int!]

  referencedEvents: [ID!]
  referencedPubkeys: [ID!]

  tagKey: String
  tagValue: String
}

enum AnalyticsDimension {
  DAY
  HOUR
  KIND
  AUTHOR
  EVENT_ID
  TARGET_EVENT
  TARGET_PUBKEY
  REACTION
  RELAY
  TAG_KEY
  TAG_VALUE
}

enum AnalyticsMetric {
  COUNT
  UNIQUE_EVENTS
  UNIQUE_AUTHORS
  UNIQUE_TARGETS
}

input AnalyticsOrderBy {
  field: String!
  direction: OrderDirection!
}

enum OrderDirection {
  ASC
  DESC
}
```

Example: count likes for a post.

```graphql
query {
  aggregateEvents(input: {
    dataset: REACTIONS
    filters: {
      referencedEvents: ["event_id_here"]
    }
    groupBy: [TARGET_EVENT, REACTION]
    metrics: [COUNT]
    limit: 20
  }) {
    rows {
      dimensions
      metrics
    }
  }
}
```

Example: top authors by notes per day.

```graphql
query {
  aggregateEvents(input: {
    dataset: EVENTS
    filters: {
      kinds: [1]
    }
    since: "2026-01-01T00:00:00Z"
    groupBy: [DAY, AUTHOR]
    metrics: [COUNT]
    limit: 100
  }) {
    rows {
      dimensions
      metrics
    }
  }
}
```

---

## 7.5 Comments with author profiles

```graphql
input CommentsForPostInput {
  eventId: ID!
  limit: Int = 100
  offset: Int = 0
  orderBy: EventOrderBy = CREATED_AT_ASC
}

type Comment {
  event: NostrEvent!
  author: Profile
}

type CommentConnection {
  nodes: [Comment!]!
  totalCount: Int
}
```

ClickHouse query shape:

```sql
SELECT
    r.reply_event_id,
    e.pubkey,
    e.content,
    e.created_at,
    p.name,
    p.display_name,
    p.picture,
    p.nip05
FROM event_replies r
INNER JOIN nostr_events e
    ON e.id = r.reply_event_id
LEFT JOIN profiles_latest p
    ON p.pubkey = e.pubkey
WHERE r.root_event_id = {event_id:String}
ORDER BY e.created_at ASC
LIMIT {limit:UInt32}
OFFSET {offset:UInt32};
```

---

# 8. GraphQL implementation in Go

Recommended libraries:

```text
gqlgen
clickhouse-go/v2
net/http, chi, or echo
```

Project layout:

```text
/cmd
  /ingester
  /api

/internal
  /crawler
  /clickhouse
  /models
  /graphql
  /sqlbuilder
  /semantic
  /config

/migrations
  001_events.sql
  002_tags.sql
  003_semantic_tables.sql
  004_aggregates.sql
```

---

# 9. Query safety rules

The generic GraphQL API must enforce:

```text
max limit
required time range for expensive queries
allowed groupBy combinations
allowed metrics per dataset
timeout per query
max result rows
query complexity score
rate limits per API key
```

Example rules:

```text
EVENTS dataset:
- max limit: 500
- max time range without author/kind filter: 7 days

REACTIONS dataset:
- max groupBy dimensions: 3
- max result rows: 1,000

TAGS dataset:
- tag_key required unless time range <= 1 day
```

---

# 10. Caching

Use Redis for:

```text
postStats(eventId)
commentsForPost(eventId, first page)
popular aggregations
rate limits
API key quotas
```

Cache TTLs:

```text
postStats: 10-60 seconds
comments first page: 10-30 seconds
daily aggregates: 5-30 minutes
historic aggregates: hours/days
```

---

# 11. Deployment

Recommended services:

```text
nostr-ingester
graphql-api
clickhouse
redis
postgres, optional, for app metadata
```

Postgres is optional but useful for:

```text
API keys
users
billing
saved dashboards
saved queries
team permissions
```

ClickHouse remains the source for event analytics.

---

# 12. MVP milestones

## Milestone 1: Raw ingestion

* Run crawler_v2
* Connect to selected relays
* Insert raw events into ClickHouse
* Insert relay provenance
* Insert flattened tags

## Milestone 2: Core semantic extraction

* Build reactions table
* Build replies table
* Build latest profiles table
* Build addressable events table

## Milestone 3: First aggregates

* Post reaction counts
* Reply counts
* Daily event counts by kind
* Author activity by day

## Milestone 4: GraphQL API

* Add `events(input:)`
* Add `aggregateEvents(input:)`
* Add `postStats(eventId:)`
* Add `commentsForPost(input:)`

## Milestone 5: Safety and performance

* Add query complexity limits
* Add Redis caching
* Add request timeouts
* Add rate limits
* Add slow query logging

## Milestone 6: Backfills and rebuilds

* Add semantic-table rebuild jobs
* Add aggregate rebuild jobs
* Add schema migration process
* Add ClickHouse data retention policies if needed

---

# 13. Key design principle

Do not make GraphQL a direct ClickHouse proxy.

Make it a constrained semantic analytics interface:

```text
client filters
   ↓
validated query AST
   ↓
safe SQL generation
   ↓
ClickHouse aggregate/semantic tables
```

This lets clients explore data flexibly without forcing you to predict every future query, while still protecting the database from arbitrary expensive queries.

```
::contentReference[oaicite:0]{index=0}
```

