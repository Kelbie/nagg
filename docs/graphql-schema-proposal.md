# GraphQL Schema Proposal: Typed Social-Stats Layer

**Status:** Revised — key decisions incorporated; basis for nagg's typed read layer (Milestones 2–4, not yet implemented)
**Date:** 2026-05-30 (revised)
**Context:** Drafted in a brainstorming session, grounded in nagg's raw ingestion layer (`nostr_events`, `event_tags`, `event_seen_relays` — already implemented) and the semantic/aggregate tables planned in `IMPLEMENTATION_PLAN.md` §4–§5.

> **Revised after review.** Decisions now baked in: nagg stores the full event, so the typed `Event` returns the **whole Nostr event** (`content`/`tags`/`sig`) alongside its derived stats — clients need no relay round-trip and can verify the event from `sig`. Counts are backed by cursor-paginated actor lists. Remaining divergences from `IMPLEMENTATION_PLAN.md` section 7 (offset vs cursor pagination, generic AST vs typed nodes) are reconciliation items — see [Relationship to IMPLEMENTATION_PLAN.md section 7](#relationship-to-implementation_plan-section-7).

---

## TL;DR

A typed, entity-centric GraphQL API over nagg's raw, semantic, and aggregate tables. Two root nodes, `Event` and `Profile`, expose engagement and social-graph stats as strongly-typed fields; the `Event` node also returns the **full Nostr event** (content/tags/sig) so clients need no relay round-trip. Every count is backed by a paginated list of the actors behind it. Query-only, kind-agnostic.

It answers the three motivating questions directly:

```graphql
# 1. Likes on an event (and who liked)
{ event(id: "abc…") { likes  likers(first: 20) { edges { node { pubkey } content } } } }

# 2. Comments on a thread for an event
{ thread(rootEventId: "abc…") { totalComments directReplies participants
    comments(first: 20) { edges { node { author { pubkey } content replyCount } } } } }

# 3. Followers per pubkey
{ profile(pubkey: "…hex") { followers following } }
```

---

## Scope and design decisions

These were settled deliberately during brainstorming and review:

| Decision | Choice | Rationale |
|---|---|---|
| **Scope** | Generic social graph, kind-agnostic | Likes, reposts, comments/threads, zaps per event; followers/following per pubkey. Works for any Nostr kind, not video-specific. |
| **Detail level** | Counts **and** paginated actor lists | Each count (likes, reposts, followers, comments) is backed by a connection to page the actual actors or the comment tree. |
| **Liveness** | Query only | No subscriptions in v1. Clients re-query for fresh numbers. |
| **Modeling** | Entity-centric typed nodes | `event(id)` / `profile(pubkey)` return `Event` / `Profile` nodes; a liker resolves straight to a `Profile`, a comment to its own engagement. Batch variants avoid N+1. |
| **Event identity** | Nodes return the **full event** (`pubkey`/`kind`/`createdAt`/`content`/`tags`/`sig`) | nagg stores the whole event, so the `Event` node returns it alongside stats — clients never need a relay round-trip and can verify the event from `sig`. These fields are null only when the event was seen merely as a reference target (e.g. a reaction's target) and never ingested. |
| **Recursion** | `Comment` implements `Engageable`; `reactionsByContent` included | Comments are events, so they carry their own like/repost/zap stats directly via the `Engageable` interface (no `engagement` indirection); emoji breakdown comes for free from the per-reaction aggregate. |
| **Pagination** | Relay-style cursor connections | Stable under the constant stream of new likes/comments (offset pages drift as rows arrive at the top); cheap deep paging in ClickHouse (`WHERE created_at < ?` seeks instead of `OFFSET n` scan-and-discard). |

---

## Proposed schema

```graphql
# =====================================================================
# nagg typed social-stats GraphQL API (proposal)
# Generic, kind-agnostic Nostr engagement + social-graph aggregates.
# Query-only. Every count is backed by a paginated list of the actors.
# The Event node also returns the full Nostr event, so clients need no
# relay round-trip.
# =====================================================================

# ---- Scalars --------------------------------------------------------
scalar Hex64      # lowercase 64-char hex: an event id or a pubkey
scalar DateTime   # RFC-3339 timestamp
scalar Long       # 64-bit int, for msat/sat sums that overflow 32-bit Int
scalar Cursor     # opaque pagination cursor

# ---- Root -----------------------------------------------------------
type Query {
  "Full event + engagement by id. Non-null if the id has been seen at all — even only as a reference target, in which case the raw-event fields are null (see Event)."
  event(id: Hex64!): Event
  "Batch form. Same order as input; null per id never seen. Avoids N+1. Max 100 ids per call."
  events(ids: [Hex64!]!): [Event]!

  "Social-graph + metadata for a pubkey. Null if never seen."
  profile(pubkey: Hex64!): Profile
  "Batch form. Same order as input; null per pubkey never seen. Max 100 pubkeys per call."
  profiles(pubkeys: [Hex64!]!): [Profile]!

  "The reply/comment tree hanging off an event."
  thread(rootEventId: Hex64!): Thread
}

# ---- Engageable -----------------------------------------------------
"""
Anything that can be reacted to, reposted, or zapped. Both top-level
events and comments are Nostr events, so both implement this — a comment
exposes its own like/repost/zap stats directly, with no `engagement`
indirection.
"""
interface Engageable {
  id: Hex64!
  likes: Int!
  likers(first: Int = 50, after: Cursor, sort: ActorSort = NEWEST): ReactionConnection!
  reactionsByContent(first: Int = 10): [ReactionTally!]!
  reposts: Int!
  reposters(first: Int = 50, after: Cursor, sort: ActorSort = NEWEST): RepostConnection!
  zaps: ZapStats!
}

# ---- Event (full event + engagement node) --------------------------
"""
One Nostr event by id: the full event plus its aggregate engagement.
Kind-agnostic: a note, a video, even a comment (comments are events).

The raw-event fields (pubkey/kind/createdAt/content/tags/sig) are null
ONLY when the id has been seen merely as a reference target (e.g. a
reaction or reply pointing at it) but the event itself was never
ingested. When present, `sig` lets a client verify the event without a
relay. Counts and connections are always available from the id alone.
"""
type Event implements Engageable {
  id: Hex64!

  # Full Nostr event (null only if referenced-but-not-ingested; see above)
  pubkey: Hex64
  kind: Int
  createdAt: DateTime
  content: String
  tags: [[String!]!]
  sig: String                # 64-byte hex signature, for client-side verification
  author: Profile            # profile node for pubkey

  # Reactions (NIP-25, kind 7). Like = content '+' or ''; '-' = dislike.
  likes: Int!
  likers(first: Int = 50, after: Cursor, sort: ActorSort = NEWEST): ReactionConnection!
  reactionsByContent(first: Int = 10): [ReactionTally!]!   # emoji breakdown

  # Reposts (NIP-18, kinds 6 & 16)
  reposts: Int!
  reposters(first: Int = 50, after: Cursor, sort: ActorSort = NEWEST): RepostConnection!

  # Replies / comments (kind 1 NIP-10, or kind 1111 NIP-22)
  commentCount: Int!         # all depths in the thread
  directReplyCount: Int!     # direct replies to this event only
  thread: Thread!

  # Zaps (NIP-57, kind 9735)
  zaps: ZapStats!

  updatedAt: DateTime!       # when these aggregates were last refreshed
}

type ReactionTally { content: String!  count: Int! }

# ---- Comments / Thread ---------------------------------------------
"""
The reply tree rooted at one event. Threading-scheme agnostic: built
from the (root_event_id, parent_event_id) edges nagg extracts into
event_replies, so it works for NIP-10 (kind 1 + e tags) and NIP-22
(kind 1111 + E/e tags) alike.
"""
type Thread {
  root: Event!
  totalComments: Int!        # every depth
  directReplies: Int!        # direct replies to the root
  participants: Int!         # distinct commenter pubkeys
  comments(first: Int = 50, after: Cursor, sort: CommentSort = NEWEST): CommentConnection!
}

"""
A comment is an event, so it implements Engageable (its own
likes/reposts/zaps) directly. It adds thread-position fields.
"""
type Comment implements Engageable {
  id: Hex64!
  author: Profile!
  content: String!
  createdAt: DateTime!
  root: Event!               # the event the thread hangs off
  parent: ThreadParent       # root event (top-level) or another comment
  replyCount: Int!
  replies(first: Int = 50, after: Cursor, sort: CommentSort = NEWEST): CommentConnection!

  # Engageable — this comment's own engagement stats
  likes: Int!
  likers(first: Int = 50, after: Cursor, sort: ActorSort = NEWEST): ReactionConnection!
  reactionsByContent(first: Int = 10): [ReactionTally!]!
  reposts: Int!
  reposters(first: Int = 50, after: Cursor, sort: ActorSort = NEWEST): RepostConnection!
  zaps: ZapStats!
}

union ThreadParent = Event | Comment
enum CommentSort { NEWEST OLDEST MOST_REPLIED MOST_LIKED }

# ---- Zaps (NIP-57) -------------------------------------------------
type ZapStats {
  count: Int!                # valid zap receipts
  totalSats: Long!           # 64-bit: lifetime sat totals overflow 32-bit Int
  totalMsats: Long!
  uniqueZappers: Int!
  top: Zap                   # largest single zap
  zaps(first: Int = 50, after: Cursor, sort: ZapSort = NEWEST): ZapConnection!
}

type Zap {
  id: Hex64!                 # zap receipt event id
  zapper: Profile            # resolved from embedded request (kind 9734)
  amountSats: Int!
  amountMsats: Long!
  comment: String            # zap memo
  zappedAt: DateTime!
}
enum ZapSort { NEWEST OLDEST LARGEST }

# ---- Profile (pubkey / social-graph node) --------------------------
type Profile {
  pubkey: Hex64!
  metadata: ProfileMetadata          # latest kind-0, null if none seen
  followers: Int!                    # accounts that follow this pubkey
  following: Int!                    # accounts this pubkey follows
  followerList(first: Int = 50, after: Cursor): FollowConnection!
  followingList(first: Int = 50, after: Cursor): FollowConnection!
  followedBy(viewerPubkey: Hex64!): Boolean!
  updatedAt: DateTime!
}

type ProfileMetadata {
  name: String  displayName: String  picture: String
  about: String  nip05: String  lud16: String
}

# ---- Connections (Relay-style cursor pagination) -------------------
# Every connection: `first` is capped server-side (max 100); deeper
# paging uses `after`. `totalCount` comes from the entity-keyed
# aggregate tables, not a COUNT over the page.
type PageInfo { hasNextPage: Boolean!  endCursor: Cursor }
enum ActorSort { NEWEST OLDEST }

type ReactionConnection { edges: [ReactionEdge!]!  pageInfo: PageInfo!  totalCount: Int! }
type ReactionEdge { node: Profile!  content: String!  reactedAt: DateTime!  cursor: Cursor! }

type RepostConnection { edges: [RepostEdge!]!  pageInfo: PageInfo!  totalCount: Int! }
type RepostEdge { node: Profile!  repostedAt: DateTime!  cursor: Cursor! }

type CommentConnection { edges: [CommentEdge!]!  pageInfo: PageInfo!  totalCount: Int! }
type CommentEdge { node: Comment!  cursor: Cursor! }

type FollowConnection { edges: [FollowEdge!]!  pageInfo: PageInfo!  totalCount: Int! }
type FollowEdge { node: Profile!  followedAt: DateTime  cursor: Cursor! }

type ZapConnection { edges: [ZapEdge!]!  pageInfo: PageInfo!  totalCount: Int! }
type ZapEdge { node: Zap!  cursor: Cursor! }
```

---

## Limits and safety (parity with `IMPLEMENTATION_PLAN.md` §9)

The typed API hits the same database as the generic AST, so it needs the same guardrails:

- **Connections:** `first` defaults to 50 and is capped at **100** server-side; deeper paging is via `after` cursors, never large offsets.
- **Batch roots:** `events(ids:)` / `profiles(pubkeys:)` accept at most **100** ids per call.
- **`totalCount`** is read from the entity-keyed aggregate tables (a point lookup), not a `COUNT(*)` over the page query.
- Per-query timeout, complexity scoring, and per-key rate limits carry over from §9 unchanged.

---

## Mapping to nagg's ClickHouse tables

How each field resolves against the tables in `IMPLEMENTATION_PLAN.md`, and what is **not yet planned** and would need adding. Only the raw ingestion layer (`nostr_events`, `event_tags`, `event_seen_relays`) is implemented today; the semantic/aggregate tables below are still planned in §4–§5.

| Schema field | Backing table(s) | Status |
|---|---|---|
| `Event.{pubkey,kind,createdAt,content,tags,sig}` | `nostr_events`, by `id` | **Exists** (raw table already stores the full event). See the read-path note below. |
| `Event.likes` | `post_reaction_counts` where `reaction IN ('+','')`, `uniqMerge` | Planned (§5) |
| `Event.likers` | `event_reactions` where `target_event_id=?` join `profiles_latest` | Planned (§4) |
| `Event.reactionsByContent` | `post_reaction_counts` grouped by `reaction` | Planned (§5) |
| `Event.commentCount` | `post_reply_counts` where `root_event_id=?`, `uniqMerge` | Planned (§5) |
| `Event.directReplyCount` | count over `event_replies` where `parent_event_id=?` | Needs a per-parent count (a `post_direct_reply_counts` MV, or live query) |
| `Thread.comments` / `Comment.replies` | `event_replies` join `nostr_events` join `profiles_latest` | Planned (§4 / §7.5) |
| `Thread.participants` | `uniq(author_pubkey)` over `event_replies` for the root | Needs a MV or live `uniq` |
| `Comment` engagement (Engageable) | resolve the comment id as an `Event` (reactions/zaps keyed by its id) | Reuses the reaction/zap tables |
| `Profile.metadata` | `profiles_latest` | Planned (§4) |
| `Event.author` | `nostr_events` (pubkey) join `profiles_latest` | Planned |
| `Event.reposts` / `reposters` | reposts semantic table | **Missing.** Plan ingests kind 6/16 but defines no `event_reposts` table or `post_repost_counts` MV |
| `Profile.followers` / `followerList` | reverse-follow index from kind 3 | **Missing.** Plan ingests kind 3 but builds no follower index |
| `Profile.following` / `followingList` | latest kind-3 contact list (p tags) | **Missing.** Needs a `contacts_latest` table or derivation from `event_tags` on the latest kind-3 |
| `Event.zaps` / `ZapStats` / `Zap` | zap semantic + aggregate tables from kind 9735 | **Missing.** Plan ingests kind 9735 but defines no `event_zaps` / `post_zap_totals` |

**Read-path note (full event by id).** `nostr_events` is `ORDER BY (kind, created_at, pubkey, id)` — tuned for analytics scans, not point lookups by `id`. But `event(id)` / `events(ids)` now return the **full event**, making by-id fetch a hot path. The read layer needs an `id`-ordered **projection** (or a bloom-filter data-skipping index on `id`) on `nostr_events` so these lookups seek instead of scan. Add it in the same migration that introduces the semantic/aggregate tables.

**Net new work this schema implies for the ingestion/aggregation layer:** reposts table + count, a follower/following index (semantic edges + counts), zaps semantic + totals, a direct-reply count, a thread-participants count, and the `id` projection on `nostr_events`. Everything else maps onto tables already planned.

---

## Relationship to IMPLEMENTATION_PLAN.md section 7

Section 7 already specifies a GraphQL API. This proposal is a **different shape**, not a patch to it. The divergences:

| Axis | Plan section 7 | This proposal |
|---|---|---|
| **Philosophy** | Generic constrained analytics AST: `aggregateEvents(dataset, filters, groupBy, metrics)` returning untyped `rows { dimensions, metrics }` | Entity-centric typed nodes: `Event` / `Profile` / `Thread` / `Comment` / `Zap` with resolved relationships |
| **Ergonomics** | Maximally flexible, fewer resolvers, weakly typed; client composes dimensions/metrics | Strongly typed and self-documenting; each field maps to a known aggregate; good for app clients |
| **Pagination** | `limit` / `offset` | Cursor connections (`first` / `after`, `edges` / `pageInfo`) |
| **Actor lists** | Not surfaced directly (could `groupBy` reactor) | First-class: `likers`, `reposters`, follower/following lists, comment tree |
| **Raw event body** | Returned by `events(input)` as full `NostrEvent` nodes | Returned inline on the typed `Event` (content/tags/sig) — the two converge on one full-event-plus-stats node |
| **Zaps / followers** | Not in the section 7 surface | First-class `ZapStats` and `followers`/`following` |
| **Shared ground** | `postStats(eventId)`, `commentsForPost`, "no raw SQL proxy" principle | Honors the same "no raw SQL proxy" principle; `postStats`/`commentsForPost` are the typed `Event`/`Thread` here |

They are **not contradictory.** Section 7 already mixes typed (`postStats`, `commentsForPost`) and generic (`aggregateEvents`) queries, and this typed design honors section 7's stated key principle ("make it a constrained semantic analytics interface," not a SQL proxy).

### Reconciliation options for the team

1. **Complement.** Ship this typed layer as the product/app-facing API and keep `aggregateEvents` for ad-hoc analytics and dashboards. Two query families: typed nodes (cursor) for apps, generic AST (offset) for analytics. Most aligned with what section 7 already sketches. Most surface area to maintain.
2. **Supersede.** Adopt this typed schema as the GraphQL design and defer the generic `aggregateEvents` AST until a concrete analytics/dashboard need appears. Smallest surface now, less flexible for unanticipated queries.
3. **Adapt.** Keep the generic AST + offset pagination as canonical and fold these needs (likers, followers, zaps, threads) into it as datasets/dimensions rather than typed nodes. Maximum flexibility, weakest typing and app ergonomics.

**Current direction:** nagg is proceeding with this typed layer as the read API (the `Event` / `Profile` / `Thread` nodes above); the generic `aggregateEvents` AST stays in `IMPLEMENTATION_PLAN.md` §7 as a deferred analytics surface (closest to **Complement**, sequenced typed-first).

---

## Decided defaults (v1)

- **`likes` semantics:** like = reaction content `'+'` or `''`; `'-'` is a dislike; any other content is a custom/emoji reaction — **excluded** from `likes` and surfaced via `reactionsByContent`.
- **Followers:** current state only. The reverse index uses each follower's **latest** kind-3 (replace semantics, latest wins); no follow/unfollow history in v1. `FollowEdge.followedAt` is the source kind-3's `created_at` — when that contact list was published, identical for every contact in it — **not** a true per-follow timestamp. So `followerList` ordered `NEWEST` means "most recently (re)published contact lists," and the follows cursor key is `(kind3_created_at, kind3_event_id)`.
- **Zap trust:** validate each receipt (kind 9735) against its embedded request (kind 9734) and the recipient's lnurl before counting, so `totalSats` / `totalMsats` can't be inflated by forged receipts.
- **Bodies:** nagg stores and returns full events; clients do not need a relay to render or verify. (Raw-content retention policy is a separate Milestone-6 decision.)

## Implementation notes

- **Library:** `gqlgen` (schema-first, matches the plan's stated Go stack). `Long` maps to a custom int64 scalar; `Hex64` and `Cursor` are custom scalars with validation.
- **N+1:** batch resolvers (`events`, `profiles`) plus a dataloader per edge type (a liker's `Profile`, a comment's `author`) are needed so a 50-item connection does not fan out into 50 ClickHouse round-trips.
- **Cursor encoding:** opaque base64 of `(created_at, id)` for reactions/reposts; `(created_at, reply_event_id)` for comments; `(kind3_created_at, kind3_event_id)` for follows. Stable under inserts.
- **Read path:** add the `id`-ordered projection / bloom skip-index on `nostr_events` (see mapping note) so full-event-by-id lookups seek instead of scan.
- **Freshness:** `Event.updatedAt` reflects the aggregate table's merge/refresh time so clients know staleness in this query-only model.
