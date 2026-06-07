# Notifications — flow & intent

The canonical "how notifications are meant to work and why" reference. Read this
before changing anything in the notification path — several pieces look
arbitrary but encode a specific intent, and it's easy to undo one without
realizing. For the perf history and benchmarks see
[`notifications-performance.md`](./notifications-performance.md).

---

## 1. Mental model

A notification = "someone did something involving me" (followed me, liked/
reposted/zapped my post, replied to or mentioned/quoted me). The product goal
for the **All** tab is a **readable mixture** of those, where high-volume noise
(especially follows) is collapsed into compact summaries rather than flooding
the page.

Three layers, each with a clear job:

| Layer | Repo | Job |
|---|---|---|
| **Server / source of truth** | `nagg` | Generate candidates, filter by policy, **group** (product semantics), paginate. |
| **Typed transport** | `nagg-ts` (`@sovranbitcoin/nagg-ts`) | One schema + recipe both transports parse to; no semantics. |
| **Render** | `sovran-app` | Turn the grouped response into rows; drive pagination; pick the transport. |

**Golden rule:** product semantics (grouping, the mixture, policy meaning) live
in the **nagg REST app-view**, *not* in the generic GraphQL layer. The GraphQL
`notifications` resolver stays per-event and dumb. This mirrors how the feed
collapses reposts in `feedResponse`, not in GraphQL.

---

## 2. Transport: app-view first, GraphQL as fallback

`sovran-app` calls `getNotifications` (`features/feed/data/naggFeedClient.ts`).
`runNaggQuery` **prefers the REST app-view** whenever a binding + base URL exist
(the app-view is where grouping lives), and **falls back to GraphQL only** when
the app-view is unavailable, errors, or returns empty. The old per-surface
`nostrNotificationsAppView` flag no longer gates this.

> Intent: the app-view is the real notification experience. GraphQL is a generic
> escape hatch, not the default. If you find the app on GraphQL for
> notifications, grouping silently disappears.

---

## 3. Data model — candidate generation

`notification_candidates` (a `ReplacingMergeTree`, ORDER BY
`(viewer, created_at, event_id, reason)`) is filled by the `mv_notification_candidates`
materialized view from `event_tags`: every `p`-tag reference to the viewer on a
kind 1/3/6/7/16/9735 event becomes one row `(viewer, event_id, actor_pubkey,
kind, created_at, reason)`. See `migrations/006_notifications.sql`.

`reason` is recomputed at read time (`notificationReasonForEvent`):

| kind | reason |
|---|---|
| 3 | `follow` |
| 1 | `reply` / `quote` / `mention` (from its `e`/`q` tags) |
| 6, 16 | `repost` |
| 7 | `reaction` |
| 9735 | `zap` |

> Gotcha: **every kind-3 republish by any follower** creates a fresh `follow`
> candidate for the viewer, with a recent timestamp. This is why follows
> *flood* the recency stream for popular accounts — and the reason for the
> two-window read below. Don't "simplify" that away.

---

## 4. The read flow (grouped — the default)

`GET /nostr/notifications` → `handler.go` `notifications` → `groupNotifications`.

The store query (`read.go` `Notifications`) is generic: it returns the most-
recent candidate rows for a viewer (bounded by a `WITH recent` CTE), with the
policy / reply-scope / reason filters applied, ordered newest-first. All
grouping is done in Go in the handler.

The grouped handler runs **two store calls and merges them:**

1. **Body window** — everything *except* follows (`ExcludeReasons=["follow"]`),
   fetched wide (`limit*6`, clamped 120–600 candidates). This is the page body:
   the actual mixture of likes/reposts/zaps/replies/mentions/quotes.
2. **Follow window** — follows only (`Reasons=["follow"]`), small (≤12), and
   **only on the first page** (`until == 0`).

> Intent: follows would otherwise dominate the recency window and starve
> everything else (the whole page collapses to one follow group with nothing
> below it). Pulling follows onto their own small window guarantees the mixture.
> The follow group is a *pinned-top summary*, not part of the scroll stream.

`groupNotifications` then collapses the combined rows into items, orders by
**representative recency** (each group's newest member), and trims to `limit`
*items*. **`limit` means items, not rows** — a page is N distinct cards, not N
notifications.

---

## 5. Grouping rules

| reason | grouped? | group key | shown as |
|---|---|---|---|
| `follow` | yes | `"follow"` (one group) | "A, B and N more followed you" |
| `repost` | yes | `repost:<targetEventId>` | per reposted post |
| `reaction` | yes | `reaction:<targetEventId>` | per liked post |
| `zap` | yes | `zap:<targetEventId>` (or `zap:profile`) | per zapped post |
| `reply` | **no** | — | individual (text must be readable) |
| `quote` | **no** | — | individual |
| `mention` | **no** | — | individual |

Each group node carries: a **representative event** (newest member), `total`,
`totalCapped` ("N+"), and ≤3 **sample actors** (`{pubkey, eventId, createdAt}`,
newest-first, for the avatar cluster). repost/reaction/zap also carry the inline
`targetEvent` (the post). A group with one member degrades to `type:"single"`.

> Intent: replies/quotes/mentions carry text you need to read, so they never
> collapse. Likes/reposts/zaps are "N people did X to post Y" — collapse them.
> The follow `total` is the **exact** `FollowCounts().Followers` *except* under
> the FOLLOWS policy (where the group is filtered to mutuals, so it uses the
> window count instead).

---

## 6. Policies — who is allowed to notify you

Set in Settings → Notifications. Four values:

| policy | gate | intent |
|---|---|---|
| `RELAXED` | none | everything |
| `MODERATE` | actor or viewer vertex score ≥ 20/60 | a broad-but-trimmed surface |
| `STRICT` | actor or viewer vertex score ≥ 50/80 | high-signal, trusted network |
| `FOLLOWS` | **actor ∈ viewer's follow set** | only people you follow |

`RELAXED/MODERATE/STRICT` are **vertex-score thresholds**
(`notificationPolicyThresholds`). `FOLLOWS` is different in kind: it ignores
scores and filters the candidate window to `actor_pubkey IN (p-tags of the
viewer's latest kind-3 contact list)`. Verified: under FOLLOWS, 100% of returned
actors are in the viewer's follow set.

> Gotcha: FOLLOWS is a **graph** filter, not a score threshold. Its thresholds
> are 0/0. Don't fold it into the score-threshold switch.

---

## 7. Reply scope & tabs

- **Reply scope** (`DIRECT` | `THREAD`): for kind-1 replies, whether to show only
  direct replies to your posts (`DIRECT`) or replies anywhere in a thread you're
  part of (`THREAD`). Applied as a join filter in the store query.
- **Tabs:** `ALL` (the grouped mixture), `MENTIONS` (server-filtered to
  `reason='mention'`; grouping is a no-op there — all singles), and `APP` (a
  **client-only** tab for app announcements like the welcome card — never hits
  the server).

---

## 8. Pagination

- **Cursor:** `until` = the oldest representative `created_at` on the page. The
  next page fetches candidates strictly older.
- **Follow group is first-page only** (`until == 0`), so it never repeats while
  scrolling.
- **`hasNextPage` is a hint, not the gate.** The server's signal is conservative
  (grouping + reply/policy filters collapse a page below the page size, so it can
  report `false` even when more exists). The **client** therefore drives
  load-more: it keeps paging while a fetch brings *genuinely new* group
  identities and the cursor advances, and stops only when a page adds nothing new
  (`NotificationsScreen` + `notificationResults.ts` `notificationDedupeKey`).
- **Cross-page dedupe** is by **group identity** (`reason`+target, or `follow`)
  for grouped nodes and `event.id` for singles — so the same post's group doesn't
  reappear as a new card on the next page.

> Intent: never trust raw item count for "has more" once grouping is involved.
> The client's "stop when a page adds nothing new" is the reliable signal.

---

## 9. Response shape (REST)

```
{ notifications: { nodes: [ NotificationNode ], pageInfo: { hasNextPage, endCursor } },
  metrics: {<id>: NoteStats}, profiles: {<pubkey>: ProfileInfo}, quoted: {<id>: FeedEvent} }

NotificationNode = {
  type: "single" | "group",
  event,                 // representative (most-recent) event
  reason,
  actorVertexScore,
  targetEventId?, targetEvent?,        // repost/reaction/zap → the post (inline)
  total?, totalCapped?, sampleActors?, // group only
}
```

Mirrors the feed's `FeedItem` convention (a `type` discriminator + flat
`omitempty` fields + inline related-event pointer + shared side maps). The
`nagg-ts` schema (`NaggNotificationNodeSchema`) keeps `targetEvent`/
`targetEventId` flowing via `.passthrough()`; `type`/`total`/`totalCapped`/
`sampleActors` are explicit. Both REST and GraphQL parse to this one schema.

---

## 10. Client rendering

- `notificationsResultFromPage` maps nodes → `FeedNotification[]` (carrying the
  group fields) + the side maps.
- `buildNotificationListItems` (`notificationGroups.ts`): a node with server
  group data becomes a `group` list item directly (synthesising avatar members
  from `sampleActors`); everything else is a `single`. The old consecutive-run
  grouping survives only as the **fallback** for the ungrouped GraphQL path.
- The group row shows the avatar cluster (from `sampleActors`), the title
  ("A, B and N more …" using `total`/`totalCapped`), and — for
  repost/reaction/zap — the target post in the **same bordered container** single
  rows use (`contained`).
- The **followers-detail screen** (`NotificationFollowersScreen`) needs the full
  ungrouped follower list, so it fetches with **`grouped: false`** and pages the
  raw follow rows.

---

## 11. `grouped=false` — the escape hatch

`GET /nostr/notifications?grouped=false` returns the raw, ungrouped rows
(today's old shape, every notification its own `type:"single"` node, follows
included on every page). Used by the followers-detail screen and available for
any consumer that wants the unsummarised stream.

---

## 12. Invariants — easy to break, please don't

1. **Follows are fetched on their own window and pinned to page 1.** Don't merge
   them back into the main window — they'll flood it.
2. **`limit` = items, not rows.** The grouped path fetches a *wide* candidate
   window (limit×6) and trims to `limit` items; don't cap the store fetch at the
   raw `limit`.
3. **Client decides "has more," not the server.** Stop on "no new group
   identities," not on item count or `hasNextPage`.
4. **Cross-page dedupe is by group identity**, not event id.
5. **Grouping lives in the REST app-view only.** Keep the GraphQL resolver
   generic.
6. **FOLLOWS is a graph filter** (0/0 thresholds), and the follow group uses the
   window count (not `FollowCounts`) under it.
7. **reply/quote/mention never group.** They carry text.

---

## 13. Where the code lives

**nagg**
- `internal/clickhouse/migrations/006_notifications.sql` — candidate MV
- `internal/clickhouse/read.go` — `Notifications` (query, `recent` CTE, policy/
  reason filters), `notificationPolicyThresholds`, `FollowCounts`
- `internal/appview/handler.go` — `notifications`, `groupNotifications`,
  `parseNotificationRequest`, `NotificationRowJSON`/`NotificationGroup` shapes
- `internal/graphqlapi/schema.go` — generic resolver + `NotificationPolicy` enum

**nagg-ts**
- `src/recipes/notifications.ts` — `NotificationsInput`, policy types/thresholds
- `src/recipes/appview-feed.ts` — `notificationsAppView` (`grouped` param)
- `src/schemas.ts` — `NaggNotificationNodeSchema`
- `src/transport.ts` — app-view vs GraphQL seam

**sovran-app**
- `features/feed/data/naggFeedClient.ts` — `getNotifications`, `runNaggQuery`
  (transport preference), `notificationsResultFromPage`
- `features/feed/data/feedClient.ts` — `FeedNotification*` types
- `features/feed/lib/notificationGroups.ts` — `buildNotificationListItems`
- `features/feed/lib/notificationResults.ts` — merge + `notificationDedupeKey`
- `features/feed/screens/NotificationsScreen.tsx` — pagination + rendering
- `features/feed/screens/NotificationFollowersScreen.tsx` — `grouped:false` list
- `features/feed/stores/notificationPolicyStore.ts` — persisted policy/replyScope
- `features/settings/screens/SettingsNotificationPolicyScreen.tsx` — policy picker
