# Retention & ingest caps

How nagg keeps `nostr_events` (and its `event_tags` shadow) from growing without
bound, in two layers: **don't store what nobody will read** (ingest cap) and
**delete what stopped mattering** (retention rules). Both are declarative:
the whole policy is one rule list and one env-tunable cap, readable in a minute.

Everything capped or pruned is recoverable — the on-demand relay backfills
(user feed, threads, DMs, profiles) fetch anything a user actually asks for and
insert it back.

## Who is exempt: the relevance model

The recurring problem with any anti-spam cap is misclassifying real users —
especially brand-new ones — and per-author reputation lookups (Vertex) are too
expensive to run for everyone. nagg sidesteps both with a signal it already has:

1. **Known viewers** (`known_viewers` table): every pubkey that uses a Sovran
   client shows up as the *viewer* on its own notifications / DM / thread
   requests. The appview records those sightings (`WithViewerTouch`, throttled
   to 1 insert/viewer/hour). A new profile is known from its **first app
   request** — it can never be treated like a spammer. Rows expire after a year
   without a sighting.
2. **Their follows**: everyone on a known viewer's latest contact list
   (`user_contacts_latest`).

`relevance.Tracker` refreshes this exemption set from ClickHouse every 15
minutes and **fails open**: until the first successful refresh, nobody is
capped. Subject pubkeys (a profile someone browses, a feed they look at) are
deliberately NOT touched — otherwise viewing a bot's profile would exempt the
bot and its whole follow list.

## Layer 1 — ingest post cap

`NAGG_POST_CAP_PER_DAY` (default **20**, `0` disables): a non-exempt author gets
at most 20 post/repost events (kinds 1, 1111, 6, 16 — `postCapKinds` in
`internal/ingest`) ingested per UTC day; the rest are dropped at the firehose.

Why 20: measured over 30 days of prod traffic (2026-07), 11.87M posts came from
102k authors, and **90% of the volume was beyond a 20/day cap** — produced by
~1–2k firehose bridge/bot accounts. Real humans a Sovran user might later care
about rarely exceed it, and if one does, the exemption list and the on-demand
backfills cover them.

Mechanics worth knowing:
- The day is the ingestion wall-clock day, not the event's author-claimed
  `created_at`, so backdating can't dodge the cap.
- The counter map is bounded (`postCapCounterMaxEntries`); overflowing it fails
  **open**, never dropping untracked authors.
- On-demand relay backfills bypass the cap by construction (they insert via
  `Store.InsertEvents`, not the firehose pipeline) — demand-driven fetches are
  definitionally relevant.
- Drops are summarized in the `ingest.capped` log line (per flush), never
  logged per event.

## Layer 2 — declarative retention rules

`RetentionRules` in `internal/clickhouse/retention.go` — the code is the policy:

| Rule | Kinds | Keeps | Why |
| --- | --- | --- | --- |
| Replaceable | 0, 3, 10050, 10051 | latest event per author | Relays and every nagg reader only ever use the newest version; the rest is dead weight. Measured 2026-07: 80–92% of stored rows superseded (~13 GiB, mostly kind-3 contact lists). |
| Param-replaceable | 30078, 38000 | latest per (author, d-tag) | Same, keyed per d-tag. Measured: 98.7% of kind-30078 superseded (~4 GiB). |
| Unengaged old posts | 1, 1111 | posts younger than 1 year OR with any like/repost/quote/zap/direct-reply ever | Nobody reads year-old posts nobody engaged with. Small today (index is young), load-bearing as the index ages. |

Each rule deletes from `nostr_events` and cascades the same predicate to
`event_tags`. Engagement is read from the aggregate tables
(`note_like_counts` etc.), which are never pruned — so deleting an engaging
event later never "un-engages" its target.

Execution (`Store.RunRetention`, scheduled by the rollup runner):
- **Lightweight `DELETE FROM`**, submitted async (`lightweight_deletes_sync=0`):
  rows mask immediately as the mutation runs; disk space reclaims through
  background merges. No part rewrite at submit time — safe on a near-full disk.
- Runs every `NAGG_RETENTION_INTERVAL` (default 24h), first pass 10 minutes
  after boot, one `chgate` slot per pass.
- Skips a pass while >4 mutations are still pending on the two tables.
- Every pass logs one `retention rule` line per rule with the matched count.

Gift wraps (kind 1059) are deliberately **not** filtered or pruned — product
decision 2026-07: keep the whole DM firehose so a new user's history is served
without a relay round-trip.

## Levers

| Env | Default | Meaning |
| --- | --- | --- |
| `NAGG_POST_CAP_PER_DAY` | `20` | Daily post/repost cap for non-exempt authors; `0` disables. |
| `NAGG_RETENTION_INTERVAL` | `24h` | Retention cadence; `0` disables retention. |
| `NAGG_RETENTION_DRY_RUN` | `false` | Log per-rule matched counts without deleting. |

## Adding a rule

Append to `RetentionRules` with an existing policy, or implement
`RetentionPolicy.deletePredicate` for a new shape (it must render for both
`id`/`nostr_events` and `event_id`/`event_tags`). Add a predicate test in
`retention_test.go`, and record the measurement that justified the rule in the
rule's comment — every current rule cites one.
