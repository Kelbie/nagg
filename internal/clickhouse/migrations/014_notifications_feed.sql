-- +module nostr
-- Denormalized notifications read-model (docs/notifications-performance.md §9).
--
-- The notifications read was the last per-request event_tags aggregation: reply
-- markers, parent authors, and viewer-thread references were derived at read
-- time over FINAL joins — multi-second cold for engaged accounts and the
-- heaviest sanctioned read on the instance. This table holds the notification
-- fully denormalized so the read is
--   WHERE viewer = ? [scope/policy on stored columns]
--   ORDER BY created_at DESC LIMIT n
-- with no joins and no tag scans.
--
-- Population: internal/rollup RecomputeNotificationsFeed — a fast incremental
-- tick (seconds-scale freshness) plus a progressive historical catch-up driven
-- by the rollup_state watermark. It is deliberately NOT backfilled here: a
-- deploy-time join over the 115M-row notification_candidates table is exactly
-- the deploy-blocking pattern the schema_migrations ledger removed. Reads use
-- the legacy query until the watermark catches up, then flip automatically
-- (see Store.Notifications).
--
-- actor_score drifts as vertex scores update; the rollup refreshes it on
-- rewrite. TTL bounds the table to the window the app actually pages.
CREATE TABLE IF NOT EXISTS viewer_feed
(
    viewer                FixedString(64),
    created_at            DateTime,
    event_id              FixedString(64),
    kind                  UInt32,
    actor_pubkey          FixedString(64),
    event_pubkey          FixedString(64),
    event_kind            UInt32,
    event_created_at      DateTime,
    is_ref                UInt8,
    target_author         String DEFAULT '',
    in_viewer_tree        UInt8,
    actor_score           Float64,
    computed_at           DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (viewer, created_at, event_id, kind)
TTL created_at + INTERVAL 14 DAY;
