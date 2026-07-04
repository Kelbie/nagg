-- Rebuild notifications_feed WITHOUT denormalized event bodies.
--
-- The first shape stored content/tags_json/sig per (viewer, event, reason)
-- row — a mention with N p-tagged viewers duplicated its content N times.
-- At only 518k rows it consumed ~14 GiB of a 137 GiB volume (free space hit
-- 988 KiB and inserts began failing with code 243); a full history at that
-- shape could never fit. The model now stores ids + precomputed flags only —
-- the expensive part of the legacy read was the reply/tag aggregation, never
-- the final hydrate-by-id, which the read path now does over one bounded
-- IN-list (page-sized).
--
-- The table was empty when this shipped (truncated during the incident), so
-- DROP + CREATE is a metadata-only rebuild; the rollup cursors are reset
-- operationally so the walker re-covers history into the slim shape.
DROP TABLE IF EXISTS notifications_feed;

CREATE TABLE IF NOT EXISTS notifications_feed
(
    viewer                FixedString(64),
    created_at            DateTime,
    event_id              FixedString(64),
    reason                LowCardinality(String),
    actor_pubkey          FixedString(64),
    event_pubkey          FixedString(64),
    event_kind            UInt32,
    event_created_at      DateTime,
    is_reply              UInt8,
    direct_parent_author  String DEFAULT '',
    replies_viewer_thread UInt8,
    actor_score           Float64,
    computed_at           DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (viewer, created_at, event_id, reason)
TTL created_at + INTERVAL 14 DAY;
