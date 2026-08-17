-- +module nostr
-- Rebuild viewer_feed WITHOUT denormalized event bodies.
--
-- The first shape stored content/tags_json/sig per (viewer, event, kind)
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
DROP TABLE IF EXISTS viewer_feed;

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
