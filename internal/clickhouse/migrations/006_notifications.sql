-- +module nostr
CREATE TABLE IF NOT EXISTS viewer_refs
(
    viewer FixedString(64),
    event_id FixedString(64),
    actor_pubkey FixedString(64),
    kind UInt32,
    created_at DateTime
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (viewer, created_at, event_id);

DROP TABLE IF EXISTS mv_notification_candidates;
DROP TABLE IF EXISTS mv_viewer_refs;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_viewer_refs
TO viewer_refs
AS
SELECT
    tag_value AS viewer,
    event_id,
    pubkey AS actor_pubkey,
    kind,
    created_at
FROM event_tags
WHERE tag_key = 'p'
  AND length(tag_value) = 64
  AND kind IN (1, 3, 6, 7, 16, 9735)
  AND pubkey != tag_value;
