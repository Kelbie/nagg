CREATE TABLE IF NOT EXISTS notification_candidates
(
    viewer FixedString(64),
    event_id FixedString(64),
    actor_pubkey FixedString(64),
    kind UInt32,
    created_at DateTime,
    reason LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (viewer, created_at, event_id, reason);

DROP TABLE IF EXISTS mv_notification_candidates;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_notification_candidates
TO notification_candidates
AS
SELECT
    tag_value AS viewer,
    event_id,
    pubkey AS actor_pubkey,
    kind,
    created_at,
    multiIf(
        kind = 3, 'follow',
        kind = 1, 'mention',
        kind IN (6, 16), 'repost',
        kind = 7, 'reaction',
        kind = 9735, 'zap',
        'mention'
    ) AS reason
FROM event_tags
WHERE tag_key = 'p'
  AND length(tag_value) = 64
  AND kind IN (1, 3, 6, 7, 16, 9735)
  AND pubkey != tag_value;
