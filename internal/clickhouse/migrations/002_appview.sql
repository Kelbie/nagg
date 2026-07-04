CREATE TABLE IF NOT EXISTS note_like_counts
(
    target_event_id FixedString(64),
    likes AggregateFunction(uniq, FixedString(64))
)
ENGINE = AggregatingMergeTree
ORDER BY target_event_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_note_like_counts
TO note_like_counts
AS
SELECT
    tag_value AS target_event_id,
    uniqState(pubkey) AS likes
FROM event_tags
WHERE kind = 7 AND tag_key = 'e' AND length(tag_value) = 64
GROUP BY target_event_id;

CREATE TABLE IF NOT EXISTS note_repost_counts
(
    target_event_id FixedString(64),
    reposts AggregateFunction(uniq, FixedString(64))
)
ENGINE = AggregatingMergeTree
ORDER BY target_event_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_note_repost_counts
TO note_repost_counts
AS
SELECT
    tag_value AS target_event_id,
    uniqState(pubkey) AS reposts
FROM event_tags
WHERE kind IN (6, 16) AND tag_key = 'e' AND length(tag_value) = 64
GROUP BY target_event_id;

-- (note_reply_counts / mv_note_reply_counts used to live here: an any-e-tag
-- reply aggregate that over-counted grandchildren. Superseded by
-- note_direct_reply_counts (007); their CREATEs were removed so the schema
-- reconciler retires them — see 007's trailer.)

CREATE TABLE IF NOT EXISTS note_zaps
(
    zap_receipt_id FixedString(64),
    target_event_id FixedString(64),
    pubkey FixedString(64),
    created_at DateTime,
    sats UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (target_event_id, zap_receipt_id);

CREATE TABLE IF NOT EXISTS note_zap_totals
(
    target_event_id FixedString(64),
    sats AggregateFunction(sum, UInt64),
    zaps AggregateFunction(uniq, FixedString(64))
)
ENGINE = AggregatingMergeTree
ORDER BY target_event_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_note_zap_totals
TO note_zap_totals
AS
SELECT
    target_event_id,
    sumState(sats) AS sats,
    uniqState(zap_receipt_id) AS zaps
FROM note_zaps
GROUP BY target_event_id;

CREATE TABLE IF NOT EXISTS profiles_latest
(
    pubkey FixedString(64),
    event_id FixedString(64),
    created_at DateTime,
    name String,
    display_name String,
    picture String,
    about String,
    nip05 String,
    lud16 String,
    lud06 String,
    banner String,
    website String,
    raw_json String
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY pubkey;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_profiles_latest
TO profiles_latest
AS
SELECT
    pubkey,
    id AS event_id,
    created_at,
    JSONExtractString(content, 'name') AS name,
    JSONExtractString(content, 'display_name') AS display_name,
    JSONExtractString(content, 'picture') AS picture,
    JSONExtractString(content, 'about') AS about,
    JSONExtractString(content, 'nip05') AS nip05,
    JSONExtractString(content, 'lud16') AS lud16,
    JSONExtractString(content, 'lud06') AS lud06,
    JSONExtractString(content, 'banner') AS banner,
    JSONExtractString(content, 'website') AS website,
    content AS raw_json
FROM nostr_events
WHERE kind = 0;

CREATE TABLE IF NOT EXISTS vertex_profile_cache
(
    pubkey FixedString(64),
    fetched_at DateTime,
    payload String
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY pubkey;
