-- +module core
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

CREATE TABLE IF NOT EXISTS event_seen_relays
(
    event_id FixedString(64),
    relay LowCardinality(String),
    first_seen_at DateTime,
    last_seen_at DateTime
)
ENGINE = ReplacingMergeTree(last_seen_at)
ORDER BY (event_id, relay);

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
