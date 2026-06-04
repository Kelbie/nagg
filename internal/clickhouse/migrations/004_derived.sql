CREATE TABLE IF NOT EXISTS derived_tags
(
    event_id FixedString(64),
    pubkey FixedString(64),
    kind UInt32,
    created_at DateTime,
    tag_index UInt16,
    tag_key LowCardinality(String),
    tag_value String,
    tag_extra Array(String),
    model_version LowCardinality(String),
    computed_at DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (tag_key, tag_value, kind, created_at, event_id);

CREATE TABLE IF NOT EXISTS derived_metrics
(
    event_id FixedString(64),
    pubkey FixedString(64),
    kind UInt32,
    created_at DateTime,
    metric LowCardinality(String),
    value Float64,
    model_version LowCardinality(String),
    computed_at DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (metric, event_id);

CREATE TABLE IF NOT EXISTS event_embeddings
(
    event_id FixedString(64),
    pubkey FixedString(64),
    kind UInt32,
    created_at DateTime,
    embedding Array(Float32),
    model_version LowCardinality(String),
    computed_at DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (model_version, event_id);

CREATE TABLE IF NOT EXISTS topic_taxonomy
(
    value String,
    parent String,
    label String,
    is_default UInt8,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY value;

CREATE TABLE IF NOT EXISTS enrichment_state
(
    task LowCardinality(String),
    cursor_created_at DateTime,
    cursor_event_id FixedString(64),
    processed UInt64,
    failed UInt64,
    updated_at DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY task;
