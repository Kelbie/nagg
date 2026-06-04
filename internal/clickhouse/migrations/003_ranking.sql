CREATE TABLE IF NOT EXISTS vertex_scores
(
    source LowCardinality(String) DEFAULT 'vertex',
    pubkey FixedString(64),
    score Float64,
    rank Float64,
    followers UInt64,
    nodes UInt64,
    fetched_at DateTime
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (source, pubkey);

CREATE TABLE IF NOT EXISTS vertex_search_cache
(
    query_norm String,
    sort LowCardinality(String) DEFAULT 'globalPagerank',
    source String DEFAULT '',
    requested_limit UInt64,
    position UInt64,
    pubkey FixedString(64),
    rank Nullable(Float64),
    score Nullable(Float64),
    nodes UInt64,
    fetched_at DateTime
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (query_norm, sort, source, requested_limit, position, pubkey);
