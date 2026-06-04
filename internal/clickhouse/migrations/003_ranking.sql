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
