CREATE TABLE IF NOT EXISTS trending_clusters
(
    id String,
    window LowCardinality(String),
    started_at DateTime,
    category String,
    subcategory String,
    title String,
    description String,
    centroid Array(Float32),
    event_count UInt64,
    score Float64,
    computed_at DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (window, category, score, started_at, id);
