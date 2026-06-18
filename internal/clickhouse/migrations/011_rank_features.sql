-- Per-event rank-feature table — the single row the For-You / trending hot path
-- scans. It replaces the per-request, per-term live aggregation
-- (weightedRankBaseScores) and the in-memory base-pool cache with one indexed
-- ClickHouse scan: recency-decay + weighted sum over precomputed columns, weights
-- supplied as bind params, ORDER BY score DESC LIMIT N.
--
-- Assembled by the rollup job each tick over the bounded recent/engaged event set:
--   raw_*   from the note_*_counts aggregates (uniqMerge / sumMerge)
--   real_*  from note_engagement_real FINAL
--   author_vertex_score / author_followers from vertex_scores (argMax by fetched_at)
--   contribution_quality from derived_metrics (argMax by computed_at)
--
-- ReplacingMergeTree(computed_at) keyed by (created_at, event_id): recompute
-- overwrites, never additive. ORDER BY leads with created_at so the trending scan
-- (created_at >= now() - INTERVAL N HOUR) is a partition-pruned range scan.

CREATE TABLE IF NOT EXISTS note_rank_features
(
    event_id             FixedString(64),
    pubkey               FixedString(64),
    kind                 UInt32,
    created_at           DateTime,
    raw_likes            UInt64,
    raw_reposts          UInt64,
    raw_replies          UInt64,
    raw_quotes           UInt64,
    raw_zaps             UInt64,
    raw_zap_sats         UInt64,
    real_likes           UInt64,
    real_reposts         UInt64,
    real_replies         UInt64,
    real_quotes          UInt64,
    real_zaps            UInt64,
    real_zap_sats        UInt64,
    real_actors          UInt64,
    author_vertex_score  Float64,
    author_followers     UInt64,
    contribution_quality Float64,
    threshold_version    LowCardinality(String),
    computed_at          DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, event_id);
