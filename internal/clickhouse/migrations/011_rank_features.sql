-- Per-event rank-feature table — the single row the For-You / trending hot path
-- scans. It replaces the per-request, per-term live aggregation
-- (weightedRankBaseScores) and the in-memory base-pool cache with one indexed
-- ClickHouse scan: recency-decay + weighted sum over precomputed columns, weights
-- supplied as bind params, ORDER BY score DESC LIMIT N.
--
-- Assembled by the rollup job each tick over the bounded recent/engaged event set:
--   rule aggregates (agg_*) (uniqMerge / sumMerge)
--   gated_* from gated_ref_counts FINAL
--   author_score / author_followers from vertex_scores (argMax by fetched_at)
--   contribution_quality from derived_metrics (argMax by computed_at)
--
-- ReplacingMergeTree(computed_at) keyed by (created_at, event_id): recompute
-- overwrites, never additive. ORDER BY leads with created_at so the trending scan
-- (created_at >= now() - INTERVAL N HOUR) is a partition-pruned range scan.

CREATE TABLE IF NOT EXISTS rank_features
(
    event_id             FixedString(64),
    pubkey               FixedString(64),
    kind                 UInt32,
    created_at           DateTime,
    k7_e_actors            UInt64,
    k6_16_e_actors          UInt64,
    k1_1111_e_reply_sources          UInt64,
    k1_q_sources           UInt64,
    k9735_e_sources             UInt64,
    k9735_e_value_total         UInt64,
    gated_k7_e_actors           UInt64,
    gated_k6_16_e_actors         UInt64,
    gated_k1_1111_e_reply_sources         UInt64,
    gated_k1_q_sources          UInt64,
    gated_k9735_e_sources            UInt64,
    gated_k9735_e_value_total        UInt64,
    gated_actors          UInt64,
    author_score  Float64,
    author_followers     UInt64,
    contribution_quality Float64,
    threshold_version    LowCardinality(String),
    computed_at          DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, event_id);
