-- Vertex-backed "real" engagement counts (bot-resistant).
--
-- Raw counts (note_*_counts) count every distinct actor. A "real" count only
-- counts actors whose saved Vertex score clears a threshold, weeding out bot
-- farms. Vertex scores arrive asynchronously (the vertex syncer fetches them on a
-- 30-min cycle), so the real count cannot be a pure incremental materialized view
-- — it is recomputed by the bounded periodic rollup job (internal/rollup), which
-- joins each metric's actor set against vertex_scores.
--
-- ReplacingMergeTree(computed_at) keyed by (event_id, threshold_version): each
-- recompute OVERWRITES the prior row (never additive), so re-running the rollup is
-- idempotent and can never double-count. threshold_version lets a threshold change
-- land as a new logical row rather than silently mutating history. Reads use FINAL
-- or argMax(..., computed_at).

CREATE TABLE IF NOT EXISTS note_engagement_real
(
    event_id          FixedString(64),
    real_likes        UInt64,
    real_reposts      UInt64,
    real_replies      UInt64,
    real_quotes       UInt64,
    real_zaps         UInt64,
    real_zap_sats     UInt64,
    -- Distinct vertex-scored engagers across ALL reaction types (likes/reposts/
    -- quotes/replies/zaps), the "actors" signal the For-You rank uses as its
    -- primary engagement term.
    real_actors       UInt64,
    threshold_version LowCardinality(String),
    computed_at       DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (event_id, threshold_version);

-- Cursor / progress for the rollup job (mirrors enrichment_state in 004). One row
-- per task; ReplacingMergeTree(last_run_at) keeps the latest cursor.
CREATE TABLE IF NOT EXISTS rollup_state
(
    task              LowCardinality(String),
    cursor_created_at DateTime,
    last_run_at       DateTime,
    processed         UInt64
)
ENGINE = ReplacingMergeTree(last_run_at)
ORDER BY task;
