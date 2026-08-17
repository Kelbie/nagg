-- +module nostr
-- Vertex-score-gated reference counts (bot-resistant).
--
-- Raw rule aggregates count every distinct actor. A gated count only
-- counts actors whose saved Vertex score clears a threshold, weeding out bot
-- farms. Vertex scores arrive asynchronously (the vertex syncer fetches them on a
-- 30-min cycle), so the gated count cannot be a pure incremental materialized view
-- — it is recomputed by the bounded periodic rollup job (internal/rollup), which
-- joins each metric's actor set against vertex_scores.
--
-- ReplacingMergeTree(computed_at) keyed by (event_id, threshold_version): each
-- recompute OVERWRITES the prior row (never additive), so re-running the rollup is
-- idempotent and can never double-count. threshold_version lets a threshold change
-- land as a new logical row rather than silently mutating history. Reads use FINAL
-- or argMax(..., computed_at).

CREATE TABLE IF NOT EXISTS gated_ref_counts
(
    event_id          FixedString(64),
    k7_e_actors        UInt64,
    k6_16_e_actors      UInt64,
    k1_1111_e_reply_sources      UInt64,
    k1_q_sources       UInt64,
    k9735_e_sources         UInt64,
    k9735_e_value_total     UInt64,
    -- Distinct vertex-scored engagers across ALL reaction types (likes/reposts/
    -- quotes/replies/zaps), the "actors" signal the For-You rank uses as its
    -- primary engagement term.
    actors       UInt64,
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
