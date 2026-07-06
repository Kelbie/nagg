-- Relay-history backfill watermarks (rules.Backfill, executed by
-- ingest.Backfiller).
--
-- One row per (rule, kind, relay): how deep into that relay's stored history
-- the initial walk has reached (oldest_synced), the top of the contiguous
-- synced range (newest_synced, a created_at unix timestamp), and whether the
-- initial walk hit relay exhaustion (completed). The walker checkpoints after
-- every page, so a partial walk resumes where it stopped — completeness is
-- carried here explicitly instead of being inferred from "some events exist",
-- which is how a partial fetch gets mistaken for a finished one.
-- ReplacingMergeTree(updated_at) collapses the per-page checkpoint rewrites;
-- readers take the newest row per key via argMax.
CREATE TABLE IF NOT EXISTS relay_backfill_state
(
    rule LowCardinality(String),
    kind UInt32,
    relay LowCardinality(String),
    oldest_synced Int64,
    newest_synced Int64,
    completed UInt8,
    updated_at DateTime64(3) DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (rule, kind, relay);
