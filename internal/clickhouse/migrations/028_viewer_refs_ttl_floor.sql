-- Extend viewer_refs retention from 45 to 120 days so notification candidates
-- survive the NAGG_HISTORY_FLOOR deep-history walk.
--
-- The 45-day TTL (016, restated in 019's CREATE) was sized for "nothing reads
-- beyond the recent window" — true then, false now on two counts: the
-- notifications read falls back to the legacy full-history query for pages the
-- 14-day read-model can't fill, and the floor walker deliberately ingests
-- months of history. Worse, the walker's inserts into old monthly partitions
-- trigger merges that ENFORCE the dormant TTL, so backfilled candidates (and
-- the pre-existing tail) were being swept minutes after arrival. 120 days
-- covers the 2026-05-10 floor with margin; disk is the trade watched here
-- (the table was ~8 GiB unbounded before 016).
--
-- materialize_ttl_after_modify=0 keeps the ALTER metadata-only, as in 016.
ALTER TABLE viewer_refs
    MODIFY TTL created_at + INTERVAL 120 DAY
    SETTINGS materialize_ttl_after_modify = 0;

-- Re-derive the candidate rows the old TTL already swept. Bounded to the
-- 44..121-day window: younger rows never expired, older ones sit beyond the
-- new TTL anyway. The tag_key prefix prunes event_tags to its p-tag range and
-- ReplacingMergeTree collapses re-inserted duplicates, so re-running this on
-- every deploy converges (same contract as 019's original derivation).
INSERT INTO viewer_refs (viewer, event_id, actor_pubkey, kind, created_at)
SELECT
    tag_value AS viewer,
    event_id,
    pubkey AS actor_pubkey,
    kind,
    created_at
FROM event_tags
WHERE tag_key = 'p'
  AND length(tag_value) = 64
  AND kind IN (1, 3, 6, 7, 16, 9735)
  AND pubkey != tag_value
  AND created_at >= now() - INTERVAL 121 DAY
  AND created_at < now() - INTERVAL 44 DAY;
