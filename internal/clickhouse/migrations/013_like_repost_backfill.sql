-- Backfill historical like / repost counts.
--
-- 002_appview created note_like_counts and note_repost_counts with their
-- materialized views, but an MV only fires on NEW inserts. Unlike the direct-reply
-- (007) and quote (008) aggregates, the like/repost tables never got a one-time
-- historical backfill, so every note ingested before nagg saw its engagement read
-- ZERO likes/reposts in NoteStats — the feed/thread showed no aggregate counts.
--
-- Backfill from event history here, the same idempotent way as 007/008: uniqState
-- over the engager pubkey is a set union, so re-running these INSERTs on every
-- deploy converges instead of double-counting. The SELECTs match
-- mv_note_like_counts / mv_note_repost_counts (002) EXACTLY so the backfilled
-- state merges cleanly with the rows the MVs add going forward.
--
-- The reconciler parses CREATE TABLE / CREATE MATERIALIZED VIEW names only, so
-- these backfill INSERTs do not perturb the declarative schema.

INSERT INTO note_like_counts
SELECT
    tag_value AS target_event_id,
    uniqState(pubkey) AS likes
FROM event_tags
WHERE kind = 7 AND tag_key = 'e' AND length(tag_value) = 64
GROUP BY target_event_id;

INSERT INTO note_repost_counts
SELECT
    tag_value AS target_event_id,
    uniqState(pubkey) AS reposts
FROM event_tags
WHERE kind IN (6, 16) AND tag_key = 'e' AND length(tag_value) = 64
GROUP BY target_event_id;
