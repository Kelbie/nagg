-- Quote-repost counts (NIP-18). A quote repost is a kind-1 note carrying a 'q'
-- tag that references the quoted event; it is distinct from a kind-6/16 repost
-- (which uses an 'e'/'a' tag and is already counted by mv_note_repost_counts) and
-- from a reply (which uses an 'e' tag with reply/root markers). The 'q' tag exists
-- precisely so quotes are not pulled into reply threads, so quotes get their own
-- aggregate keyed by the quoted event.
--
-- uniqState over the quoting event id keeps repeated backfills idempotent (set
-- union), matching the like/repost/reply aggregates.

CREATE TABLE IF NOT EXISTS note_quote_counts
(
    target_event_id FixedString(64),
    quotes AggregateFunction(uniq, FixedString(64))
)
ENGINE = AggregatingMergeTree
ORDER BY target_event_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_note_quote_counts
TO note_quote_counts
AS
SELECT
    tag_value AS target_event_id,
    uniqState(event_id) AS quotes
FROM event_tags
WHERE kind = 1 AND tag_key = 'q' AND length(tag_value) = 64
GROUP BY target_event_id;

-- Backfill historical quotes. The MV only fires on new inserts; replies is a
-- uniq aggregate so re-inserting the same quoting event ids is idempotent.
INSERT INTO note_quote_counts
SELECT
    tag_value AS target_event_id,
    uniqState(event_id) AS quotes
FROM event_tags
WHERE kind = 1 AND tag_key = 'q' AND length(tag_value) = 64
GROUP BY target_event_id;
