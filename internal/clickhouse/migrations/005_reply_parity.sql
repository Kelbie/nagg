-- Reply-count parity: include NIP-22 comments (kind 1111) alongside kind 1 in
-- the reply aggregate so display reply counts match the For You reply rank term
-- (which counts kinds [1, 1111]). 002_appview.sql created mv_note_reply_counts
-- with kind = 1 only; CREATE ... IF NOT EXISTS there is a no-op on existing
-- deployments, so the SELECT body is updated here via DROP + CREATE.
--
-- The reconciler parses CREATE MATERIALIZED VIEW names only (DROP VIEW / INSERT
-- are ignored), so this migration does not perturb the declarative schema.

DROP VIEW IF EXISTS mv_note_reply_counts;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_note_reply_counts
TO note_reply_counts
AS
SELECT
    tag_value AS target_event_id,
    uniqState(event_id) AS replies
FROM event_tags
WHERE kind IN (1, 1111) AND tag_key = 'e' AND length(tag_value) = 64
GROUP BY target_event_id;

-- Backfill historical replies. The MV only fires on new inserts, so without
-- this the existing target table keeps the old kind-1-only aggregates and never
-- sees historical kind-1111 comments. replies is AggregateFunction(uniq, ...),
-- whose merge is a set union, so re-inserting kind-1 event_ids is idempotent —
-- backfilling the full (1, 1111) set is safe and also closes the brief
-- DROP/CREATE gap above.
INSERT INTO note_reply_counts
SELECT
    tag_value AS target_event_id,
    uniqState(event_id) AS replies
FROM event_tags
WHERE kind IN (1, 1111) AND tag_key = 'e' AND length(tag_value) = 64
GROUP BY target_event_id;
