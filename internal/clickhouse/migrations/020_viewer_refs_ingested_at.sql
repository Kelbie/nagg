-- viewer_refs gains an arrival-time column and the feeding view starts
-- stamping it. The viewer_feed read-model rollup used to window on event
-- created_at, which permanently skips history that arrives late (relay
-- backfills, post-wipe relistens): the backward history walker had already
-- passed those windows and the head slice only covers the last hour of event
-- time. Windowing on arrival time makes the read-model eventually consistent
-- with ingest no matter how old the arriving events are.
--
-- The column is also declared on the CREATE TABLE in 019 (the reconciler's
-- desired schema); this ALTER exists so already-migrated databases gain it
-- before the view below starts writing it. Pre-existing rows default to
-- created_at — the best arrival estimate available for them; the rollup task
-- keys were bumped in the same change, so the fresh backward walk re-covers
-- those rows regardless.

ALTER TABLE viewer_refs ADD COLUMN IF NOT EXISTS ingested_at DateTime DEFAULT created_at;

-- Materialized views cannot ALTER their SELECT: replace it. Tag rows inserted
-- in the instant between DROP and CREATE skip viewer_refs (milliseconds; a
-- Store.Backfill re-derives the table from event_tags if it ever matters).
DROP VIEW IF EXISTS mv_viewer_refs;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_viewer_refs
TO viewer_refs
AS
SELECT
    tag_value AS viewer,
    event_id,
    pubkey AS actor_pubkey,
    kind,
    created_at,
    now() AS ingested_at
FROM event_tags
WHERE tag_key = 'p'
  AND length(tag_value) = 64
  AND kind IN (1, 3, 6, 7, 16, 9735)
  AND pubkey != tag_value;
