-- +module nostr
-- Prune the notifications-feed tick's viewer_refs scan by arrival time.
--
-- viewer_refs is ORDER BY (viewer, created_at, event_id), so the tick's
-- `WHERE ingested_at >= X` window candidates scan read the ENTIRE table every
-- run — measured live at 1.93 GiB per one-minute tick, 2.66 TiB/day, the
-- single largest I/O source on the instance (and, via the page cache Railway
-- bills as memory, the dominant memory cost).
--
-- Parts are written in arrival order, so a minmax index on ingested_at is
-- near-perfectly selective: a steady-state tick touches only the handful of
-- granules written in the last few minutes. Relay backfills that insert into
-- old created_at partitions still land in NEW parts whose ingested_at minmax
-- covers the tick window, so late-arriving history keeps flowing to the
-- read-model exactly as before (the arrival-window semantics are unchanged —
-- this only skips granules that provably cannot match).
--
-- MATERIALIZE INDEX is a mutation that writes only the small per-part index
-- files (it reads just the ingested_at column); it runs in the background
-- after the deploy.

ALTER TABLE viewer_refs ADD INDEX IF NOT EXISTS viewer_refs_ingested_minmax ingested_at TYPE minmax GRANULARITY 1;

ALTER TABLE viewer_refs MATERIALIZE INDEX viewer_refs_ingested_minmax;
