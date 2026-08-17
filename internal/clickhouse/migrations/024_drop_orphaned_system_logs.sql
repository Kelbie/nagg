-- +module core
-- Reclaim the orphaned `_0` system log tables (~20.2 GB, ~40% of the volume).
--
-- Migration 023 set TTLs with ALTER TABLE ... MODIFY TTL. That changed each
-- table's structure, so on ClickHouse's next restart the server found a
-- mismatch against the schema it expects for a system log, renamed the existing
-- table to `<name>_0`, and created a fresh one — without the TTL. Net effect
-- measured 2026-07-28: the old data was orphaned rather than expired, and the
-- volume grew 45.88 -> 52.47 GB.
--
-- The `_0` tables are inert: ClickHouse writes only to the live names, nothing
-- reads them, and they have no TTL so they would sit there forever. DROP is the
-- only way back. Sizes at time of writing: text_log_0 9.00, trace_log_0 3.56,
-- processors_profile_log_0 2.31, part_log_0 1.65, query_log_0 1.50,
-- asynchronous_metric_log_0 1.06, query_views_log_0 0.58, metric_log_0 0.56.
--
-- DROP TABLE is metadata + async file removal, not a mutation, so this does not
-- rewrite parts and cannot wedge the instance the way a big ALTER DELETE can
-- (see retention.go's ErrRetentionBusy notes).
--
-- Going forward the live log tables are bounded by ClickHouse config
-- (config.d/system-logs.xml on the ClickHouse image), NOT by ALTER — repeating
-- the ALTER here would just produce `_1`, `_2`, ... on each restart.

DROP TABLE IF EXISTS system.text_log_0;

DROP TABLE IF EXISTS system.trace_log_0;

DROP TABLE IF EXISTS system.processors_profile_log_0;

DROP TABLE IF EXISTS system.part_log_0;

DROP TABLE IF EXISTS system.query_log_0;

DROP TABLE IF EXISTS system.asynchronous_metric_log_0;

DROP TABLE IF EXISTS system.query_views_log_0;

DROP TABLE IF EXISTS system.metric_log_0;
