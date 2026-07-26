-- Bound ClickHouse's own system log tables.
--
-- Measured 2026-07-26 (via the /healthz memory probe, since this ClickHouse has
-- no public domain and railway ssh fails project-wide): the `system` database
-- held 20.22 GB against 22.96 GB for `default` — i.e. ~44% of the volume was
-- ClickHouse logging about itself. text_log alone was 8.99 GB, and it is a
-- duplicate of the server log we already read with `railway logs`. Stock
-- ClickHouse ships these tables with no effective TTL, so they grow forever.
--
-- This is disk AND memory work. Railway bills cgroup memory, which includes
-- page cache; the probe showed ClickHouse's own MemoryResident is only 1.88 GB
-- against a 6.81 GB billed average, so the bill is dominated by page cache fed
-- by disk I/O. Continuously writing and merging 20 GB of logs is a large,
-- entirely self-inflicted share of that I/O.
--
-- Why here, of all places: nagg's pre-deploy migrate is the only channel that
-- can execute SQL against this instance. The alternative — injecting a config.d
-- drop-in — means converting the ClickHouse service from an image deploy to a
-- Dockerfile build, which restarts the production database and cannot be
-- reverted from the CLI. TTLs get most of the same reclaim with none of that
-- risk, and can be widened again by editing this file.
--
-- Retention reflects what each log is actually for: query_log and part_log keep
-- a week (slow-query and merge/mutation forensics — this instance has a history
-- of mutations wedging, see retention.go), the high-frequency samplers and
-- profilers keep 1-3 days.
--
-- These ALTERs are idempotent (re-running sets the same TTL). Migrate() runs
-- with materialize_ttl_after_modify = 0, so the deploy does NOT trigger a
-- one-shot mutation over the whole table — background merges apply the TTL
-- lazily, which is what keeps this from saturating the instance. Every table
-- below was confirmed present before writing this migration; ClickHouse has no
-- ALTER TABLE IF EXISTS, so an absent table would fail the deploy.

ALTER TABLE system.text_log MODIFY TTL event_date + INTERVAL 1 DAY;

ALTER TABLE system.processors_profile_log MODIFY TTL event_date + INTERVAL 1 DAY;

ALTER TABLE system.query_views_log MODIFY TTL event_date + INTERVAL 1 DAY;

ALTER TABLE system.trace_log MODIFY TTL event_date + INTERVAL 2 DAY;

ALTER TABLE system.asynchronous_metric_log MODIFY TTL event_date + INTERVAL 2 DAY;

ALTER TABLE system.metric_log MODIFY TTL event_date + INTERVAL 3 DAY;

ALTER TABLE system.query_log MODIFY TTL event_date + INTERVAL 7 DAY;

ALTER TABLE system.part_log MODIFY TTL event_date + INTERVAL 7 DAY;
