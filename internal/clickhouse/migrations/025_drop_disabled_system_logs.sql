-- Final system-log cleanup (~2.3 GB), after config.d/system-logs.xml landed.
--
-- Two groups, both inert:
--
-- 1. `<name>_1` — declaring a TTL in config changes the structure ClickHouse
--    expects for these tables, so on the first boot with the new config it
--    renamed the existing (TTL-less) tables aside and created fresh ones that
--    carry the TTL. This is the same rename mechanic that migration 023's ALTER
--    triggered, but it happens exactly ONCE here: from now on the config and
--    the table agree, so no further renames and no further orphans.
--
-- 2. text_log / processors_profile_log / query_views_log — removed outright in
--    config (`remove="1"`), so ClickHouse no longer creates or writes them. The
--    tables and their data survive the config change; only a DROP reclaims them.
--    They will not come back while the config keeps removing them.
--
-- Nothing reads or writes either group. Sizes when written: text_log 1.16,
-- trace_log_1 0.36, processors_profile_log 0.22, query_log_1 0.19, part_log_1
-- 0.18, asynchronous_metric_log_1 0.12, metric_log_1 0.05, query_views_log 0.05.
--
-- Do NOT add the un-suffixed trace_log / query_log / part_log / metric_log /
-- asynchronous_metric_log here: those are the live, config-TTL'd tables that
-- keep the debugging history.

DROP TABLE IF EXISTS system.text_log;

DROP TABLE IF EXISTS system.processors_profile_log;

DROP TABLE IF EXISTS system.query_views_log;

DROP TABLE IF EXISTS system.trace_log_1;

DROP TABLE IF EXISTS system.query_log_1;

DROP TABLE IF EXISTS system.part_log_1;

DROP TABLE IF EXISTS system.asynchronous_metric_log_1;

DROP TABLE IF EXISTS system.metric_log_1;
