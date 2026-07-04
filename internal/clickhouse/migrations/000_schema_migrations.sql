-- Applied-migrations ledger. Before this table existed, EVERY deploy re-ran
-- every migration — including the full-table backfill INSERT…SELECTs over
-- event_tags (2B+ rows) — twice (preDeploy + in-process), single-threaded.
-- That was survivable when the tables were small and is what made deploys
-- slow and CH-crashy at current volume.
--
-- Migrate() now skips files recorded here. Statement-level idempotency is
-- STILL required of every migration (migrations_test.go) — the ledger is a
-- fast-path, not the safety net. To deliberately re-run one migration:
--   ALTER TABLE schema_migrations DELETE WHERE name = 'NNN_xxx.sql'
-- (or use the nagg-backfill command for the count tables).
CREATE TABLE IF NOT EXISTS schema_migrations (
    name String,
    applied_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(applied_at)
ORDER BY name;
