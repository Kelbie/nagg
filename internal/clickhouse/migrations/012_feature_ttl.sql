-- Bound the rollup-maintained hot-path feature tables.
--
-- The rollup re-inserts its whole target set every tick, and old notes' rows
-- persist indefinitely, so these tables grow without limit and accumulate parts
-- that the background merges keep collapsing — merge work that competes with reads
-- on the capacity-limited ClickHouse. For-You reads only the recent window
-- (featureRankWindow, 48h) and is partition-pruned by created_at, so dropping rows
-- older than 3 days is invisible to reads.
--
-- These ALTERs are idempotent (re-running sets the same TTL). Migrate() runs with
-- materialize_ttl_after_modify = 0, so the deploy does NOT trigger a one-shot
-- mutation over the whole table — the TTL is applied lazily by background merges.

ALTER TABLE note_rank_features MODIFY TTL created_at + INTERVAL 3 DAY;

ALTER TABLE note_engagement_real MODIFY TTL computed_at + INTERVAL 3 DAY;
