-- +module core
-- Make id IN (...) lookups on nostr_events granule-prunable.
--
-- id is the LAST component of ORDER BY (kind, created_at, pubkey, id), so
-- every hydration-by-id read — eventsByID behind each notifications page,
-- the notifications tick's candidate_events/referenced_events subqueries,
-- thread hydration — scans the table's narrow columns end to end. Measured
-- live: the tick paid ~1.5 GiB per minute across its two id IN subqueries,
-- and the top user-facing read shape averaged 641 MiB per page.
--
-- A per-granule bloom filter answers "can this granule contain any of these
-- ids" from a small in-memory index. Page-sized id sets (~50) prune to a few
-- dozen granules out of the whole table. MATERIALIZE reads only the id
-- column (~330 MB) and runs in the background.

ALTER TABLE nostr_events ADD INDEX IF NOT EXISTS nostr_events_id_bloom id TYPE bloom_filter(0.01) GRANULARITY 1;

ALTER TABLE nostr_events MATERIALIZE INDEX nostr_events_id_bloom;
