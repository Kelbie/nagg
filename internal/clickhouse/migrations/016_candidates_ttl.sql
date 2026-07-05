-- Retention for viewer_refs (was unbounded: 115M rows / ~8 GiB on
-- a volume that hit 1.88 GiB free — CH error 243 "Cannot reserve … not enough
-- space" blocked the notifications_feed history writer).
--
-- Safe to expire: candidates are DERIVED data (rebuildable from nostr_events /
-- event_tags via the backfill command), and nothing reads them beyond the
-- recent window — the legacy notifications query scans a bounded recent slice
-- and the notifications_feed read-model keeps its own 30-day copy. 45 days
-- comfortably exceeds both.
--
-- materialize_ttl_after_modify=0 keeps this a metadata-only change; space
-- reclaims gradually through background merges instead of a deploy-time
-- rewrite the near-full disk could not absorb.
ALTER TABLE viewer_refs
    MODIFY TTL created_at + INTERVAL 45 DAY
    SETTINGS materialize_ttl_after_modify = 0;
