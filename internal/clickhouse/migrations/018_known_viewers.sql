-- +module nostr
-- Known viewers: pubkeys that have actually used a Sovran client against this
-- app-view (observed as the viewer on notifications / DM / thread reads). This
-- is the no-cost "is this a real Sovran user?" signal that the ingest post cap
-- and the retention rules use as their exemption root — a brand-new profile is
-- known from its first app request, with no Vertex (reputation) lookup.
--
-- ReplacingMergeTree(last_seen_at) keyed by pubkey collapses the throttled
-- once-an-hour touches to one row per viewer. The TTL lets exemption lapse for
-- accounts that stop using Sovran for a year; using the app again re-adds them.
CREATE TABLE IF NOT EXISTS known_viewers
(
    pubkey       FixedString(64),
    last_seen_at DateTime
)
ENGINE = ReplacingMergeTree(last_seen_at)
ORDER BY pubkey
TTL last_seen_at + INTERVAL 365 DAY;
