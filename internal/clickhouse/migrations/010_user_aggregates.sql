-- Per-user aggregates: following / followers / posts.
--
-- These are computed live today in scattered places, and the follower count is
-- WRONG: BatchFollowCounts / RecentAuthorPubkeysByFollowers count every pubkey
-- that EVER p-tagged you across all kind-3 history, ignoring that kind 3 is
-- replaceable (only the latest contact list per author counts). This migration
-- centralizes the aggregates and fixes follower counting.

-- Latest contact list per user. ReplacingMergeTree(created_at) + FINAL collapses
-- kind-3 history to the single newest list per pubkey (NIP-02 replaceable). This
-- is the only new materialized view that parses tags_json, and kind 3 is
-- low-volume (one replaceable list per user), so the ingest cost is negligible.
CREATE TABLE IF NOT EXISTS user_contacts_latest
(
    pubkey     FixedString(64),
    event_id   FixedString(64),
    created_at DateTime,
    contacts   Array(FixedString(64))
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY pubkey;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_contacts_latest
TO user_contacts_latest
AS
SELECT
    pubkey,
    id AS event_id,
    created_at,
    arrayMap(t -> t[2],
        arrayFilter(t -> length(t) >= 2 AND t[1] = 'p' AND length(t[2]) = 64,
            JSONExtract(tags_json, 'Array(Array(String))'))) AS contacts
FROM nostr_events
WHERE kind = 3;

-- Denormalized per-user stats read by the API. following = length of the latest
-- contact list; followers = fan-in (how many users' LATEST list contains you) —
-- too heavy for a row-trigger MV, so the rollup job computes followers for touched
-- pubkeys into this table. ReplacingMergeTree(computed_at) keeps the latest row.
CREATE TABLE IF NOT EXISTS user_stats
(
    pubkey      FixedString(64),
    following   UInt64,
    followers   UInt64,
    posts       UInt64,
    computed_at DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY pubkey;

-- Backfill the incremental aggregates over history (idempotent: contacts is
-- replaced by latest created_at, posts is a uniq set union).
INSERT INTO user_contacts_latest
SELECT
    pubkey,
    id AS event_id,
    created_at,
    arrayMap(t -> t[2],
        arrayFilter(t -> length(t) >= 2 AND t[1] = 'p' AND length(t[2]) = 64,
            JSONExtract(tags_json, 'Array(Array(String))'))) AS contacts
FROM nostr_events
WHERE kind = 3;

