-- +module nostr
-- Per-pubkey aggregates: latest outbound kind-3 references, inbound fan-in, authored events.
--
-- These are computed live today in scattered places, and the follower count is
-- WRONG: BatchFollowCounts / RecentAuthorPubkeysByFollowers count every pubkey
-- that EVER p-tagged you across all kind-3 history, ignoring that kind 3 is
-- replaceable (only the latest contact list per author counts). This migration
-- centralizes the aggregates and fixes follower counting.

-- The latest-kind-3 projection that lived here (user_contacts_latest +
-- mv_user_contacts_latest) is RETIRED: it is now the registry projection
-- latest_k3 (field `refs` = 64-hex p-tag values), generated at migrate time.

-- Denormalized per-user stats read by the API. following = length of the latest
-- contact list; followers = fan-in (how many users' LATEST list contains you) —
-- too heavy for a row-trigger MV, so the rollup job computes followers for touched
-- pubkeys into this table. ReplacingMergeTree(computed_at) keeps the latest row.
CREATE TABLE IF NOT EXISTS pubkey_stats
(
    pubkey      FixedString(64),
    k3_out          UInt64,
    k3_in           UInt64,
    k1_1111_authored UInt64,
    computed_at DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY pubkey;

-- Backfill the incremental aggregates over history (idempotent: contacts is
-- replaced by latest created_at, posts is a uniq set union).


