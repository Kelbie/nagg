-- One-time full population of user_stats (010 created it rollup-fed only, so
-- coverage was 16k of 551k profiled pubkeys — every uncovered pubkey read
-- follower counts through the legacy global event_tags scan, which is both
-- wrong (counts all kind-3 history, ignoring NIP-02 replaceability) and heavy.
-- After this backfill the read path serves user_stats exclusively; the rollup
-- keeps touched pubkeys fresh.
--
-- Idempotent: user_stats is ReplacingMergeTree(computed_at) keyed by pubkey —
-- a re-run converges to the recomputed row. Runs once under the ledger.
INSERT INTO user_stats
WITH latest_contacts AS (
    SELECT pubkey, argMax(contacts, created_at) AS contacts
    FROM user_contacts_latest
    GROUP BY pubkey
),
fan_in AS (
    SELECT arrayJoin(contacts) AS pubkey, toUInt64(count()) AS followers
    FROM latest_contacts
    GROUP BY pubkey
),
post_counts AS (
    SELECT pubkey, toUInt64(uniqMerge(posts)) AS posts
    FROM user_post_counts
    GROUP BY pubkey
),
population AS (
    SELECT DISTINCT pubkey FROM
    (
        SELECT pubkey FROM profiles_latest
        UNION ALL
        SELECT pubkey FROM latest_contacts
        UNION ALL
        SELECT pubkey FROM fan_in
        UNION ALL
        SELECT pubkey FROM post_counts
    )
)
SELECT
    population.pubkey AS pubkey,
    toUInt64(length(ifNull(lc.contacts, []))) AS following,
    ifNull(fi.followers, 0) AS followers,
    ifNull(pc.posts, 0) AS posts,
    now() AS computed_at
FROM population
LEFT JOIN latest_contacts AS lc ON lc.pubkey = population.pubkey
LEFT JOIN fan_in AS fi ON fi.pubkey = population.pubkey
LEFT JOIN post_counts AS pc ON pc.pubkey = population.pubkey;
