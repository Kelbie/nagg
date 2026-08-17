-- +module nostr
-- One-time full population of pubkey_stats (010 created it rollup-fed only, so
-- coverage was 16k of 551k profiled pubkeys — every uncovered pubkey read
-- follower counts through the legacy global event_tags scan, which is both
-- wrong (counts all kind-3 history, ignoring NIP-02 replaceability) and heavy.
-- After this backfill the read path serves pubkey_stats exclusively; the rollup
-- keeps touched pubkeys fresh.
--
-- Idempotent: pubkey_stats is ReplacingMergeTree(computed_at) keyed by pubkey —
-- a re-run converges to the recomputed row. Runs once under the ledger.
INSERT INTO pubkey_stats
WITH latest_contacts AS (
    SELECT pubkey, argMax(refs, created_at) AS refs
    FROM (
        SELECT
            pubkey,
            created_at,
            arrayMap(t -> t[2],
                arrayFilter(t -> length(t) >= 2 AND t[1] = 'p' AND length(t[2]) = 64,
                    JSONExtract(tags_json, 'Array(Array(String))'))) AS refs
        FROM nostr_events
        WHERE kind = 3
    )
    GROUP BY pubkey
),
fan_in AS (
    SELECT arrayJoin(refs) AS pubkey, toUInt64(count()) AS followers
    FROM latest_contacts
    GROUP BY pubkey
),
post_counts AS (
    SELECT pubkey, toUInt64(uniq(id)) AS posts
    FROM nostr_events
    WHERE kind IN (1, 1111)
    GROUP BY pubkey
),
population AS (
    SELECT DISTINCT pubkey FROM
    (
        SELECT DISTINCT pubkey FROM nostr_events WHERE kind = 0
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
    toUInt64(length(ifNull(lc.refs, []))) AS k3_out,
    ifNull(fi.followers, 0) AS k3_in,
    ifNull(pc.posts, 0) AS k1_1111_authored,
    now() AS computed_at
FROM population
LEFT JOIN latest_contacts AS lc ON lc.pubkey = population.pubkey
LEFT JOIN fan_in AS fi ON fi.pubkey = population.pubkey
LEFT JOIN post_counts AS pc ON pc.pubkey = population.pubkey;
