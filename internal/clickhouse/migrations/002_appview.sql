-- The engagement aggregates that used to open this file (note_like_counts,
-- note_repost_counts, note_zaps, note_zap_totals and their MVs, plus the
-- legacy note_reply_counts) are RETIRED: kind-to-kind aggregations are now
-- declared in the rules registry (internal/rules), which generates the
-- agg_<rule> tables, their materialized views, and the event_refs landing
-- table at migrate time. The schema reconciler drops the undeclared legacy
-- tables on deploy.

CREATE TABLE IF NOT EXISTS profiles_latest
(
    pubkey FixedString(64),
    event_id FixedString(64),
    created_at DateTime,
    name String,
    display_name String,
    picture String,
    about String,
    nip05 String,
    lud16 String,
    lud06 String,
    banner String,
    website String,
    raw_json String
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY pubkey;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_profiles_latest
TO profiles_latest
AS
SELECT
    pubkey,
    id AS event_id,
    created_at,
    JSONExtractString(content, 'name') AS name,
    JSONExtractString(content, 'display_name') AS display_name,
    JSONExtractString(content, 'picture') AS picture,
    JSONExtractString(content, 'about') AS about,
    JSONExtractString(content, 'nip05') AS nip05,
    JSONExtractString(content, 'lud16') AS lud16,
    JSONExtractString(content, 'lud06') AS lud06,
    JSONExtractString(content, 'banner') AS banner,
    JSONExtractString(content, 'website') AS website,
    content AS raw_json
FROM nostr_events
WHERE kind = 0;

-- vertex_profile_cache is RETIRED from static SQL: the Vertex DVM plugin
-- (internal/vertex/plugin.go) declares its own cache tables through the dvm
-- registry, applied at migrate time alongside the rule-generated schema.
