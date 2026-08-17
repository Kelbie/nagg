-- +module nostr
-- The engagement aggregates that used to open this file (note_like_counts,
-- note_repost_counts, note_zaps, note_zap_totals and their MVs, plus the
-- legacy note_reply_counts) are RETIRED: kind-to-kind aggregations are now
-- declared in the rules registry (internal/rules), which generates the
-- agg_<rule> tables, their materialized views, and the event_refs landing
-- table at migrate time. The schema reconciler drops the undeclared legacy
-- tables on deploy.

-- The latest-kind-0 projection that lived here (profiles_latest +
-- mv_profiles_latest) is RETIRED: latest-per-author extractions are now
-- declared as registry projections (internal/rules Projection), which
-- generate latest_k0 and its materialized view at migrate time.

-- vertex_profile_cache is RETIRED from static SQL: the Vertex DVM plugin
-- (internal/vertex/plugin.go) declares its own cache tables through the dvm
-- registry, applied at migrate time alongside the rule-generated schema.
