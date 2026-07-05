-- Terminology purge: the concept-named tables (note_reply_edges,
-- note_engagement_real, note_rank_features, notification_candidates,
-- notifications_feed, user_stats) were renamed to kind/reference vocabulary.
-- The earlier migration files declare the new names for fresh databases, but
-- databases that already recorded those files in schema_migrations skip them
-- — this migration re-runs the renamed CREATEs (all IF NOT EXISTS, so it is a
-- no-op on a fresh database) and seeds the rebuildable ones from raw history.
-- The schema reconciler drops the old-named tables.

CREATE TABLE IF NOT EXISTS ref_edges
(
    source_id     FixedString(64),
    target_id     FixedString(64),
    source_pubkey FixedString(64),
    kind          UInt32,
    created_at    DateTime
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (target_id, source_id);

CREATE TABLE IF NOT EXISTS gated_ref_counts
(
    event_id                FixedString(64),
    k7_e_actors             UInt64,
    k6_16_e_actors          UInt64,
    k1_1111_e_reply_sources UInt64,
    k1_q_sources            UInt64,
    k9735_e_sources         UInt64,
    k9735_e_value_total     UInt64,
    actors                  UInt64,
    threshold_version       LowCardinality(String),
    computed_at             DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (event_id, threshold_version)
TTL computed_at + INTERVAL 3 DAY;

CREATE TABLE IF NOT EXISTS rank_features
(
    event_id                      FixedString(64),
    pubkey                        FixedString(64),
    kind                          UInt32,
    created_at                    DateTime,
    k7_e_actors                   UInt64,
    k6_16_e_actors                UInt64,
    k1_1111_e_reply_sources       UInt64,
    k1_q_sources                  UInt64,
    k9735_e_sources               UInt64,
    k9735_e_value_total           UInt64,
    gated_k7_e_actors             UInt64,
    gated_k6_16_e_actors          UInt64,
    gated_k1_1111_e_reply_sources UInt64,
    gated_k1_q_sources            UInt64,
    gated_k9735_e_sources         UInt64,
    gated_k9735_e_value_total     UInt64,
    gated_actors                  UInt64,
    author_score                  Float64,
    author_followers              UInt64,
    contribution_quality          Float64,
    threshold_version             LowCardinality(String),
    computed_at                   DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, event_id)
TTL created_at + INTERVAL 3 DAY;

CREATE TABLE IF NOT EXISTS viewer_refs
(
    viewer FixedString(64),
    event_id FixedString(64),
    actor_pubkey FixedString(64),
    kind UInt32,
    created_at DateTime
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (viewer, created_at, event_id)
TTL created_at + INTERVAL 45 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_viewer_refs
TO viewer_refs
AS
SELECT
    tag_value AS viewer,
    event_id,
    pubkey AS actor_pubkey,
    kind,
    created_at
FROM event_tags
WHERE tag_key = 'p'
  AND length(tag_value) = 64
  AND kind IN (1, 3, 6, 7, 16, 9735)
  AND pubkey != tag_value;

CREATE TABLE IF NOT EXISTS viewer_feed
(
    viewer                FixedString(64),
    created_at            DateTime,
    event_id              FixedString(64),
    kind                  UInt32,
    actor_pubkey          FixedString(64),
    event_pubkey          FixedString(64),
    event_kind            UInt32,
    event_created_at      DateTime,
    is_ref                UInt8,
    target_author         String DEFAULT '',
    in_viewer_tree        UInt8,
    actor_score           Float64,
    computed_at           DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY (viewer, created_at, event_id, kind)
TTL created_at + INTERVAL 14 DAY;

CREATE TABLE IF NOT EXISTS pubkey_stats
(
    pubkey           FixedString(64),
    k3_out           UInt64,
    k3_in            UInt64,
    k1_1111_authored UInt64,
    computed_at      DateTime
)
ENGINE = ReplacingMergeTree(computed_at)
ORDER BY pubkey;

-- Seed the rebuildable renamed tables from raw history (empty-table inserts:
-- viewer_refs from p-tag references, ref_edges via the NIP-10 direct-parent
-- coalesce). viewer_feed and the rollup-fed tables repopulate from their
-- watermarks/ticks.

INSERT INTO viewer_refs
SELECT
    tag_value AS viewer,
    event_id,
    pubkey AS actor_pubkey,
    kind,
    created_at
FROM event_tags
WHERE tag_key = 'p'
  AND length(tag_value) = 64
  AND kind IN (1, 3, 6, 7, 16, 9735)
  AND pubkey != tag_value;

INSERT INTO ref_edges
SELECT
    source_id,
    target_id,
    source_pubkey,
    kind,
    created_at
FROM (
    SELECT
        event_id AS source_id,
        any(pubkey) AS source_pubkey,
        any(kind) AS kind,
        any(created_at) AS created_at,
        coalesce(
            nullIf(argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'reply'), ''),
            nullIf(argMaxIf(tag_value, tag_index, tag_key = 'e' AND marker = ''), ''),
            nullIf(argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'root'), '')
        ) AS target_id,
        groupArrayIf(tag_value, tag_key = 'q') AS quote_targets
    FROM (
        SELECT
            event_id,
            pubkey,
            kind,
            created_at,
            tag_key,
            tag_value,
            tag_index,
            lower(if(length(tag_extra) >= 2, tag_extra[2], '')) AS marker
        FROM event_tags
        WHERE kind IN (1, 1111)
          AND tag_key IN ('e', 'q')
          AND length(tag_value) = 64
    )
    GROUP BY event_id
)
WHERE target_id != ''
  AND length(target_id) = 64
  AND NOT has(quote_targets, target_id);
