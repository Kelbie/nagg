-- Direct-reply edges and counts (NIP-10 / NIP-22).
--
-- The existing mv_note_reply_counts (002 + 005) counts ANY kind-1/1111 event that
-- carries an 'e' tag referencing a target. That over-counts: a grandchild reply
-- e-tags both its parent AND the thread root, so the root's reply count includes
-- replies-to-replies, and the thread view leaks grandchildren. NIP-10 defines a
-- single DIRECT parent per reply (reply marker, else the unmarked last 'e' tag,
-- else the root marker); quotes use 'q' tags and are not replies.
--
-- We cannot compute the direct parent in a per-row materialized view: the choice
-- needs ALL of a child's 'e' tags together. But once event_tags is GROUPed BY the
-- child event_id, every one of that child's 'e' tags is collocated, so the same
-- argMinIf/argMaxIf logic already used by the notification reply join
-- (read.go: direct_parent_id) yields one edge per child. The rollup job maintains
-- these tables incrementally for recent children; this migration creates them and
-- backfills history once.
--
-- The reconciler parses CREATE TABLE / CREATE MATERIALIZED VIEW names only (DROP /
-- INSERT are ignored), so the explicit DROPs and backfill INSERTs below do not
-- perturb the declarative schema.

-- One authoritative direct-reply edge per child reply event. ReplacingMergeTree
-- keyed by child_id: re-seeing a child overwrites its (identical) edge row.
-- ORDER BY (parent_id, child_id): a child's direct parent is fixed (event ids are
-- content-addressed, so tags never change), so (parent_id, child_id) is one row
-- per child and ReplacingMergeTree still dedups re-seen children. Leading with
-- parent_id makes "direct replies to X" an indexed range scan for the thread view.
CREATE TABLE IF NOT EXISTS note_reply_edges
(
    child_id     FixedString(64),
    parent_id    FixedString(64),
    child_pubkey FixedString(64),
    kind         UInt32,
    created_at   DateTime
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (parent_id, child_id);

-- Backfill edges over all history. Per child, pick the NIP-10 direct parent:
--   reply marker  >  unmarked last 'e' (max tag_index)  >  root marker.
-- For kind 1111 (NIP-22) comments markers are typically absent, so the unmarked
-- branch selects the lowercase parent 'e' tag. Children whose chosen parent is
-- only referenced via a 'q' tag (a quote, not a reply) are excluded.
INSERT INTO note_reply_edges
SELECT
    child_id,
    parent_id,
    child_pubkey,
    kind,
    created_at
FROM (
    SELECT
        event_id AS child_id,
        any(pubkey) AS child_pubkey,
        any(kind) AS kind,
        any(created_at) AS created_at,
        coalesce(
            nullIf(argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'reply'), ''),
            nullIf(argMaxIf(tag_value, tag_index, tag_key = 'e' AND marker = ''), ''),
            nullIf(argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'root'), '')
        ) AS parent_id,
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
WHERE parent_id != ''
  AND length(parent_id) = 64
  AND NOT has(quote_targets, parent_id);

-- The direct-reply COUNT aggregate lives in the rules registry as the
-- periodic relationship k1_1111_e_reply (agg_k1_1111_e_reply); the rollup
-- rebuilds it from these edges. The legacy note_reply_counts /
-- note_direct_reply_counts tables are undeclared and reconciled away.
