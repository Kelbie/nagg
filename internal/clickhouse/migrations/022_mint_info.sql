-- Mint-info snapshots (internal/mintinfo, executed by mintinfo.Snapshotter).
--
-- Two tables mirror the two facts the feature records: a SNAPSHOT is a distinct
-- canonical NUT-06 document (stored full, only when it changes); an OBSERVATION
-- is a single daily poll (cheap, every time, even no-change). The observation
-- log is also the poller's state — the due-check and the last-seen hash both
-- come from it, so there is no separate cursor table. Both follow the repo's
-- state-table idiom: ReplacingMergeTree(updated_at), read with argMax / FINAL.

-- One row per distinct canonical document. Idempotent: re-seeing a hash (a mint
-- reverting to an old config) is a no-op merge. first_seen_at is informational —
-- the history timeline derives its timestamps from the observation log, not from
-- here — so a revert re-stamping it is harmless.
CREATE TABLE IF NOT EXISTS mint_info_snapshots
(
    mint_url      String,            -- normalized (lower, no trailing slash)
    content_hash  FixedString(64),   -- sha256 of the canonical document
    document      String,            -- canonical NUT-06 JSON (volatile stripped, keys sorted)
    first_seen_at DateTime,
    updated_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (mint_url, content_hash);

-- One row per poll. No blob — just which hash we saw, when, and whether it
-- changed / was reachable. A failed poll writes an empty hash with reachable=0;
-- LastMintObservations reads the last hash from reachable rows only, so a down
-- mint never triggers a false re-snapshot on recovery.
CREATE TABLE IF NOT EXISTS mint_info_observations
(
    mint_url     String,
    checked_at   DateTime,
    content_hash String,             -- '' when unreachable
    changed      UInt8,              -- 1 if different from the prior reachable observation
    reachable    UInt8,              -- 0 if the poll failed (mint down / bad body)
    updated_at   DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (mint_url, checked_at);
