-- +module mint
-- Weekly unpaid-quote probes (internal/mintprobe, executed by mintprobe.Runner).
--
-- One row per probe of one mint method/unit: the runner requests a NUT-04 mint
-- quote, never pays it, and records whether the mint marked it paid anyway and
-- signed outputs for it. A mint whose latest verdict for any method is paid is
-- a testnut (/nostr/mint/discover `testnut`). Mint-level rows (method = '')
-- record info_unreachable / no_methods so the due gate still moves. The table is
-- also the runner's state: LastMintProbes reads max(probed_at) per mint.
CREATE TABLE IF NOT EXISTS mint_quote_probes
(
    mint_url   String,
    method     LowCardinality(String),   -- NUT-04 method ('' for mint-level rows)
    unit       LowCardinality(String),
    probed_at  DateTime,
    amount     UInt64,                   -- quote amount requested, in unit
    status     LowCardinality(String),   -- mintprobe.Status
    paid       UInt8,                    -- 1 if the unpaid quote was marked paid
    issued     UInt8,                    -- 1 if the mint signed outputs for it
    error      String,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (mint_url, method, unit, probed_at);
