package clickhouse

import (
	"context"
	"encoding/json"
	"time"

	"github.com/vertex-lab/nagg/internal/mintinfo"
)

// This file implements mintinfo's storage seams (mintinfo.SnapshotStore,
// ReaderStore, MintSource) over ClickHouse — see internal/mintinfo.

// PutSnapshot stores a distinct canonical document version. Plain insert;
// ReplacingMergeTree(updated_at) on (mint_url, content_hash) makes re-seeing a
// hash idempotent.
func (s *Store) PutSnapshot(ctx context.Context, mintURL, hash string, document []byte, at time.Time) error {
	return s.conn.Exec(ctx, `
		INSERT INTO mint_info_snapshots (mint_url, content_hash, document, first_seen_at, updated_at)
		VALUES (?, ?, ?, ?, now())
	`, mintURL, hash, string(document), at.UTC())
}

// PutObservation appends one poll to the observation log.
func (s *Store) PutObservation(ctx context.Context, o mintinfo.Observation) error {
	changed, reachable := uint8(0), uint8(0)
	if o.Changed {
		changed = 1
	}
	if o.Reachable {
		reachable = 1
	}
	return s.conn.Exec(ctx, `
		INSERT INTO mint_info_observations (mint_url, checked_at, content_hash, changed, reachable, updated_at)
		VALUES (?, ?, ?, ?, ?, now())
	`, o.MintURL, o.CheckedAt.UTC(), o.Hash, changed, reachable)
}

// LastMintObservations returns the poller's per-mint state: the last hash from a
// REACHABLE poll (the change basis — argMaxIf on reachable, so a failed poll
// never becomes a false new state) and the last check time of ANY poll (the
// due-gate clock, so a down mint isn't re-hammered every tick). Output aliases
// deliberately avoid the source column names — reusing "checked_at" as an alias
// makes ClickHouse resolve it inside argMaxIf as an aggregate-in-aggregate.
func (s *Store) LastMintObservations(ctx context.Context) (map[string]mintinfo.LastObservation, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT
			mint_url,
			argMaxIf(content_hash, checked_at, reachable = 1) AS last_hash,
			max(checked_at) AS last_checked
		FROM mint_info_observations
		GROUP BY mint_url
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]mintinfo.LastObservation{}
	for rows.Next() {
		var (
			url     string
			hash    string
			checked time.Time
		)
		if err := rows.Scan(&url, &hash, &checked); err != nil {
			return nil, err
		}
		out[url] = mintinfo.LastObservation{Hash: hash, CheckedAt: checked}
	}
	return out, rows.Err()
}

// MintObservations returns one mint's full poll log, oldest first — the sequence
// the reader collapses into revisions.
func (s *Store) MintObservations(ctx context.Context, mintURL string) ([]mintinfo.Observation, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT checked_at, content_hash, changed, reachable
		FROM mint_info_observations FINAL
		WHERE mint_url = ?
		ORDER BY checked_at ASC
	`, mintURL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []mintinfo.Observation
	for rows.Next() {
		var (
			o                  mintinfo.Observation
			changed, reachable uint8
		)
		if err := rows.Scan(&o.CheckedAt, &o.Hash, &changed, &reachable); err != nil {
			return nil, err
		}
		o.MintURL = mintURL
		o.Changed = changed != 0
		o.Reachable = reachable != 0
		out = append(out, o)
	}
	return out, rows.Err()
}

// MintSnapshots returns the canonical documents for the given hashes of one
// mint, keyed by hash.
func (s *Store) MintSnapshots(ctx context.Context, mintURL string, hashes []string) (map[string]mintinfo.Snapshot, error) {
	out := map[string]mintinfo.Snapshot{}
	if len(hashes) == 0 {
		return out, nil
	}
	rows, err := s.conn.Query(ctx, `
		SELECT content_hash, document, first_seen_at
		FROM mint_info_snapshots FINAL
		WHERE mint_url = ? AND content_hash IN (?)
	`, mintURL, hashes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			hash      string
			document  string
			firstSeen time.Time
		)
		if err := rows.Scan(&hash, &document, &firstSeen); err != nil {
			return nil, err
		}
		out[hash] = mintinfo.Snapshot{
			Hash:        hash,
			Document:    json.RawMessage(document),
			FirstSeenAt: firstSeen,
		}
	}
	return out, rows.Err()
}

// CashuMintURLs returns the distinct mint URLs recommended via NIP-87 kind-38000
// events tagged k=38172 (cashu mints). Values are raw u-tags — the caller
// normalizes. This is the Nostr half of the snapshot work-list (the auditor is
// the other half), mirroring how discoverMints unions the two sources.
func (s *Store) CashuMintURLs(ctx context.Context) ([]string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT DISTINCT tag_value
		FROM event_tags
		WHERE kind = 38000 AND tag_key = 'u' AND tag_value != ''
		  AND event_id IN (
		    SELECT event_id FROM event_tags
		    WHERE kind = 38000 AND tag_key = 'k' AND tag_value = '38172'
		  )
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return nil, err
		}
		out = append(out, url)
	}
	return out, rows.Err()
}
