package clickhouse

import (
	"context"
	"time"
)

// RelayBackfillState is one (rule, kind, relay) watermark row of the
// relay-history backfill (rules.Backfill, executed by ingest.Backfiller).
// OldestSynced/NewestSynced are created_at unix timestamps bounding the
// contiguous range already fetched from that relay; Completed records that
// the initial walk reached relay exhaustion. Zero values mean "never synced".
type RelayBackfillState struct {
	Rule         string
	Kind         int
	Relay        string
	OldestSynced int64
	NewestSynced int64
	Completed    bool
	UpdatedAt    time.Time
}

// RelayBackfillStates returns the newest state row per (kind, relay) for one
// backfill rule.
func (s *Store) RelayBackfillStates(ctx context.Context, rule string) ([]RelayBackfillState, error) {
	// Output aliases must NOT reuse source column names: ClickHouse resolves
	// a shadowing alias inside the other aggregates ("max(updated_at) AS
	// updated_at" turns argMax(x, updated_at) into an aggregate-in-aggregate,
	// error 184).
	rows, err := s.conn.Query(ctx, `
		SELECT
			kind,
			relay,
			argMax(oldest_synced, updated_at) AS oldest_state,
			argMax(newest_synced, updated_at) AS newest_state,
			argMax(completed, updated_at) AS completed_state,
			max(updated_at) AS last_updated
		FROM relay_backfill_state
		WHERE rule = ?
		GROUP BY kind, relay
	`, rule)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []RelayBackfillState
	for rows.Next() {
		var (
			kind      uint32
			completed uint8
			st        = RelayBackfillState{Rule: rule}
		)
		if err := rows.Scan(&kind, &st.Relay, &st.OldestSynced, &st.NewestSynced, &completed, &st.UpdatedAt); err != nil {
			return nil, err
		}
		st.Kind = int(kind)
		st.Completed = completed != 0
		out = append(out, st)
	}
	return out, rows.Err()
}

// UpsertRelayBackfillState checkpoints a backfill watermark. Plain insert;
// ReplacingMergeTree(updated_at) collapses the per-page rewrites.
func (s *Store) UpsertRelayBackfillState(ctx context.Context, st RelayBackfillState) error {
	completed := uint8(0)
	if st.Completed {
		completed = 1
	}
	return s.conn.Exec(ctx, `
		INSERT INTO relay_backfill_state
			(rule, kind, relay, oldest_synced, newest_synced, completed, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, now64(3))
	`, st.Rule, uint32(st.Kind), st.Relay, st.OldestSynced, st.NewestSynced, completed)
}
