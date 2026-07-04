package clickhouse

import (
	"context"
)

// TouchKnownViewer records that a pubkey acted as a Sovran viewer (its own
// notifications / DM / thread reads). Callers throttle (internal/relevance), so
// this is a plain insert; ReplacingMergeTree(last_seen_at) collapses repeats.
func (s *Store) TouchKnownViewer(ctx context.Context, pubkey string) error {
	return s.conn.Exec(ctx, "INSERT INTO known_viewers (pubkey, last_seen_at) VALUES (?, now())", pubkey)
}

// ExemptPubkeys returns the ingest-cap exemption set: every known Sovran viewer
// plus everyone those viewers follow (their latest kind-3 contact list). This is
// the entire "relevant author" definition — deliberately derivable from data the
// app-view already maintains, with no external reputation lookups.
func (s *Store) ExemptPubkeys(ctx context.Context) ([]string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT DISTINCT p
		FROM (
			SELECT pubkey AS p FROM known_viewers
			UNION ALL
			SELECT arrayJoin(contacts) AS p
			FROM user_contacts_latest FINAL
			WHERE pubkey IN (SELECT pubkey FROM known_viewers)
		)
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return nil, err
		}
		out = append(out, pubkey)
	}
	return out, rows.Err()
}
