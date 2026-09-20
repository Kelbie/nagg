package clickhouse

import (
	"context"
	"time"

	"github.com/vertex-lab/nagg/internal/mintprobe"
)

// This file implements mintprobe.Store and the discover testnut lookup over
// ClickHouse — see internal/mintprobe.

// PutProbeResults appends one pass's probes for a mint in a single batch.
func (s *Store) PutProbeResults(ctx context.Context, results []mintprobe.Result) error {
	if len(results) == 0 {
		return nil
	}
	batch, err := s.prepareInsertBatch(ctx, `
		INSERT INTO mint_quote_probes (mint_url, method, unit, probed_at, amount, status, paid, issued, error)
	`)
	if err != nil {
		return err
	}
	defer closeUnsentBatch(batch)
	for _, r := range results {
		if err := batch.Append(
			r.MintURL, r.Method, r.Unit, r.ProbedAt.UTC(), r.Amount,
			string(r.Status), boolUInt8(r.Paid), boolUInt8(r.Issued), r.Error,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func boolUInt8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}

// LastMintProbes is the runner's due-gate clock: each mint's latest probe of
// any status.
func (s *Store) LastMintProbes(ctx context.Context) (map[string]time.Time, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT mint_url, max(probed_at) AS last_probed
		FROM mint_quote_probes
		GROUP BY mint_url
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]time.Time{}
	for rows.Next() {
		var (
			url string
			at  time.Time
		)
		if err := rows.Scan(&url, &at); err != nil {
			return nil, err
		}
		out[url] = at
	}
	return out, rows.Err()
}

// MintProbeVerdict is a mint's standing unpaid-quote verdict.
type MintProbeVerdict struct {
	// Testnut is true when the latest verdict for at least one method/unit was
	// paid: an unpaid quote the mint marked paid.
	Testnut bool
	// ProbedAt is the newest verdict's time.
	ProbedAt time.Time
}

// MintProbeVerdicts returns every mint that has a probe verdict, keyed by the
// stored mint URL. Only verdict rows count, so a week where the mint was down
// or refused the quote neither sets nor clears the flag, and a mint with no
// verdict yet is absent.
func (s *Store) MintProbeVerdicts(ctx context.Context) (map[string]MintProbeVerdict, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT mint_url, max(last_paid) AS testnut, max(last_at) AS probed_at
		FROM (
			SELECT mint_url, argMax(paid, probed_at) AS last_paid, max(probed_at) AS last_at
			FROM mint_quote_probes
			WHERE status IN ('unpaid', 'paid_not_issued', 'issued')
			GROUP BY mint_url, method, unit
		)
		GROUP BY mint_url
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]MintProbeVerdict{}
	for rows.Next() {
		var (
			url     string
			testnut uint8
			at      time.Time
		)
		if err := rows.Scan(&url, &testnut, &at); err != nil {
			return nil, err
		}
		out[url] = MintProbeVerdict{Testnut: testnut == 1, ProbedAt: at}
	}
	return out, rows.Err()
}
