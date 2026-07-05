package clickhouse

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// The read-model rollup must window on ARRIVAL time (viewer_refs.ingested_at),
// not event created_at. Event-time windows permanently lose late-delivered
// history: after a wipe-and-relisten, relay backfills delivered weeks-old
// likes/replies hours after the backward walker had passed their created_at
// windows, and the read-model served follows only (kind-3 keeps flowing solely
// because contact lists are republished with fresh timestamps).
func TestBuildNotificationsFeedSQL_WindowsOnArrivalTime(t *testing.T) {
	from := time.Unix(1_700_000_000, 0)
	to := time.Unix(1_700_003_600, 0)
	sql := buildNotificationsFeedSQL(from, to, to)

	for _, want := range []string{
		"ingested_at >= toDateTime(1700000000)",
		"ingested_at < toDateTime(1700003600)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("window predicate %q missing:\n%s", want, sql)
		}
	}
	// The window bounds must not leak into created_at predicates anywhere —
	// that was the exact shape of the old, history-losing window.
	if re := regexp.MustCompile(`created_at\s*(>=|<)\s*toDateTime\((1700000000|1700003600)\)`); re.MatchString(sql) {
		t.Errorf("created_at compared against window bounds — window must be arrival-time only:\n%s", sql)
	}
}

// With an arrival window, candidates' event times are unbounded, so the
// event_tags scans must derive their granule-pruning bounds from the
// candidates themselves — losing the bound regresses to the unbounded scan
// that read >100M tag rows per one-hour slice.
func TestBuildNotificationsFeedSQL_TagScanBoundedByCandidateSpan(t *testing.T) {
	sql := buildNotificationsFeedSQL(time.Unix(0, 0), time.Unix(1, 0), time.Unix(1, 0))

	for _, want := range []string{
		"created_at >= (SELECT lo FROM bounds)",
		"created_at <= (SELECT hi FROM bounds)",
		"event_id IN (SELECT event_id FROM window_candidates)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("candidate_tags bound %q missing:\n%s", want, sql)
		}
	}
}
