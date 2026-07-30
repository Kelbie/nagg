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

// The store's window clamp must admit the grouped handler's wide candidate
// windows (bodyWindow tops out at 600). The old <=100 clamp silently shrank
// the 300-row body window to 50, which buried the notification mixture and
// made the saturation ("has more") signal unreachable.
func TestViewerFeedWindowLimit(t *testing.T) {
	cases := []struct {
		in   uint64
		want uint64
	}{
		{0, 50},
		{12, 12},
		{50, 50},
		{300, 300},
		{600, 600},
		{601, 600},
	}
	for _, c := range cases {
		if got := viewerFeedWindowLimit(c.in); got != c.want {
			t.Errorf("viewerFeedWindowLimit(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}

// A short read-model page means the rest of the viewer's history lives beyond
// the model's retention floor — the request must fall back to the legacy
// full-history read instead of serving a truncated, unpageable page.
func TestNotificationsModelPageShort(t *testing.T) {
	if !notificationsModelPageShort(0, 50) || !notificationsModelPageShort(49, 50) {
		t.Error("a page below the window must count as short")
	}
	if notificationsModelPageShort(50, 50) || notificationsModelPageShort(51, 50) {
		t.Error("a filled window must not count as short")
	}
}

// The MENTIONS tab is exempt from the DIRECT/THREAD reply-scope filter: its
// meaning is "kind-1 events that tag you", and most of those live inside other
// people's threads — the scope filter dropped nearly every real mention.
func TestReplyScopeApplies(t *testing.T) {
	cases := []struct {
		tab, scope string
		want       bool
	}{
		{"ALL", "DIRECT", true},
		{"ALL", "THREAD", true},
		{"ALL", "NONE", false},
		{"MENTIONS", "DIRECT", false},
		{"MENTIONS", "THREAD", false},
	}
	for _, c := range cases {
		if got := replyScopeApplies(c.tab, c.scope); got != c.want {
			t.Errorf("replyScopeApplies(%q, %q) = %v, want %v", c.tab, c.scope, got, c.want)
		}
	}
}

// The page read must overlay LIVE vertex scores over the baked actor_score:
// history slices never re-run, so a baked-only filter freezes whatever the
// graph knew at denormalization time and hides actors who score up later.
func TestNotificationsFromFeedQuery_OverlaysLiveActorScores(t *testing.T) {
	for _, want := range []string{
		"if(empty(sc.pubkey), vf.actor_score, sc.score) AS actor_score",
		"argMax(score, fetched_at) AS score",
		"ON sc.pubkey = vf.actor_pubkey",
	} {
		if !strings.Contains(notificationsFromFeedQueryTemplate, want) {
			t.Errorf("live-score overlay %q missing from page query", want)
		}
	}
}
