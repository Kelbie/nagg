package clickhouse

import (
	"strings"
	"testing"
	"time"
)

func TestSanitizeVersion(t *testing.T) {
	cases := map[string]string{
		"":            "v1",
		"  ":          "v1",
		"v2.1":        "v2.1",
		"v2; DROP":    "v2DROP",
		"a-b_c.1":     "a-b_c.1",
		"'; DELETE--": "DELETE--",
	}
	for in, want := range cases {
		if got := sanitizeVersion(in); got != want {
			t.Errorf("sanitizeVersion(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBuildReplyEdgesSQL_DirectParentAndBounding(t *testing.T) {
	since := time.Unix(1_700_000_000, 0)
	sql := buildReplyEdgesSQL(since, 1234)
	for _, want := range []string{
		"INSERT INTO note_reply_edges",
		"argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'reply')",
		"argMaxIf(tag_value, tag_index, tag_key = 'e' AND marker = '')",
		"argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'root')",
		"NOT has(quote_targets, parent_id)", // quote exclusion
		"kind IN (1, 1111)",
		"created_at >= toDateTime(1700000000)", // bounded
		"LIMIT 1234",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("reply edges SQL missing %q", want)
		}
	}
}

func TestBuildDirectReplyCountsSQL_IdempotentUnion(t *testing.T) {
	sql := buildDirectReplyCountsSQL(time.Unix(1_700_000_000, 0), 10)
	if !strings.Contains(sql, "uniqState(child_id)") {
		t.Error("direct reply counts must use uniqState(child_id) for idempotent union")
	}
	if !strings.Contains(sql, "INSERT INTO note_direct_reply_counts") {
		t.Error("must insert into note_direct_reply_counts")
	}
}

func TestTargetIDsSubquery_RecencyOrderedEngagedOnly(t *testing.T) {
	sql := targetIDsSubquery(time.Unix(1_700_000_000, 0), 50000)
	for _, want := range []string{
		"max(engaged_at) AS engaged_at", // engagement recency
		"ORDER BY engaged_at DESC",      // freshest engaged notes first
		"LIMIT 50000",                   // bounded
		"note_zaps",                     // zapped notes
		"tag_key = 'e'",                 // reaction/repost/reply/quote targets
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("targetIDsSubquery missing %q", want)
		}
	}
	// Must NOT enumerate every recent post (the old UNION DISTINCT flood) nor use a
	// bare LIMIT with no ordering.
	if strings.Contains(sql, "UNION DISTINCT") {
		t.Error("targetIDsSubquery should not UNION DISTINCT the full recent-post set")
	}
	if strings.Contains(sql, "kind IN (1, 1111)") {
		t.Error("targetIDsSubquery should not enumerate all recent posts")
	}
}

func TestBuildEngagementRealSQL_ScoredActorsSelfExclusionAndOverwrite(t *testing.T) {
	since := time.Unix(1_700_000_000, 0)
	computedAt := time.Unix(1_700_009_999, 0)
	sql := buildEngagementRealSQL(since, 500, Thresholds{MinActorScore: 0.42, Version: "v3"}, computedAt)
	for _, want := range []string{
		"INSERT INTO note_engagement_real",
		"sc >= 0.42",                          // threshold gate
		"et.pubkey IN (scored)",               // only scored actors
		"uniqExactIf(et.pubkey, et.pubkey != a2.author)", // self-exclusion
		"'v3' AS threshold_version",           // stamped version
		"toDateTime(1700009999) AS computed_at",
		"note_reply_edges",                    // real replies via edge authors
		"note_zaps",                           // real zaps
		"AS real_actors",                      // combined distinct-engager count
		"uniqExact(actor)",                    // actors = distinct engagers across types
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("engagement-real SQL missing %q", want)
		}
	}
}

func TestBuildUserStatsSQL_FollowerFanInAndPosts(t *testing.T) {
	sql := buildUserStatsSQL(time.Unix(1_700_000_000, 0), 1000, time.Unix(1_700_001_000, 0))
	for _, want := range []string{
		"INSERT INTO user_stats",
		"length(u.contacts) AS following",
		"arrayJoin(contacts)",            // follower fan-in
		"user_contacts_latest FINAL",     // latest list only (fixes the bug)
		"uniqMerge(posts)",
		"WHERE follow IN (touched)",       // bounded emission
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("user-stats SQL missing %q", want)
		}
	}
}

func TestBuildRankFeaturesSQL_AssemblesRawAndReal(t *testing.T) {
	sql := buildRankFeaturesSQL(time.Unix(1_700_000_000, 0), 50000, Thresholds{Version: "v1"}, time.Unix(1_700_002_000, 0))
	for _, want := range []string{
		"INSERT INTO note_rank_features",
		"uniqMerge(likes)",
		"note_direct_reply_counts",                  // direct replies, not any-e-tag
		"uniqMerge(quotes)",
		"sumMerge(sats)",
		"note_engagement_real",                      // real columns
		"argMax(score, fetched_at) AS score",        // author vertex score
		"metric = 'contribution_quality'",
		"'v1' AS threshold_version",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("rank-features SQL missing %q", want)
		}
	}
}
