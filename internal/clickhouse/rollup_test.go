package clickhouse

import (
	"regexp"
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
	sql := buildRefEdgesSQL(since, 1234)
	for _, want := range []string{
		"INSERT INTO ref_edges",
		"argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'reply')",
		"argMaxIf(tag_value, tag_index, tag_key = 'e' AND marker = '')",
		"argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'root')",
		"NOT has(quote_targets, target_id)", // quote exclusion
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
	sql := buildRefSourceCountsSQL(time.Unix(1_700_000_000, 0), 10)
	if !strings.Contains(sql, "uniqState(source_id)") {
		t.Error("direct reply counts must use uniqState(source_id) for idempotent union")
	}
	if !strings.Contains(sql, "INSERT INTO agg_k1_1111_e_reply") {
		t.Error("must insert into agg_k1_1111_e_reply")
	}
}

func TestTargetIDsSubquery_RecencyOrderedEngagedOnly(t *testing.T) {
	sql := targetIDsSubquery(time.Unix(1_700_000_000, 0), 50000)
	for _, want := range []string{
		"max(engaged_at) AS engaged_at", // engagement recency
		"ORDER BY engaged_at DESC",      // freshest engaged notes first
		"LIMIT 50000",                   // bounded
		"event_refs",                    // zapped notes (rule = 'k9735_e')
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
	sql := buildGatedRefCountsSQL(since, 500, Thresholds{MinActorScore: 0.42, Version: "v3"}, computedAt)
	for _, want := range []string{
		"INSERT INTO gated_ref_counts",
		// explicit column list (actors is ALTER-appended / physically last)
		"(event_id, k7_e_actors, k6_16_e_actors, k1_1111_e_reply_sources, k1_q_sources, k9735_e_sources, k9735_e_value_total, actors, threshold_version, computed_at)",
		"sc >= 0.42",            // threshold gate
		"et.pubkey IN (scored)", // only scored actors
		"uniqExactIf(et.pubkey, et.pubkey != a2.author)", // self-exclusion
		"'v3' AS threshold_version",                      // stamped version
		"toDateTime(1700009999) AS computed_at",
		"ref_edges",        // real replies via edge authors
		"event_refs",       // real zaps
		"AS actors",        // combined distinct-engager count
		"uniqExact(actor)", // actors = distinct engagers across types
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("engagement-real SQL missing %q", want)
		}
	}
}

func TestBuildPubkeyStatsSQL_FanInAndAuthored(t *testing.T) {
	sql := buildPubkeyStatsSQL(time.Unix(1_700_000_000, 0), 1000, time.Unix(1_700_001_000, 0))
	for _, want := range []string{
		"INSERT INTO pubkey_stats",
		"length(u.refs) AS k3_out",
		"arrayJoin(refs)", // fan-in over latest reference lists
		"latest_k3 FINAL", // latest list only (fixes the bug)
		"uniqMerge(sources)",
		"WHERE ref IN (touched)", // bounded emission
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("user-stats SQL missing %q", want)
		}
	}
}

func TestBuildRankFeaturesSQL_AssemblesRawAndReal(t *testing.T) {
	sql := buildRankFeaturesSQL(time.Unix(1_700_000_000, 0), 50000, Thresholds{Version: "v1"}, time.Unix(1_700_002_000, 0))
	for _, want := range []string{
		"INSERT INTO rank_features",
		// explicit column list maps by name (gated_actors is physically last)
		"gated_actors, author_score, author_followers, contribution_quality, threshold_version, computed_at)",
		"uniqMerge(actors)",
		"agg_k1_1111_e_reply", // direct replies, not any-e-tag
		"uniqMerge(sources)",
		"sumMerge(value_total)",
		"gated_ref_counts",                   // real columns
		"argMax(score, fetched_at) AS score", // author vertex score
		"metric = 'contribution_quality'",
		"'v1' AS threshold_version",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("rank-features SQL missing %q", want)
		}
	}
}

// TestRollupSQLHasNoConsecutiveWhereClauses guards the class of bug that
// silently killed the whole rollup: a table swap that left "FROM x WHERE a"
// followed by the template's own "WHERE b" — a syntax error string
// assertions cannot see. Every built statement is scanned for stacked WHEREs.
func TestRollupSQLHasNoConsecutiveWhereClauses(t *testing.T) {
	since := time.Unix(1_700_000_000, 0)
	at := time.Unix(1_700_001_000, 0)
	statements := map[string]string{
		"ref_edges":     buildRefEdgesSQL(since, 100),
		"ref_counts":    buildRefSourceCountsSQL(since, 100),
		"gated_counts":  buildGatedRefCountsSQL(since, 100, Thresholds{MinActorScore: 1, Version: "v"}, at),
		"pubkey_stats":  buildPubkeyStatsSQL(since, 100, at),
		"rank_features": buildRankFeaturesSQL(since, 100, Thresholds{Version: "v"}, at),
	}
	re := regexp.MustCompile(`(?i)\bWHERE\b[^()]*?\n\s*WHERE\b`)
	for name, sql := range statements {
		if m := re.FindString(sql); m != "" {
			t.Errorf("%s: consecutive WHERE clauses:\n%s", name, m)
		}
	}
}
