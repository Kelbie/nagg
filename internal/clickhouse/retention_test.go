package clickhouse

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSortedEventKindsDedupesSortsAndDropsNegativeKinds(t *testing.T) {
	got := sortedEventKinds([]int{30078, -1, 1, 30078, 0})
	want := []int{0, 1, 30078}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sortedEventKinds = %v, want %v", got, want)
	}
}

func TestEventKindRetentionMutationsUseConfiguredKindAllowlist(t *testing.T) {
	stmts := eventKindRetentionMutations([]int{0, 1, 9735})
	if len(stmts) != 6 {
		t.Fatalf("statements = %d, want 6: %v", len(stmts), stmts)
	}
	if !strings.HasPrefix(stmts[0], "ALTER TABLE event_seen_relays DELETE") {
		t.Fatalf("first statement should delete relay provenance before raw events: %s", stmts[0])
	}
	if !strings.Contains(stmts[0], "SELECT id FROM nostr_events WHERE kind NOT IN (0,1,9735)") {
		t.Fatalf("relay delete should use raw event-id subquery: %s", stmts[0])
	}
	if stmts[len(stmts)-1] != "ALTER TABLE nostr_events DELETE WHERE kind NOT IN (0,1,9735)" {
		t.Fatalf("last statement = %s", stmts[len(stmts)-1])
	}
	for _, stmt := range stmts[1:] {
		if !strings.Contains(stmt, "kind NOT IN (0,1,9735)") {
			t.Fatalf("statement did not use allowlist predicate: %s", stmt)
		}
	}
}

func TestEventKindRetentionMutationsSkipEmptyAllowlist(t *testing.T) {
	if got := eventKindRetentionMutations(nil); got != nil {
		t.Fatalf("eventKindRetentionMutations(nil) = %v, want nil", got)
	}
}

func TestShouldRebuildAppViewAfterKindPrune(t *testing.T) {
	if shouldRebuildAppViewAfterKindPrune(map[int]uint64{30078: 10}) {
		t.Fatal("app-view rebuild should not run for generic app-data events")
	}
	if !shouldRebuildAppViewAfterKindPrune(map[int]uint64{7: 1}) {
		t.Fatal("app-view rebuild should run when an engagement source kind is pruned")
	}
	if shouldRebuildAppViewAfterKindPrune(map[int]uint64{7: 0}) {
		t.Fatal("app-view rebuild should ignore zero-count pruned kinds")
	}
}

func TestRetentionRulesCoverOnlyIngestedReplaceableKinds(t *testing.T) {
	// The rule list is the declarative source of truth; keep its kind sets in
	// known shape so an accidental edit is loud.
	if len(RetentionRules) != 3 {
		t.Fatalf("RetentionRules = %d rules, want 3", len(RetentionRules))
	}
	if got, want := RetentionRules[0].Kinds, []int{0, 3, 10050, 10051}; !reflect.DeepEqual(got, want) {
		t.Fatalf("replaceable rule kinds = %v, want %v", got, want)
	}
	if got, want := RetentionRules[1].Kinds, []int{30078, 38000}; !reflect.DeepEqual(got, want) {
		t.Fatalf("param-replaceable rule kinds = %v, want %v", got, want)
	}
	if got, want := RetentionRules[2].Kinds, []int{1, 1111}; !reflect.DeepEqual(got, want) {
		t.Fatalf("engagement-age rule kinds = %v, want %v", got, want)
	}
}

func TestKeepLatestPerAuthorPredicate(t *testing.T) {
	pred := KeepLatestPerAuthor{}.deletePredicate([]int{0, 3}, "id")
	for _, want := range []string{
		"kind IN (0,3) AND id NOT IN (",
		"argMax(id, created_at)",
		"WHERE kind IN (0,3)",
		"GROUP BY kind, pubkey",
	} {
		if !strings.Contains(pred, want) {
			t.Fatalf("predicate missing %q:\n%s", want, pred)
		}
	}
	// The event_tags cascade swaps only the id column.
	tags := KeepLatestPerAuthor{}.deletePredicate([]int{0, 3}, "event_id")
	if !strings.Contains(tags, "event_id NOT IN (") {
		t.Fatalf("cascade predicate should key on event_id:\n%s", tags)
	}
	// The keep-set subquery must still select nostr_events.id regardless.
	if !strings.Contains(tags, "argMax(id, created_at)") {
		t.Fatalf("cascade keep-set must select nostr_events ids:\n%s", tags)
	}
}

func TestKeepLatestPerAuthorDTagPredicateGroupsByDTag(t *testing.T) {
	pred := KeepLatestPerAuthorDTag{}.deletePredicate([]int{30078}, "id")
	for _, want := range []string{
		"GROUP BY kind, pubkey",
		"t[1] = 'd'",
		"JSONExtract(tags_json, 'Array(Array(String))')",
	} {
		if !strings.Contains(pred, want) {
			t.Fatalf("predicate missing %q:\n%s", want, pred)
		}
	}
}

func TestMaxAgeWithoutEngagementPredicate(t *testing.T) {
	pred := MaxAgeWithoutEngagement{Age: 365 * 24 * time.Hour}.deletePredicate([]int{1, 1111}, "id")
	for _, want := range []string{
		"kind IN (1,1111)",
		"created_at < now() - INTERVAL 365 DAY",
		"note_like_counts",
		"note_repost_counts",
		"note_quote_counts",
		"note_zap_totals",
		"note_direct_reply_counts",
	} {
		if !strings.Contains(pred, want) {
			t.Fatalf("predicate missing %q:\n%s", want, pred)
		}
	}
}
