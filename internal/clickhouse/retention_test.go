package clickhouse

import (
	"reflect"
	"strings"
	"testing"
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

func TestActiveMutationCountQueryUsesCurrentDatabaseAndPendingMutations(t *testing.T) {
	query := activeMutationCountQuery()
	for _, want := range []string{"system.mutations", "database = currentDatabase()", "is_done = 0"} {
		if !strings.Contains(query, want) {
			t.Fatalf("active mutation query missing %q: %s", want, query)
		}
	}
}

func TestDiskFreeRatioQueryUsesClickHouseDisks(t *testing.T) {
	query := diskFreeRatioQuery()
	for _, want := range []string{"system.disks", "free_space", "total_space"} {
		if !strings.Contains(query, want) {
			t.Fatalf("disk free ratio query missing %q: %s", want, query)
		}
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
