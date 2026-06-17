package clickhouse

import (
	"context"
	"fmt"
	"sort"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

type EventKindPruneResult struct {
	ConfiguredKinds []int
	RemovedCounts   map[int]uint64
	RemovedEvents   uint64
	RebuiltAppView  bool
	Skipped         bool
}

func (s *Store) PruneRemovedEventKinds(ctx context.Context, configuredKinds []int) (EventKindPruneResult, error) {
	kinds := sortedEventKinds(configuredKinds)
	result := EventKindPruneResult{
		ConfiguredKinds: kinds,
		RemovedCounts:   map[int]uint64{},
	}
	if len(kinds) == 0 {
		result.Skipped = true
		return result, nil
	}

	predicate := eventKindRetentionPredicate(kinds)
	removed, total, err := s.eventKindCountsMatching(ctx, predicate)
	if err != nil {
		return result, fmt.Errorf("count pruned event kinds: %w", err)
	}
	result.RemovedCounts = removed
	result.RemovedEvents = total
	if total == 0 {
		return result, nil
	}

	mutationCtx := ch.Context(ctx, ch.WithSettings(ch.Settings{"mutations_sync": 1}))
	for _, stmt := range eventKindRetentionMutations(kinds) {
		if err := s.conn.Exec(mutationCtx, stmt); err != nil {
			return result, fmt.Errorf("prune removed event kinds: %w", err)
		}
	}

	if shouldRebuildAppViewAfterKindPrune(removed) {
		if err := s.Backfill(ctx); err != nil {
			return result, fmt.Errorf("rebuild app-view after pruning event kinds: %w", err)
		}
		result.RebuiltAppView = true
	}

	return result, nil
}

func (s *Store) eventKindCountsMatching(ctx context.Context, predicate string) (map[int]uint64, uint64, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
		SELECT kind, count()
		FROM nostr_events
		WHERE %s
		GROUP BY kind
	`, predicate))
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	counts := map[int]uint64{}
	var total uint64
	for rows.Next() {
		var kind uint32
		var count uint64
		if err := rows.Scan(&kind, &count); err != nil {
			return nil, 0, err
		}
		counts[int(kind)] = count
		total += count
	}
	return counts, total, rows.Err()
}

func sortedEventKinds(values []int) []int {
	out := uniqueEventKinds(values)
	sort.Ints(out)
	return out
}

func eventKindRetentionPredicate(allowed []int) string {
	return fmt.Sprintf("kind NOT IN (%s)", ints(allowed))
}

func eventKindRetentionMutations(allowed []int) []string {
	if len(allowed) == 0 {
		return nil
	}
	predicate := eventKindRetentionPredicate(allowed)
	prunedEventIDs := fmt.Sprintf("SELECT id FROM nostr_events WHERE %s", predicate)
	return []string{
		fmt.Sprintf("ALTER TABLE event_seen_relays DELETE WHERE event_id IN (%s)", prunedEventIDs),
		fmt.Sprintf("ALTER TABLE derived_tags DELETE WHERE %s", predicate),
		fmt.Sprintf("ALTER TABLE derived_metrics DELETE WHERE %s", predicate),
		fmt.Sprintf("ALTER TABLE notification_candidates DELETE WHERE %s", predicate),
		fmt.Sprintf("ALTER TABLE event_tags DELETE WHERE %s", predicate),
		fmt.Sprintf("ALTER TABLE nostr_events DELETE WHERE %s", predicate),
	}
}

func shouldRebuildAppViewAfterKindPrune(removed map[int]uint64) bool {
	appViewSourceKinds := map[int]struct{}{
		0:    {},
		1:    {},
		3:    {},
		6:    {},
		7:    {},
		16:   {},
		9735: {},
	}
	for kind, count := range removed {
		if count == 0 {
			continue
		}
		if _, ok := appViewSourceKinds[kind]; ok {
			return true
		}
	}
	return false
}
