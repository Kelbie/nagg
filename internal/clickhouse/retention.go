package clickhouse

import (
	"context"
	"fmt"
	"sort"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const eventKindRetentionMinDiskFreeRatio = 0.10

type EventKindPruneResult struct {
	ConfiguredKinds  []int
	RemovedCounts    map[int]uint64
	RemovedEvents    uint64
	ActiveMutations  uint64
	DiskFreeRatio    float64
	MinDiskFreeRatio float64
	RebuiltAppView   bool
	Skipped          bool
	SkipReason       string
}

func (s *Store) PruneRemovedEventKinds(ctx context.Context, configuredKinds []int) (EventKindPruneResult, error) {
	kinds := sortedEventKinds(configuredKinds)
	result := EventKindPruneResult{
		ConfiguredKinds: kinds,
		RemovedCounts:   map[int]uint64{},
	}
	if len(kinds) == 0 {
		result.Skipped = true
		result.SkipReason = "no_configured_kinds"
		return result, nil
	}

	activeMutations, err := s.activeMutationCount(ctx)
	if err != nil {
		return result, fmt.Errorf("count active mutations before event kind prune: %w", err)
	}
	result.ActiveMutations = activeMutations
	if activeMutations > 0 {
		result.Skipped = true
		result.SkipReason = "active_mutations"
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

	diskFreeRatio, err := s.diskFreeRatio(ctx)
	if err != nil {
		return result, fmt.Errorf("check clickhouse disk free space before event kind prune: %w", err)
	}
	result.DiskFreeRatio = diskFreeRatio
	result.MinDiskFreeRatio = eventKindRetentionMinDiskFreeRatio
	if diskFreeRatio < eventKindRetentionMinDiskFreeRatio {
		result.Skipped = true
		result.SkipReason = "low_disk_headroom"
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

func (s *Store) activeMutationCount(ctx context.Context) (uint64, error) {
	var count uint64
	if err := s.conn.QueryRow(ctx, activeMutationCountQuery()).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func activeMutationCountQuery() string {
	return `
		SELECT count()
		FROM system.mutations
		WHERE database = currentDatabase()
			AND is_done = 0
	`
}

func (s *Store) diskFreeRatio(ctx context.Context) (float64, error) {
	var ratio float64
	if err := s.conn.QueryRow(ctx, diskFreeRatioQuery()).Scan(&ratio); err != nil {
		return 0, err
	}
	return ratio, nil
}

func diskFreeRatioQuery() string {
	return `
		SELECT ifNull(min(toFloat64(free_space) / nullIf(toFloat64(total_space), 0)), 1)
		FROM system.disks
	`
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
