package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

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

// ---------------------------------------------------------------------------
// Declarative retention
//
// RetentionRules is the single, top-to-bottom-readable statement of what this
// app-view keeps in nostr_events (and, in cascade, event_tags). Everything
// deleted here is either superseded (replaceable events — only the latest
// version is ever read) or stale-and-unengaged, and all of it is recoverable
// from relays via the on-demand backfills. Plain-language docs and the
// measurements behind each rule: docs/retention.md.
//
// Deletes are ClickHouse lightweight DELETEs submitted asynchronously: rows
// are masked immediately as the mutation runs and the space reclaims through
// background merges — no part rewrite at submit time, so a near-full disk is
// never at risk.
// ---------------------------------------------------------------------------

// RetentionRules — edit this list to change what is pruned.
var RetentionRules = []RetentionRule{
	{
		// Replaceable events (NIP-01): relays and every reader keep only the
		// newest event per author; the older versions are pure dead weight.
		// Measured on prod 2026-07: 80–92% of stored kind-0/3/10050 rows were
		// superseded (~13 GiB, dominated by kind-3 contact lists).
		Name:   "replaceable: keep only the latest event per author",
		Kinds:  []int{0, 3, 10050, 10051},
		Policy: KeepLatestPerAuthor{},
	},
	{
		// Parameterized-replaceable events: same, but versioned per (author,
		// d-tag). Measured: 98.7% of kind-30078 rows were superseded (~4 GiB).
		Name:   "param-replaceable: keep only the latest event per (author, d-tag)",
		Kinds:  []int{30078, 38000},
		Policy: KeepLatestPerAuthorDTag{},
	},
	{
		// Old posts nobody engaged with. "Engaged" = any like, repost, quote,
		// zap, or direct reply ever recorded (the aggregate tables outlive the
		// engaging events themselves, so pruning a reply never un-engages its
		// parent).
		Name:   "posts: drop after 1 year without any engagement",
		Kinds:  []int{1, 1111},
		Policy: MaxAgeWithoutEngagement{Age: 365 * 24 * time.Hour},
	},
}

type RetentionRule struct {
	Name   string
	Kinds  []int
	Policy RetentionPolicy
}

// RetentionPolicy renders the WHERE predicate selecting rows to DELETE.
// idColumn abstracts the event-id column name: "id" on nostr_events,
// "event_id" on the event_tags cascade (which carries kind + created_at too).
type RetentionPolicy interface {
	deletePredicate(kinds []int, idColumn string) string
}

// KeepLatestPerAuthor deletes every event whose (kind, author) has a newer
// version. Ties on created_at keep one arbitrary winner — the same semantics
// relays apply to replaceable events.
type KeepLatestPerAuthor struct{}

func (KeepLatestPerAuthor) deletePredicate(kinds []int, idColumn string) string {
	return fmt.Sprintf(`kind IN (%s) AND %s NOT IN (
		SELECT argMax(id, created_at)
		FROM nostr_events
		WHERE kind IN (%s)
		GROUP BY kind, pubkey
	)`, ints(kinds), idColumn, ints(kinds))
}

// KeepLatestPerAuthorDTag is KeepLatestPerAuthor keyed by (kind, author,
// d-tag) for parameterized-replaceable kinds. Events without a d tag group
// together under the empty key, which is exactly NIP-01's treatment ("d" absent
// = empty d).
type KeepLatestPerAuthorDTag struct{}

func (KeepLatestPerAuthorDTag) deletePredicate(kinds []int, idColumn string) string {
	return fmt.Sprintf(`kind IN (%s) AND %s NOT IN (
		SELECT argMax(id, created_at)
		FROM nostr_events
		WHERE kind IN (%s)
		GROUP BY kind, pubkey,
			arrayFirst(t -> length(t) >= 2 AND t[1] = 'd', JSONExtract(tags_json, 'Array(Array(String))'))
	)`, ints(kinds), idColumn, ints(kinds))
}

// MaxAgeWithoutEngagement deletes events older than Age with zero recorded
// engagement. The engagement tables are AggregatingMergeTree state that is
// never pruned, so this reads them as a permanent ledger.
type MaxAgeWithoutEngagement struct {
	Age time.Duration
}

func (p MaxAgeWithoutEngagement) deletePredicate(kinds []int, idColumn string) string {
	days := int(p.Age.Hours() / 24)
	return fmt.Sprintf(`kind IN (%s) AND created_at < now() - INTERVAL %d DAY AND %s NOT IN (
		SELECT target_event_id FROM note_like_counts
		UNION ALL SELECT target_event_id FROM note_repost_counts
		UNION ALL SELECT target_event_id FROM note_quote_counts
		UNION ALL SELECT target_event_id FROM note_zap_totals
		UNION ALL SELECT target_event_id FROM note_direct_reply_counts
	)`, ints(kinds), days, idColumn)
}

type RetentionRunResult struct {
	Rule          string
	MatchedEvents uint64
	Deleted       bool
}

// retentionMaxPendingMutations skips a run while earlier delete mutations are
// still chewing — resubmitting identical deletes would only pile mutations up.
const retentionMaxPendingMutations = 4

// RunRetention evaluates every RetentionRule: counts matching events (always —
// that count is the run's log line), then submits the lightweight DELETEs for
// event_tags and nostr_events unless dryRun. Rule failures don't stop later
// rules; they are joined into the returned error.
func (s *Store) RunRetention(ctx context.Context, dryRun bool) ([]RetentionRunResult, error) {
	pending, err := s.pendingRetentionMutations(ctx)
	if err != nil {
		return nil, fmt.Errorf("check pending mutations: %w", err)
	}
	if pending > retentionMaxPendingMutations {
		return nil, fmt.Errorf("skipping retention run: %d mutations still pending on nostr_events/event_tags", pending)
	}

	// Bounded like every other background job on this capacity-limited
	// instance; deletes submit async (lightweight_deletes_sync=0) so the
	// statement returns as soon as the mutation is registered instead of
	// blocking into the ~300s infra connection ceiling.
	qctx := ch.Context(ctx, ch.WithSettings(ch.Settings{
		"max_threads":              2,
		"max_execution_time":       240,
		"lightweight_deletes_sync": 0,
	}))

	var results []RetentionRunResult
	var errs []error
	for _, rule := range RetentionRules {
		eventsPredicate := rule.Policy.deletePredicate(rule.Kinds, "id")
		var matched uint64
		if err := s.conn.QueryRow(qctx, "SELECT count() FROM nostr_events WHERE "+eventsPredicate).Scan(&matched); err != nil {
			errs = append(errs, fmt.Errorf("retention rule %q count: %w", rule.Name, err))
			continue
		}
		result := RetentionRunResult{Rule: rule.Name, MatchedEvents: matched}
		if matched > 0 && !dryRun {
			tagsPredicate := rule.Policy.deletePredicate(rule.Kinds, "event_id")
			if err := s.conn.Exec(qctx, "DELETE FROM event_tags WHERE "+tagsPredicate); err != nil {
				errs = append(errs, fmt.Errorf("retention rule %q delete event_tags: %w", rule.Name, err))
				results = append(results, result)
				continue
			}
			if err := s.conn.Exec(qctx, "DELETE FROM nostr_events WHERE "+eventsPredicate); err != nil {
				errs = append(errs, fmt.Errorf("retention rule %q delete nostr_events: %w", rule.Name, err))
				results = append(results, result)
				continue
			}
			result.Deleted = true
		}
		results = append(results, result)
	}
	return results, errors.Join(errs...)
}

func (s *Store) pendingRetentionMutations(ctx context.Context) (uint64, error) {
	var pending uint64
	err := s.conn.QueryRow(ctx, `
		SELECT count()
		FROM system.mutations
		WHERE is_done = 0
		  AND database = currentDatabase()
		  AND table IN ('nostr_events', 'event_tags')
	`).Scan(&pending)
	return pending, err
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
