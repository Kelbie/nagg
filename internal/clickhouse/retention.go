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
// app-view keeps in nostr_events (see retentionTargets for why event_tags is
// left alone). Everything
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
	Rule        string
	Table       string
	MatchedRows uint64
	Deleted     bool
}

// ErrRetentionBusy: a previous delete mutation is still executing, so this
// pass did nothing. Field-learned the hard way (2026-07-04): a single
// multi-part mutation on this capacity-limited instance fans out across the
// whole background pool and starves user reads with error 439 — so retention
// runs AT MOST ONE partition-scoped mutation at a time, and never overlaps an
// in-flight one.
var ErrRetentionBusy = errors.New("retention mutation still pending")

// retentionTargets: rules delete from nostr_events ONLY.
//
// event_tags is deliberately NOT cascaded. Measured live (2026-07-04): the
// rule-1 cascade matched 46.7M of event_tags' 2.05B rows — roughly half a GiB
// compressed — and the mutation, even running alone, saturated the instance
// (per-part NOT IN evaluation over hundreds of millions of rows) and took
// user reads down until it was KILLed. Orphaned tag rows for deleted events
// are inert: every aggregate that reads event_tags is either already
// materialized or bounded to recent created_at windows. A sub-GiB reclaim is
// never worth a read outage.
var retentionTargets = []struct {
	table    string
	idColumn string
}{
	{table: "nostr_events", idColumn: "id"},
}

// retentionMinMatchedRows: don't spend a full-table mutation on a trickle.
// Superseded replaceable versions accumulate continuously from the firehose;
// mutating for a few thousand rows would churn the table's parts all day.
const retentionMinMatchedRows = 50_000

// ErrRetentionNoHeadroom: the disk can't fit a mutation of the table's
// largest part right now. A ClickHouse mutation reserves up to the full part
// size while rewriting it; submitting without headroom wedges the mutation in
// a reserve-fail retry loop (observed live: "Cannot reserve 17.89 GiB" against
// a 24 GiB-free disk, retrying forever and blocking retention). Space frees up
// as background merges compact already-masked rows — the next pass retries.
var ErrRetentionNoHeadroom = errors.New("not enough disk headroom for a retention mutation")

// RunRetention advances retention by ONE bounded step: it finds the first
// (rule, table) with enough matching rows and submits a single lightweight
// DELETE for it, returning immediately (the mutation executes in the
// background). The caller re-ticks; the ErrRetentionBusy guard serializes
// mutations across ticks — NEVER run two at once, one multi-part mutation can
// occupy this instance's whole background pool. dryRun instead reports every
// rule's matches without deleting anything.
//
// NOTE deletes are deliberately table-wide, not partition-scoped: lightweight
// DELETE ... IN PARTITION does not restrict the rewrite on this ClickHouse
// version (observed live: an IN PARTITION 202512 delete rewrote 202606 parts),
// so partition scoping only adds a false sense of boundedness.
func (s *Store) RunRetention(ctx context.Context, dryRun bool) ([]RetentionRunResult, error) {
	pending, err := s.pendingRetentionMutations(ctx)
	if err != nil {
		return nil, fmt.Errorf("check pending mutations: %w", err)
	}
	if pending > 0 {
		return nil, fmt.Errorf("%w (%d pending)", ErrRetentionBusy, pending)
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
		for _, target := range retentionTargets {
			predicate := rule.Policy.deletePredicate(rule.Kinds, target.idColumn)
			var matched uint64
			if err := s.conn.QueryRow(qctx, fmt.Sprintf("SELECT count() FROM %s WHERE %s", target.table, predicate)).Scan(&matched); err != nil {
				errs = append(errs, fmt.Errorf("retention rule %q count %s: %w", rule.Name, target.table, err))
				continue
			}
			if matched < retentionMinMatchedRows {
				continue
			}
			result := RetentionRunResult{
				Rule:        rule.Name,
				Table:       target.table,
				MatchedRows: matched,
			}
			if dryRun {
				results = append(results, result)
				continue
			}
			if err := s.checkRetentionHeadroom(qctx, target.table); err != nil {
				return results, errors.Join(append(errs, err)...)
			}
			stmt := fmt.Sprintf("DELETE FROM %s WHERE %s", target.table, predicate)
			if err := s.conn.Exec(qctx, stmt); err != nil {
				return results, errors.Join(append(errs, fmt.Errorf("retention rule %q delete %s: %w", rule.Name, target.table, err))...)
			}
			result.Deleted = true
			results = append(results, result)
			// One mutation per pass — see ErrRetentionBusy.
			return results, errors.Join(errs...)
		}
	}
	return results, errors.Join(errs...)
}

// checkRetentionHeadroom refuses to submit a mutation unless the disk can hold
// a rewrite of the table's largest active part with 25% margin.
func (s *Store) checkRetentionHeadroom(ctx context.Context, table string) error {
	var largestPart, freeSpace uint64
	err := s.conn.QueryRow(ctx, `
		SELECT
			(SELECT max(bytes_on_disk) FROM system.parts WHERE active AND database = currentDatabase() AND table = ?),
			(SELECT min(free_space) FROM system.disks)
	`, table).Scan(&largestPart, &freeSpace)
	if err != nil {
		return fmt.Errorf("check disk headroom: %w", err)
	}
	need := largestPart + largestPart/4
	if freeSpace < need {
		return fmt.Errorf("%w: table %s largest part %d bytes needs %d, disk has %d free", ErrRetentionNoHeadroom, table, largestPart, need, freeSpace)
	}
	return nil
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
