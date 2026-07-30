package ingest

import (
	"context"
	"errors"
	"log/slog"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/relayquery"
	"github.com/vertex-lab/nagg/internal/rules"
)

// Relay-history backfill: executes the declarative rules.Backfill set. A live
// firehose only ever sees NEW publications, so kinds whose events are
// long-lived and rarely republished would otherwise take months to
// accumulate. Per (rule, kind, relay):
//
//   - Initial walk: NIP-01 until/limit pagination, newest first, down to
//     relay exhaustion (an empty page). Cursors are PER RELAY — a merged
//     multi-relay cursor advances at the pace of whichever relay's page
//     reached oldest, silently skipping the deeper relays' middle ranges.
//   - Checkpoint after every page (relay_backfill_state), so an interrupted
//     walk resumes where it stopped, and a walk that inserted SOMETHING is
//     never mistaken for a finished one (the failure mode of gating repeat
//     fetches on "count > 0").
//   - Top-up: once the initial walk completed, every rule.Resync re-walk from
//     the top down to overlap with newest_synced — the events published while
//     nagg was down (the firehose subscription looks back at most NAGG_SINCE)
//     or throttled off its relays.
//
// Alternatives considered: NIP-77 negentropy reconciliation answers "which
// events am I missing" precisely, but it is draft/optional with thin relay
// support and its own fallback is exactly this REQ walk; NIP-45 COUNT is
// optional and approximate, so it cannot gate completeness. until/limit
// pagination is the only mechanism NIP-01 guarantees.
//
// Inserts go through Store.InsertEvents directly — never Pipeline.add — but
// walked pages pass the same declarative ingest rules as the live firehose
// when WithBackfillFilter is installed: per-author caps (bucketed by the
// EVENT's created_at day, so history is admitted the way the firehose would
// have admitted it at the time) and addressee gates. Both rule kinds are
// kind-scoped, so curated exhaustion walks (kind 38000, matched by no cap or
// gate) are unaffected. Id + signature verification already happened in
// relayquery (validateEvent drops anything invalid before it reaches this
// package).

const (
	backfillPageLimit = 500
	// backfillMaxPages bounds one (kind, relay) walk per pass: 100k events,
	// far above any expected corpus for a backfilled kind. Hitting it is
	// logged and the checkpoint resumes the walk on the next pass.
	backfillMaxPages  = 200
	backfillQueryTime = 15 * time.Second
	backfillPagePause = 500 * time.Millisecond
	// backfillTick is how often Run re-checks for due top-ups; actual re-walk
	// cadence is each rule's Resync.
	backfillTick = time.Hour
)

// BackfillStore is the store subset the backfiller needs.
type BackfillStore interface {
	InsertEvents(ctx context.Context, records []chstore.EventRecord) error
	RelayBackfillStates(ctx context.Context, rule string) ([]chstore.RelayBackfillState, error)
	UpsertRelayBackfillState(ctx context.Context, st chstore.RelayBackfillState) error
}

// relayQuerier is the transport seam (relayquery.Client in production).
type relayQuerier interface {
	QueryOne(ctx context.Context, relay string, filter map[string]any, timeout time.Duration) ([]relayquery.Event, error)
}

// Backfiller walks relay history for the declared rules.Backfill set.
type Backfiller struct {
	store   BackfillStore
	querier relayQuerier
	relays  []string
	rules   []rules.Backfill
	logger  *slog.Logger
	filter  *backfillFilter // nil = insert everything (see WithBackfillFilter)

	now   func() time.Time // test seam; nil means time.Now
	pause time.Duration    // test seam; page pause
}

// BackfillOption customizes a Backfiller beyond its required wiring.
type BackfillOption func(*Backfiller)

// WithBackfillFilter runs every walked page through the firehose ingest rules
// before insert: per-author caps (bucketed by event day) and addressee gates,
// with the live pipeline's exemption source. Both rule kinds are kind-scoped,
// so curated exhaustion walks (e.g. kind 38000, matched by no cap or gate)
// are unaffected; the filter only bites on firehose kinds.
func WithBackfillFilter(caps []rules.Cap, gates []rules.AddresseeGate, exempt func(pubkey string) bool) BackfillOption {
	return func(b *Backfiller) {
		b.filter = &backfillFilter{caps: newCapCounters(caps), gates: gates, exempt: exempt}
	}
}

func NewBackfiller(store BackfillStore, relays []string, backfills []rules.Backfill, logger *slog.Logger, opts ...BackfillOption) *Backfiller {
	b := &Backfiller{
		store:   store,
		querier: relayquery.Client{Relays: relays, Health: relayquery.NewRelayHealth()},
		relays:  relays,
		rules:   backfills,
		logger:  logger,
		now:     time.Now,
		pause:   backfillPagePause,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Run executes one pass immediately, then re-checks hourly for due work until
// the context ends. Errors are logged, never fatal — checkpointed state makes
// every pass resumable.
func (b *Backfiller) Run(ctx context.Context) {
	if len(b.rules) == 0 || len(b.relays) == 0 {
		return
	}
	b.RunOnce(ctx)
	ticker := time.NewTicker(backfillTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.RunOnce(ctx)
		}
	}
}

// RunOnce syncs every (rule, kind, relay) that has outstanding work: an
// incomplete initial walk, or a completed one whose Resync interval elapsed.
func (b *Backfiller) RunOnce(ctx context.Context) {
	for _, rule := range b.rules {
		states, err := b.store.RelayBackfillStates(ctx, rule.Name)
		if err != nil {
			b.logger.Warn("backfill: state read failed", "rule", rule.Name, "error", err)
			continue
		}
		type key struct {
			kind  int
			relay string
		}
		byKey := make(map[key]chstore.RelayBackfillState, len(states))
		for _, st := range states {
			byKey[key{st.Kind, st.Relay}] = st
		}
		for _, kind := range rule.Kinds {
			for _, relay := range b.relays {
				if ctx.Err() != nil {
					return
				}
				st := byKey[key{kind, relay}]
				st.Rule, st.Kind, st.Relay = rule.Name, kind, relay
				switch {
				case !st.Completed:
					b.walkDown(ctx, rule, st)
				case rule.Floor > 0 && st.OldestSynced > rule.Floor:
					// The floor moved further back since this walk completed
					// (OldestSynced records the floor it stopped at): resume
					// from the checkpoint down to the new floor. Cleared so a
					// page-budget interruption checkpoints as incomplete and
					// resumes through the normal case next pass.
					st.Completed = false
					b.walkDown(ctx, rule, st)
				case rule.Resync > 0 && b.now().Sub(st.UpdatedAt) >= rule.Resync:
					b.topUp(ctx, rule, st)
				}
			}
		}
	}
}

// walkDown runs (or resumes) the initial history walk: newest first, down to
// relay exhaustion — or, for a floor rule (rule.Floor > 0), down to the floor
// — checkpointing after every page. Floor completion records OldestSynced =
// rule.Floor, the sentinel RunOnce compares against to detect a floor that
// later moved further back.
func (b *Backfiller) walkDown(ctx context.Context, rule rules.Backfill, st chstore.RelayBackfillState) {
	nowUnix := b.now().Unix()
	until := nowUnix + 60
	if st.OldestSynced > 0 {
		// Resume below the checkpoint. Inclusive on purpose: the boundary
		// page re-fetches a handful of already-stored events (deduped by id
		// downstream) rather than skipping same-second siblings.
		until = st.OldestSynced
	}
	if st.NewestSynced == 0 {
		// Top of the contiguous range this walk establishes. Wall time, not
		// max(created_at) seen: an event with a bogus future created_at would
		// wedge the watermark above real time and every later top-up would
		// stop at its first page, missing everything since.
		st.NewestSynced = nowUnix
	}

	total, pages := 0, 0
	for ; pages < backfillMaxPages; pages++ {
		events, err := b.queryPage(ctx, st.Kind, st.Relay, until)
		if err != nil {
			b.logWalkError(rule, st, err)
			return // checkpointed; resumes next pass
		}
		if len(events) == 0 {
			st.Completed = true
			break
		}
		records, oldest, _ := toRecords(events, until)
		if len(records) == 0 {
			st.Completed = true
			break
		}
		if rule.Floor > 0 {
			records = keepAtOrAbove(records, rule.Floor)
		}
		records = b.filterRecords(records)
		// A page the floor cut or the filter emptied still advances the
		// cursor (oldest comes from the RAW page) and never completes the
		// walk — only a raw empty page means relay exhaustion.
		if len(records) > 0 {
			if err := b.store.InsertEvents(ctx, records); err != nil {
				b.logger.Warn("backfill: insert failed", "rule", rule.Name, "kind", st.Kind, "relay", st.Relay, "error", err)
				return
			}
			total += len(records)
		}
		st.OldestSynced = oldest
		if rule.Floor > 0 && oldest <= rule.Floor {
			// Covered down to the floor (inclusive). OldestSynced records the
			// floor itself, not the page's oldest: it is the resume point if
			// the floor later moves further back, and page-oldest may sit far
			// below the floor (one bogus-ancient stamp).
			st.Completed = true
			st.OldestSynced = rule.Floor
		}
		if err := b.checkpoint(ctx, st); err != nil {
			return
		}
		if st.Completed {
			break
		}
		if oldest >= until {
			// A full page inside a single second cannot advance the inclusive
			// cursor; step past it (accepting the rare same-second skip).
			until = until - 1
		} else {
			until = oldest
		}
		if !b.sleep(ctx) {
			return
		}
	}
	if !st.Completed {
		b.logger.Warn("backfill: walk page budget hit; resuming next pass",
			"rule", rule.Name, "kind", st.Kind, "relay", st.Relay, "pages", pages, "events", total)
	}
	if rule.Floor > 0 && st.Completed {
		// Relay exhaustion above the floor also claims floor coverage: there
		// is nothing older on this relay, and a floor that never moves again
		// must not re-query an exhausted relay every hourly pass.
		st.OldestSynced = rule.Floor
	}
	if err := b.checkpoint(ctx, st); err != nil {
		return
	}
	b.logger.Info("backfill: walk finished",
		"rule", rule.Name, "kind", st.Kind, "relay", st.Relay,
		"events", total, "completed", st.Completed, "oldest_synced", st.OldestSynced)
}

// topUp re-walks from the top of history down to the overlap with the synced
// range, then advances newest_synced. Checkpointing also bumps updated_at,
// which is the resync clock.
func (b *Backfiller) topUp(ctx context.Context, rule rules.Backfill, st chstore.RelayBackfillState) {
	nowUnix := b.now().Unix()
	until := nowUnix + 60
	overlap := st.NewestSynced
	newest := overlap
	reachedOverlap := false

	total, pages := 0, 0
	for ; pages < backfillMaxPages; pages++ {
		events, err := b.queryPage(ctx, st.Kind, st.Relay, until)
		if err != nil {
			b.logWalkError(rule, st, err)
			return // updated_at untouched → due again next tick
		}
		if len(events) == 0 {
			reachedOverlap = true
			break
		}
		records, oldest, pageNewest := toRecords(events, nowUnix+60)
		records = b.filterRecords(records)
		if len(records) > 0 {
			if err := b.store.InsertEvents(ctx, records); err != nil {
				b.logger.Warn("backfill: insert failed", "rule", rule.Name, "kind", st.Kind, "relay", st.Relay, "error", err)
				return
			}
			total += len(records)
			if pageNewest > newest {
				newest = pageNewest
			}
		}
		if oldest <= overlap {
			reachedOverlap = true
			break
		}
		if oldest >= until {
			until = until - 1
		} else {
			until = oldest
		}
		if !b.sleep(ctx) {
			return
		}
	}
	if !reachedOverlap {
		// The page budget ran out above the synced range. Advance anyway —
		// re-walking the same head every pass would never converge — but say
		// so: the range (overlap, oldest page reached) was not covered.
		b.logger.Warn("backfill: top-up page budget hit before overlap; gap not covered",
			"rule", rule.Name, "kind", st.Kind, "relay", st.Relay, "pages", pages, "overlap", overlap)
	}
	st.NewestSynced = newest
	if err := b.checkpoint(ctx, st); err != nil {
		return
	}
	if total > 0 {
		b.logger.Info("backfill: top-up finished",
			"rule", rule.Name, "kind", st.Kind, "relay", st.Relay, "events", total, "newest_synced", newest)
	}
}

func (b *Backfiller) queryPage(ctx context.Context, kind int, relay string, until int64) ([]relayquery.Event, error) {
	return b.querier.QueryOne(ctx, relay, map[string]any{
		"kinds": []int{kind},
		"limit": backfillPageLimit,
		"until": until,
	}, backfillQueryTime)
}

func (b *Backfiller) checkpoint(ctx context.Context, st chstore.RelayBackfillState) error {
	if err := b.store.UpsertRelayBackfillState(ctx, st); err != nil {
		b.logger.Warn("backfill: checkpoint failed",
			"rule", st.Rule, "kind", st.Kind, "relay", st.Relay, "error", err)
		return err
	}
	return nil
}

func (b *Backfiller) logWalkError(rule rules.Backfill, st chstore.RelayBackfillState, err error) {
	if errors.Is(err, relayquery.ErrRelayBackoff) {
		b.logger.Debug("backfill: relay in backoff", "rule", rule.Name, "kind", st.Kind, "relay", st.Relay)
		return
	}
	b.logger.Warn("backfill: relay query failed", "rule", rule.Name, "kind", st.Kind, "relay", st.Relay, "error", err)
}

// sleep pauses between pages so a deep walk does not hammer a relay; false
// means the context ended.
func (b *Backfiller) sleep(ctx context.Context) bool {
	if b.pause <= 0 {
		return ctx.Err() == nil
	}
	select {
	case <-ctx.Done():
		return false
	case <-time.After(b.pause):
		return true
	}
}

// toRecords converts a relay page into insertable records, reporting the
// page's oldest created_at (the next cursor) and its newest created_at
// clamped to maxCreatedAt (the watermark guard against bogus future stamps).
func toRecords(events []relayquery.Event, maxCreatedAt int64) (records []chstore.EventRecord, oldest, newest int64) {
	oldest = maxCreatedAt
	for _, ev := range events {
		if ev.Event == nil {
			continue
		}
		created := int64(ev.Event.CreatedAt)
		if created < oldest {
			oldest = created
		}
		if created > newest && created <= maxCreatedAt {
			newest = created
		}
		records = append(records, chstore.EventRecord{
			Event: ev.Event,
			Relay: ev.Relay,
			Seen:  time.Now().UTC(),
		})
	}
	return records, oldest, newest
}

// keepAtOrAbove drops records older than floor (created_at < floor); the
// floor itself is kept. In-place: records is page-local.
func keepAtOrAbove(records []chstore.EventRecord, floor int64) []chstore.EventRecord {
	kept := records[:0]
	for _, rec := range records {
		if int64(rec.Event.CreatedAt) >= floor {
			kept = append(kept, rec)
		}
	}
	return kept
}

// filterRecords applies the optional firehose-rule filter (caps + gates).
func (b *Backfiller) filterRecords(records []chstore.EventRecord) []chstore.EventRecord {
	if b.filter == nil {
		return records
	}
	kept := records[:0]
	for _, rec := range records {
		if b.filter.keep(rec) {
			kept = append(kept, rec)
		}
	}
	return kept
}
