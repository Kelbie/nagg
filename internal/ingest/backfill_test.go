package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/relayquery"
	"github.com/vertex-lab/nagg/internal/rules"
)

// backfillStoreFake records inserts and checkpoint upserts; states seed the
// RelayBackfillStates read.
type backfillStoreFake struct {
	states   []chstore.RelayBackfillState
	inserted []chstore.EventRecord
	upserts  []chstore.RelayBackfillState
}

func (s *backfillStoreFake) InsertEvents(_ context.Context, records []chstore.EventRecord) error {
	s.inserted = append(s.inserted, records...)
	return nil
}

func (s *backfillStoreFake) RelayBackfillStates(_ context.Context, rule string) ([]chstore.RelayBackfillState, error) {
	var out []chstore.RelayBackfillState
	for _, st := range s.states {
		if st.Rule == rule {
			out = append(out, st)
		}
	}
	return out, nil
}

func (s *backfillStoreFake) UpsertRelayBackfillState(_ context.Context, st chstore.RelayBackfillState) error {
	s.upserts = append(s.upserts, st)
	return nil
}

func (s *backfillStoreFake) lastUpsert(t *testing.T) chstore.RelayBackfillState {
	t.Helper()
	if len(s.upserts) == 0 {
		t.Fatal("no checkpoints written")
	}
	return s.upserts[len(s.upserts)-1]
}

type queryCall struct {
	relay string
	until int64
}

// querierFake serves scripted per-relay histories: each relay holds events
// sorted newest-first, and a query returns up to `limit` events with
// created_at <= until, like a NIP-01 relay. ignoreUntil models a misbehaving
// relay that returns events past the until bound.
type querierFake struct {
	histories   map[string][]*nostr.Event
	limit       int
	ignoreUntil bool
	calls       []queryCall
}

func (q *querierFake) QueryOne(_ context.Context, relay string, filter map[string]any, _ time.Duration) ([]relayquery.Event, error) {
	until := filter["until"].(int64)
	q.calls = append(q.calls, queryCall{relay: relay, until: until})
	limit := q.limit
	if limit <= 0 {
		limit = backfillPageLimit
	}
	var out []relayquery.Event
	for _, ev := range q.histories[relay] {
		if !q.ignoreUntil && int64(ev.CreatedAt) > until {
			continue
		}
		out = append(out, relayquery.Event{Relay: relay, Event: ev})
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func uniqueInsertedIDs(records []chstore.EventRecord) int {
	ids := map[string]struct{}{}
	for _, r := range records {
		ids[r.Event.ID] = struct{}{}
	}
	return len(ids)
}

func testEvent(kind int, createdAt int64, seq int) *nostr.Event {
	return &nostr.Event{
		ID:        fmt.Sprintf("%032d%032d", createdAt, seq),
		PubKey:    fmt.Sprintf("%064d", seq),
		CreatedAt: nostr.Timestamp(createdAt),
		Kind:      kind,
	}
}

// history builds a newest-first event list with one event per timestamp.
func history(kind int, newest int64, count int) []*nostr.Event {
	out := make([]*nostr.Event, 0, count)
	for i := 0; i < count; i++ {
		out = append(out, testEvent(kind, newest-int64(i), i))
	}
	return out
}

func testBackfiller(store *backfillStoreFake, q *querierFake, relays []string, now time.Time) *Backfiller {
	return testBackfillerRules(store, q, relays, now,
		[]rules.Backfill{{Name: "k38000_history", Kinds: []int{38000}, Resync: 24 * time.Hour}})
}

func testBackfillerRules(store *backfillStoreFake, q *querierFake, relays []string, now time.Time, backfills []rules.Backfill, opts ...BackfillOption) *Backfiller {
	b := NewBackfiller(store, relays, backfills, slog.Default(), opts...)
	b.querier = q
	b.pause = 0
	b.now = func() time.Time { return now }
	return b
}

// authoredEvent is testEvent with an explicit author and optional tags, for
// cap/gate scenarios where the pubkey matters.
func authoredEvent(kind int, createdAt int64, seq int, pubkey string, tags ...nostr.Tag) *nostr.Event {
	ev := testEvent(kind, createdAt, seq)
	ev.PubKey = pubkey
	ev.Tags = tags
	return ev
}

func TestBackfillerWalksEachRelayToExhaustion(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	// relayA holds 3 pages of history, relayB a single short page: per-relay
	// cursors must walk A to its own bottom regardless of B finishing first.
	q := &querierFake{
		limit: 10,
		histories: map[string][]*nostr.Event{
			"wss://a": history(38000, now.Unix()-100, 25),
			"wss://b": history(38000, now.Unix()-500_000, 3),
		},
	}
	store := &backfillStoreFake{}
	testBackfiller(store, q, []string{"wss://a", "wss://b"}, now).RunOnce(context.Background())

	// Boundary pages re-fetch a few already-inserted events (the cursor is
	// inclusive on purpose; the store dedups by id), so count unique ids.
	if got := uniqueInsertedIDs(store.inserted); got != 28 {
		t.Fatalf("unique inserted = %d, want 28 (25 + 3)", got)
	}
	completed := map[string]chstore.RelayBackfillState{}
	for _, st := range store.upserts {
		completed[st.Relay] = st
	}
	for relay, wantOldest := range map[string]int64{
		"wss://a": now.Unix() - 100 - 24,
		"wss://b": now.Unix() - 500_000 - 2,
	} {
		st := completed[relay]
		if !st.Completed {
			t.Errorf("%s: not marked completed", relay)
		}
		if st.OldestSynced != wantOldest {
			t.Errorf("%s: oldest_synced = %d, want %d", relay, st.OldestSynced, wantOldest)
		}
		if st.NewestSynced != now.Unix() {
			t.Errorf("%s: newest_synced = %d, want walk start %d", relay, st.NewestSynced, now.Unix())
		}
	}
	// Per-relay cursors: every query against A must carry A's own cursor, so
	// with one event per second the untils form A's descending chain and are
	// never dragged down to B's far-older range.
	for _, call := range q.calls {
		if call.relay == "wss://a" && call.until < now.Unix()-200 {
			t.Fatalf("relay A queried with relay B's cursor range: until=%d", call.until)
		}
	}
}

func TestBackfillerResumesIncompleteWalk(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	checkpointOldest := now.Unix() - 50
	q := &querierFake{
		limit:     10,
		histories: map[string][]*nostr.Event{"wss://a": history(38000, now.Unix()-10, 100)},
	}
	store := &backfillStoreFake{states: []chstore.RelayBackfillState{{
		Rule: "k38000_history", Kind: 38000, Relay: "wss://a",
		OldestSynced: checkpointOldest, NewestSynced: now.Unix() - 5,
		Completed: false, UpdatedAt: now.Add(-time.Minute),
	}}}
	testBackfiller(store, q, []string{"wss://a"}, now).RunOnce(context.Background())

	if got := q.calls[0].until; got != checkpointOldest {
		t.Fatalf("resume until = %d, want checkpoint oldest %d", got, checkpointOldest)
	}
	st := store.lastUpsert(t)
	if !st.Completed {
		t.Fatal("resumed walk must complete")
	}
	if st.NewestSynced != now.Unix()-5 {
		t.Fatalf("resume must keep the existing newest_synced, got %d", st.NewestSynced)
	}
}

func TestBackfillerTopUpStopsAtOverlapAndAdvances(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	floor := now.Unix() - 1000
	q := &querierFake{
		limit: 10,
		// 5 new events above the floor, plus older history that must NOT be
		// re-walked.
		histories: map[string][]*nostr.Event{"wss://a": history(38000, now.Unix()-1, 300)},
	}
	store := &backfillStoreFake{states: []chstore.RelayBackfillState{{
		Rule: "k38000_history", Kind: 38000, Relay: "wss://a",
		OldestSynced: now.Unix() - 5000, NewestSynced: floor,
		Completed: true, UpdatedAt: now.Add(-25 * time.Hour),
	}}}
	testBackfiller(store, q, []string{"wss://a"}, now).RunOnce(context.Background())

	st := store.lastUpsert(t)
	if st.NewestSynced != now.Unix()-1 {
		t.Fatalf("newest_synced = %d, want %d", st.NewestSynced, now.Unix()-1)
	}
	if !st.Completed {
		t.Fatal("top-up must keep completed=true")
	}
	// The walk should stop shortly after crossing the floor, not page through
	// all 300 events: ≤ (1000 above floor)/10-per-page + 1 overlap page.
	maxCalls := 1000/10 + 1
	if len(q.calls) > maxCalls {
		t.Fatalf("top-up made %d calls, want <= %d (must stop at overlap)", len(q.calls), maxCalls)
	}
}

func TestBackfillerSkipsFreshCompletedState(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	q := &querierFake{histories: map[string][]*nostr.Event{"wss://a": history(38000, now.Unix(), 5)}}
	store := &backfillStoreFake{states: []chstore.RelayBackfillState{{
		Rule: "k38000_history", Kind: 38000, Relay: "wss://a",
		OldestSynced: 1, NewestSynced: now.Unix() - 60,
		Completed: true, UpdatedAt: now.Add(-time.Hour), // resync is 24h
	}}}
	testBackfiller(store, q, []string{"wss://a"}, now).RunOnce(context.Background())

	if len(q.calls) != 0 {
		t.Fatalf("fresh completed state must not query, made %d calls", len(q.calls))
	}
}

func TestBackfillerProgressesThroughSameSecondPages(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	// More same-second events than one page holds: the inclusive cursor
	// cannot advance, so the walker must step past the second instead of
	// spinning forever.
	same := make([]*nostr.Event, 0, 30)
	for i := 0; i < 30; i++ {
		same = append(same, testEvent(38000, now.Unix()-10, i))
	}
	q := &querierFake{limit: 10, histories: map[string][]*nostr.Event{"wss://a": same}}
	store := &backfillStoreFake{}
	testBackfiller(store, q, []string{"wss://a"}, now).RunOnce(context.Background())

	st := store.lastUpsert(t)
	if !st.Completed {
		t.Fatal("walk must terminate and complete despite a same-second page")
	}
}

func TestBackfillerClampsBogusFutureTimestamps(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	floor := now.Unix() - 100
	q := &querierFake{
		limit:       10,
		ignoreUntil: true, // a relay that serves the bogus event despite until
		histories: map[string][]*nostr.Event{"wss://a": {
			testEvent(38000, now.Unix()+99_999, 1), // bogus future stamp
			testEvent(38000, now.Unix()-10, 2),
			testEvent(38000, floor-1, 3),
		}},
	}
	store := &backfillStoreFake{states: []chstore.RelayBackfillState{{
		Rule: "k38000_history", Kind: 38000, Relay: "wss://a",
		OldestSynced: floor - 5000, NewestSynced: floor,
		Completed: true, UpdatedAt: now.Add(-25 * time.Hour),
	}}}
	testBackfiller(store, q, []string{"wss://a"}, now).RunOnce(context.Background())

	st := store.lastUpsert(t)
	if st.NewestSynced > now.Unix()+60 {
		t.Fatalf("newest_synced = %d wedged above real time by a bogus created_at", st.NewestSynced)
	}
	if st.NewestSynced < now.Unix()-10 {
		t.Fatalf("newest_synced = %d, want at least the real newest %d", st.NewestSynced, now.Unix()-10)
	}
}

func floorRule(kinds []int, floor int64) []rules.Backfill {
	return []rules.Backfill{{Name: "firehose_floor", Kinds: kinds, Floor: floor}}
}

func TestBackfillerFloorWalkStopsAtFloor(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	floor := now.Unix() - 1000
	// History straddles the floor: 51 events at or above it, 50 below.
	q := &querierFake{limit: 10, histories: map[string][]*nostr.Event{
		"wss://a": history(1, floor+50, 101),
	}}
	store := &backfillStoreFake{}
	testBackfillerRules(store, q, []string{"wss://a"}, now, floorRule([]int{1}, floor)).RunOnce(context.Background())

	if got := uniqueInsertedIDs(store.inserted); got != 51 {
		t.Fatalf("unique inserted = %d, want 51 (only created_at >= floor)", got)
	}
	for _, rec := range store.inserted {
		if int64(rec.Event.CreatedAt) < floor {
			t.Fatalf("inserted event below the floor: created_at=%d floor=%d", rec.Event.CreatedAt, floor)
		}
	}
	st := store.lastUpsert(t)
	if !st.Completed {
		t.Fatal("floor walk must complete once the floor is reached")
	}
	if st.OldestSynced != floor {
		t.Fatalf("oldest_synced = %d, want the floor sentinel %d", st.OldestSynced, floor)
	}
}

func TestBackfillerFloorExhaustionClaimsFloor(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	floor := now.Unix() - 10_000
	// The relay runs out well above the floor: exhaustion must still claim
	// floor coverage, or the resume case would re-walk this relay every pass.
	q := &querierFake{limit: 10, histories: map[string][]*nostr.Event{
		"wss://a": history(1, now.Unix()-100, 6),
	}}
	store := &backfillStoreFake{}
	testBackfillerRules(store, q, []string{"wss://a"}, now, floorRule([]int{1}, floor)).RunOnce(context.Background())

	st := store.lastUpsert(t)
	if !st.Completed {
		t.Fatal("exhausted walk must complete")
	}
	if st.OldestSynced != floor {
		t.Fatalf("oldest_synced = %d, want the floor sentinel %d after exhaustion", st.OldestSynced, floor)
	}

	// A second pass over the persisted state makes no queries.
	q2 := &querierFake{limit: 10, histories: map[string][]*nostr.Event{"wss://a": history(1, now.Unix()-100, 6)}}
	store2 := &backfillStoreFake{states: []chstore.RelayBackfillState{st}}
	testBackfillerRules(store2, q2, []string{"wss://a"}, now, floorRule([]int{1}, floor)).RunOnce(context.Background())
	if len(q2.calls) != 0 {
		t.Fatalf("floor-claimed exhausted relay must not be re-queried, made %d calls", len(q2.calls))
	}
}

func TestBackfillerFloorUnchangedNoWork(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	floor := now.Unix() - 1000
	q := &querierFake{limit: 10, histories: map[string][]*nostr.Event{"wss://a": history(1, now.Unix()-1, 50)}}
	store := &backfillStoreFake{states: []chstore.RelayBackfillState{{
		Rule: "firehose_floor", Kind: 1, Relay: "wss://a",
		OldestSynced: floor, NewestSynced: now.Unix() - 60,
		Completed: true, UpdatedAt: now.Add(-time.Hour),
	}}}
	testBackfillerRules(store, q, []string{"wss://a"}, now, floorRule([]int{1}, floor)).RunOnce(context.Background())

	if len(q.calls) != 0 {
		t.Fatalf("unchanged floor must not query, made %d calls", len(q.calls))
	}
}

func TestBackfillerFloorMovedForwardNoWork(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	oldFloor := now.Unix() - 5000
	newFloor := now.Unix() - 1000 // forward in time: shallower than covered
	q := &querierFake{limit: 10, histories: map[string][]*nostr.Event{"wss://a": history(1, now.Unix()-1, 50)}}
	store := &backfillStoreFake{states: []chstore.RelayBackfillState{{
		Rule: "firehose_floor", Kind: 1, Relay: "wss://a",
		OldestSynced: oldFloor, NewestSynced: now.Unix() - 60,
		Completed: true, UpdatedAt: now.Add(-time.Hour),
	}}}
	testBackfillerRules(store, q, []string{"wss://a"}, now, floorRule([]int{1}, newFloor)).RunOnce(context.Background())

	if len(q.calls) != 0 {
		t.Fatalf("forward-moved floor must not query, made %d calls", len(q.calls))
	}
}

func TestBackfillerFloorMovedBackResumes(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	oldFloor := now.Unix() - 200
	newFloor := now.Unix() - 400
	// Events span both floors; the resumed walk must start at the old floor
	// (the recorded sentinel) and only cover the gap down to the new floor.
	q := &querierFake{limit: 10, histories: map[string][]*nostr.Event{
		"wss://a": history(1, now.Unix()-1, 500),
	}}
	store := &backfillStoreFake{states: []chstore.RelayBackfillState{{
		Rule: "firehose_floor", Kind: 1, Relay: "wss://a",
		OldestSynced: oldFloor, NewestSynced: now.Unix() - 60,
		Completed: true, UpdatedAt: now.Add(-time.Hour),
	}}}
	testBackfillerRules(store, q, []string{"wss://a"}, now, floorRule([]int{1}, newFloor)).RunOnce(context.Background())

	if len(q.calls) == 0 {
		t.Fatal("moved-back floor must resume the walk")
	}
	if got := q.calls[0].until; got != oldFloor {
		t.Fatalf("resume until = %d, want the old floor %d", got, oldFloor)
	}
	for _, rec := range store.inserted {
		created := int64(rec.Event.CreatedAt)
		if created < newFloor || created > oldFloor {
			t.Fatalf("resumed walk inserted outside the gap: created_at=%d want [%d, %d]", created, newFloor, oldFloor)
		}
	}
	st := store.lastUpsert(t)
	if !st.Completed || st.OldestSynced != newFloor {
		t.Fatalf("state = completed:%v oldest:%d, want completed at the new floor %d", st.Completed, st.OldestSynced, newFloor)
	}
}

func TestBackfillerFloorBudgetHitCheckpointsIncomplete(t *testing.T) {
	now := time.Unix(10_000_000, 0)
	floor := now.Unix() - 5000
	// More history above the floor than one pass's page budget (200 pages x
	// 10 events) can cover: the pass must checkpoint incomplete and a second
	// pass must resume from the checkpoint and finish.
	q := &querierFake{limit: 10, histories: map[string][]*nostr.Event{
		"wss://a": history(1, now.Unix()-10, 2100),
	}}
	store := &backfillStoreFake{}
	testBackfillerRules(store, q, []string{"wss://a"}, now, floorRule([]int{1}, floor)).RunOnce(context.Background())

	st := store.lastUpsert(t)
	if st.Completed {
		t.Fatal("budget-hit walk must checkpoint incomplete")
	}

	q2 := &querierFake{limit: 10, histories: q.histories}
	store2 := &backfillStoreFake{states: []chstore.RelayBackfillState{st}}
	testBackfillerRules(store2, q2, []string{"wss://a"}, now, floorRule([]int{1}, floor)).RunOnce(context.Background())
	if got := q2.calls[0].until; got != st.OldestSynced {
		t.Fatalf("second pass resume until = %d, want checkpoint %d", got, st.OldestSynced)
	}
	st2 := store2.lastUpsert(t)
	if !st2.Completed || st2.OldestSynced != floor {
		t.Fatalf("second pass = completed:%v oldest:%d, want completed at floor %d", st2.Completed, st2.OldestSynced, floor)
	}
}

func TestBackfillerFilterCapsPerEventDay(t *testing.T) {
	now := time.Unix(1_000_000_000, 0)
	day1 := now.Unix() - 4*86_400
	day2 := day1 + 86_400
	spammer := "1111111111111111111111111111111111111111111111111111111111111111"
	vip := "2222222222222222222222222222222222222222222222222222222222222222"
	events := []*nostr.Event{
		// Newest-first, as a relay serves them. 3 spam + 3 vip per day; the
		// cap admits 2 per author per EVENT day, exempt authors unlimited.
		authoredEvent(1, day2+30, 1, spammer),
		authoredEvent(1, day2+20, 2, spammer),
		authoredEvent(1, day2+10, 3, spammer),
		authoredEvent(1, day2+3, 4, vip),
		authoredEvent(1, day2+2, 5, vip),
		authoredEvent(1, day2+1, 6, vip),
		authoredEvent(1, day1+30, 7, spammer),
		authoredEvent(1, day1+20, 8, spammer),
		authoredEvent(1, day1+10, 9, spammer),
	}
	q := &querierFake{limit: 100, histories: map[string][]*nostr.Event{"wss://a": events}}
	store := &backfillStoreFake{}
	caps := []rules.Cap{{Name: "k1_daily", Kinds: []int{1}, Max: 2, Window: 24 * time.Hour, ExemptKnownViewers: true}}
	exempt := func(pk string) bool { return pk == vip }
	testBackfillerRules(store, q, []string{"wss://a"}, now,
		floorRule([]int{1}, day1-100),
		WithBackfillFilter(caps, nil, exempt),
	).RunOnce(context.Background())

	byAuthor := map[string]int{}
	for _, rec := range store.inserted {
		byAuthor[rec.Event.PubKey]++
	}
	if byAuthor[spammer] != 4 {
		t.Fatalf("spammer inserted = %d, want 4 (2 per historical day)", byAuthor[spammer])
	}
	if byAuthor[vip] != 3 {
		t.Fatalf("exempt author inserted = %d, want all 3", byAuthor[vip])
	}
}

func TestBackfillerFilterGatesUnaddressedWraps(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	viewer := "3333333333333333333333333333333333333333333333333333333333333333"
	stranger := "4444444444444444444444444444444444444444444444444444444444444444"
	events := []*nostr.Event{
		authoredEvent(1059, now.Unix()-10, 1, stranger, nostr.Tag{"p", viewer}),
		authoredEvent(1059, now.Unix()-20, 2, stranger, nostr.Tag{"p", stranger}),
		authoredEvent(1059, now.Unix()-30, 3, stranger),
	}
	gates := []rules.AddresseeGate{{Name: "k1059_known_addressee", Kinds: []int{1059}}}
	exempt := func(pk string) bool { return pk == viewer }

	q := &querierFake{limit: 100, histories: map[string][]*nostr.Event{"wss://a": events}}
	store := &backfillStoreFake{}
	testBackfillerRules(store, q, []string{"wss://a"}, now,
		floorRule([]int{1059}, now.Unix()-100),
		WithBackfillFilter(nil, gates, exempt),
	).RunOnce(context.Background())
	if got := uniqueInsertedIDs(store.inserted); got != 1 {
		t.Fatalf("gated inserts = %d, want 1 (only the wrap addressing the viewer)", got)
	}

	// No exemption source: the gate FAILS OPEN, mirroring Pipeline semantics.
	q2 := &querierFake{limit: 100, histories: map[string][]*nostr.Event{"wss://a": events}}
	store2 := &backfillStoreFake{}
	testBackfillerRules(store2, q2, []string{"wss://a"}, now,
		floorRule([]int{1059}, now.Unix()-100),
		WithBackfillFilter(nil, gates, nil),
	).RunOnce(context.Background())
	if got := uniqueInsertedIDs(store2.inserted); got != 3 {
		t.Fatalf("nil-exempt gated inserts = %d, want all 3 (fail open)", got)
	}
}

func TestBackfillerFilteredPageAdvancesCursor(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	author := "5555555555555555555555555555555555555555555555555555555555555555"
	// 30 same-day events from one capped author with Max 1: every page after
	// the first admit is fully filtered, yet the cursor must keep advancing
	// to the relay bottom and the walk must complete by exhaustion.
	events := make([]*nostr.Event, 0, 30)
	for i := 0; i < 30; i++ {
		events = append(events, authoredEvent(1, now.Unix()-10-int64(i), i, author))
	}
	q := &querierFake{limit: 10, histories: map[string][]*nostr.Event{"wss://a": events}}
	store := &backfillStoreFake{}
	caps := []rules.Cap{{Name: "k1_daily", Kinds: []int{1}, Max: 1, Window: 24 * time.Hour}}
	testBackfillerRules(store, q, []string{"wss://a"}, now,
		floorRule([]int{1}, now.Unix()-100_000),
		WithBackfillFilter(caps, nil, nil),
	).RunOnce(context.Background())

	if got := uniqueInsertedIDs(store.inserted); got != 1 {
		t.Fatalf("capped inserts = %d, want 1", got)
	}
	if len(q.calls) < 3 {
		t.Fatalf("filtered pages must keep paginating, made only %d calls", len(q.calls))
	}
	st := store.lastUpsert(t)
	if !st.Completed {
		t.Fatal("walk must complete by exhaustion despite filtered pages")
	}
}

func TestBackfillerExhaustionRuleUnaffectedByFilter(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	q := &querierFake{limit: 10, histories: map[string][]*nostr.Event{"wss://a": history(38000, now.Unix()-100, 25)}}
	store := &backfillStoreFake{}
	// The default firehose cap/gate kinds match no 38000 event, so the filter
	// is a structural no-op for the curated exhaustion rule.
	caps := []rules.Cap{{Name: "k1_daily", Kinds: []int{1, 1111, 6, 16}, Max: 1, Window: 24 * time.Hour}}
	gates := []rules.AddresseeGate{{Name: "k1059_known_addressee", Kinds: []int{1059}}}
	testBackfillerRules(store, q, []string{"wss://a"}, now,
		[]rules.Backfill{{Name: "k38000_history", Kinds: []int{38000}, Resync: 24 * time.Hour}},
		WithBackfillFilter(caps, gates, func(string) bool { return false }),
	).RunOnce(context.Background())

	if got := uniqueInsertedIDs(store.inserted); got != 25 {
		t.Fatalf("unique inserted = %d, want all 25 (filter must not bite on 38000)", got)
	}
}
