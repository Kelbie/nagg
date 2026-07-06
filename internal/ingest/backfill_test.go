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
	b := NewBackfiller(store, relays, []rules.Backfill{{Name: "k38000_history", Kinds: []int{38000}, Resync: 24 * time.Hour}}, slog.Default())
	b.querier = q
	b.pause = 0
	b.now = func() time.Time { return now }
	return b
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
