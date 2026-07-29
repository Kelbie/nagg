package appview

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

func hexID(c string) string { return strings.Repeat(c, 64) }

// --- follow-status ---

type followEdgeStore struct {
	fakeStore
	edges map[string]chstore.FollowEdge
}

func (s *followEdgeStore) FollowEdges(_ context.Context, _ string, candidates []string) (map[string]chstore.FollowEdge, error) {
	out := make(map[string]chstore.FollowEdge, len(candidates))
	for _, c := range candidates {
		out[c] = s.edges[c]
	}
	return out, nil
}

func TestFollowStatusDerivesRelationships(t *testing.T) {
	a, b, c, d := hexID("a"), hexID("b"), hexID("c"), hexID("d")
	store := &followEdgeStore{edges: map[string]chstore.FollowEdge{
		a: {Following: true, FollowsYou: true},
		b: {Following: true},
		c: {FollowsYou: true},
		d: {},
	}}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/follow-status?viewer="+testPubkey+"&candidates="+a+","+b+","+c+","+d, nil)
	handler.followStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp ReferenceEdgesEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	want := map[string]ReferenceEdge{
		a: {Out: true, In: true},
		b: {Out: true},
		c: {In: true},
		d: {},
	}
	if len(resp.Edges) != 4 {
		t.Fatalf("edges = %d, want 4", len(resp.Edges))
	}
	for pubkey, edge := range want {
		if resp.Edges[pubkey] != edge {
			t.Fatalf("edge[%s] = %+v, want %+v", pubkey[:4], resp.Edges[pubkey], edge)
		}
	}
}

// --- own profiles ---

type ownProfileStore struct {
	fakeStore
	profileRows map[string]chstore.K0Row
	counts      map[string]chstore.PubkeyStats
}

func (s *ownProfileStore) LatestK0(_ context.Context, pubkeys []string) (map[string]chstore.K0Row, error) {
	out := make(map[string]chstore.K0Row, len(pubkeys))
	for _, p := range pubkeys {
		out[p] = s.profileRows[p]
	}
	return out, nil
}

func (s *ownProfileStore) BatchPubkeyStats(_ context.Context, pubkeys []string) (map[string]chstore.PubkeyStats, error) {
	out := make(map[string]chstore.PubkeyStats, len(pubkeys))
	for _, p := range pubkeys {
		out[p] = s.counts[p]
	}
	return out, nil
}

func TestOwnProfilesReturnsMetadataAndCounts(t *testing.T) {
	a := hexID("a")
	store := &ownProfileStore{
		profileRows: map[string]chstore.K0Row{a: {
			PubKey: a, Name: "alice", Picture: "pic",
			EventID: hexID("e"), RawJSON: `{"name":"alice","picture":"pic"}`,
			CreatedAt: time.Unix(1_700_000_000, 0),
		}},
		counts: map[string]chstore.PubkeyStats{a: {Follows: 12, Followers: 34}},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/own/profiles?pubkeys="+a, nil)
	handler.ownProfiles(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp Envelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Events) != 1 || resp.Events[0].Kind != 0 || resp.Events[0].PubKey != a {
		t.Fatalf("events = %+v, want alice's kind-0", resp.Events)
	}
	if !strings.Contains(resp.Events[0].Content, "alice") {
		t.Fatalf("profile event content = %q", resp.Events[0].Content)
	}
	if len(resp.Order) != 1 || resp.Order[0] != hexID("e") {
		t.Fatalf("order = %v", resp.Order)
	}
	if resp.Aggregates[a]["k3_p_latest"]["actors"] != 34 || resp.Aggregates[a]["k3_author_latest"]["sources"] != 12 {
		t.Fatalf("aggregates = %+v, want followers 34 / following 12", resp.Aggregates[a])
	}
}

// --- events/query ---

type queryEventsStore struct {
	fakeStore
	lastInput chstore.EventQueryInput
	events    []chstore.EventView
}

func (s *queryEventsStore) QueryEvents(_ context.Context, input chstore.EventQueryInput) ([]chstore.EventView, error) {
	s.lastInput = input
	return s.events, nil
}

func TestEventsQueryRejectsUnboundedScan(t *testing.T) {
	handler := New(&queryEventsStore{}, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/nostr/events/query", strings.NewReader(`{"limit":10}`))
	handler.eventsQuery(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a filterless query", rec.Code)
	}
}

func TestEventsQueryReturnsConnection(t *testing.T) {
	store := &queryEventsStore{events: []chstore.EventView{
		{ID: hexID("a"), PubKey: testPubkey, Kind: 1063, CreatedAt: time.Unix(1_700_000_000, 0), Tags: [][]string{}},
	}}
	handler := New(store, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/nostr/events/query", strings.NewReader(`{"kinds":[1063],"limit":20}`))
	handler.eventsQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if store.lastInput.Limit != 20 || len(store.lastInput.Kinds) != 1 || store.lastInput.Kinds[0] != 1063 {
		t.Fatalf("query input = %+v, want kind 1063 limit 20", store.lastInput)
	}
	var resp Envelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Order) != 1 || resp.Order[0] != hexID("a") {
		t.Fatalf("order = %v, want the result event id", resp.Order)
	}
	if len(resp.Events) != 1 || resp.Events[0].ID != hexID("a") {
		t.Fatalf("events = %+v", resp.Events)
	}
}

// --- thread relevance merge ---

type relevantThreadStore struct {
	fakeStore
	root            chstore.EventView
	replies         []chstore.EventView
	authoredSources []string
	authoredErr     error
	followed        map[string]string
	rankedByLike    []string
	allByNew        []string
}

func (s *relevantThreadStore) DescendantEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error) {
	return &s.root, s.replies, nil
}

func (s *relevantThreadStore) AuthoredRefSources(context.Context, string, string, int) ([]string, error) {
	return s.authoredSources, s.authoredErr
}

func (s *relevantThreadStore) FollowedRefs(_ context.Context, _ string, parents []string) (map[string]string, error) {
	out := map[string]string{}
	for _, p := range parents {
		if id, ok := s.followed[p]; ok {
			out[p] = id
		}
	}
	return out, nil
}

func (s *relevantThreadStore) RankedRefSources(_ context.Context, _ string, sort string, _ int, _ int) ([]string, error) {
	if sort == "new" {
		return s.allByNew, nil
	}
	return s.rankedByLike, nil
}

func eventWithID(id, pubkey string) chstore.EventView {
	return chstore.EventView{ID: id, PubKey: pubkey, Kind: 1, CreatedAt: time.Unix(1_700_000_000, 0), Tags: [][]string{}}
}

func threadRequest(t *testing.T, handler *Handler, path string) ThreadEnvelope {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	handler.thread(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp ThreadEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestThreadRelevantMergeOrder(t *testing.T) {
	root := hexID("0")
	author := testPubkey
	ar1, ar2 := hexID("1"), hexID("2") // ALL the author's direct replies to the root
	fol := hexID("3")                  // followed reply to the root
	rk1, rk2 := hexID("4"), hexID("5") // ranked direct replies
	nested := hexID("6")               // a reply the merge doesn't surface

	store := &relevantThreadStore{
		root: eventWithID(root, author),
		replies: []chstore.EventView{
			eventWithID(ar1, author), eventWithID(ar2, author), eventWithID(fol, hexID("f")),
			eventWithID(rk1, hexID("d")), eventWithID(rk2, hexID("e")), eventWithID(nested, hexID("c")),
		},
		authoredSources: []string{ar1, ar2},
		followed:        map[string]string{root: fol},
		rankedByLike:    []string{rk1, rk2},
		allByNew:        []string{rk2, rk1, ar1}, // direct replies, recency (with dups vs ranked/author)
	}
	handler := New(store, WithNIP05Validation(false))
	resp := threadRequest(t, handler, "/nostr/thread?id="+root+"&sort=relevant&viewer="+testPubkey)

	want := []string{root, ar1, ar2, fol, rk1, rk2, nested}
	if strings.Join(resp.Order, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v\nwant    %v", resp.Order, want)
	}
	if resp.OrderBy != orderByRank {
		t.Fatalf("orderBy = %q, want %q", resp.OrderBy, orderByRank)
	}
	if resp.Total != 6 {
		t.Fatalf("total = %d, want 6", resp.Total)
	}
	if resp.Cursor != nil {
		t.Fatalf("cursor = %q, want absent on an unpaged response", *resp.Cursor)
	}
}

func TestThreadRelevantOpFirstOnlyDirectReplies(t *testing.T) {
	// The OP pin covers the OP's DIRECT replies only (AuthoredRefSources is
	// keyed on the root): an OP reply to another commenter is not in the
	// authored set and must NOT be pinned. Explicit sorts stay literal.
	root := hexID("0")
	author := testPubkey
	opDirect := hexID("1")             // OP direct reply to root
	opNested := hexID("2")             // OP reply to someone else's reply
	rk1, rk2 := hexID("4"), hexID("5") // ranked direct replies by others

	store := &relevantThreadStore{
		root: eventWithID(root, author),
		replies: []chstore.EventView{
			eventWithID(rk1, hexID("d")), eventWithID(rk2, hexID("e")),
			eventWithID(opDirect, author), eventWithID(opNested, author),
		},
		authoredSources: []string{opDirect},
		rankedByLike:    []string{rk1, rk2},
	}
	handler := New(store, WithNIP05Validation(false))

	resp := threadRequest(t, handler, "/nostr/thread?id="+root+"&sort=relevant")
	want := []string{root, opDirect, rk1, rk2, opNested}
	if strings.Join(resp.Order, ",") != strings.Join(want, ",") {
		t.Fatalf("relevant order = %v\nwant %v", resp.Order, want)
	}

	// sort=ranked is literal: no OP pin, ranked then leftovers in fetch order.
	resp = threadRequest(t, handler, "/nostr/thread?id="+root+"&sort=ranked")
	want = []string{root, rk1, rk2, opDirect, opNested}
	if strings.Join(resp.Order, ",") != strings.Join(want, ",") {
		t.Fatalf("ranked order = %v\nwant %v", resp.Order, want)
	}

	// sort=new is literal: the DescendantEvents order verbatim.
	resp = threadRequest(t, handler, "/nostr/thread?id="+root+"&sort=new")
	want = []string{root, rk1, rk2, opDirect, opNested}
	if strings.Join(resp.Order, ",") != strings.Join(want, ",") {
		t.Fatalf("new order = %v\nwant %v", resp.Order, want)
	}
}

func TestThreadDefaultSortDeterministicPaged(t *testing.T) {
	// The default sort now honors offset/replyLimit (it used to bypass
	// pageElements entirely) and reports total/cursor truthfully.
	root := hexID("0")
	r1, r2, r3 := hexID("1"), hexID("2"), hexID("3")
	store := &relevantThreadStore{
		root:    eventWithID(root, testPubkey),
		replies: []chstore.EventView{eventWithID(r1, hexID("a")), eventWithID(r2, hexID("b")), eventWithID(r3, hexID("c"))},
	}
	handler := New(store, WithNIP05Validation(false))

	resp := threadRequest(t, handler, "/nostr/thread?id="+root)
	if strings.Join(resp.Order, ",") != strings.Join([]string{root, r1, r2, r3}, ",") {
		t.Fatalf("order = %v, want root + descendant order", resp.Order)
	}
	if resp.Total != 3 || resp.Cursor != nil {
		t.Fatalf("total = %d cursor = %v, want 3 and no cursor", resp.Total, resp.Cursor)
	}

	resp = threadRequest(t, handler, "/nostr/thread?id="+root+"&offset=1&replyLimit=1")
	if strings.Join(resp.Order, ",") != strings.Join([]string{root, r2}, ",") {
		t.Fatalf("paged order = %v, want [root r2]", resp.Order)
	}
	if resp.Total != 3 {
		t.Fatalf("paged total = %d, want 3", resp.Total)
	}
	if resp.Cursor == nil || *resp.Cursor != "0|2" {
		t.Fatalf("cursor = %v, want 0|2", resp.Cursor)
	}
}

func TestThreadPagingTotalAndCursor(t *testing.T) {
	root := hexID("0")
	author := testPubkey
	ids := []string{hexID("1"), hexID("2"), hexID("3"), hexID("4"), hexID("5")}
	replies := make([]chstore.EventView, 0, len(ids))
	for _, id := range ids {
		replies = append(replies, eventWithID(id, hexID("d")))
	}
	store := &relevantThreadStore{
		root:            eventWithID(root, author),
		replies:         replies,
		authoredSources: nil,
		rankedByLike:    ids,
	}
	handler := New(store, WithNIP05Validation(false))

	var collected []string
	offset := 0
	for page := 0; page < 5; page++ {
		resp := threadRequest(t, handler,
			"/nostr/thread?id="+root+"&sort=relevant&offset="+strconv.Itoa(offset)+"&replyLimit=2")
		if resp.Order[0] != root {
			t.Fatalf("page %d order[0] = %q, want the root on every page", page, resp.Order[0])
		}
		if resp.Total != len(ids) {
			t.Fatalf("page %d total = %d, want %d", page, resp.Total, len(ids))
		}
		if len(resp.Events) < len(ids)+1 {
			t.Fatalf("page %d events = %d, want the full fetched set on every page", page, len(resp.Events))
		}
		collected = append(collected, resp.Order[1:]...)
		if resp.Cursor == nil {
			break
		}
		parts := strings.SplitN(*resp.Cursor, "|", 2)
		next, err := strconv.Atoi(parts[1])
		if err != nil || parts[0] != "0" {
			t.Fatalf("cursor = %q, want 0|<offset>", *resp.Cursor)
		}
		offset = next
	}
	if strings.Join(collected, ",") != strings.Join(ids, ",") {
		t.Fatalf("paged union = %v, want %v exactly once each", collected, ids)
	}
}

func TestThreadOrderingPrimitiveFailureDegrades(t *testing.T) {
	root := hexID("0")
	r1, r2 := hexID("1"), hexID("2")
	store := &relevantThreadStore{
		root:        eventWithID(root, testPubkey),
		replies:     []chstore.EventView{eventWithID(r1, hexID("a")), eventWithID(r2, hexID("b"))},
		authoredErr: errors.New("clickhouse down"),
	}
	handler := New(store, WithNIP05Validation(false))
	resp := threadRequest(t, handler, "/nostr/thread?id="+root+"&sort=relevant")
	// Ordering primitives failing must degrade to the fetched tail order, not 500.
	if strings.Join(resp.Order, ",") != strings.Join([]string{root, r1, r2}, ",") {
		t.Fatalf("degraded order = %v, want root + fetched order", resp.Order)
	}
	if resp.Total != 2 {
		t.Fatalf("degraded total = %d, want 2", resp.Total)
	}
}

// --- dm conversation scoping ---

type dmConversationStore struct {
	fakeStore
	inputs []chstore.EventQueryInput
}

func (s *dmConversationStore) QueryEvents(_ context.Context, input chstore.EventQueryInput) ([]chstore.EventView, error) {
	s.inputs = append(s.inputs, input)
	return nil, nil
}

func TestDmConversationScopesDirectKindsToCounterparty(t *testing.T) {
	counterparty := hexID("c")
	store := &dmConversationStore{}
	handler := New(store, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/dm/conversation?viewer="+testPubkey+"&counterparty="+counterparty+"&kinds=4", nil)
	handler.dmConversation(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if len(store.inputs) != 2 {
		t.Fatalf("queries = %d, want 2 (sent + received scoped to the pair)", len(store.inputs))
	}
	// Both queries must carry a p-tag scoping to the conversation pair.
	for _, in := range store.inputs {
		if len(in.Tags) != 1 || in.Tags[0].Key != "p" {
			t.Fatalf("query missing p-tag pair scope: %+v", in)
		}
	}
}
