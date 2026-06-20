package appview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// ownHistoryStore captures the QueryEvents input and returns canned events, so
// the handler's filtering + cursor wiring can be asserted without CH.
type ownHistoryStore struct {
	fakeStore
	events    []chstore.EventView
	gotInput  chstore.EventQueryInput
	gotCalled bool
}

func (s *ownHistoryStore) QueryEvents(_ context.Context, input chstore.EventQueryInput) ([]chstore.EventView, error) {
	s.gotInput = input
	s.gotCalled = true
	return s.events, nil
}

func ownEvent(id string, kind int, createdAt int64, tags [][]string) chstore.EventView {
	return chstore.EventView{ID: id, PubKey: testPubkey, Kind: kind, CreatedAt: time.Unix(createdAt, 0), Tags: tags}
}

func TestOwnHistoryLikesByAuthor(t *testing.T) {
	store := &ownHistoryStore{events: []chstore.EventView{
		ownEvent("1", 7, 200, [][]string{{"e", "tgt"}}),
		ownEvent("2", 7, 100, [][]string{{"e", "tgt2"}}),
	}}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/own/likes?pubkey="+testPubkey, nil)
	handler.ownHistory(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	// queried authors=[me], kinds=[7]
	if len(store.gotInput.PubKeys) != 1 || store.gotInput.PubKeys[0] != testPubkey {
		t.Fatalf("pubkeys = %+v", store.gotInput.PubKeys)
	}
	if len(store.gotInput.Kinds) != 1 || store.gotInput.Kinds[0] != 7 {
		t.Fatalf("kinds = %+v", store.gotInput.Kinds)
	}
	var resp OwnHistoryResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if len(resp.Events) != 2 {
		t.Fatalf("events = %d", len(resp.Events))
	}
	if resp.PaginationUntil != 100 {
		t.Fatalf("paginationUntil = %d, want 100 (oldest)", resp.PaginationUntil)
	}
}

func TestOwnHistoryAuthoredVsRepliesSplit(t *testing.T) {
	events := []chstore.EventView{
		ownEvent("authored", 1, 200, [][]string{}),               // no #e → authored
		ownEvent("reply", 1, 100, [][]string{{"e", "some-note"}}), // #e → reply
	}
	authoredStore := &ownHistoryStore{events: events}
	repliesStore := &ownHistoryStore{events: events}

	recA := httptest.NewRecorder()
	New(authoredStore, WithNIP05Validation(false)).ownHistory(recA,
		httptest.NewRequest(http.MethodGet, "/nostr/own/authored?pubkey="+testPubkey, nil))
	recR := httptest.NewRecorder()
	New(repliesStore, WithNIP05Validation(false)).ownHistory(recR,
		httptest.NewRequest(http.MethodGet, "/nostr/own/replies?pubkey="+testPubkey, nil))

	var authored, replies OwnHistoryResponse
	_ = json.NewDecoder(recA.Body).Decode(&authored)
	_ = json.NewDecoder(recR.Body).Decode(&replies)

	if len(authored.Events) != 1 || authored.Events[0].ID != "authored" {
		t.Fatalf("authored = %+v", authored.Events)
	}
	if len(replies.Events) != 1 || replies.Events[0].ID != "reply" {
		t.Fatalf("replies = %+v", replies.Events)
	}
}

func TestOwnHistoryExcludesCursorBoundary(t *testing.T) {
	store := &ownHistoryStore{}
	handler := New(store, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/nostr/own/likes?pubkey="+testPubkey+"&until=500&cursorId=boundary", nil)
	handler.ownHistory(rec, req)

	if store.gotInput.Until != 500 {
		t.Fatalf("until = %d, want 500", store.gotInput.Until)
	}
	if len(store.gotInput.ExcludeIDs) != 1 || store.gotInput.ExcludeIDs[0] != "boundary" {
		t.Fatalf("excludeIDs = %+v, want [boundary]", store.gotInput.ExcludeIDs)
	}
}

func TestOwnHistoryZapsSentNotImplemented(t *testing.T) {
	handler := New(&ownHistoryStore{}, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/own/zaps-sent?pubkey="+testPubkey, nil)
	handler.ownHistory(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501 (Primal serves zaps-sent)", rec.Code)
	}
}

func TestOwnHistoryUnknownType(t *testing.T) {
	handler := New(&ownHistoryStore{}, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/own/nonsense?pubkey="+testPubkey, nil)
	handler.ownHistory(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}
