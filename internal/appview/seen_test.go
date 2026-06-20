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

type seenStore struct {
	fakeStore
	events []chstore.EventView
}

func (s seenStore) QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error) {
	return s.events, nil
}

func seenEvent(seenUntil int64, createdAt int64) chstore.EventView {
	content, _ := json.Marshal(map[string]int64{"seenUntil": seenUntil})
	return chstore.EventView{
		Kind: 30078, PubKey: testPubkey, CreatedAt: time.Unix(createdAt, 0),
		Tags: [][]string{{"d", SeenNotificationsDTag}}, Content: string(content),
	}
}

func TestNotificationsSeenReturnsLatestTimestamp(t *testing.T) {
	store := seenStore{events: []chstore.EventView{
		seenEvent(1000, 100),
		seenEvent(2000, 200), // newer publish → wins
	}}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/notifications/seen?viewer="+testPubkey, nil)
	handler.notificationsSeen(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp SeenStateResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.SeenUntil != 2000 {
		t.Fatalf("seenUntil = %d, want 2000 (latest)", resp.SeenUntil)
	}
}

func TestNotificationsSeenDefaultsToZero(t *testing.T) {
	handler := New(seenStore{}, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/notifications/seen?viewer="+testPubkey, nil)
	handler.notificationsSeen(rec, req)

	var resp SeenStateResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.SeenUntil != 0 {
		t.Fatalf("seenUntil = %d, want 0 (never marked)", resp.SeenUntil)
	}
}
