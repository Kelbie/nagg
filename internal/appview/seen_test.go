package appview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
	var resp Envelope
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if len(resp.Events) != 1 {
		t.Fatalf("events = %+v, want the latest marker event", resp.Events)
	}
	if !strings.Contains(resp.Events[0].Content, "2000") {
		t.Fatalf("marker content = %q, want the latest (2000) marker", resp.Events[0].Content)
	}
}

func TestNotificationsSeenDefaultsToZero(t *testing.T) {
	handler := New(seenStore{}, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/notifications/seen?viewer="+testPubkey, nil)
	handler.notificationsSeen(rec, req)

	var resp Envelope
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if len(resp.Events) != 0 || len(resp.Order) != 0 {
		t.Fatalf("envelope = %+v, want empty (never marked)", resp)
	}
}
