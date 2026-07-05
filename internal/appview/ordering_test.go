package appview

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// The ranked feed is rank-ordered (no live prepend); the chronological feeds are
// created_at-ordered. The manifest must carry that semantic so the client knows.
func TestFeedResponseCarriesCreatedAtOrdering(t *testing.T) {
	const noteID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	store := &appViewHydrationStore{
		feed: []chstore.EventView{{
			ID: noteID, PubKey: testPubkey, Kind: 1, CreatedAt: time.Unix(1_710_000_000, 0), Tags: [][]string{},
		}},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/nostr/feed",
		strings.NewReader(`{"spec":"{\"id\":\"feed\",\"pubkey\":\"`+testPubkey+`\"}","limit":10}`))
	handler.feed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp Envelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.OrderBy != "created_at" {
		t.Fatalf("orderBy = %q, want created_at for the chronological feed", resp.OrderBy)
	}
	if len(resp.Order) != 1 || resp.Order[0] != noteID {
		t.Fatalf("order = %v, want [%s]", resp.Order, noteID)
	}
}
