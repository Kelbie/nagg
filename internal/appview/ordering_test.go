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

func TestFeedItemIDUsesNoteOrRepostAnchor(t *testing.T) {
	note := FeedItem{Type: "note", Event: &FeedEvent{ID: "note-id"}}
	if got := feedItemID(note); got != "note-id" {
		t.Fatalf("note id = %q", got)
	}
	repost := FeedItem{Type: "repost", OriginalEventID: "orig", RepostEvent: &FeedEvent{ID: "repost-id"}}
	if got := feedItemID(repost); got != "orig" {
		t.Fatalf("repost id = %q, want the original (anchor)", got)
	}
	repostNoOrig := FeedItem{Type: "repost", RepostEvent: &FeedEvent{ID: "repost-id"}}
	if got := feedItemID(repostNoOrig); got != "repost-id" {
		t.Fatalf("repost id = %q, want the repost id fallback", got)
	}
}

func TestFeedItemsOrderingPreservesOrderAndSemantic(t *testing.T) {
	items := []FeedItem{
		{Type: "note", Event: &FeedEvent{ID: "a"}},
		{Type: "repost", OriginalEventID: "b"},
		{Type: "note", Event: &FeedEvent{ID: "c"}},
	}
	m := feedItemsOrdering(items, orderByRank)
	if m.OrderBy != "rank" {
		t.Fatalf("orderBy = %q", m.OrderBy)
	}
	if strings.Join(m.Elements, ",") != "a,b,c" {
		t.Fatalf("elements = %v, want [a b c]", m.Elements)
	}
}

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
	var resp FeedResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Ordering.OrderBy != "created_at" {
		t.Fatalf("orderBy = %q, want created_at for the chronological feed", resp.Ordering.OrderBy)
	}
	if len(resp.Ordering.Elements) != 1 || resp.Ordering.Elements[0] != noteID {
		t.Fatalf("elements = %v, want [%s]", resp.Ordering.Elements, noteID)
	}
}
