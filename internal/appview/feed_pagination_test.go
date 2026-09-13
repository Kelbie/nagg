package appview

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

func TestFeedPagePagination(t *testing.T) {
	for _, route := range []struct {
		name, method, path, body string
		limit                    uint64
		orderBy                  string
	}{
		{"feed_get", "GET", "/nostr/feed?pubkeys=" + testPubkey + "&limit=2", "", 2, orderByCreatedAt},
		{"feed_post", "POST", "/nostr/feed", `{"spec":"{\"pubkeys\":[\"` + testPubkey + `\"]}","limit":2}`, 2, orderByCreatedAt},
		{"user", "GET", "/nostr/feed/user?pubkey=" + testPubkey + "&limit=2", "", 2, orderByCreatedAt},
		// Deliberately differs from the request: use the provider's effective limit.
		{"ranked", "POST", "/nostr/feed/ranked", `{"limit":99,"target":{"limit":1}}`, 2, orderByRank},
		{"feed_get_default", "GET", "/nostr/feed?pubkeys=" + testPubkey, "", 30, orderByCreatedAt},
		{"feed_post_default", "POST", "/nostr/feed", `{"spec":"{\"pubkeys\":[\"` + testPubkey + `\"]}"}`, 30, orderByCreatedAt},
		{"user_default", "GET", "/nostr/feed/user?pubkey=" + testPubkey, "", 50, orderByCreatedAt},
		{"ranked_default", "POST", "/nostr/feed/ranked", `{}`, 30, orderByRank},
		{"feed_zero", "GET", "/nostr/feed?pubkeys=" + testPubkey + "&limit=0", "", 30, orderByCreatedAt},
		{"feed_oversized", "GET", "/nostr/feed?pubkeys=" + testPubkey + "&limit=101", "", 30, orderByCreatedAt},
		{"user_zero", "GET", "/nostr/feed/user?pubkey=" + testPubkey + "&limit=0", "", 30, orderByCreatedAt},
		{"user_negative", "GET", "/nostr/feed/user?pubkey=" + testPubkey + "&limit=-1", "", 30, orderByCreatedAt},
		{"user_oversized", "GET", "/nostr/feed/user?pubkey=" + testPubkey + "&limit=101", "", 30, orderByCreatedAt},
	} {
		for _, page := range []struct {
			name string
			len  int
		}{
			{"full", int(route.limit)},
			{"short", int(route.limit) - 1},
			{"empty", 0},
		} {
			t.Run(route.name+"/"+page.name, func(t *testing.T) {
				events := makeFeedEvents(page.len)
				store := &appViewHydrationStore{feed: events}
				ranker := &fakeRanker{events: events, limit: route.limit}
				handler := New(store, WithRankedFeed(ranker), WithNIP05Validation(false))
				mux := http.NewServeMux()
				handler.Register(mux)
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest(route.method, route.path, strings.NewReader(route.body)))
				assertFeedPage(t, rec, page.len == int(route.limit), page.len)
				var response FeedPageEnvelope
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if response.OrderBy != route.orderBy || len(response.Order) != page.len {
					t.Fatalf("orderBy/order = %s/%v", response.OrderBy, response.Order)
				}
			})
		}
	}
}

func assertFeedPage(t *testing.T, rec *httptest.ResponseRecorder, wantMore bool, pageLen int) {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if string(response["hasMore"]) != fmt.Sprint(wantMore) {
		t.Fatalf("hasMore = %s, want %t", response["hasMore"], wantMore)
	}
	cursor, present := response["cursor"]
	if present != wantMore {
		t.Fatalf("cursor = %s, presence = %t, want %t", cursor, present, wantMore)
	}
	if wantMore {
		want := fmt.Sprintf(`"%d|%d"`, 1_710_000_000, pageLen)
		if string(cursor) != want {
			t.Fatalf("cursor = %s, want %s", cursor, want)
		}
	}
	for _, key := range []string{"order", "orderBy", "events", "aggregates"} {
		if _, ok := response[key]; !ok {
			t.Errorf("missing envelope field %s", key)
		}
	}
}

func TestFeedWithoutAuthorsHasNoMore(t *testing.T) {
	h := New(fakeStore{}, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	h.feed(rec, httptest.NewRequest("POST", "/nostr/feed", strings.NewReader(`{"spec":"{}"}`)))
	assertFeedPage(t, rec, false, 0)
}

func TestFeedPageSaturationBeforeHydrationAndDeduplication(t *testing.T) {
	events := makeFeedEvents(2)
	original := makeFeedEvents(3)[2]
	for i := range events {
		events[i].Kind = 6
		events[i].Tags = [][]string{{"e", original.ID}}
	}
	store := &appViewHydrationStore{events: map[string]chstore.EventView{original.ID: original}}
	h := New(store, WithNIP05Validation(false))
	for _, tc := range []struct {
		limit uint64
		more  bool
	}{{2, true}, {3, false}, {0, false}} {
		response, err := h.feedEnvelope(context.Background(), events, orderByCreatedAt, tc.limit)
		if err != nil {
			t.Fatal(err)
		}
		if len(response.Order) != 1 || len(response.Events) != 3 {
			t.Fatalf("expected one anchor and three hydrated events: %+v", response)
		}
		if response.HasMore != tc.more || (response.Cursor != nil) != tc.more {
			t.Fatalf("limit %d: hasMore = %t, cursor = %v", tc.limit, response.HasMore, response.Cursor)
		}
	}
}

func makeFeedEvents(count int) []chstore.EventView {
	events := make([]chstore.EventView, count)
	for i := range events {
		events[i] = chstore.EventView{
			ID: fmt.Sprintf("%064x", i+1), PubKey: testPubkey, Kind: 1,
			CreatedAt: time.Unix(1_710_000_000+int64(i), 0), Tags: [][]string{},
		}
	}
	return events
}
