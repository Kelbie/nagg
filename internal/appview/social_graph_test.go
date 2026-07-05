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

// socialGraphStore returns canned list events for QueryEvents and inherits
// fakeStore's profile map for LatestProfiles.
type socialGraphStore struct {
	fakeStore
	events []chstore.EventView
}

func (s socialGraphStore) QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error) {
	return s.events, nil
}

const (
	follow1 = "1111111111111111111111111111111111111111111111111111111111111111"
	follow2 = "2222222222222222222222222222222222222222222222222222222222222222"
	muted1  = "3333333333333333333333333333333333333333333333333333333333333333"
)

func TestSocialGraphBundlesFollowsProfilesRelaysMutes(t *testing.T) {
	store := socialGraphStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{
			follow1: {Name: "alice", Picture: "http://x/a.png", EventID: "p1", RawJSON: `{"name":"alice"}`},
		}},
		events: []chstore.EventView{
			{ID: "c3", Kind: 3, PubKey: testPubkey, CreatedAt: time.Unix(200, 0),
				Tags: [][]string{{"p", follow1}, {"p", follow2}}},
			{ID: "r10002", Kind: 10002, PubKey: testPubkey, CreatedAt: time.Unix(200, 0),
				Tags: [][]string{{"r", "wss://a"}, {"r", "wss://b", "write"}, {"r", "wss://c", "read"}}},
			{ID: "m10000", Kind: 10000, PubKey: testPubkey, CreatedAt: time.Unix(200, 0),
				Tags: [][]string{{"p", muted1}}},
		},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/social-graph?viewer="+testPubkey, nil)
	handler.socialGraph(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp Envelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// The three latest list events are embedded in order; the client derives
	// references, relays, and mutes from their tags.
	if len(resp.Order) != 3 || resp.Order[0] != "c3" || resp.Order[1] != "r10002" || resp.Order[2] != "m10000" {
		t.Fatalf("order = %v", resp.Order)
	}
	byID := map[string]FeedEvent{}
	for _, event := range resp.Events {
		byID[event.ID] = event
	}
	if len(byID["c3"].Tags) != 2 || byID["c3"].Tags[0][1] != follow1 {
		t.Fatalf("contact list event = %+v", byID["c3"])
	}
	if len(byID["r10002"].Tags) != 3 || len(byID["m10000"].Tags) != 1 {
		t.Fatalf("relay/mute list events = %+v / %+v", byID["r10002"], byID["m10000"])
	}
	// The referenced pubkeys' kind-0 profile events ride along.
	if _, ok := byID["p1"]; !ok {
		t.Fatalf("follow1's kind-0 profile event should be embedded: %v", resp.Events)
	}
}

func TestSocialGraphPrefersNewerContactList(t *testing.T) {
	store := socialGraphStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
		events: []chstore.EventView{
			{ID: "old", Kind: 3, PubKey: testPubkey, CreatedAt: time.Unix(100, 0), Tags: [][]string{{"p", follow1}}},
			{ID: "new", Kind: 3, PubKey: testPubkey, CreatedAt: time.Unix(200, 0), Tags: [][]string{{"p", follow1}, {"p", follow2}}},
		},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/social-graph?viewer="+testPubkey, nil)
	handler.socialGraph(rec, req)

	var resp Envelope
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if len(resp.Order) != 1 || resp.Order[0] != "new" {
		t.Fatalf("order = %v, want only the created_at:200 list (LWW)", resp.Order)
	}
	if len(resp.Events) != 1 || len(resp.Events[0].Tags) != 2 {
		t.Fatalf("events = %+v, want the newer contact list", resp.Events)
	}
}
