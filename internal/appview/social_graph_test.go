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
			follow1: {Name: "alice", Picture: "http://x/a.png"},
		}},
		events: []chstore.EventView{
			{Kind: 3, PubKey: testPubkey, CreatedAt: time.Unix(200, 0),
				Tags: [][]string{{"p", follow1}, {"p", follow2}}},
			{Kind: 10002, PubKey: testPubkey, CreatedAt: time.Unix(200, 0),
				Tags: [][]string{{"r", "wss://a"}, {"r", "wss://b", "write"}, {"r", "wss://c", "read"}}},
			{Kind: 10000, PubKey: testPubkey, CreatedAt: time.Unix(200, 0),
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
	var resp SocialGraphResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Pubkey != testPubkey {
		t.Fatalf("pubkey = %s", resp.Pubkey)
	}
	if len(resp.Follows) != 2 || resp.Follows[0] != follow1 || resp.Follows[1] != follow2 {
		t.Fatalf("follows = %+v", resp.Follows)
	}
	if resp.Mutes[0] != muted1 {
		t.Fatalf("mutes = %+v", resp.Mutes)
	}
	if resp.Profiles[follow1].Name != "alice" {
		t.Fatalf("profiles = %+v (follow1 should be bundled inline)", resp.Profiles)
	}
	want := []RelayListEntry{
		{URL: "wss://a", Read: true, Write: true},
		{URL: "wss://b", Read: false, Write: true},
		{URL: "wss://c", Read: true, Write: false},
	}
	if len(resp.Relays) != 3 {
		t.Fatalf("relays = %+v", resp.Relays)
	}
	for i, r := range want {
		if resp.Relays[i] != r {
			t.Fatalf("relay[%d] = %+v, want %+v", i, resp.Relays[i], r)
		}
	}
}

func TestSocialGraphPrefersNewerContactList(t *testing.T) {
	store := socialGraphStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
		events: []chstore.EventView{
			{Kind: 3, PubKey: testPubkey, CreatedAt: time.Unix(100, 0), Tags: [][]string{{"p", follow1}}},
			{Kind: 3, PubKey: testPubkey, CreatedAt: time.Unix(200, 0), Tags: [][]string{{"p", follow1}, {"p", follow2}}},
		},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/social-graph?viewer="+testPubkey, nil)
	handler.socialGraph(rec, req)

	var resp SocialGraphResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if len(resp.Follows) != 2 {
		t.Fatalf("follows = %+v, want the created_at:200 list (LWW)", resp.Follows)
	}
}
