package vertex

import (
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

func TestPagerankToScore(t *testing.T) {
	rank := 0.001
	nodes := 1000
	score := PagerankToScore(&rank, &nodes)
	if score == nil {
		t.Fatal("expected score")
	}
	if *score != 46.42 {
		t.Fatalf("score = %.2f, want 46.42", *score)
	}

	if got := PagerankToScore(nil, &nodes); got != nil {
		t.Fatalf("nil rank score = %v, want nil", *got)
	}
	if got := PagerankToScore(&rank, nil); got != nil {
		t.Fatalf("nil nodes score = %v, want nil", *got)
	}
}

func TestParseSearchResultsNormalizesSortsAndLimits(t *testing.T) {
	low := 0.001
	high := 0.01
	event := &nostr.Event{
		Content: `[{"pubkey":"82341f05fdb1dffbc78894993292171ed03abbed34a95f22f55f9b6371723ee6","rank":0.001},{"pubkey":"invalid","rank":100},{"pubkey":"3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d","rank":0.01}]`,
		Tags:    nostr.Tags{{"nodes", "1000"}},
	}

	results, err := parseSearchResults(event, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	if results[0].PubKey != "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d" {
		t.Fatalf("first pubkey = %s", results[0].PubKey)
	}
	if results[0].Rank == nil || *results[0].Rank != high {
		t.Fatalf("first rank = %v, want %v", results[0].Rank, high)
	}
	if results[0].Score == nil {
		t.Fatal("expected score")
	}

	allResults, err := parseSearchResults(event, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(allResults) != 2 {
		t.Fatalf("all results len = %d, want 2", len(allResults))
	}
	if allResults[1].Rank == nil || *allResults[1].Rank != low {
		t.Fatalf("second rank = %v, want %v", allResults[1].Rank, low)
	}
}

func TestParseProfileResult(t *testing.T) {
	event := &nostr.Event{
		Content: `[{"pubkey":"82341f05fdb1dffbc78894993292171ed03abbed34a95f22f55f9b6371723ee6","rank":0.02,"followers":10,"follows":3,"created_at":1710000000},{"pubkey":"3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d","rank":0.01}]`,
		Tags:    nostr.Tags{{"nodes", "1000"}},
	}

	profile, err := parseProfileResult(event)
	if err != nil {
		t.Fatal(err)
	}
	if profile.PubKey != "82341f05fdb1dffbc78894993292171ed03abbed34a95f22f55f9b6371723ee6" {
		t.Fatalf("pubkey = %s", profile.PubKey)
	}
	if profile.Rank != 0.02 {
		t.Fatalf("rank = %v, want 0.02", profile.Rank)
	}
	if profile.Followers == nil || *profile.Followers != 10 {
		t.Fatalf("followers = %v, want 10", profile.Followers)
	}
	if len(profile.TopFollowers) != 1 {
		t.Fatalf("top followers len = %d, want 1", len(profile.TopFollowers))
	}
	if profile.TopFollowers[0].Score == nil {
		t.Fatal("expected top follower score")
	}
}
