package vertex

import (
	"context"
	"errors"
	"testing"
)

const scoreProviderTestPubkey = "82341f05fdb1dffbc78894993292171ed03abbed34a95f22f55f9b6371723ee6"

type scoreProviderStore struct {
	cached   ProfileResult
	cacheOK  bool
	cacheErr error
	saved    []ProfileResult
}

func (s *scoreProviderStore) CachedVertexProfile(context.Context, string) (ProfileResult, bool, error) {
	return s.cached, s.cacheOK, s.cacheErr
}

func (s *scoreProviderStore) SaveVertexProfile(_ context.Context, profile ProfileResult) error {
	s.saved = append(s.saved, profile)
	return nil
}

type scoreProviderClient struct {
	profile ProfileResult
	err     error
	calls   int
}

func (c *scoreProviderClient) ProfileRefresh(context.Context, string) (ProfileResult, error) {
	c.calls++
	return c.profile, c.err
}

func TestScoreProviderSkipsBelowFollowerThreshold(t *testing.T) {
	store := &scoreProviderStore{}
	client := &scoreProviderClient{}
	provider := NewScoreProvider(store, client, 500)

	profile, fromCache, err := provider.AuthorProfileWithFollowers(context.Background(), scoreProviderTestPubkey, 499)
	if err != nil {
		t.Fatal(err)
	}
	if profile.PubKey != "" || fromCache {
		t.Fatalf("profile = %+v fromCache=%v, want empty fresh miss", profile, fromCache)
	}
	if client.calls != 0 || len(store.saved) != 0 {
		t.Fatalf("client calls=%d saved=%d, want no work", client.calls, len(store.saved))
	}
}

func TestScoreProviderRefreshesAndSavesAtThreshold(t *testing.T) {
	score := 88.5
	store := &scoreProviderStore{}
	client := &scoreProviderClient{profile: ProfileResult{PubKey: scoreProviderTestPubkey, Score: &score}}
	provider := NewScoreProvider(store, client, 500)

	profile, fromCache, err := provider.AuthorProfileWithFollowers(context.Background(), scoreProviderTestPubkey, 500)
	if err != nil {
		t.Fatal(err)
	}
	if fromCache || profile.Score == nil || *profile.Score != score {
		t.Fatalf("profile = %+v fromCache=%v, want fresh score %v", profile, fromCache, score)
	}
	if client.calls != 1 || len(store.saved) != 1 {
		t.Fatalf("client calls=%d saved=%d, want refresh and save", client.calls, len(store.saved))
	}
}

func TestScoreProviderFallsBackToCacheWhenRefreshFails(t *testing.T) {
	score := 42.0
	store := &scoreProviderStore{
		cached:  ProfileResult{PubKey: scoreProviderTestPubkey, Score: &score},
		cacheOK: true,
	}
	client := &scoreProviderClient{err: errors.New("dvm unavailable")}
	provider := NewScoreProvider(store, client, 500)

	profile, fromCache, err := provider.AuthorProfileWithFollowers(context.Background(), scoreProviderTestPubkey, 900)
	if err != nil {
		t.Fatal(err)
	}
	if !fromCache || profile.Score == nil || *profile.Score != score {
		t.Fatalf("profile = %+v fromCache=%v, want cached score", profile, fromCache)
	}
}
