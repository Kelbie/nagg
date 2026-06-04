package vertex

import (
	"context"
	"testing"
)

type syncStore struct {
	pubkeys []string
	saved   []ProfileResult
}

func (s *syncStore) RecentAuthorPubkeysByFollowers(context.Context, uint64, int) ([]string, error) {
	return s.pubkeys, nil
}

func (s *syncStore) SaveVertexProfile(_ context.Context, profile ProfileResult) error {
	s.saved = append(s.saved, profile)
	return nil
}

type syncClient struct {
	profiles map[string]ProfileResult
}

func (c syncClient) ProfileRefresh(_ context.Context, pubkey string) (ProfileResult, error) {
	return c.profiles[pubkey], nil
}

func TestSyncerRefreshesRecentAuthorScores(t *testing.T) {
	store := &syncStore{pubkeys: []string{scoreProviderTestPubkey}}
	client := syncClient{profiles: map[string]ProfileResult{
		scoreProviderTestPubkey: {PubKey: scoreProviderTestPubkey},
	}}
	syncer := NewSyncer(store, client, SyncConfig{MinFollowers: 500, BatchSize: 10}, nil)

	refreshed, failed, err := syncer.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if refreshed != 1 || failed != 0 {
		t.Fatalf("refreshed=%d failed=%d, want 1/0", refreshed, failed)
	}
	if len(store.saved) != 1 || store.saved[0].PubKey != scoreProviderTestPubkey {
		t.Fatalf("saved = %+v", store.saved)
	}
}
