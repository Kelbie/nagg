package vertex

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type syncStore struct {
	pubkeys []string
	saved   []ProfileResult
}

func (s *syncStore) RecentAuthorPubkeysByFollowers(context.Context, uint64, time.Duration, int) ([]string, error) {
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

type exhaustedSyncClient struct{ calls int }

func (c *exhaustedSyncClient) ProfileRefresh(context.Context, string) (ProfileResult, error) {
	c.calls++
	return ProfileResult{}, ErrInsufficientCredits
}
func TestSyncerStopsTickOnInsufficientCredits(t *testing.T) {
	store := &syncStore{pubkeys: []string{"one", "two", "three"}}
	client := &exhaustedSyncClient{}
	var log bytes.Buffer
	syncer := NewSyncer(store, client, SyncConfig{BatchSize: 20}, slog.New(slog.NewTextHandler(&log, nil)))
	refreshed, failed, err := syncer.RunOnce(context.Background())
	if err != nil || refreshed != 0 || failed != 1 || client.calls != 1 || len(store.saved) != 0 {
		t.Fatalf("refreshed=%d failed=%d calls=%d err=%v", refreshed, failed, client.calls, err)
	}
	if strings.Count(log.String(), "vertex.sync.credits_exhausted") != 1 {
		t.Fatalf("log: %s", log.String())
	}
}
