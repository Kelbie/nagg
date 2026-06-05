package appview

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/nagg/internal/relayquery"
)

func TestHydrationJobCompletesInsideWait(t *testing.T) {
	backfiller := NewRelayUserFeedBackfiller(nil, UserFeedBackfillConfig{Wait: 100 * time.Millisecond})
	job := backfiller.scheduleJob(context.Background(), "fast", func(context.Context) error {
		return nil
	})

	completed, err := backfiller.waitJobs(context.Background(), []*hydrationJob{job})
	if err != nil {
		t.Fatalf("waitJobs err = %v", err)
	}
	if !completed {
		t.Fatal("waitJobs completed = false, want true")
	}
}

func TestHydrationJobReturnsSlowAndContinues(t *testing.T) {
	backfiller := NewRelayUserFeedBackfiller(nil, UserFeedBackfillConfig{Wait: 10 * time.Millisecond})
	release := make(chan struct{})
	job := backfiller.scheduleJob(context.Background(), "slow", func(context.Context) error {
		<-release
		return nil
	})

	completed, err := backfiller.waitJobs(context.Background(), []*hydrationJob{job})
	if err != nil {
		t.Fatalf("waitJobs err = %v", err)
	}
	if completed {
		t.Fatal("waitJobs completed = true, want false")
	}

	close(release)
	waitForHydrationJob(t, job)
}

func TestHydrationJobReturnsImmediatelyWhenWaitIsZero(t *testing.T) {
	backfiller := NewRelayUserFeedBackfiller(nil, UserFeedBackfillConfig{Wait: 0})
	release := make(chan struct{})
	job := backfiller.scheduleJob(context.Background(), "instant", func(context.Context) error {
		<-release
		return nil
	})

	started := time.Now()
	completed, err := backfiller.waitJobs(context.Background(), []*hydrationJob{job})
	if err != nil {
		t.Fatalf("waitJobs err = %v", err)
	}
	if completed {
		t.Fatal("waitJobs completed = true, want false")
	}
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("waitJobs took %s, want immediate return", elapsed)
	}

	close(release)
	waitForHydrationJob(t, job)
}

func TestHydrationJobDeduplicatesInFlightWork(t *testing.T) {
	backfiller := NewRelayUserFeedBackfiller(nil, UserFeedBackfillConfig{Wait: 100 * time.Millisecond})
	release := make(chan struct{})
	runs := 0
	work := func(context.Context) error {
		runs++
		<-release
		return nil
	}

	first := backfiller.scheduleJob(context.Background(), "same", work)
	second := backfiller.scheduleJob(context.Background(), "same", work)
	if first != second {
		t.Fatal("scheduleJob returned different jobs for duplicate key")
	}

	close(release)
	waitForHydrationJob(t, first)
	if runs != 1 {
		t.Fatalf("runs = %d, want 1", runs)
	}
}

func TestHydrationJobIgnoresRequestCancellation(t *testing.T) {
	backfiller := NewRelayUserFeedBackfiller(nil, UserFeedBackfillConfig{Wait: 100 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	job := backfiller.scheduleJob(ctx, "cancelled-request", func(jobCtx context.Context) error {
		started <- jobCtx
		<-release
		return nil
	})

	cancel()
	jobCtx := <-started
	select {
	case <-jobCtx.Done():
		t.Fatal("job context was canceled with the request")
	default:
	}

	close(release)
	waitForHydrationJob(t, job)
}

func TestHydrationJobReturnsFirstError(t *testing.T) {
	wantErr := errors.New("relay read failed")
	backfiller := NewRelayUserFeedBackfiller(nil, UserFeedBackfillConfig{Wait: 100 * time.Millisecond})
	job := backfiller.scheduleJob(context.Background(), "error", func(context.Context) error {
		return wantErr
	})

	completed, err := backfiller.waitJobs(context.Background(), []*hydrationJob{job})
	if !completed {
		t.Fatal("waitJobs completed = false, want true")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("waitJobs err = %v, want %v", err, wantErr)
	}
}

func waitForHydrationJob(t *testing.T, job *hydrationJob) {
	t.Helper()
	select {
	case <-job.done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for hydration job")
	}
}

func TestDMRelayKindsKeepsOnlyRelayFacingDMKinds(t *testing.T) {
	if got := dmRelayKinds(nil); !equalInts(got, []int{4, 1059}) {
		t.Fatalf("default dm relay kinds = %+v", got)
	}
	got := dmRelayKinds([]int{14, 15, 7, 1059, 4, 21059, 1059})
	if !equalInts(got, []int{4, 1059, 21059}) {
		t.Fatalf("dm relay kinds = %+v", got)
	}
}

func TestDMInboxRelaysParsesKind10050RelayTags(t *testing.T) {
	events := []relayquery.Event{
		{Event: &nostr.Event{
			Kind: 10050,
			Tags: nostr.Tags{
				{"relay", " wss://inbox.nostr.wine "},
				{"relay", "https://not-a-relay.example"},
				{"relay", "ws://localhost:7777/path#frag"},
				{"relay", "wss://inbox.nostr.wine"},
			},
		}},
		{Event: &nostr.Event{Kind: 1, Tags: nostr.Tags{{"relay", "wss://ignored.example"}}}},
	}

	got := dmInboxRelays(events)
	want := []string{"ws://localhost:7777/path", "wss://inbox.nostr.wine"}
	if !equalStrings(got, want) {
		t.Fatalf("dm inbox relays = %+v, want %+v", got, want)
	}
}

func TestOldestRelayEventCreatedAt(t *testing.T) {
	events := []relayquery.Event{
		{Event: &nostr.Event{CreatedAt: nostr.Timestamp(1_710_000_100)}},
		{Event: &nostr.Event{CreatedAt: nostr.Timestamp(1_710_000_050)}},
		{Event: nil},
	}
	if got := oldestRelayEventCreatedAt(events); got != 1_710_000_050 {
		t.Fatalf("oldest = %d", got)
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
