package appview

import (
	"context"
	"errors"
	"testing"
	"time"
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
