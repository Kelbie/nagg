package rollup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vertex-lab/nagg/internal/clickhouse"
)

type mockStore struct {
	calls      []string
	failOn     string
	savedState *clickhouse.RollupState
	gotSince   time.Time
	gotLimit   int
	gotThVer   string
}

func (m *mockStore) record(name string) error {
	m.calls = append(m.calls, name)
	if m.failOn == name {
		return errors.New("boom: " + name)
	}
	return nil
}

func (m *mockStore) RecomputeRefEdges(_ context.Context, since time.Time, limit int) error {
	m.gotSince, m.gotLimit = since, limit
	return m.record("reply_edges")
}
func (m *mockStore) RecomputeGatedRefCounts(_ context.Context, _ time.Time, _ int, th clickhouse.Thresholds, _ time.Time) error {
	m.gotThVer = th.Version
	return m.record("engagement_real")
}
func (m *mockStore) RecomputePubkeyStats(_ context.Context, _ time.Time, _ int, _ time.Time) error {
	return m.record("pubkey_stats")
}
func (m *mockStore) RecomputeRankFeatures(_ context.Context, _ time.Time, _ int, _ clickhouse.Thresholds, _ time.Time) error {
	return m.record("rank_features")
}
func (m *mockStore) RecomputeNotificationsFeed(_ context.Context, _ time.Time) (bool, error) {
	return true, m.record("viewer_feed")
}
func (m *mockStore) RunRetention(_ context.Context, _ bool) ([]clickhouse.RetentionRunResult, error) {
	return nil, m.record("retention")
}
func (m *mockStore) LoadRollupState(_ context.Context, task string) (clickhouse.RollupState, error) {
	return clickhouse.RollupState{Task: task}, nil
}
func (m *mockStore) SaveRollupState(_ context.Context, st clickhouse.RollupState) error {
	m.savedState = &st
	return m.record("save_state")
}

func TestRunOnce_RunsStagesInDependencyOrder(t *testing.T) {
	m := &mockStore{}
	r := NewRunner(m, Config{RecentWindow: 24 * time.Hour, MaxTargets: 100, Thresholds: clickhouse.Thresholds{Version: "v9", MinActorScore: 0.5}}, nil)
	if err := r.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce error: %v", err)
	}
	want := []string{"reply_edges", "engagement_real", "pubkey_stats", "rank_features", "save_state"}
	if len(m.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", m.calls, want)
	}
	for i := range want {
		if m.calls[i] != want[i] {
			t.Fatalf("stage %d = %q, want %q (order: %v)", i, m.calls[i], want[i], m.calls)
		}
	}
	if m.gotLimit != 100 {
		t.Errorf("limit threaded = %d, want 100", m.gotLimit)
	}
	if m.gotThVer != "v9" {
		t.Errorf("threshold version threaded = %q, want v9", m.gotThVer)
	}
	if m.savedState == nil || m.savedState.Task != rollupTask {
		t.Errorf("cursor not persisted with task %q", rollupTask)
	}
}

func TestRunOnce_AbortsOnStageError(t *testing.T) {
	m := &mockStore{failOn: "engagement_real"}
	r := NewRunner(m, Config{}, nil)
	err := r.RunOnce(context.Background())
	if err == nil {
		t.Fatal("expected error from failing stage")
	}
	// reply_edges ran, engagement_real failed, nothing after should run.
	for _, c := range m.calls {
		if c == "pubkey_stats" || c == "rank_features" || c == "save_state" {
			t.Errorf("stage %q ran after an earlier failure; calls=%v", c, m.calls)
		}
	}
}

func TestNewRunner_Defaults(t *testing.T) {
	r := NewRunner(&mockStore{}, Config{}, nil)
	if r.config.Interval != 15*time.Minute {
		t.Errorf("default interval = %s, want 15m", r.config.Interval)
	}
	if r.config.RecentWindow != 48*time.Hour {
		t.Errorf("default window = %s, want 48h", r.config.RecentWindow)
	}
	if r.config.MaxTargets != 50000 {
		t.Errorf("default max targets = %d, want 50000", r.config.MaxTargets)
	}
	if r.config.Thresholds.Version != "v1" {
		t.Errorf("default threshold version = %q, want v1", r.config.Thresholds.Version)
	}
}

func TestRun_NilGuards(t *testing.T) {
	var r *Runner
	r.Run(context.Background()) // must not panic
	r2 := &Runner{}
	r2.Run(context.Background()) // nil store -> returns
}
