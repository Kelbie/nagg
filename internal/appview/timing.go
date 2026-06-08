package appview

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// phaseTimer accumulates per-request phase durations so the app-view can emit a
// Server-Timing header. It is request-scoped and carried in the context, letting
// handlers attribute time to "db", "hydrate", etc. without threading a struct
// through every signature. The header is read by scripts/parity-check.ts to
// compare server compute (independent of the HTTP/Redis layer) across deploys.
type phaseTimer struct {
	mu       sync.Mutex
	segments map[string]time.Duration
	order    []string
}

type timingCtxKey struct{}

func newPhaseTimer() *phaseTimer {
	return &phaseTimer{segments: map[string]time.Duration{}}
}

// withTimer attaches a fresh phaseTimer to ctx and returns both.
func withTimer(ctx context.Context) (context.Context, *phaseTimer) {
	t := newPhaseTimer()
	return context.WithValue(ctx, timingCtxKey{}, t), t
}

func timerFrom(ctx context.Context) *phaseTimer {
	t, _ := ctx.Value(timingCtxKey{}).(*phaseTimer)
	return t
}

func (t *phaseTimer) add(name string, d time.Duration) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.segments[name]; !ok {
		t.order = append(t.order, name)
	}
	t.segments[name] += d
}

// recordPhase times fn and records its duration under name on the context timer.
// It is a no-op (other than running fn) when no timer is present.
func recordPhase(ctx context.Context, name string, fn func() error) error {
	start := time.Now()
	err := fn()
	timerFrom(ctx).add(name, time.Since(start))
	return err
}

// header renders the accumulated phases plus the total as a Server-Timing value.
func (t *phaseTimer) header(total time.Duration) string {
	parts := []string{fmt.Sprintf("app;dur=%.1f", ms(total))}
	if t != nil {
		t.mu.Lock()
		for _, name := range t.order {
			parts = append(parts, fmt.Sprintf("%s;dur=%.1f", name, ms(t.segments[name])))
		}
		t.mu.Unlock()
	}
	return strings.Join(parts, ", ")
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

// timingWriter injects the Server-Timing header on the first write. Because the
// app-view marshals each response fully before writing (writeJSON), time-to-first
// -byte is effectively total server compute, so this is an accurate per-request
// compute metric without per-handler plumbing.
type timingWriter struct {
	http.ResponseWriter
	timer       *phaseTimer
	start       time.Time
	wroteHeader bool
}

func (w *timingWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.wroteHeader = true
		w.Header().Set("Server-Timing", w.timer.header(time.Since(w.start)))
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *timingWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}
