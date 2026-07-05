// Package chgate is the process-wide concurrency gate for heavy ClickHouse
// work. The shared Railway ClickHouse sheds load fast once more than a handful
// of heavy reads run at once — but the old per-surface semaphore only covered
// heavy REST routes: GraphQL (its own mux), detached backfill jobs, and the
// periodic rollup all piled onto CH past the ceiling, producing exactly the
// 5xx/connection-reset behavior the semaphore existed to prevent. One gate,
// one owner, every heavy path acquires it.
package chgate

import (
	"context"
	"net/http"
)

type Gate struct {
	slots chan struct{}
}

// New creates a gate with n slots. n <= 0 returns a nil gate, on which every
// method is a no-op (unlimited).
func New(n int) *Gate {
	if n <= 0 {
		return nil
	}
	return &Gate{slots: make(chan struct{}, n)}
}

// Acquire blocks until a slot frees or ctx expires. The returned release func
// must be called exactly once (typically deferred).
func (g *Gate) Acquire(ctx context.Context) (func(), error) {
	if g == nil {
		return func() {}, nil
	}
	select {
	case g.slots <- struct{}{}:
		return func() { <-g.slots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Middleware gates an HTTP handler: queued requests wait bounded by their own
// context and get a retryable 503 if it expires first. Wrap INSIDE any
// response cache so cache hits never wait.
func (g *Gate) Middleware(next http.HandlerFunc) http.HandlerFunc {
	if g == nil {
		return next
	}
	return func(w http.ResponseWriter, r *http.Request) {
		release, err := g.Acquire(r.Context())
		if err != nil {
			http.Error(w, "server busy, retry shortly", http.StatusServiceUnavailable)
			return
		}
		defer release()
		next(w, r)
	}
}
