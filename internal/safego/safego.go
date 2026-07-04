// Package safego is the mandatory way to start detached goroutines. net/http
// recovers panics on its request goroutines, but a panic in a bare `go func()`
// — cache revalidation, on-demand backfill, relay fan-out, firehose — kills the
// WHOLE process, and one binary hosts the API and every background worker. A
// single malformed relay event or nil deref in a detached job must degrade that
// job, not take production down.
package safego

import (
	"log/slog"
	"runtime/debug"
)

// Go runs fn on a new goroutine, converting a panic into an error log with a
// stack trace. `name` identifies the job family in logs.
func Go(name string, fn func()) {
	go func() {
		defer Recover(name)
		fn()
	}()
}

// Recover is the deferred half of Go, exposed for goroutines that need custom
// spawning (worker pools, per-item loops): `defer safego.Recover("job")`.
func Recover(name string) {
	if r := recover(); r != nil {
		slog.Error("goroutine panicked (recovered)",
			"job", name,
			"panic", r,
			"stack", string(debug.Stack()),
		)
	}
}
