// Package runtimelimits pins the Go runtime to the container's actual
// resources. Railway runs these binaries in cgroup-limited containers, but by
// default Go sizes itself off the HOST: GOMAXPROCS = host CPU count
// (oversubscribing the CPU quota) and no GOMEMLIMIT (GOGC=100 lets the heap
// reach ~2x the live set, which cgroup-OOM-kills the process instead of
// triggering GC). Every entrypoint calls Apply() first.
package runtimelimits

import (
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // registers /debug/pprof on the default mux, served only on NAGG_PPROF_ADDR
	"os"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"go.uber.org/automaxprocs/maxprocs"
)

// Apply sets GOMAXPROCS from the cgroup CPU quota and GOMEMLIMIT to 85% of the
// cgroup memory limit (leaving headroom for non-heap memory), then starts the
// env-gated pprof listener. An explicit GOMEMLIMIT env var wins — automemlimit
// respects it and does nothing.
func Apply() {
	if _, err := maxprocs.Set(maxprocs.Logger(func(format string, args ...any) {
		slog.Info("runtime: " + fmt.Sprintf(format, args...))
	})); err != nil {
		slog.Warn("runtime: automaxprocs failed", "error", err)
	}
	limit, err := memlimit.SetGoMemLimitWithOpts(memlimit.WithRatio(0.85))
	if err != nil {
		// No cgroup limit visible (local dev, tests) — nothing to pin.
		slog.Info("runtime: no cgroup memory limit detected; GOMEMLIMIT unchanged", "reason", err.Error())
	} else {
		slog.Info("runtime: GOMEMLIMIT set from cgroup", "bytes", limit)
	}

	// Live heap/goroutine profiles for diagnosing a degrading instance —
	// previously impossible ("keeps blowing up" with no profile to read).
	// Off unless NAGG_PPROF_ADDR is set; bind it to localhost or rely on
	// Railway's private networking, never expose it on the public port.
	if addr := os.Getenv("NAGG_PPROF_ADDR"); addr != "" {
		go func() {
			slog.Info("runtime: pprof listening", "addr", addr)
			if err := http.ListenAndServe(addr, nil); err != nil {
				slog.Warn("runtime: pprof listener stopped", "error", err)
			}
		}()
	}
}
