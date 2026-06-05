// Package cache provides an optional, best-effort shared response cache for the
// nagg GraphQL and REST app-view endpoints. It is keyed by the normalized
// request (query + variables for GraphQL, method + path + query for REST) so
// identical requests reuse a computed result.
//
// The cache is intentionally best-effort: every method is safe to call when the
// cache is disabled, and backend errors are swallowed and logged rather than
// propagated, so a cache outage never breaks a request.
package cache

import (
	"context"
	"time"
)

// Cache is a best-effort byte cache. A miss (or any backend error) returns
// ok=false from Get; callers then compute the response as normal.
type Cache interface {
	// Get returns the cached bytes for key and whether it was a hit.
	Get(ctx context.Context, key string) ([]byte, bool)
	// Set stores value under key for ttl. A non-positive ttl is a no-op.
	Set(ctx context.Context, key string, value []byte, ttl time.Duration)
	// Enabled reports whether the cache actually stores anything.
	Enabled() bool
}

// noop is the disabled cache used when no Redis URL is configured.
type noop struct{}

func (noop) Get(context.Context, string) ([]byte, bool)         { return nil, false }
func (noop) Set(context.Context, string, []byte, time.Duration) {}
func (noop) Enabled() bool                                      { return false }

// Disabled returns a cache that never stores anything.
func Disabled() Cache { return noop{} }
