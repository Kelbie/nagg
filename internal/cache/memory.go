package cache

import (
	"context"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

// memoryCache is the in-process response cache used when no Redis URL is
// configured.
//
// Without it, `NAGG_REDIS_URL` being unset meant the response cache was the
// noop — every identical feed/notification request recomputed against
// ClickHouse. That is the expensive direction on this deployment: Railway bills
// ClickHouse's cgroup memory (page cache included), so repeated scans are
// literally the bill. An in-process cache costs no extra infrastructure and
// keeps the hot repeats off the database entirely.
//
// It is bounded by total bytes rather than entry count: feed responses vary
// from a few KB to hundreds of KB, so a count-based bound cannot promise a
// memory ceiling — and nagg runs under a container memory cap.
//
// Per-entry expiry is tracked here rather than using an expirable LRU, because
// callers pass a per-entry TTL and the middleware layers its own
// stale-while-revalidate window on top of the stored timestamp.
type memoryCache struct {
	mu       sync.Mutex
	lru      *lru.Cache[string, memoryEntry]
	bytes    int64
	maxBytes int64
}

type memoryEntry struct {
	value     []byte
	expiresAt time.Time
}

// memoryCacheEntries is the LRU's entry-count ceiling. Byte accounting is the
// real bound; this only stops an unbounded key space from growing the index
// itself when every entry is tiny.
const memoryCacheEntries = 8192

// NewMemory returns an in-process cache bounded to maxBytes of response bodies.
// A non-positive maxBytes disables caching rather than growing without limit.
func NewMemory(maxBytes int64) Cache {
	if maxBytes <= 0 {
		return Disabled()
	}
	c := &memoryCache{maxBytes: maxBytes}
	// The evict callback keeps the byte counter honest no matter which path
	// removed the entry (capacity eviction, explicit remove, or overwrite).
	l, err := lru.NewWithEvict[string, memoryEntry](memoryCacheEntries, func(_ string, e memoryEntry) {
		c.bytes -= int64(len(e.value))
	})
	if err != nil {
		return Disabled()
	}
	c.lru = l
	return c
}

func (c *memoryCache) Get(ctx context.Context, key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.lru.Get(key)
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		// Expired entries are removed on read rather than swept: reads are the
		// only thing that cares, and a sweep goroutine would be pure overhead.
		c.lru.Remove(key)
		return nil, false
	}
	// Copy: the caller must not be able to mutate a live cache entry, and the
	// middleware hands this straight to a ResponseWriter.
	out := make([]byte, len(entry.value))
	copy(out, entry.value)
	return out, true
}

func (c *memoryCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) {
	if ttl <= 0 || len(value) == 0 {
		return
	}
	size := int64(len(value))
	// A single oversized response must never evict the whole cache to fit.
	if size > c.maxBytes/4 {
		return
	}
	stored := make([]byte, len(value))
	copy(stored, value)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.lru.Add(key, memoryEntry{value: stored, expiresAt: time.Now().Add(ttl)})
	c.bytes += size
	for c.bytes > c.maxBytes {
		if _, _, ok := c.lru.RemoveOldest(); !ok {
			break
		}
	}
}

func (c *memoryCache) Enabled() bool { return true }
