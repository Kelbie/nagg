package cache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestMemoryCacheHitAndExpiry(t *testing.T) {
	ctx := context.Background()
	c := NewMemory(1 << 20)
	if !c.Enabled() {
		t.Fatal("in-process cache should report enabled; a disabled cache is what left every request hitting ClickHouse")
	}

	c.Set(ctx, "k", []byte("value"), time.Minute)
	got, ok := c.Get(ctx, "k")
	if !ok || string(got) != "value" {
		t.Fatalf("Get = %q, %v; want hit", got, ok)
	}

	// A negative TTL must not store: the middleware uses it to mean "do not cache".
	c.Set(ctx, "no-ttl", []byte("v"), 0)
	if _, ok := c.Get(ctx, "no-ttl"); ok {
		t.Fatal("zero TTL should not be stored")
	}

	c.Set(ctx, "short", []byte("v"), time.Nanosecond)
	time.Sleep(2 * time.Millisecond)
	if _, ok := c.Get(ctx, "short"); ok {
		t.Fatal("expired entry should miss")
	}
}

func TestMemoryCacheDoesNotAliasCallerBytes(t *testing.T) {
	ctx := context.Background()
	c := NewMemory(1 << 20)

	value := []byte("original")
	c.Set(ctx, "k", value, time.Minute)
	value[0] = 'X' // caller reuses its buffer

	got, ok := c.Get(ctx, "k")
	if !ok || string(got) != "original" {
		t.Fatalf("cache aliased the caller's buffer: got %q", got)
	}
	got[0] = 'Y' // mutating the returned copy must not poison the entry
	again, _ := c.Get(ctx, "k")
	if string(again) != "original" {
		t.Fatalf("returned slice aliased the stored entry: got %q", again)
	}
}

func TestMemoryCacheHonoursByteBound(t *testing.T) {
	ctx := context.Background()
	// 16 KiB ceiling, 1 KiB entries: the bound must evict, not grow.
	c := NewMemory(16 << 10).(*memoryCache)
	payload := make([]byte, 1<<10)

	for i := 0; i < 200; i++ {
		c.Set(ctx, fmt.Sprintf("k%d", i), payload, time.Minute)
	}
	if c.bytes > c.maxBytes {
		t.Fatalf("cache grew past its bound: %d > %d", c.bytes, c.maxBytes)
	}
	// The most recent write must survive; the oldest must not.
	if _, ok := c.Get(ctx, "k199"); !ok {
		t.Fatal("most recent entry should be retained")
	}
	if _, ok := c.Get(ctx, "k0"); ok {
		t.Fatal("oldest entry should have been evicted")
	}
}

func TestMemoryCacheRejectsOversizedEntry(t *testing.T) {
	ctx := context.Background()
	c := NewMemory(1 << 10).(*memoryCache)
	// A single response bigger than a quarter of the cache must be skipped
	// rather than evicting everything else to fit.
	c.Set(ctx, "small", []byte("keep me"), time.Minute)
	c.Set(ctx, "huge", make([]byte, 1<<10), time.Minute)

	if _, ok := c.Get(ctx, "huge"); ok {
		t.Fatal("oversized entry should not be stored")
	}
	if _, ok := c.Get(ctx, "small"); !ok {
		t.Fatal("oversized write should not have evicted the existing entry")
	}
}

func TestNewFallsBackToMemoryWithoutRedis(t *testing.T) {
	if c := New("", 1<<20, nil); !c.Enabled() {
		t.Fatal("no Redis URL should yield the in-process cache, not the noop")
	}
	if c := New("", 0, nil); c.Enabled() {
		t.Fatal("a non-positive memory bound should disable rather than grow unbounded")
	}
}
