package appview

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

type rateLimiter struct {
	mu        sync.Mutex
	limit     int
	window    time.Duration
	buckets   map[string]rateBucket
	lastPrune time.Time
}

type rateBucket struct {
	resetAt time.Time
	count   int
}

func newRateLimiter(limit int, window time.Duration) *rateLimiter {
	if limit <= 0 {
		limit = 120
	}
	if window <= 0 {
		window = time.Minute
	}
	return &rateLimiter{
		limit:   limit,
		window:  window,
		buckets: map[string]rateBucket{},
	}
}

func (l *rateLimiter) allow(r *http.Request) bool {
	if l == nil {
		return true
	}
	key := clientIP(r)
	now := time.Now()

	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastPrune) > l.window {
		for ip, bucket := range l.buckets {
			if now.After(bucket.resetAt) {
				delete(l.buckets, ip)
			}
		}
		l.lastPrune = now
	}

	bucket := l.buckets[key]
	if bucket.resetAt.IsZero() || now.After(bucket.resetAt) {
		l.buckets[key] = rateBucket{resetAt: now.Add(l.window), count: 1}
		return true
	}
	if bucket.count >= l.limit {
		return false
	}
	bucket.count++
	l.buckets[key] = bucket
	return true
}

func clientIP(r *http.Request) string {
	for _, header := range []string{"X-Forwarded-For", "X-Real-IP"} {
		value := strings.TrimSpace(r.Header.Get(header))
		if value == "" {
			continue
		}
		if first, _, ok := strings.Cut(value, ","); ok {
			value = strings.TrimSpace(first)
		}
		if value != "" {
			return value
		}
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil && host != "" {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}
	return "unknown"
}
