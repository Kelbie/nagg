package appview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const nip05TTL = 24 * time.Hour

type nip05Validator struct {
	enabled bool
	client  *http.Client
	mu      sync.Mutex
	cache   map[string]nip05CacheEntry
}

type nip05CacheEntry struct {
	resolved  nip05Resolution
	expiresAt time.Time
}

type nip05Status struct {
	valid    bool
	conflict bool
}

type nip05Resolution struct {
	pubkey string
	found  bool
}

func newNIP05Validator(enabled bool) *nip05Validator {
	return &nip05Validator{
		enabled: enabled,
		client:  &http.Client{Timeout: 5 * time.Second},
		cache:   map[string]nip05CacheEntry{},
	}
}

func (v *nip05Validator) validate(ctx context.Context, nip05 string, pubkey string) nip05Status {
	if v == nil || !v.enabled {
		return nip05Status{}
	}
	nip05 = strings.TrimSpace(nip05)
	if nip05 == "" {
		return nip05Status{}
	}
	cacheKey := strings.ToLower(nip05)
	now := time.Now()

	v.mu.Lock()
	if entry, ok := v.cache[cacheKey]; ok && now.Before(entry.expiresAt) {
		v.mu.Unlock()
		return entry.resolved.statusFor(pubkey)
	}
	v.mu.Unlock()

	resolved := v.fetch(ctx, nip05)
	v.mu.Lock()
	v.cache[cacheKey] = nip05CacheEntry{resolved: resolved, expiresAt: now.Add(nip05TTL)}
	v.mu.Unlock()
	return resolved.statusFor(pubkey)
}

func (v *nip05Validator) fetch(ctx context.Context, nip05 string) nip05Resolution {
	name, domain, ok := strings.Cut(nip05, "@")
	if !ok || name == "" || domain == "" {
		return nip05Resolution{}
	}
	reqURL := "https://" + domain + "/.well-known/nostr.json?name=" + url.QueryEscape(name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nip05Resolution{}
	}
	res, err := v.client.Do(req)
	if err != nil {
		return nip05Resolution{}
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nip05Resolution{}
	}
	var payload struct {
		Names map[string]string `json:"names"`
	}
	if err := json.NewDecoder(res.Body).Decode(&payload); err != nil {
		return nip05Resolution{}
	}
	actual, ok := payload.Names[name]
	if !ok || actual == "" {
		return nip05Resolution{}
	}
	return nip05Resolution{pubkey: strings.ToLower(actual), found: true}
}

func (r nip05Resolution) statusFor(pubkey string) nip05Status {
	if !r.found {
		return nip05Status{}
	}
	if r.pubkey != strings.ToLower(pubkey) {
		return nip05Status{conflict: true}
	}
	return nip05Status{valid: true}
}
