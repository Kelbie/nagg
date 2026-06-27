package auditor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

const sampleMints = `[
  {"id":1,"url":"https://mint.one","name":"","state":"OK","n_mints":10,"n_melts":5,"n_errors":1,
   "info":"{\"name\":\"Mint One\",\"icon_url\":\"https://i/1.png\",\"description\":\"first\",\"contact\":[{\"method\":\"nostr\",\"info\":\"npub1xyz\"},{\"method\":\"email\",\"info\":\"a@b.c\"}],\"nuts\":{\"4\":{\"methods\":[{\"method\":\"bolt11\",\"unit\":\"sat\"},{\"method\":\"bolt11\",\"unit\":\"usd\"},{\"method\":\"bolt11\",\"unit\":\"sat\"}]}}}"},
  {"id":2,"url":"https://mint.two","name":"Mint Two","state":"OK","n_mints":0,"n_melts":0,"n_errors":0,"info":""},
  {"id":3,"url":"","name":"skip me","state":"OK","info":"null"}
]`

func TestParseMints(t *testing.T) {
	mints, err := parseMints([]byte(sampleMints))
	if err != nil {
		t.Fatalf("parseMints: %v", err)
	}
	if len(mints) != 2 {
		t.Fatalf("got %d mints, want 2 (empty-url entry dropped)", len(mints))
	}
	one := mints[0]
	if one.Name != "Mint One" { // name falls back to NUT-06 info.name
		t.Errorf("name = %q, want Mint One", one.Name)
	}
	if one.IconURL != "https://i/1.png" || one.Description != "first" {
		t.Errorf("icon/desc = %q/%q", one.IconURL, one.Description)
	}
	if one.OperatorContact != "npub1xyz" {
		t.Errorf("operator contact = %q, want npub1xyz", one.OperatorContact)
	}
	if len(one.Units) != 2 || one.Units[0] != "sat" || one.Units[1] != "usd" {
		t.Errorf("units = %v, want [sat usd] deduped+sorted", one.Units)
	}
	if one.State != "OK" || one.NMints != 10 || one.NErrors != 1 {
		t.Errorf("audit fields wrong: %+v", one)
	}
	// info "" → no enrichment, name from the top-level field
	if mints[1].Name != "Mint Two" || len(mints[1].Units) != 0 {
		t.Errorf("mint two = %+v", mints[1])
	}
}

func TestNostrContactPairForm(t *testing.T) {
	info := parseInfo(`{"contact":[["nostr","abc123"],["email","x@y.z"]]}`)
	if got := info.nostrContact(); got != "abc123" {
		t.Fatalf("pair-form contact = %q, want abc123", got)
	}
}

func TestMintsCachesAndServesStaleOnError(t *testing.T) {
	var calls int32
	failNow := int32(0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		if atomic.LoadInt32(&failNow) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(sampleMints))
	}))
	defer srv.Close()

	c := NewHTTPClient(srv.URL, WithTTL(time.Hour, 24*time.Hour), WithHTTPClient(srv.Client()))
	ctx := context.Background()

	if _, err := c.Mints(ctx); err != nil {
		t.Fatalf("first fetch: %v", err)
	}
	// Second call within TTL → served from cache, no extra upstream hit.
	if _, err := c.Mints(ctx); err != nil {
		t.Fatalf("cached fetch: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("upstream called %d times, want 1 (TTL cache)", got)
	}

	// Force a refresh (expire) and make upstream fail → stale snapshot served.
	c.mu.Lock()
	c.fetchedAt = time.Now().Add(-2 * time.Hour) // past TTL, within StaleFor
	c.mu.Unlock()
	atomic.StoreInt32(&failNow, 1)
	mints, err := c.Mints(ctx)
	if err != nil || len(mints) != 2 {
		t.Fatalf("stale serve failed: err=%v len=%d", err, len(mints))
	}
}
