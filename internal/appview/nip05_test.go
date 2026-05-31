package appview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
)

func TestNIP05ValidatorRechecksCachedResolutionAgainstPubkey(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/.well-known/nostr.json" || r.URL.Query().Get("name") != "alice" {
			t.Fatalf("unexpected request %s", r.URL.String())
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"names": map[string]string{"alice": testPubkey},
		})
	}))
	defer server.Close()

	client := server.Client()
	targetURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client.Transport = rewriteHostTransport{
		base:   client.Transport,
		scheme: targetURL.Scheme,
		host:   targetURL.Host,
	}

	validator := newNIP05Validator(true)
	validator.client = client

	status := validator.validate(context.Background(), "alice@example.test", testPubkey)
	if !status.valid || status.conflict {
		t.Fatalf("first status = %+v", status)
	}

	conflict := validator.validate(
		context.Background(),
		"alice@example.test",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	)
	if !conflict.conflict || conflict.valid {
		t.Fatalf("conflict status = %+v", conflict)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

type rewriteHostTransport struct {
	base   http.RoundTripper
	scheme string
	host   string
}

func (t rewriteHostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	next := req.Clone(req.Context())
	next.URL.Scheme = t.scheme
	next.URL.Host = t.host
	return t.base.RoundTrip(next)
}
