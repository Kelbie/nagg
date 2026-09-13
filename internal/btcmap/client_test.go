package btcmap

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestBtcmapPassthroughAndDefaults(t *testing.T) {
	var path string
	var query url.Values
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, query = r.URL.Path, r.URL.Query()
		if r.Header.Get("Accept") != "application/json" || r.Header.Get("Authorization") != "" {
			t.Error("unexpected headers")
		}
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(path, "/places") {
			_, _ = w.Write([]byte(`[{"id":1,"lat":2,"lon":3,"icon":"cafe","updated_at":"2026-01-01Z"}]`))
		} else {
			_, _ = w.Write([]byte(`{"id":1,"osm:payment:lightning":"yes"}`))
		}
	}))
	defer server.Close()
	c := NewClient(server.URL + "/")
	body, err := c.Fetch(context.Background(), "", nil)
	if err != nil || !json.Valid(body) || path != "/v4/places" || query.Get("fields") != ListFields || query.Get("include_deleted") != "false" {
		t.Fatalf("default %s %v %s %v", body, err, path, query)
	}
	params := url.Values{"fields": {"id,lat,lon"}, "include_deleted": {"true"}, "updated_since": {"2026-01-01T00:00:00Z"}, "limit": {"3"}, "refresh": {"1"}, "url": {"https://bad.example"}}
	_, err = c.Fetch(context.Background(), "", params)
	if err != nil || query.Get("limit") != "3" || query.Get("fields") != "id,lat,lon" || query.Get("include_deleted") != "true" || query.Get("updated_since") != params.Get("updated_since") || query.Has("refresh") || query.Has("url") {
		t.Fatalf("query %v err=%v", query, err)
	}
	for _, id := range []string{"1", "node:28", "way:55", "relation:3"} {
		body, err = c.Fetch(context.Background(), id, nil)
		if err != nil || path != "/v4/places/"+id || query.Get("fields") != DetailFields || !strings.Contains(string(body), `"osm:payment:lightning":"yes"`) {
			t.Fatalf("detail %s %s %v", id, body, err)
		}
	}
}

func TestBtcmapErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   int
	}{
		{"not found", 404, `private upstream error`, 404},
		{"bad request", 400, `private upstream error`, 400},
		{"limited", 429, `private upstream error`, 429},
		{"server", 500, `private upstream error`, 502},
		{"unavailable", 503, `private upstream error`, 502},
		{"timeout", 504, `private upstream error`, 504},
		{"redirect", 302, ``, 502},
		{"html", 200, `<html>error</html>`, 502},
		{"wrong shape", 200, `{}`, 502},
		{"null", 200, `null`, 502},
		{"trailing", 200, `[] {}`, 502},
		{"oversize", 200, `["` + strings.Repeat("x", maxBodyBytes) + `"]`, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", "https://unrelated.example")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			_, err := NewClient(server.URL).Fetch(context.Background(), "", nil)
			assertStatus(t, err, tc.want)
		})
	}
}

func assertStatus(t *testing.T, err error, want int) {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Status != want || strings.Contains(err.Error(), "private") {
		t.Fatalf("error %v, want %d", err, want)
	}
}

func TestBtcmapInvalidIDsAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("unexpected upstream request") }))
	defer server.Close()
	c := NewClient(server.URL)
	for _, id := range []string{"..", "../saved", "1/comments", "1?fields=*", "https://elsewhere", "saved", "-1", strings.Repeat("1", 129)} {
		_, err := c.Fetch(context.Background(), id, nil)
		assertStatus(t, err, 400)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := c.Fetch(ctx, "1", nil)
	assertStatus(t, err, 504)
	if c.http.Timeout != 8*time.Second {
		t.Fatal("incorrect HTTP timeout")
	}
}

func TestBtcmapTransportAndReadTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	c := NewClient(server.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := c.Fetch(ctx, "", nil)
	assertStatus(t, err, 504)
	server.Close()
	_, err = c.Fetch(context.Background(), "", nil)
	assertStatus(t, err, 502)
}
