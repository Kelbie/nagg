package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

type fakeEventCounter struct {
	count          uint64
	kindStats      map[int]chstore.EventKindStats
	eventCountErr  error
	kindStatsErr   error
	requestedKinds []int
}

func (f *fakeEventCounter) EventCount(context.Context) (uint64, error) {
	return f.count, f.eventCountErr
}

func (f *fakeEventCounter) EventKindStats(_ context.Context, kinds []int) (map[int]chstore.EventKindStats, error) {
	f.requestedKinds = append([]int(nil), kinds...)
	return f.kindStats, f.kindStatsErr
}

func TestHealthHandlerReturnsEventCount(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	counter := &fakeEventCounter{
		count: 12345,
		kindStats: map[int]chstore.EventKindStats{
			1:    {Count: 42, StoredBytesRaw: 1_500_000_000},
			9735: {Count: 7, StoredBytesRaw: 250_000_000},
		},
	}

	configuredKinds := []int{0, 1, 9735, 38000}

	healthHandler(counter, configuredKinds)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.OK != "true" || body.EventCount != 12345 || body.Error != "" {
		t.Fatalf("body = %+v", body)
	}
	if !reflect.DeepEqual(counter.requestedKinds, configuredKinds) {
		t.Fatalf("requested kinds = %v, want %v", counter.requestedKinds, configuredKinds)
	}
	if len(body.EventKinds) != len(configuredKinds) {
		t.Fatalf("eventKinds len = %d, want %d", len(body.EventKinds), len(configuredKinds))
	}
	if body.EventKinds[0] != (eventKindBreakdown{Kind: 0, Description: "User Metadata", Source: "NIP-01", Count: 0, StoredBytes: 0, StoredGB: 0}) {
		t.Fatalf("first event kind = %+v", body.EventKinds[0])
	}
	if kind := eventKindByNumber(body.EventKinds, 1); kind == nil || kind.Description != "Short Text Note" || kind.Source != "NIP-10" || kind.Count != 42 || kind.StoredBytes != 1_500_000_000 || kind.StoredGB != 1.5 {
		t.Fatalf("kind 1 breakdown = %+v", kind)
	}
	if kind := eventKindByNumber(body.EventKinds, 9735); kind == nil || kind.Description != "Zap" || kind.Source != "NIP-57" || kind.Count != 7 || kind.StoredBytes != 250_000_000 || kind.StoredGB != 0.25 {
		t.Fatalf("kind 9735 breakdown = %+v", kind)
	}
	if kind := eventKindByNumber(body.EventKinds, 38000); kind == nil || kind.Description != "Ecash Mint Recommendation" || kind.Source != "NIP-87" {
		t.Fatalf("kind 38000 breakdown = %+v", kind)
	}
}

func TestHealthHandlerReturnsUnavailableOnEventCountError(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	healthHandler(&fakeEventCounter{eventCountErr: errors.New("clickhouse unavailable")}, []int{1})(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.OK != "false" || body.Error == "" || body.EventCount != 0 {
		t.Fatalf("body = %+v", body)
	}
}

func TestHealthHandlerReturnsUnavailableOnEventKindCountError(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	healthHandler(&fakeEventCounter{
		count:        12345,
		kindStatsErr: errors.New("clickhouse unavailable"),
	}, []int{1})(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.OK != "false" || body.Error != "clickhouse event kind stats failed" || body.EventCount != 0 || body.EventKinds != nil {
		t.Fatalf("body = %+v", body)
	}
}

func TestHealthHandlerReportsOnlyConfiguredKinds(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	counter := &fakeEventCounter{
		count: 9,
		kindStats: map[int]chstore.EventKindStats{
			1:     {Count: 3, StoredBytesRaw: 100},
			30078: {Count: 6, StoredBytesRaw: 200},
		},
	}

	healthHandler(counter, []int{1, 1, 30079})(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(counter.requestedKinds, []int{1, 30079}) {
		t.Fatalf("requested kinds = %v", counter.requestedKinds)
	}
	if len(body.EventKinds) != 2 {
		t.Fatalf("eventKinds len = %d, want 2", len(body.EventKinds))
	}
	if kind := eventKindByNumber(body.EventKinds, 30078); kind != nil {
		t.Fatalf("removed kind 30078 should not be reported: %+v", kind)
	}
	unknown := eventKindByNumber(body.EventKinds, 30079)
	if unknown == nil || unknown.Description != "Unknown Nostr event kind" || unknown.Source != "" {
		t.Fatalf("unknown kind = %+v", unknown)
	}
}

func eventKindByNumber(kinds []eventKindBreakdown, want int) *eventKindBreakdown {
	for i := range kinds {
		if kinds[i].Kind == want {
			return &kinds[i]
		}
	}
	return nil
}

func TestListenAddrPrefersExplicitNaggAddr(t *testing.T) {
	addr := listenAddr(func(key string) string {
		switch key {
		case "NAGG_API_ADDR":
			return "127.0.0.1:9090"
		case "PORT":
			return "8080"
		default:
			return ""
		}
	})
	if addr != "127.0.0.1:9090" {
		t.Fatalf("addr = %q", addr)
	}
}

func TestListenAddrUsesRailwayPort(t *testing.T) {
	addr := listenAddr(func(key string) string {
		if key == "PORT" {
			return "4567"
		}
		return ""
	})
	if addr != ":4567" {
		t.Fatalf("addr = %q", addr)
	}
}

func TestListenAddrDefaultsToLocalPort(t *testing.T) {
	addr := listenAddr(func(string) string { return "" })
	if addr != ":8080" {
		t.Fatalf("addr = %q", addr)
	}
}
