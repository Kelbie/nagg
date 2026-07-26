package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

type fakeEventCounter struct {
	count          uint64
	kindCounts     map[int]uint64
	eventCountErr  error
	kindCountsErr  error
	requestedKinds []int
}

func (f *fakeEventCounter) EventCount(context.Context) (uint64, error) {
	return f.count, f.eventCountErr
}

func (f *fakeEventCounter) EventKindCounts(_ context.Context, kinds []int) (map[int]uint64, error) {
	f.requestedKinds = append([]int(nil), kinds...)
	return f.kindCounts, f.kindCountsErr
}

func TestLiveHandlerReturnsOK(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/livez", nil)

	liveHandler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["ok"] != "true" {
		t.Fatalf("body = %+v", body)
	}
}

func TestAPIRuntimeReturnsUnavailableUntilReady(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	(&apiRuntime{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if rec.Header().Get("Retry-After") != "5" {
		t.Fatalf("Retry-After = %q, want 5", rec.Header().Get("Retry-After"))
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["ok"] != "false" || body["error"] != "api initializing" {
		t.Fatalf("body = %+v", body)
	}
}

func TestAPIRuntimeDelegatesWhenReady(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	runtime := &apiRuntime{}
	runtime.SetReady(nil, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	runtime.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
}

func TestHealthHandlerReturnsEventCount(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	counter := &fakeEventCounter{
		count: 12345,
		kindCounts: map[int]uint64{
			1:    42,
			9735: 7,
		},
	}

	configuredKinds := []int{0, 1, 9735, 38000}

	healthHandler(counter, configuredKinds, func() healthStorageSnapshot {
		return healthStorageSnapshot{
			Ready: true,
			StoredBytes: map[int]uint64{
				1:    1_500_000_000,
				9735: 250_000_000,
			},
		}
	})(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.OK != "true" || body.EventCount != 12345 || !body.StorageStatsReady || body.Error != "" {
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

	healthHandler(&fakeEventCounter{eventCountErr: errors.New("clickhouse unavailable")}, []int{1}, nil)(rec, req)

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
		count:         12345,
		kindCountsErr: errors.New("clickhouse unavailable"),
	}, []int{1}, nil)(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.OK != "false" || body.Error != "clickhouse event kind count failed" || body.EventCount != 0 || body.EventKinds != nil {
		t.Fatalf("body = %+v", body)
	}
}

func TestHealthHandlerReportsOnlyConfiguredKinds(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	counter := &fakeEventCounter{
		count: 9,
		kindCounts: map[int]uint64{
			1:     3,
			30078: 6,
		},
	}

	healthHandler(counter, []int{1, 1, 30079}, func() healthStorageSnapshot {
		return healthStorageSnapshot{
			Ready:       true,
			StoredBytes: map[int]uint64{1: 100, 30078: 200},
		}
	})(rec, req)

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

func TestHealthHandlerSucceedsWhenStorageStatsAreNotReady(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	counter := &fakeEventCounter{
		count:      9,
		kindCounts: map[int]uint64{1: 3},
	}

	healthHandler(counter, []int{1}, nil)(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.StorageStatsReady {
		t.Fatalf("storage stats should not be ready: %+v", body)
	}
	kind := eventKindByNumber(body.EventKinds, 1)
	if kind == nil || kind.Count != 3 || kind.StoredBytes != 0 || kind.StoredGB != 0 {
		t.Fatalf("kind 1 breakdown = %+v", kind)
	}
}

func TestHealthHandlerSurfacesClickHouseMemory(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	counter := &fakeEventCounter{
		count:      9,
		kindCounts: map[int]uint64{1: 3},
	}

	healthHandler(counter, []int{1}, func() healthStorageSnapshot {
		return healthStorageSnapshot{
			Ready: true,
			Memory: map[string]uint64{
				"MarkCacheBytes": 5_368_709_120,
				"SystemLogBytes": 2_000_000_000,
			},
		}
	})(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Memory["MarkCacheBytes"] != 5_368_709_120 {
		t.Fatalf("memory = %+v", body.Memory)
	}
	// The GB view is what makes the endpoint readable at a glance; it must
	// track the byte counts exactly, not just be present.
	if body.MemoryGB["MarkCacheBytes"] != 5.368709 {
		t.Fatalf("memoryGB = %+v", body.MemoryGB)
	}
	if body.MemoryGB["SystemLogBytes"] != 2 {
		t.Fatalf("memoryGB = %+v", body.MemoryGB)
	}
}

func TestHealthHandlerOmitsMemoryWhenProbeFailed(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	counter := &fakeEventCounter{
		count:      9,
		kindCounts: map[int]uint64{1: 3},
	}

	// A failed memory probe leaves Memory nil; the rest of health must still
	// report normally rather than the endpoint degrading.
	healthHandler(counter, []int{1}, func() healthStorageSnapshot {
		return healthStorageSnapshot{Ready: true, StoredBytes: map[int]uint64{1: 100}}
	})(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body healthResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Memory != nil || body.MemoryGB != nil {
		t.Fatalf("memory should be omitted: %+v", body)
	}
	if !body.StorageStatsReady {
		t.Fatalf("storage stats should still be ready: %+v", body)
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
