package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fakeEventCounter struct {
	count uint64
	err   error
}

func (f fakeEventCounter) EventCount(context.Context) (uint64, error) {
	return f.count, f.err
}

func TestHealthHandlerReturnsEventCount(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	healthHandler(fakeEventCounter{count: 12345})(rec, req)

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
}

func TestHealthHandlerReturnsUnavailableOnEventCountError(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	healthHandler(fakeEventCounter{err: errors.New("clickhouse unavailable")})(rec, req)

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
