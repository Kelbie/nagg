package auditor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestUcashPagesFormsAndInfo(t *testing.T) {
	var pages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/search_mintstest-suffix" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" || r.Header.Get("Accept") != "application/json" {
			t.Error("missing form/JSON headers")
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if _, ok := r.PostForm["search"]; !ok || r.PostForm.Get("search") != "" {
			t.Error("missing empty search")
		}
		page := r.PostForm.Get("page")
		pages = append(pages, page)
		w.Header().Set("Content-Type", "application/json")
		info := `{"name":"Info name","icon_url":"https://mint/icon","description":"About","nuts":{"4":{"methods":[{"unit":"sat"}]},"5":{"methods":[{"unit":"usd"}]}},"contact":[["nostr","npub1test"]]}`
		_ = json.NewEncoder(w).Encode(map[string]any{"total": 2, "mints": []any{map[string]any{
			"id": len(pages), "url": "https://mint/" + page, "state": "OK", "info": info,
			"n_mints": 12, "n_melts": 7, "n_errors": 1, "updated_at": "2026-09-12T22:22:06Z",
		}}})
	}))
	defer server.Close()
	c := NewUcashClient(server.URL, "test-suffix", true)
	mints, err := c.Mints(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(pages, []string{"0", "1"}) || len(mints) != 2 {
		t.Fatalf("pages=%v mints=%v", pages, mints)
	}
	m := mints[0]
	if m.Name != "Info name" || m.IconURL != "https://mint/icon" || m.Description != "About" || m.OperatorContact != "npub1test" || !reflect.DeepEqual(m.Units, []string{"sat", "usd"}) || len(m.Nuts) == 0 {
		t.Fatalf("info not decoded: %+v", m)
	}
	if m.State != "OK" || m.NMints != 12 || m.NMelts != 7 || m.NErrors != 1 || m.Source != "ucash" || m.UpdatedAt != 1789251726 {
		t.Fatalf("fields: %+v", m)
	}
	if m.Uptime24h != nil || m.AvgLatencyMs != nil {
		t.Fatal("roster must not enrich")
	}
}

func TestUcashPageCap(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"mints":[{"id":%d,"url":"https://mint/%d"}],"total":100}`, calls, calls)
	}))
	defer server.Close()
	c := NewUcashClient(server.URL, "hash", false)
	mints, err := c.Mints(context.Background())
	if err != nil || calls != 20 || len(mints) != 20 {
		t.Fatalf("calls=%d mints=%d err=%v", calls, len(mints), err)
	}
}

func TestUcashRejectsUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name, contentType, body string
		status                  int
	}{
		{"HTML200", "text/html", "<html>app</html>", 200},
		{"HTMLAsJSON", "application/json", "<html>app</html>", 200},
		{"404", "application/json", `{}`, 404},
		{"null", "application/json", `null`, 200},
		{"trailingHTML", "application/json", `{}<html>`, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(tc.status)
				fmt.Fprint(w, tc.body)
			}))
			defer server.Close()
			c := NewUcashClient(server.URL, "hash", true)
			if err := c.Probe(context.Background()); err == nil {
				t.Fatal("probe accepted unavailable")
			}
			if _, err := c.Mints(context.Background()); err == nil {
				t.Fatal("roster accepted unavailable")
			}
		})
	}
}

func TestUcashDualEnrichment(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			var enrichmentTimes []time.Time
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if err := r.ParseForm(); err != nil {
					t.Error(err)
				}
				switch strings.TrimSuffix(r.URL.Path, "hash") {
				case "/api/get_stats":
					fmt.Fprint(w, `{"total_swaps":572}`)
				case "/api/search_mints":
					fmt.Fprint(w, `{"mints":[{"id":7,"url":"https://mint","state":"OK"}],"total":1}`)
				case "/api/get_mint_uptime", "/api/get_mint_metrics":
					if r.PostForm.Get("id") != "7" {
						t.Errorf("id = %q", r.PostForm.Get("id"))
					}
					enrichmentTimes = append(enrichmentTimes, time.Now())
					if strings.Contains(r.URL.Path, "uptime") {
						fmt.Fprint(w, `{"uptime_percent":0,"last_error":"offline"}`)
					} else {
						fmt.Fprint(w, `{"avg_latency_ms":4446.2}`)
					}
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
				}
			}))
			defer server.Close()
			d := NewDual(NewUcashClient(server.URL, "hash", enabled), nil, time.Hour)
			if err := d.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			mints, err := d.Mints(context.Background())
			if err != nil || len(mints) != 1 {
				t.Fatalf("mints=%v err=%v", mints, err)
			}
			m := mints[0]
			if enabled {
				if m.Uptime24h == nil || *m.Uptime24h != 0 || m.AvgLatencyMs == nil || *m.AvgLatencyMs != 4446.2 || m.LastError != "offline" {
					t.Fatalf("enrichment: %+v", m)
				}
				if len(enrichmentTimes) != 2 || enrichmentTimes[1].Sub(enrichmentTimes[0]) < 200*time.Millisecond {
					t.Fatal("enrichment not throttled")
				}
			} else if len(enrichmentTimes) != 0 || m.Uptime24h != nil || m.AvgLatencyMs != nil {
				t.Fatal("disabled enrichment made requests")
			}
		})
	}
}

func TestUcashEnrichmentFailureKeepsRoster(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/get_statshash":
			fmt.Fprint(w, `{}`)
		case "/api/search_mintshash":
			fmt.Fprint(w, `{"mints":[{"id":1,"url":"https://mint","state":"OK"}],"total":1}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	fallback := clientFunc(func(context.Context) ([]Mint, error) { t.Error("enrichment failure invoked fallback"); return nil, nil })
	d := NewDual(NewUcashClient(server.URL, "hash", true), fallback, time.Hour)
	if err := d.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	mints, err := d.Mints(context.Background())
	if err != nil || len(mints) != 1 || mints[0].Source != "ucash" || mints[0].Uptime24h != nil || mints[0].AvgLatencyMs != nil {
		t.Fatalf("snapshot=%v err=%v", mints, err)
	}
}

func TestUcashEnrichmentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c := NewUcashClient("http://unused.invalid", "hash", true)
	started := time.Now()
	if got := c.enrich(ctx, []Mint{{id: 1}}); got != 0 {
		t.Fatal("canceled enrichment produced data")
	}
	if time.Since(started) >= 200*time.Millisecond {
		t.Fatal("cancellation waited for throttle")
	}
}
