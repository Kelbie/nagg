package auditor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// UcashClient calls the versioned Leptos server functions. Mints fetches only
// the roster; optional per-mint enrichment belongs to Dual's background pass.
type UcashClient struct {
	baseURL       string
	fnSuffix      string
	pageCap       int
	http          *http.Client
	uptimeEnabled bool
}

func NewUcashClient(baseURL, fnSuffix string, uptimeEnabled bool) *UcashClient {
	return &UcashClient{baseURL: strings.TrimRight(baseURL, "/"), fnSuffix: fnSuffix,
		pageCap: 20, http: &http.Client{Timeout: 8 * time.Second}, uptimeEnabled: uptimeEnabled}
}

func (c *UcashClient) fn(name string) string { return c.baseURL + "/api/" + name + c.fnSuffix }

func (c *UcashClient) call(ctx context.Context, name string, form url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.fn(name), strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ucash: %s status %d", name, resp.StatusCode)
	}
	mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return fmt.Errorf("ucash: %s non-JSON response", name)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
	if err != nil {
		return err
	}
	if len(body) > 8<<20 {
		return fmt.Errorf("ucash: %s response too large", name)
	}
	// All used functions return objects; reject null, HTML, and scalar JSON.
	if trimmed := strings.TrimSpace(string(body)); !strings.HasPrefix(trimmed, "{") {
		return fmt.Errorf("ucash: %s expected JSON object", name)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("ucash: %s invalid JSON", name)
	}
	return nil
}

func (c *UcashClient) Probe(ctx context.Context) error {
	var stats map[string]json.RawMessage
	return c.call(ctx, "get_stats", nil, &stats)
}

func (c *UcashClient) Mints(ctx context.Context) ([]Mint, error) {
	var out []Mint
	seen := map[string]bool{}
	received := 0
	for page := 0; page < c.pageCap; page++ {
		var result struct {
			Mints []struct {
				auditorMint
				ID        int    `json:"id"`
				UpdatedAt string `json:"updated_at"`
			} `json:"mints"`
			Total int `json:"total"`
		}
		if err := c.call(ctx, "search_mints", url.Values{"search": {""}, "page": {strconv.Itoa(page)}}, &result); err != nil {
			return nil, err
		}
		for _, raw := range result.Mints {
			if strings.TrimSpace(raw.URL) == "" || seen[raw.URL] {
				continue
			}
			seen[raw.URL] = true
			mint := Mint{id: raw.ID, URL: raw.URL, Name: raw.Name, State: raw.State,
				NMints: raw.NMints, NMelts: raw.NMelts, NErrors: raw.NErrors, Source: "ucash"}
			if at, err := time.Parse(time.RFC3339, raw.UpdatedAt); err == nil {
				mint.UpdatedAt = at.Unix()
			}
			applyInfo(&mint, raw.Info)
			out = append(out, mint)
		}
		received += len(result.Mints)
		if received >= result.Total || len(result.Mints) == 0 {
			break
		}
	}
	return out, nil
}

// Uptime contains the discovery subset of get_mint_uptime.
type Uptime struct {
	UptimePercent *float64 `json:"uptime_percent"`
	LastError     string   `json:"last_error"`
}

func (c *UcashClient) Uptime(ctx context.Context, id int) (Uptime, error) {
	var out Uptime
	err := c.call(ctx, "get_mint_uptime", url.Values{"id": {strconv.Itoa(id)}}, &out)
	if err == nil && out.UptimePercent != nil && (*out.UptimePercent < 0 || *out.UptimePercent > 100) {
		return Uptime{}, fmt.Errorf("ucash: invalid uptime percentage")
	}
	return out, err
}

// Metrics contains the discovery subset of get_mint_metrics (lifetime latency).
type Metrics struct {
	AvgLatencyMs *float64 `json:"avg_latency_ms"`
	LastError    string   `json:"last_error"`
}

func (c *UcashClient) Metrics(ctx context.Context, id int) (Metrics, error) {
	var out Metrics
	err := c.call(ctx, "get_mint_metrics", url.Values{"id": {strconv.Itoa(id)}}, &out)
	if err == nil && out.AvgLatencyMs != nil && *out.AvgLatencyMs < 0 {
		return Metrics{}, fmt.Errorf("ucash: invalid latency")
	}
	return out, err
}

func (c *UcashClient) enrich(ctx context.Context, mints []Mint) int {
	if !c.uptimeEnabled {
		return 0
	}
	enriched := 0
	for i := range mints {
		if mints[i].id <= 0 {
			continue
		}
		if !pause(ctx, 200*time.Millisecond) {
			break
		}
		uptime, err := c.Uptime(ctx, mints[i].id)
		if err == nil {
			mints[i].Uptime24h = uptime.UptimePercent
			mints[i].LastError = uptime.LastError
			if uptime.UptimePercent != nil {
				enriched++
			}
		}
		if !pause(ctx, 200*time.Millisecond) {
			break
		}
		metrics, err := c.Metrics(ctx, mints[i].id)
		if err == nil {
			mints[i].AvgLatencyMs = metrics.AvgLatencyMs
			if mints[i].LastError == "" {
				mints[i].LastError = metrics.LastError
			}
		}
	}
	return enriched
}

func pause(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
