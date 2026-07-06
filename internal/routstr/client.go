// Package routstr is a thin, cached client for a Routstr node's OpenAI-style
// model catalog (https://api.routstr.com/v1/models). It powers nagg's
// /app/ai-lineup so the Sovran app reads a server-curated AI model lineup
// through nagg instead of deriving one client-side from the raw catalog —
// letting the lineup (and even the node base URL) be updated for already
// shipped app builds without an app release.
package routstr

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Model is the node's view of one model, flattened to what lineup curation
// needs. All sats_pricing values are SATS (floating point, per RIP-05); the
// node's internal accounting is msats but its public catalog is sats.
type Model struct {
	ID              string
	Name            string
	Created         int64
	ContextLength   int
	CanonicalSlug   string
	Enabled         bool
	InputModalities []string
	// OutputModalities distinguishes chat models (text) from image/embedding
	// models the lineup must never pick.
	OutputModalities    []string
	MaxCompletionTokens int
	Pricing             Pricing
}

// Pricing is the model's sats_pricing subset the app needs: per-token prompt/
// completion rates for turn-cost estimates, the per-request fee, and the
// max-cost fields that drive the node's upfront balance reservation.
type Pricing struct {
	Prompt            float64 `json:"prompt"`
	Completion        float64 `json:"completion"`
	Request           float64 `json:"request"`
	MaxCost           float64 `json:"maxCost"`
	MaxPromptCost     float64 `json:"maxPromptCost"`
	MaxCompletionCost float64 `json:"maxCompletionCost"`
}

// Vendor returns the model-vendor slug ("anthropic", "openai", "x-ai", …):
// the canonical_slug prefix when present, else the id's prefix.
func (m Model) Vendor() string {
	slug := m.CanonicalSlug
	if slug == "" {
		slug = m.ID
	}
	vendor, _, _ := strings.Cut(slug, "/")
	return strings.ToLower(vendor)
}

// Client fetches a Routstr node's model catalog. Implementations cache internally.
type Client interface {
	Models(ctx context.Context) ([]Model, error)
	// BaseURL is the node the catalog came from; the app pays this node, so
	// the lineup response must name it.
	BaseURL() string
}

// HTTPClient is a cached HTTP client for one Routstr node. It serves a
// TTL-fresh snapshot and falls back to a stale snapshot (up to StaleFor) when
// the node is briefly unavailable — same shape as the auditor client.
type HTTPClient struct {
	baseURL  string
	ttl      time.Duration
	staleFor time.Duration
	http     *http.Client

	mu        sync.Mutex
	cached    []Model
	fetchedAt time.Time
}

// Option configures the HTTPClient.
type Option func(*HTTPClient)

// WithTTL sets the fresh window and the stale-serve window (default 15m / 24h).
// Model catalogs churn slowly; sats prices drift with the BTC/USD rate, so the
// fresh window stays short enough for pricing to track.
func WithTTL(ttl, staleFor time.Duration) Option {
	return func(c *HTTPClient) {
		c.ttl = ttl
		c.staleFor = staleFor
	}
}

// WithHTTPClient overrides the underlying *http.Client (tests inject a stub).
func WithHTTPClient(h *http.Client) Option {
	return func(c *HTTPClient) { c.http = h }
}

// NewHTTPClient builds a catalog client for baseURL (e.g. https://api.routstr.com).
func NewHTTPClient(baseURL string, opts ...Option) *HTTPClient {
	c := &HTTPClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		ttl:      15 * time.Minute,
		staleFor: 24 * time.Hour,
		// Tight timeout so a slow/down node degrades the lineup to the last
		// snapshot quickly instead of hanging past the request budget.
		http: &http.Client{Timeout: 8 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *HTTPClient) BaseURL() string { return c.baseURL }

// Models returns the cached catalog, refreshing when the TTL has elapsed. On a
// refresh failure it returns the last good snapshot if still within StaleFor,
// otherwise the error.
func (c *HTTPClient) Models(ctx context.Context) ([]Model, error) {
	c.mu.Lock()
	age := time.Since(c.fetchedAt)
	fresh := c.cached != nil && age < c.ttl
	cached := c.cached
	c.mu.Unlock()

	if fresh {
		return cached, nil
	}

	models, err := c.fetch(ctx)
	if err != nil {
		if cached != nil && age < c.staleFor {
			return cached, nil
		}
		return nil, err
	}

	c.mu.Lock()
	c.cached = models
	c.fetchedAt = time.Now()
	c.mu.Unlock()
	return models, nil
}

func (c *HTTPClient) fetch(ctx context.Context) ([]Model, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/models", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("routstr: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return nil, err
	}
	return parseModels(body)
}

// rawModel is the upstream /v1/models entry subset nagg reads. Unknown fields
// are ignored so node upgrades can't break the lineup.
type rawModel struct {
	ID            string `json:"id"`
	Name          string `json:"name"`
	Created       int64  `json:"created"`
	ContextLength int    `json:"context_length"`
	CanonicalSlug string `json:"canonical_slug"`
	Enabled       *bool  `json:"enabled"`
	Architecture  struct {
		InputModalities  []string `json:"input_modalities"`
		OutputModalities []string `json:"output_modalities"`
	} `json:"architecture"`
	TopProvider struct {
		MaxCompletionTokens int `json:"max_completion_tokens"`
	} `json:"top_provider"`
	SatsPricing *struct {
		Prompt            float64 `json:"prompt"`
		Completion        float64 `json:"completion"`
		Request           float64 `json:"request"`
		MaxCost           float64 `json:"max_cost"`
		MaxPromptCost     float64 `json:"max_prompt_cost"`
		MaxCompletionCost float64 `json:"max_completion_cost"`
	} `json:"sats_pricing"`
}

func parseModels(body []byte) ([]Model, error) {
	var raw struct {
		Data []rawModel `json:"data"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("routstr: decode catalog: %w", err)
	}
	out := make([]Model, 0, len(raw.Data))
	for _, m := range raw.Data {
		if strings.TrimSpace(m.ID) == "" || m.SatsPricing == nil {
			continue
		}
		out = append(out, Model{
			ID:                  m.ID,
			Name:                m.Name,
			Created:             m.Created,
			ContextLength:       m.ContextLength,
			CanonicalSlug:       m.CanonicalSlug,
			Enabled:             m.Enabled == nil || *m.Enabled,
			InputModalities:     m.Architecture.InputModalities,
			OutputModalities:    m.Architecture.OutputModalities,
			MaxCompletionTokens: m.TopProvider.MaxCompletionTokens,
			Pricing: Pricing{
				Prompt:            m.SatsPricing.Prompt,
				Completion:        m.SatsPricing.Completion,
				Request:           m.SatsPricing.Request,
				MaxCost:           m.SatsPricing.MaxCost,
				MaxPromptCost:     m.SatsPricing.MaxPromptCost,
				MaxCompletionCost: m.SatsPricing.MaxCompletionCost,
			},
		})
	}
	return out, nil
}
