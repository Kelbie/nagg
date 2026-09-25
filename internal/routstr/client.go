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
	"log/slog"
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
	// UpstreamProviderID is the node's own id for the account it forwards this
	// model to ("openrouter", "tinfoil", "generic", …). One node commonly
	// fronts several, and they fail independently: a node whose OpenRouter
	// credit is exhausted still serves a perfect catalog and still answers
	// every OpenRouter completion with 402, while its other upstreams are
	// fine. It is also the only honest signal that a model runs in a TEE.
	UpstreamProviderID string
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
//
// Rows that carry neither — no canonical_slug and an unqualified id, which is
// how nodes list their non-OpenRouter upstreams — fall back to the upstream id.
// Without that fallback each such row becomes its own single-model vendor
// bucket (id "tinfoil-glm-5-2" → vendor "tinfoil-glm-5-2"), so it can never be
// curated into an auto/pro/max ladder no matter what the vendor allowlist says.
func (m Model) Vendor() string {
	slug := m.CanonicalSlug
	if slug == "" {
		slug = m.ID
	}
	vendor, _, hadSeparator := strings.Cut(slug, "/")
	if !hadSeparator && m.CanonicalSlug == "" && m.UpstreamProviderID != "" {
		return strings.ToLower(m.UpstreamProviderID)
	}
	return strings.ToLower(vendor)
}

// tinfoilModelPrefix is what a Routstr CLIENT gates sealed transport on.
// @routstr/sdk: `isTinfoilModel(modelId) = modelId.startsWith("tinfoil-")`,
// and `getTinfoilUpstreamModelId` strips exactly this prefix — so
// "tinfoil-glm-5-3" is the sealed route to the same model "glm-5-3" serves in
// the clear. Matched byte for byte, case included: a node publishing
// "Tinfoil-foo" would be sent in the clear by the SDK, so nagg must not claim
// otherwise.
const tinfoilModelPrefix = "tinfoil-"

// tinfoilUpstreamID is the upstream a Routstr node declares when it forwards a
// model to a Tinfoil enclave.
const tinfoilUpstreamID = "tinfoil"

// Encrypted reports whether a CLIENT will seal its prompt to the enclave for
// this model — end-to-end, with the node unable to read it.
//
// This is the id prefix and nothing else, because the prefix is the whole of
// what the SDK checks before switching on sealed transport. The node's
// `upstream_provider_id` is a different claim: see TEEHosted.
//
// It is a property of the MODEL, never of the node serving it. A live node
// badged "E2EE" serves 564 priced models of which 9 are client-sealable; most
// of those have an identically named plaintext twin in the same catalog. Any
// caller tempted to lift this to "this provider is E2EE" is asserting
// something the catalog contradicts.
func (m Model) Encrypted() bool {
	return strings.HasPrefix(m.ID, tinfoilModelPrefix)
}

// TEEHosted reports whether the NODE says it forwards this model to a Tinfoil
// enclave. That is a weaker promise than Encrypted and must never be sold as
// the same one: for a model the node declares as tinfoil but does not prefix,
// the client sends the prompt in the clear and the node decrypts, reads and
// forwards it. The prompt is exposed to the operator; only the operator's own
// hop to the enclave is protected.
//
// The gap is not hypothetical and it is not safe-looking. On the live
// redsh1ft catalog 13 rows declare a tinfoil upstream while 9 carry the
// prefix, and three of the four in the gap are NAMED "Private (E2EE) …". The
// name is the node's marketing; the prefix is what the client actually does.
//
// A prefixed model is enclave-hosted by construction, so this is a superset of
// Encrypted — including on older nodes that report no upstream at all.
func (m Model) TEEHosted() bool {
	return strings.EqualFold(m.UpstreamProviderID, tinfoilUpstreamID) || m.Encrypted()
}

// Catalog keeps models and the node serving them in one atomic snapshot.
// UpdatedAt changes only after a successful refresh. Treat Models as read-only.
type Catalog struct {
	Models       []Model
	BaseURL      string
	UpdatedAt    time.Time
	FallbackUsed bool
}

type Client interface {
	Catalog(context.Context) (Catalog, error)
	ActiveBaseURL() string
}

// HTTPClient tries the primary first on every refresh, then ordered fallbacks.
// A failed refresh preserves the last good snapshot, regardless of its age.
type HTTPClient struct {
	baseURL      string
	fallbackURLs []string
	ttl          time.Duration
	http         *http.Client
	mu           sync.Mutex
	cached       Catalog
	refreshing   chan struct{}
}

type Option func(*HTTPClient)

// WithTTL sets the fresh window (default 15m). Stale catalogs never expire
// during an outage; a successful refresh replaces them.
func WithTTL(ttl time.Duration) Option {
	return func(c *HTTPClient) { c.ttl = ttl }
}

func WithFallbackURLs(urls []string) Option {
	return func(c *HTTPClient) {
		for _, baseURL := range urls {
			baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
			if baseURL != "" && baseURL != c.baseURL {
				c.fallbackURLs = append(c.fallbackURLs, baseURL)
			}
		}
	}
}

func WithHTTPClient(h *http.Client) Option {
	return func(c *HTTPClient) { c.http = h }
}

func NewHTTPClient(baseURL string, opts ...Option) *HTTPClient {
	c := &HTTPClient{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		ttl:     15 * time.Minute,
		http:    &http.Client{Timeout: 8 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// BaseURL returns the configured primary, even while a fallback is active.
func (c *HTTPClient) BaseURL() string { return c.baseURL }

// ActiveBaseURL returns the last successful node (the primary before warm-up).
func (c *HTTPClient) ActiveBaseURL() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cached.BaseURL != "" {
		return c.cached.BaseURL
	}
	return c.baseURL
}

func (c *HTTPClient) Models(ctx context.Context) ([]Model, error) {
	catalog, err := c.Catalog(ctx)
	return catalog.Models, err
}

func (c *HTTPClient) Catalog(ctx context.Context) (Catalog, error) {
	c.mu.Lock()
	cached := c.cached
	if cached.Models != nil && time.Since(cached.UpdatedAt) < c.ttl {
		c.mu.Unlock()
		return cached, nil
	}
	if pending := c.refreshing; pending != nil {
		c.mu.Unlock()
		if cached.Models != nil {
			return cached, nil
		}
		select {
		case <-pending:
			return c.Catalog(ctx)
		case <-ctx.Done():
			return Catalog{}, ctx.Err()
		}
	}
	c.refreshing = make(chan struct{})
	c.mu.Unlock()

	var err error
	var next Catalog
	urls := append([]string{c.baseURL}, c.fallbackURLs...)
	for i, baseURL := range urls {
		if ctx.Err() != nil {
			err = ctx.Err()
			break
		}
		var models []Model
		// Share the remaining request budget so slow nodes cannot starve later fallbacks.
		fetchCtx := ctx
		cancel := func() {}
		if deadline, ok := ctx.Deadline(); ok {
			fetchCtx, cancel = context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(urls)-i))
		}
		models, err = c.fetch(fetchCtx, baseURL)
		cancel()
		if err == nil {
			next = Catalog{Models: models, BaseURL: baseURL, UpdatedAt: time.Now(), FallbackUsed: baseURL != c.baseURL}
			break
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	defer func() { close(c.refreshing); c.refreshing = nil }()
	if err == nil {
		from := c.cached.BaseURL
		if from == "" {
			from = c.baseURL
		}
		if from != next.BaseURL {
			slog.Info("routstr.node.switched", "from", from, "to", next.BaseURL)
		}
		c.cached = next
	}
	if c.cached.Models != nil {
		return c.cached, nil
	}
	return Catalog{}, err
}

func (c *HTTPClient) fetch(ctx context.Context, baseURL string) ([]Model, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/models", nil)
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
	models, err := parseModels(body)
	if err != nil {
		return nil, err
	}
	for _, m := range models {
		if m.Enabled {
			return models, nil
		}
	}
	return nil, fmt.Errorf("routstr: empty enabled catalog")
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

	UpstreamProviderID string `json:"upstream_provider_id"`
	Architecture       struct {
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

// ParseModels decodes a node's OpenAI-style /v1/models body into flattened
// Models, applying the same exclusions the cached client applies. Exported so
// every reader of that body — the lineup's cached client here, the provider
// directory's health sweep in internal/aiproviders — shares one parser, and a
// node-format change is fixed in one place instead of two.
func ParseModels(body []byte) ([]Model, error) { return parseModels(body) }

func parseModels(body []byte) ([]Model, error) {
	var raw struct {
		Data []rawModel `json:"data"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("routstr: decode catalog: %w", err)
	}
	out := make([]Model, 0, len(raw.Data))
	for _, m := range raw.Data {
		if strings.TrimSpace(m.ID) == "" || m.SatsPricing == nil || strings.HasPrefix(m.ID, "~") || strings.HasPrefix(m.CanonicalSlug, "~") {
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
			UpstreamProviderID:  m.UpstreamProviderID,
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
