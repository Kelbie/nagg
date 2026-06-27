// Package auditor is a thin, cached client for the upstream cashu mint auditor
// (https://api.audit.8333.space). It powers nagg's /nostr/mint/discover so the
// Sovran app can read mint audit data (state, operation counts, supported
// units, NUT-06 info) through nagg instead of talking to api.sovran.money
// directly — the same data api.sovran.money itself fetches from this auditor.
package auditor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Mint is the auditor's view of a cashu mint, flattened to what discovery needs.
// OperatorContact is the raw NUT-06 nostr contact (npub or hex), left
// un-normalized so the caller owns nostr decoding; "" when the mint published none.
type Mint struct {
	URL             string
	Name            string
	State           string
	NMints          int
	NMelts          int
	NErrors         int
	Units           []string
	IconURL         string
	Description     string
	OperatorContact string
}

// Client fetches the auditor's mint list. Implementations cache internally.
type Client interface {
	Mints(ctx context.Context) ([]Mint, error)
}

// HTTPClient is a cached HTTP client for the auditor. It serves a TTL-fresh
// snapshot, refreshes in the background, and serves a stale snapshot (up to
// StaleFor) when the upstream is briefly unavailable — mirroring how
// api.sovran.money wraps the same upstream so behavior is unchanged.
type HTTPClient struct {
	baseURL string
	limit   int
	ttl     time.Duration
	staleFor time.Duration
	http    *http.Client

	mu        sync.Mutex
	cached    []Mint
	fetchedAt time.Time
	inflight  bool
}

// Option configures the HTTPClient.
type Option func(*HTTPClient)

// WithLimit sets how many mints to request from the auditor (default 200).
func WithLimit(n int) Option {
	return func(c *HTTPClient) {
		if n > 0 {
			c.limit = n
		}
	}
}

// WithTTL sets the fresh window and the stale-serve window (default 1h / 24h),
// matching api.sovran.money's audit.mints cache.
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

// NewHTTPClient builds an auditor client for baseURL (e.g. https://api.audit.8333.space).
func NewHTTPClient(baseURL string, opts ...Option) *HTTPClient {
	c := &HTTPClient{
		baseURL:  strings.TrimRight(baseURL, "/"),
		limit:    200,
		ttl:      time.Hour,
		staleFor: 24 * time.Hour,
		http:     &http.Client{Timeout: 20 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Mints returns the cached mint list, refreshing when the TTL has elapsed. On a
// refresh failure it returns the last good snapshot if still within StaleFor,
// otherwise the error.
func (c *HTTPClient) Mints(ctx context.Context) ([]Mint, error) {
	c.mu.Lock()
	age := time.Since(c.fetchedAt)
	fresh := c.cached != nil && age < c.ttl
	cached := c.cached
	c.mu.Unlock()

	if fresh {
		return cached, nil
	}

	mints, err := c.fetch(ctx)
	if err != nil {
		// Serve stale within the SWR window rather than failing discovery.
		if cached != nil && age < c.staleFor {
			return cached, nil
		}
		return nil, err
	}

	c.mu.Lock()
	c.cached = mints
	c.fetchedAt = time.Now()
	c.mu.Unlock()
	return mints, nil
}

func (c *HTTPClient) fetch(ctx context.Context) ([]Mint, error) {
	url := fmt.Sprintf("%s/mints/?skip=0&limit=%d", c.baseURL, c.limit)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("auditor: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	return parseMints(body)
}

// auditorMint is the raw upstream shape. `info` is a JSON-encoded STRING (a
// NUT-06 GetInfoResponse), not a nested object, so it is decoded separately.
type auditorMint struct {
	URL     string `json:"url"`
	Name    string `json:"name"`
	State   string `json:"state"`
	NMints  int    `json:"n_mints"`
	NMelts  int    `json:"n_melts"`
	NErrors int    `json:"n_errors"`
	Info    string `json:"info"`
}

func parseMints(body []byte) ([]Mint, error) {
	var raw []auditorMint
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("auditor: decode list: %w", err)
	}
	out := make([]Mint, 0, len(raw))
	for _, m := range raw {
		if strings.TrimSpace(m.URL) == "" {
			continue
		}
		mint := Mint{
			URL:     m.URL,
			Name:    m.Name,
			State:   m.State,
			NMints:  m.NMints,
			NMelts:  m.NMelts,
			NErrors: m.NErrors,
		}
		if info := parseInfo(m.Info); info != nil {
			mint.Units = info.units()
			mint.IconURL = info.IconURL
			mint.Description = info.Description
			mint.OperatorContact = info.nostrContact()
			if mint.Name == "" {
				mint.Name = info.Name
			}
		}
		out = append(out, mint)
	}
	return out, nil
}

// nut06Info is the subset of NUT-06 GetInfoResponse discovery needs.
type nut06Info struct {
	Name        string          `json:"name"`
	IconURL     string          `json:"icon_url"`
	Description string          `json:"description"`
	Contact     json.RawMessage `json:"contact"`
	Nuts        struct {
		Four struct {
			Methods []struct {
				Unit string `json:"unit"`
			} `json:"methods"`
		} `json:"4"`
	} `json:"nuts"`
}

func parseInfo(encoded string) *nut06Info {
	encoded = strings.TrimSpace(encoded)
	if encoded == "" || encoded == "null" {
		return nil
	}
	var info nut06Info
	if err := json.Unmarshal([]byte(encoded), &info); err != nil {
		return nil
	}
	return &info
}

func (n *nut06Info) units() []string {
	seen := map[string]struct{}{}
	var units []string
	for _, m := range n.Nuts.Four.Methods {
		u := strings.ToLower(strings.TrimSpace(m.Unit))
		if u == "" {
			continue
		}
		if _, ok := seen[u]; ok {
			continue
		}
		seen[u] = struct{}{}
		units = append(units, u)
	}
	sort.Strings(units)
	return units
}

// nostrContact extracts the operator's nostr identity from the NUT-06 contact
// field, which the spec has expressed two ways over time:
//   - objects:  [{"method":"nostr","info":"npub1..."}]
//   - pairs:    [["nostr","npub1..."]]
func (n *nut06Info) nostrContact() string {
	if len(n.Contact) == 0 {
		return ""
	}
	var objs []struct {
		Method string `json:"method"`
		Info   string `json:"info"`
	}
	if err := json.Unmarshal(n.Contact, &objs); err == nil {
		for _, o := range objs {
			if strings.EqualFold(o.Method, "nostr") && strings.TrimSpace(o.Info) != "" {
				return strings.TrimSpace(o.Info)
			}
		}
	}
	var pairs [][]string
	if err := json.Unmarshal(n.Contact, &pairs); err == nil {
		for _, p := range pairs {
			if len(p) >= 2 && strings.EqualFold(p[0], "nostr") && strings.TrimSpace(p[1]) != "" {
				return strings.TrimSpace(p[1])
			}
		}
	}
	return ""
}
