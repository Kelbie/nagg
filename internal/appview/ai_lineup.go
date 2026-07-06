package appview

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/vertex-lab/nagg/internal/routstr"
)

// RoutstrClient supplies the upstream Routstr node's model catalog that
// /app/ai-lineup curates. Satisfied by *routstr.HTTPClient.
type RoutstrClient interface {
	Models(ctx context.Context) ([]routstr.Model, error)
	BaseURL() string
}

// AILineupResponse is the server-curated AI model lineup. The app renders its
// AI tab straight from this instead of deriving a lineup from the raw node
// catalog, so the model set — and even the node the app pays — can be changed
// for already-shipped builds by a nagg deploy alone. Old builds ignore
// provider ids and fields they don't know.
type AILineupResponse struct {
	Version   int          `json:"version"`
	UpdatedAt int64        `json:"updatedAt"`
	Node      AINode       `json:"node"`
	Providers []AIProvider `json:"providers"`
}

// AINode names the Routstr node the lineup was derived from; the app sends
// its chat/wallet traffic there.
type AINode struct {
	BaseURL string `json:"baseUrl"`
}

// AIProvider is one provider tab: ID is the app-facing tab id ("openai",
// "claude", "grok", "google", or the vendor slug for vendors the app learns
// later); Vendor is the catalog's canonical_slug prefix.
type AIProvider struct {
	ID     string    `json:"id"`
	Vendor string    `json:"vendor"`
	Models []AIModel `json:"models"`
}

// AIModel is one tier entry. Pricing is the node's sats_pricing snapshot
// (SATS floats) so the app can estimate turn cost and mirror the node's
// admission check (prompt-estimate + max_tokens×completion + request) without
// fetching the full catalog.
type AIModel struct {
	Tier                string          `json:"tier"`
	ID                  string          `json:"id"`
	Name                string          `json:"name"`
	Created             int64           `json:"created"`
	ContextLength       int             `json:"contextLength"`
	MaxCompletionTokens int             `json:"maxCompletionTokens,omitempty"`
	InputModalities     []string        `json:"inputModalities"`
	Pricing             routstr.Pricing `json:"pricing"`
}

// aiLineupTiers is the app's tier order: auto = cheapest, pro = middle,
// max = most capable/expensive.
var aiLineupTiers = [3]string{"auto", "pro", "max"}

// vendorToProviderID maps catalog vendor slugs to the app's provider tab ids.
// Vendors without an entry pass their slug through (forward-compatible: old
// builds skip unknown ids, new builds can add tabs without a nagg change).
var vendorToProviderID = map[string]string{
	"openai":    "openai",
	"anthropic": "claude",
	"x-ai":      "grok",
	"google":    "google",
}

// aiLineupFreshWindow bounds how old a model may be for automatic tier picks,
// so retired-but-listed flagships (o1-pro class) never shadow current ones.
// Pins bypass it.
const aiLineupFreshWindow = 548 * 24 * time.Hour // ~18 months

// minAIContextLength filters toy/legacy chat models out of automatic picks.
const minAIContextLength = 16_000

// aiLineup serves GET /app/ai-lineup.
func (h *Handler) aiLineup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /app/ai-lineup only", http.StatusMethodNotAllowed)
		return
	}
	if h.routstrClient == nil {
		http.Error(w, "ai lineup not configured", http.StatusServiceUnavailable)
		return
	}
	models, err := h.routstrClient.Models(r.Context())
	if err != nil {
		http.Error(w, "ai lineup upstream unavailable", http.StatusBadGateway)
		return
	}
	writeJSON(w, buildAILineup(models, h.routstrClient.BaseURL(), h.aiLineupVendors, h.aiLineupPins, time.Now()))
}

func buildAILineup(models []routstr.Model, nodeURL string, vendors []string, pins map[string]map[string]string, now time.Time) AILineupResponse {
	byVendor := make(map[string][]routstr.Model)
	byID := make(map[string]routstr.Model, len(models))
	for _, m := range models {
		if m.Enabled {
			byID[m.ID] = m
		}
		if !qualifiesForAILineup(m) {
			continue
		}
		v := m.Vendor()
		byVendor[v] = append(byVendor[v], m)
	}

	providers := make([]AIProvider, 0, len(vendors))
	for _, vendor := range vendors {
		picks := pickAITiers(byVendor[vendor], now)
		// Pins override the derived pick per tier; a pin only applies when the
		// pinned id exists enabled in the catalog, so a typo'd or retired pin
		// degrades to the derived model instead of a dead entry.
		for tier, id := range pins[vendor] {
			if m, ok := byID[id]; ok {
				picks[tier] = &m
			}
		}
		entries := make([]AIModel, 0, len(aiLineupTiers))
		for _, tier := range aiLineupTiers {
			m := picks[tier]
			if m == nil {
				continue
			}
			entries = append(entries, AIModel{
				Tier:                tier,
				ID:                  m.ID,
				Name:                m.Name,
				Created:             m.Created,
				ContextLength:       m.ContextLength,
				MaxCompletionTokens: m.MaxCompletionTokens,
				InputModalities:     m.InputModalities,
				Pricing:             m.Pricing,
			})
		}
		if len(entries) == 0 {
			continue
		}
		id := vendorToProviderID[vendor]
		if id == "" {
			id = vendor
		}
		providers = append(providers, AIProvider{ID: id, Vendor: vendor, Models: entries})
	}

	return AILineupResponse{
		Version:   1,
		UpdatedAt: now.Unix(),
		Node:      AINode{BaseURL: strings.TrimRight(nodeURL, "/")},
		Providers: providers,
	}
}

// qualifiesForAILineup keeps enabled, priced, text-chat models with a usable
// context window; embeddings/image/audio generators and rolling "~vendor"
// aliases are excluded.
func qualifiesForAILineup(m routstr.Model) bool {
	if !m.Enabled || m.ContextLength < minAIContextLength {
		return false
	}
	if strings.HasPrefix(m.Vendor(), "~") {
		return false
	}
	// Chat models bill completions; embedding rows price prompt-only.
	if m.Pricing.Completion <= 0 || m.Pricing.MaxCost <= 0 {
		return false
	}
	textOut := false
	for _, out := range m.OutputModalities {
		switch out {
		case "text":
			textOut = true
		case "image", "audio", "embeddings":
			return false
		}
	}
	return textOut
}

// aiTurnCost is the tier-ranking metric: the sats cost of a typical turn
// (~8k prompt / ~2k completion tokens), matching the app's own estimate. Within
// one vendor this tracks the cheap→flagship capability ladder well; pins exist
// for the cases it doesn't.
func aiTurnCost(m routstr.Model) float64 {
	return m.Pricing.Request + m.Pricing.Prompt*8000 + m.Pricing.Completion*2000
}

// pickAITiers picks auto/pro/max per vendor: sort fresh qualifying models by
// turn cost, then auto = cheapest, max = priciest, pro = median. Fewer than
// three fresh models degrade to auto(+max); an empty fresh set falls back to
// all qualifying models so a vendor with only older listings still appears.
func pickAITiers(models []routstr.Model, now time.Time) map[string]*routstr.Model {
	picks := make(map[string]*routstr.Model, 3)
	if len(models) == 0 {
		return picks
	}
	cutoff := now.Add(-aiLineupFreshWindow).Unix()
	fresh := make([]routstr.Model, 0, len(models))
	for _, m := range models {
		if m.Created >= cutoff {
			fresh = append(fresh, m)
		}
	}
	if len(fresh) == 0 {
		fresh = append(fresh, models...)
	}
	sort.Slice(fresh, func(i, j int) bool {
		ci, cj := aiTurnCost(fresh[i]), aiTurnCost(fresh[j])
		if ci != cj {
			return ci < cj
		}
		// Same price point: prefer the newer model earlier so ties resolve
		// deterministically and toward current releases.
		if fresh[i].Created != fresh[j].Created {
			return fresh[i].Created > fresh[j].Created
		}
		return fresh[i].ID < fresh[j].ID
	})

	picks["auto"] = &fresh[0]
	if len(fresh) >= 2 {
		// Max = the NEWEST model in the top price quartile. Price alone picks
		// legacy ultra-priced relics (o1-pro class, ~4× the real flagship);
		// recency alone picks whatever was listed last. Price finds the
		// premium band, listing recency finds the current flagship in it.
		quartile := fresh[max(1, len(fresh)*3/4):]
		maxPick := &quartile[0]
		for i := range quartile {
			if quartile[i].Created > maxPick.Created {
				maxPick = &quartile[i]
			}
		}
		picks["max"] = maxPick
	}
	if len(fresh) >= 3 {
		picks["pro"] = &fresh[len(fresh)/2]
	}
	return picks
}

// ParseAILineupPins decodes the NAGG_AI_LINEUP_PINS env JSON:
// {"anthropic":{"max":"claude-opus-4.7","auto":"claude-3-haiku"}, …} —
// vendor slug → tier → catalog model id. Invalid JSON yields nil (derived
// lineup only) rather than a boot failure.
func ParseAILineupPins(raw string) map[string]map[string]string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var pins map[string]map[string]string
	if err := json.Unmarshal([]byte(raw), &pins); err != nil {
		return nil
	}
	return pins
}
