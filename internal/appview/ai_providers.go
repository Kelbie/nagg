package appview

import (
	"net/http"

	"github.com/vertex-lab/nagg/internal/aiproviders"
	"github.com/vertex-lab/nagg/internal/vertex"
)

// AIProvidersDirectory supplies the server-curated provider directory behind
// GET /app/ai-providers. Satisfied by *aiproviders.Service.
//
// This is the sibling of RoutstrClient, not a replacement for it:
// /app/ai-lineup curates the MODELS of one chosen node, /app/ai-providers lists
// the PROVIDERS to choose between. The app discovered the latter itself on
// every cold start — a relay round-trip plus a per-node probe fan-out before it
// could draw a picker — which is work a server does once for every client.
type AIProvidersDirectory interface {
	// Directory returns the current snapshot. false means nothing is known
	// yet (the warm-up window), never "the directory is empty".
	Directory() (aiproviders.Directory, bool)
	// Operators is the reverse index operator pubkey → provider base URLs,
	// feeding Identity.Operates on every route that names a pubkey.
	Operators() map[string][]string
}

// AIProvidersResponse is the directory plus the identity group of every
// operator it names. The wrapper lives here rather than in aiproviders
// because that package has no store access; the rows themselves are
// unchanged so older builds keep reading `pubkey`/`followers`/`mints`.
type AIProvidersResponse struct {
	aiproviders.Directory
	Identities map[string]Identity `json:"identities"`
}

// WithAIProviders wires the provider directory service. Without it the route
// responds 503 and the app falls back to discovering providers client-side, so
// an unset NAGG_AI_PROVIDERS_* config costs one route and nothing else.
func WithAIProviders(directory AIProvidersDirectory) Option {
	return func(h *Handler) { h.aiProviderDir = directory }
}

// aiProviders serves GET /app/ai-providers.
func (h *Handler) aiProviders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /app/ai-providers only", http.StatusMethodNotAllowed)
		return
	}
	if h.aiProviderDir == nil {
		http.Error(w, "ai providers not configured", http.StatusServiceUnavailable)
		return
	}
	directory, ok := h.aiProviderDir.Directory()
	if !ok {
		http.Error(w, "ai providers warming", http.StatusServiceUnavailable)
		return
	}
	operators := make([]string, 0, len(directory.Providers))
	// The rows' own reach is the fallback, so a deployment without the shared
	// resolver still publishes the count the sweep established; when the
	// resolver is wired it answers first, from the same cache the sweep
	// reads, so the two do not drift.
	fallback := make(map[string]IdentityReach, len(directory.Providers))
	for _, p := range directory.Providers {
		pk, ok := vertex.NormalizePubkey(p.Pubkey)
		if !ok {
			continue
		}
		operators = append(operators, pk)
		if p.Followers != nil {
			// A row counts followers only, so follows stays null.
			followers := *p.Followers
			fallback[pk] = IdentityReach{Followers: &followers, Source: p.FollowersSource}
		}
	}
	writeJSON(w, AIProvidersResponse{
		Directory:  directory,
		Identities: h.identitiesWith(r.Context(), operators, identityOptions{reachFallback: fallback}),
	})
}
