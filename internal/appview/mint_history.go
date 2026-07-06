package appview

import (
	"context"
	"net/http"
	"strings"

	"github.com/vertex-lab/nagg/internal/mintinfo"
)

// MintHistoryProvider serves a mint's NUT-06 info history (initial document +
// RFC 6902 diffs). Satisfied by *mintinfo.Reader. Cashu-specific, like the
// sibling reviews/discover surfaces under /nostr/mint/*.
type MintHistoryProvider interface {
	History(ctx context.Context, mintURL string, includeObservations bool) (*mintinfo.History, bool, error)
}

// mintHistory serves GET /nostr/mint/history?u=<mintUrl>[&observations=true].
// The default response is the initial full document plus per-change diffs, with
// no-change checks collapsed into top-level metadata; observations=true adds the
// full per-poll log.
func (h *Handler) mintHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/mint/history only", http.StatusMethodNotAllowed)
		return
	}
	if h.mintInfo == nil {
		http.Error(w, "mint info history not configured", http.StatusServiceUnavailable)
		return
	}
	mintURL := strings.TrimSpace(r.URL.Query().Get("u"))
	if mintURL == "" {
		http.Error(w, "u (mint url) is required", http.StatusBadRequest)
		return
	}
	includeObservations := r.URL.Query().Get("observations") == "true"

	history, found, err := h.mintInfo.History(r.Context(), mintURL, includeObservations)
	if err != nil {
		writeError(w, err)
		return
	}
	if !found {
		http.Error(w, "no info history for this mint", http.StatusNotFound)
		return
	}
	writeJSON(w, history)
}
