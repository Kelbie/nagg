package appview

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/nagg/internal/vertex"
)

type SignedVertexClient interface {
	RelaySigned(context.Context, nostr.Event) (vertex.SignedResult, error)
}

func WithVertexRelay(client SignedVertexClient, enabled bool, maxPerMin int, allowPersonalized bool) Option {
	return func(h *Handler) {
		h.vertexRelay = client
		h.vertexRelayEnabled = enabled
		h.vertexClientLimiter = newRateLimiter(maxPerMin, time.Minute)
		h.vertexAllowPersonalized = allowPersonalized
	}
}

const maxVertexRequestBytes = 64 << 10

func decodeVertexEvent(data []byte) (*nostr.Event, error) {
	var event nostr.Event
	if len(data) > maxVertexRequestBytes {
		return nil, errors.New("Vertex request too large")
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, errors.New("invalid signed Vertex event JSON")
	}
	return &event, nil
}

func signedVertexQuery(r *http.Request) (*nostr.Event, error) {
	if !r.URL.Query().Has("svr") {
		return nil, nil
	}
	encoded := r.URL.Query().Get("svr")
	if len(encoded) > maxVertexRequestBytes*4/3+4 {
		return nil, errors.New("Vertex request too large")
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, errors.New("svr must be unpadded base64url event JSON")
	}
	return decodeVertexEvent(data)
}

func (h *Handler) validateVertex(w http.ResponseWriter, event nostr.Event) (vertex.SignedRequestArgs, bool) {
	args, err := vertex.ValidateSignedRequest(event, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return args, false
	}
	if args.Search.Sort == "personalizedPagerank" && !h.vertexAllowPersonalized {
		http.Error(w, "personalizedPagerank is disabled", http.StatusBadRequest)
		return args, false
	}
	return args, true
}

func (h *Handler) relayVertex(ctx context.Context, w http.ResponseWriter, event nostr.Event, args vertex.SignedRequestArgs) (vertex.SignedResult, bool) {
	w.Header().Set("Cache-Control", "no-store")
	if !h.vertexRelayEnabled || h.vertexRelay == nil {
		http.Error(w, "Vertex client relay disabled", http.StatusServiceUnavailable)
		return vertex.SignedResult{}, false
	}
	if !h.vertexClientLimiter.allowKey(event.PubKey) {
		http.Error(w, "Vertex client rate limit exceeded", http.StatusTooManyRequests)
		return vertex.SignedResult{}, false
	}
	result, err := h.vertexRelay.RelaySigned(ctx, event)
	if err == nil {
		switch result.Kind {
		case "profile":
			// The profile/score tables are global, keyed only by target. Never replace
			// global reputation with a source-scoped or follower-count ranking.
			if args.Search.Sort == vertex.DefaultSearchSort && args.Search.Source == "" {
				err = h.store.SaveVertexProfile(ctx, *result.Profile)
			}
		case "search":
			if store, ok := h.store.(vertex.SearchCacheStore); ok {
				err = store.SaveVertexSearch(ctx, args.Search, result.Results)
			} else {
				err = errors.New("Vertex search cache unavailable")
			}
		}
	}
	if err != nil {
		writeSignedVertexError(w, err)
		return result, false
	}
	return result, true
}

func writeSignedVertexError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json")
	if errors.Is(err, vertex.ErrInsufficientCredits) {
		writeJSON(w, map[string]any{"ok": false, "reason": "insufficient_credits", "message": "Insufficient Vertex credits"})
		return
	}
	var rejected *vertex.ErrDVMRejected
	if errors.As(err, &rejected) {
		writeJSON(w, map[string]any{"ok": false, "reason": "rejected", "message": "Vertex rejected the request"})
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		w.WriteHeader(http.StatusGatewayTimeout)
		writeJSON(w, map[string]any{"ok": false, "reason": "timeout"})
		return
	}
	w.WriteHeader(http.StatusBadGateway)
	writeJSON(w, map[string]any{"ok": false, "reason": "unavailable"})
}

func (h *Handler) vertexRelayRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodPost {
		http.Error(w, "POST /nostr/vertex/relay only", http.StatusMethodNotAllowed)
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxVertexRequestBytes))
	if err != nil {
		http.Error(w, "invalid Vertex request body", http.StatusBadRequest)
		return
	}
	event, err := decodeVertexEvent(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	args, ok := h.validateVertex(w, *event)
	if !ok {
		return
	}
	result, ok := h.relayVertex(r.Context(), w, *event, args)
	if !ok {
		return
	}
	var payload any = result.Results
	cached := result.Kind == "search"
	if result.Kind == "profile" {
		payload = result.Profile
		cached = args.Search.Sort == vertex.DefaultSearchSort && args.Search.Source == ""
	}
	writeJSON(w, map[string]any{"ok": true, "kind": result.Kind, "result": payload, "fetchedAt": result.FetchedAt, "cached": cached})
}
