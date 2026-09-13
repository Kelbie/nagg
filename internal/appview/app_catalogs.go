package appview

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	"github.com/vertex-lab/nagg/internal/btcmap"
)

type WallpapersProvider interface {
	Snapshot() (json.RawMessage, bool)
}
type BtcmapClient interface {
	Fetch(context.Context, string, url.Values) (json.RawMessage, error)
}

func WithWallpapers(provider WallpapersProvider) Option {
	return func(h *Handler) { h.wallpapers = provider }
}

func WithBtcmap(client BtcmapClient) Option {
	return func(h *Handler) { h.btcmap = client }
}

func (h *Handler) appWallpapers(w http.ResponseWriter, r *http.Request) {
	if !catalogGET(w, r) {
		return
	}
	if h.wallpapers != nil {
		if body, ok := h.wallpapers.Snapshot(); ok {
			w.Header().Set("Cache-Control", "public, max-age=300")
			writeJSON(w, body)
			return
		}
	}
	catalogError(w, http.StatusServiceUnavailable, "wallpapers warming")
}

func (h *Handler) appBtcmap(w http.ResponseWriter, r *http.Request) {
	if !catalogGET(w, r) {
		return
	}
	if h.btcmap == nil {
		catalogError(w, http.StatusServiceUnavailable, "btcmap disabled")
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		catalogError(w, http.StatusBadRequest, "invalid query")
		return
	}
	body, err := h.btcmap.Fetch(r.Context(), r.PathValue("id"), query)
	if err != nil {
		status := http.StatusBadGateway
		var upstream *btcmap.Error
		if errors.As(err, &upstream) {
			status = upstream.Status
		}
		catalogError(w, status, http.StatusText(status))
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=3600")
	writeJSON(w, body)
}

func catalogGET(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodGet {
		return true
	}
	w.Header().Set("Allow", "GET")
	catalogError(w, http.StatusMethodNotAllowed, "GET only")
	return false
}

func catalogError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	writeJSON(w, map[string]string{"error": message})
}
