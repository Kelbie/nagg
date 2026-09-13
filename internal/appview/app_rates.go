package appview

import (
	"net/http"

	"github.com/vertex-lab/nagg/internal/rates"
)

type RatesProvider interface{ Snapshot() (rates.Snapshot, bool) }

func WithRates(provider RatesProvider) Option {
	return func(h *Handler) { h.rates = provider }
}

func (h *Handler) appRates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		writeJSON(w, map[string]string{"error": "GET /app/rates only"})
		return
	}
	var snapshot rates.Snapshot
	var ok bool
	if h.rates != nil {
		snapshot, ok = h.rates.Snapshot()
	}
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		writeJSON(w, map[string]string{"error": "rates warming"})
		return
	}
	w.Header().Set("Cache-Control", "public, max-age=60")
	writeJSON(w, snapshot)
}
