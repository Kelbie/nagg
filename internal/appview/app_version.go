package appview

import (
	"encoding/json"
	"net/http"
	"strings"
)

// LatestVersionRequest mirrors sovran-schemas LatestVersionRequest. The client
// reports its current version; nagg replies with the latest it advertises.
type LatestVersionRequest struct {
	Storage struct {
		Version string `json:"version"`
	} `json:"storage"`
}

// LatestVersionResponse mirrors sovran-schemas LatestVersionResponse.
type LatestVersionResponse struct {
	Version    string `json:"version"`
	Message    string `json:"message,omitempty"`
	MinVersion string `json:"minVersion,omitempty"`
}

// latestVersion serves GET/POST /app/latest-version so the app's update check no
// longer needs api.sovran.money. The version, message, and minimum version come
// from config (NAGG_APP_LATEST_VERSION / NAGG_APP_UPDATE_MESSAGE / NAGG_APP_MIN_VERSION).
func (h *Handler) latestVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.Header().Set("Allow", "GET, POST")
		http.Error(w, "GET/POST /app/latest-version only", http.StatusMethodNotAllowed)
		return
	}
	// Body is accepted for parity with api.sovran.money (the client sends its
	// current version) but the response doesn't depend on it today.
	if r.Method == http.MethodPost {
		var req LatestVersionRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	w.Header().Set("Cache-Control", "public, max-age=60")

	writeJSON(w, LatestVersionResponse{
		Version:    strings.TrimSpace(h.appLatestVersion),
		Message:    strings.TrimSpace(h.appUpdateMessage),
		MinVersion: strings.TrimSpace(h.appMinVersion),
	})
}
