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
	Version string `json:"version"`
	Message string `json:"message,omitempty"`
}

// latestVersion serves POST /app/latest-version so the app's update check no
// longer needs api.sovran.money. The advertised version + optional message come
// from config (NAGG_APP_LATEST_VERSION / NAGG_APP_UPDATE_MESSAGE).
func (h *Handler) latestVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST /app/latest-version only", http.StatusMethodNotAllowed)
		return
	}
	// Body is accepted for parity with api.sovran.money (the client sends its
	// current version) but the response doesn't depend on it today.
	var req LatestVersionRequest
	_ = json.NewDecoder(r.Body).Decode(&req)

	writeJSON(w, LatestVersionResponse{
		Version: strings.TrimSpace(h.appLatestVersion),
		Message: strings.TrimSpace(h.appUpdateMessage),
	})
}
