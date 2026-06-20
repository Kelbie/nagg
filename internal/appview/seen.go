package appview

import (
	"encoding/json"
	"net/http"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// NIP-78 notifications seen-state. The client publishes a kind-30078 app-data
// event (d = SeenNotificationsDTag) whose content carries the "seen-up-to"
// timestamp, so the read marker syncs across devices via relays AND is readable
// on the backend-free relay tier. nagg reads the latest such event and returns
// the timestamp; unread = count(notification.created_at > seenUntil) is a client
// computation (it already holds the notifications), so unreadByType is omitted
// here and left optional in the contract.

// SeenNotificationsDTag is the NIP-78 `d` tag identifying the notifications
// seen-up-to marker. Stable so every device reads/writes the same event.
const SeenNotificationsDTag = "sovran/notifications/seen"

// SeenStateResponse matches the nagg-ts SeenState contract.
type SeenStateResponse struct {
	SeenUntil int64 `json:"seenUntil"`
}

func (h *Handler) notificationsSeen(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/notifications/seen only", http.StatusMethodNotAllowed)
		return
	}
	pubkey, err := h.viewerPubkeyOr(queryViewerParam(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	events, err := h.store.QueryEvents(r.Context(), chstore.EventQueryInput{
		PubKeys: []string{pubkey},
		Kinds:   []int{kindAppData},
		Tags:    []chstore.TagFilter{{Key: "d", Value: SeenNotificationsDTag}},
	})
	if err != nil {
		writeError(w, err)
		return
	}

	// Latest wins (replaceable addressable event). 0 means "never marked seen".
	latest := latestEventByKind(events)[kindAppData]
	writeJSON(w, SeenStateResponse{SeenUntil: parseSeenUntil(latest)})
}

const kindAppData = 30078

func parseSeenUntil(event *chstore.EventView) int64 {
	if event == nil {
		return 0
	}
	var payload struct {
		SeenUntil int64 `json:"seenUntil"`
	}
	if err := json.Unmarshal([]byte(event.Content), &payload); err != nil {
		return 0
	}
	if payload.SeenUntil < 0 {
		return 0
	}
	return payload.SeenUntil
}
