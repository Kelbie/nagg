package appview

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/vertex-lab/nagg/internal/auditor"
	"github.com/vertex-lab/nagg/internal/vertex"
)

// mintInfoMaxURLs bounds one /nostr/mint/info request. A wallet holds a
// handful of mints; the cap only keeps a hostile query from fanning out.
const mintInfoMaxURLs = 50

// MintInfo is what nagg knows about one requested mint: the distilled NUT-06
// metadata discover also serves, plus the unpaid-quote probe verdict. Unlike a
// discover row it is keyed by the caller's own mint list, so a wallet can
// classify the mints it already holds without walking the discovery feed.
type MintInfo struct {
	// MintURL echoes the requested URL so the caller can key on its own form.
	MintURL string `json:"mintUrl"`
	// Known is false when nagg has never seen this mint: no auditor row, no
	// stored info, no probe verdict. Every other field is then empty.
	Known          bool            `json:"known"`
	Name           string          `json:"name,omitempty"`
	IconURL        string          `json:"iconUrl,omitempty"`
	Description    string          `json:"description,omitempty"`
	SupportedUnits []string        `json:"supportedUnits,omitempty"`
	Nuts           json.RawMessage `json:"nuts,omitempty"`
	// Testnut is true when the probe saw the mint mark a never-paid mint quote
	// as paid (internal/mintprobe). It is only a verdict when ProbedAt is set;
	// false without ProbedAt means "not probed yet", not "real mint".
	Testnut bool `json:"testnut"`
	// ProbedAt is the newest probe verdict's time, Unix seconds; omitted until
	// the mint has one.
	ProbedAt int64 `json:"probedAt,omitempty"`
	// OperatorPubkey is the NUT-06 nostr contact, hex, from the same source
	// as the metadata; omitted when the mint publishes none.
	OperatorPubkey string `json:"operatorPubkey,omitempty"`
	// Liveness, exactly as on a discover row: Status is always present
	// ("online", "offline", or "unknown" when not established), CheckedAt is
	// omitted while unknown, LatencyMs only follows a successful probe. It is
	// independent of Known: a mint outside the work-list is unknown here even
	// when nagg has stored its info.
	Status    string     `json:"status"`
	CheckedAt *time.Time `json:"checkedAt,omitempty"`
	LatencyMs int        `json:"latencyMs,omitempty"`
}

type MintInfoResponse struct {
	Mints []MintInfo `json:"mints"`
	// Identities is the identity group of every operator the rows name.
	Identities map[string]Identity `json:"identities"`
}

// mintInfos serves GET /nostr/mint/info?u=<mintUrl>[&u=<mintUrl>...]: one row
// per distinct requested mint, in request order. It reads only what nagg has
// already stored and never fetches a mint on demand.
func (h *Handler) mintInfos(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/mint/info only", http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()

	seen := map[string]struct{}{}
	var requested []string
	for _, raw := range r.URL.Query()["u"] {
		raw = strings.TrimSpace(raw)
		key := normalizeMintURL(raw)
		if key == "" {
			continue
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		requested = append(requested, raw)
	}
	if len(requested) == 0 {
		http.Error(w, "u (mint url) is required", http.StatusBadRequest)
		return
	}
	if len(requested) > mintInfoMaxURLs {
		http.Error(w, fmt.Sprintf("at most %d mint urls per request", mintInfoMaxURLs), http.StatusBadRequest)
		return
	}

	// The caller caches these verdicts, so a failed lookup fails the request
	// rather than answering testnut=false for a mint that is one.
	verdicts, err := h.probeVerdicts(ctx)
	if err != nil {
		writeError(w, err)
		return
	}

	auditByKey := map[string]auditor.Mint{}
	if h.auditor != nil {
		if mints, aerr := h.auditor.Mints(ctx); aerr == nil {
			for _, m := range mints {
				auditByKey[normalizeMintURL(m.URL)] = m
			}
		}
	}

	rows := make([]MintInfo, 0, len(requested))
	operators := make([]string, 0, len(requested))
	for _, raw := range requested {
		key := normalizeMintURL(raw)
		row := MintInfo{MintURL: raw}
		if audit, ok := auditByKey[key]; ok {
			row.Known = true
			row.Name, row.IconURL, row.Description = audit.Name, audit.IconURL, audit.Description
			row.SupportedUnits, row.Nuts = audit.Units, audit.Nuts
			row.OperatorPubkey = operatorPubkey(audit.OperatorContact)
		} else if h.mintInfo != nil {
			if document, ierr := h.mintInfo.LatestInfo(ctx, raw); ierr == nil && len(document) > 0 {
				info := auditor.MintFromInfo(document)
				row.Known = true
				row.Name, row.IconURL, row.Description = info.Name, info.IconURL, info.Description
				row.SupportedUnits, row.Nuts = info.Units, info.Nuts
				row.OperatorPubkey = operatorPubkey(info.OperatorContact)
			}
		}
		if row.OperatorPubkey != "" {
			operators = append(operators, row.OperatorPubkey)
		}
		if verdict, ok := verdicts[key]; ok {
			row.Known = true
			row.Testnut = verdict.Testnut
			row.ProbedAt = verdict.ProbedAt.Unix()
		}
		rows = append(rows, row)
	}
	liveness := h.mintLivenessFor(requested)
	for i := range rows {
		st := liveness[rows[i].MintURL]
		rows[i].Status, rows[i].CheckedAt, rows[i].LatencyMs = st.Status, st.CheckedAt, st.LatencyMs
	}
	writeJSON(w, MintInfoResponse{Mints: rows, Identities: h.identities(ctx, operators)})
}

// operatorPubkey decodes a NUT-06 nostr contact (npub or hex) to lowercase
// hex; "" when absent or malformed.
func operatorPubkey(contact string) string {
	pk, ok := vertex.NormalizePubkey(contact)
	if !ok {
		return ""
	}
	return pk
}
