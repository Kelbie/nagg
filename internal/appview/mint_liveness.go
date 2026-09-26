package appview

import "github.com/vertex-lab/nagg/internal/mintliveness"

// MintLiveness supplies each mint's current reachability for the `status`,
// `checkedAt` and `latencyMs` fields of /nostr/mint/discover and
// /nostr/mint/info rows. Satisfied by *mintliveness.Service.
//
// Liveness answers under the caller's own keys, so a handler indexes the
// result with the strings it passed; a key the sweep has never reached, or
// whose probe is older than the sweep's MaxAge, reads unknown.
type MintLiveness interface {
	Liveness(keys []string) map[string]mintliveness.Status
}

// WithMintLiveness wires the mint liveness sweep. Without it every row reads
// `status: "unknown"` with no checkedAt or latencyMs — the fields are always
// present, so a client can key on them whether or not the sweep runs.
func WithMintLiveness(sweep MintLiveness) Option {
	return func(h *Handler) { h.mintLiveness = sweep }
}

// mintLivenessFor is the nil-safe read: unknown for everything when no sweep
// is wired, otherwise whatever the sweep has established.
func (h *Handler) mintLivenessFor(keys []string) map[string]mintliveness.Status {
	if h.mintLiveness == nil {
		out := make(map[string]mintliveness.Status, len(keys))
		for _, key := range keys {
			out[key] = mintliveness.Unknown()
		}
		return out
	}
	return h.mintLiveness.Liveness(keys)
}
