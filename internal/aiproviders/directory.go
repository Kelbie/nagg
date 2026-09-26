// Package aiproviders is nagg's server-curated directory of Routstr AI
// providers: who exists, whether they are up, how big their catalog is, how
// much of it is sealed, and how much Nostr reach their operator has.
//
// It is the sibling of internal/routstr, not a replacement. That package
// curates the MODELS of one chosen node for /app/ai-lineup; this one lists the
// PROVIDERS to choose between for /app/ai-providers. The Sovran app used to do
// this itself on every cold start — a relay round-trip plus a fan-out of
// per-node HTTP probes before it could draw a picker — which is exactly the
// kind of work a server does once and every client reads.
package aiproviders

import (
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/vertex-lab/nagg/internal/socialgraph"
)

// Status is a provider's reachability as of CheckedAt.
//
// StatusUnknown is deliberately distinct from StatusOffline: it means "nagg has
// not established this provider's state", either because it was discovered
// after the last sweep or because its last probe is older than the service's
// MaxAge. The app sorts and labels on the difference — an unprobed provider is
// worth showing above one we have watched fail.
const (
	StatusOnline  = "online"
	StatusOffline = "offline"
	StatusUnknown = "unknown"
)

// Provider is one row of the directory.
//
// EncryptedModelCount is a COUNT, never a boolean. "This provider is E2EE" is
// not a true property: on a live node badged E2EE, 9 of its 564 priced models
// are client-sealable and the rest are plaintext, and most sealed models have
// an identically named unsealed twin in the same catalog. Encryption is
// per-model routing, so the honest thing to publish is how many of a
// provider's models are sealed and let the app say "9 private models" rather
// than "private".
//
// It counts models the CLIENT will seal (routstr.Model.Encrypted), which is
// the same test the app's model picker badges on, so the directory and the
// picker can never disagree about a provider's number.
type Provider struct {
	BaseURL string `json:"baseUrl"`
	Name    string `json:"name"`
	// Pubkey is the operator's Nostr identity, hex. Omitted when neither the
	// announcement nor the node's own /v1/info gave one.
	Pubkey string `json:"pubkey,omitempty"`
	// Followers is the operator's Nostr reach, or NULL when nagg could not
	// establish it. Null and 0 are different facts and the app sorts on the
	// difference, the same way it does for an unknown versus an offline
	// status: a provider whose operator nobody follows is not a provider we
	// failed to look up. Publishing the failure as 0 is exactly what made
	// every row on two endpoints report 0 followers while looking measured.
	//
	// A provider that publishes no operator pubkey reports 0, not null: there
	// is nobody to count, which is an established fact.
	Followers *uint64 `json:"followers"`
	// FollowersSource is which source answered — "graph" (nagg's own kind-3
	// rollup, exact), "vertex" (the Vertex DVM cache, exact) or "relays" (a
	// live kind-3 scan, a LOWER BOUND, safe to render as "174+" but not as an
	// exact count). Omitted when Followers is null.
	FollowersSource     string `json:"followersSource,omitempty"`
	ModelCount          int    `json:"modelCount"`
	EncryptedModelCount int    `json:"encryptedModelCount"`
	// TEEModelCount is how many models the NODE declares it forwards to a
	// Tinfoil enclave. It is a superset of EncryptedModelCount and a WEAKER
	// promise: the models in the gap are sent in the clear, so the node reads
	// the prompt before forwarding it. Published because it is real
	// information about where inference runs, and named so it can never be
	// mistaken for the end-to-end claim.
	TEEModelCount int `json:"teeModelCount"`
	// MinMessageSats is the smallest balance that pays for one chat message
	// here: the cheapest reservation across the provider's chat models, priced
	// the way the app's send gate prices it (see minMessageSats). An aggregate
	// so the directory can answer "can I afford this provider" without the
	// client downloading a catalog per row. Omitted until a catalog read has
	// found at least one priced chat model.
	MinMessageSats *int     `json:"minMessageSats,omitempty"`
	Mints          []string `json:"mints"`
	Status         string   `json:"status"`
	// CheckedAt is when THIS provider's status was last established. Omitted
	// while the status is unknown, because nothing has been established yet.
	CheckedAt *time.Time `json:"checkedAt,omitempty"`
	// LatencyMs is the round trip of the last successful probe. Omitted when
	// the provider has never answered one.
	LatencyMs int `json:"latencyMs,omitempty"`
}

// Directory is the whole response. CheckedAt is when the SWEEP ran, which is
// not the same as any one provider's CheckedAt: a provider discovered late, or
// one whose probe is failing, carries an older stamp of its own.
type Directory struct {
	Providers  []Provider `json:"providers"`
	CheckedAt  time.Time  `json:"checkedAt"`
	TTLSeconds int        `json:"ttlSeconds"`
}

// statusRank orders the three states best-first for sorting.
func statusRank(status string) int {
	switch status {
	case StatusOnline:
		return 0
	case StatusUnknown:
		return 1
	default:
		return 2
	}
}

// Sort orders the directory best-first, server-side, so every client renders
// the same picker and the order does not churn between requests:
//
//	online, then unknown, then offline
//	then providers with sealed models first
//	then operator reach: a known positive count, then an unresolved one, then
//	a known zero (socialgraph.CompareBest — the same ladder as the statuses,
//	and the same rule /nostr/mint/discover ranks operators by)
//	then base URL ascending — a total order, so equal rows never swap places
//	between two requests the way Go's unstable sort would allow.
func Sort(providers []Provider) {
	sort.Slice(providers, func(i, j int) bool {
		a, b := providers[i], providers[j]
		if ra, rb := statusRank(a.Status), statusRank(b.Status); ra != rb {
			return ra < rb
		}
		if ea, eb := a.EncryptedModelCount > 0, b.EncryptedModelCount > 0; ea != eb {
			return ea
		}
		ra, rb := providerReach(a), providerReach(b)
		if ra != rb {
			return socialgraph.CompareBest(ra, rb)
		}
		return a.BaseURL < b.BaseURL
	})
}

// normalizeBaseURL collapses the spellings of one node onto a single key.
// Trailing slashes and a trailing "/v1" are noise; two spellings of one node
// must not render as two rows or select as two different providers. It mirrors
// the app's own normalizeNodeUrl so both sides agree on identity.
//
// Only https survives: a .onion address is unreachable without Tor, and http
// would put a bearer Cashu token on the wire in the clear. Everything else
// returns "" and is dropped by the caller.
func normalizeBaseURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || !strings.EqualFold(parsed.Scheme, "https") || parsed.Host == "" || parsed.User != nil {
		return ""
	}
	host := strings.ToLower(parsed.Host)
	if strings.HasSuffix(host, ".onion") {
		return ""
	}
	path := strings.TrimRight(parsed.Path, "/")
	path = strings.TrimSuffix(path, "/v1")
	return "https://" + host + strings.TrimRight(path, "/")
}

// providerReach recovers the ranking value from the published fields, so the
// exported Sort behaves identically whether a caller built the slice from the
// service or from a fixture.
func providerReach(p Provider) socialgraph.Reach {
	if p.Followers == nil {
		return socialgraph.Unknown()
	}
	return socialgraph.Reach{
		Followers:   *p.Followers,
		Known:       true,
		Source:      p.FollowersSource,
		Approximate: p.FollowersSource == socialgraph.SourceRelays,
	}
}

// displayName falls back to the host, which is what the operator is known as
// when it publishes no name at all.
func displayName(name, baseURL string) string {
	if name = strings.TrimSpace(name); name != "" {
		return name
	}
	return strings.TrimPrefix(baseURL, "https://")
}
