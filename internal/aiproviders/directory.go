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
// not a true property: on a live node badged E2EE, 9 of 582 models are sealed
// and the rest are plaintext, and most sealed models have an identically named
// unsealed twin in the same catalog. Encryption is per-model routing, so the
// honest thing to publish is how many of a provider's models are sealed and
// let the app say "9 private models" rather than "private".
type Provider struct {
	BaseURL string `json:"baseUrl"`
	Name    string `json:"name"`
	// Pubkey is the operator's Nostr identity, hex. Omitted when neither the
	// announcement nor the node's own /v1/info gave one.
	Pubkey string `json:"pubkey,omitempty"`
	// Followers is the operator's Nostr follower count from nagg's own social
	// graph. 0 also covers "no pubkey" and "graph not available here".
	Followers           uint64   `json:"followers"`
	ModelCount          int      `json:"modelCount"`
	EncryptedModelCount int      `json:"encryptedModelCount"`
	Mints               []string `json:"mints"`
	Status              string   `json:"status"`
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
//	then operator followers, descending
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
		if a.Followers != b.Followers {
			return a.Followers > b.Followers
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

// displayName falls back to the host, which is what the operator is known as
// when it publishes no name at all.
func displayName(name, baseURL string) string {
	if name = strings.TrimSpace(name); name != "" {
		return name
	}
	return strings.TrimPrefix(baseURL, "https://")
}
