// Package socialgraph resolves one question for the whole app-view: how much
// Nostr reach does this pubkey have?
//
// It exists because the answer was being derived twice, from one source that
// is empty in half of nagg's deployments, and the emptiness was being
// published as the number zero. /nostr/mint/discover reported every mint
// operator as having 0 followers, and /app/ai-providers reported every AI
// provider the same way — one defect wearing two endpoint names. Both now read
// this package, so they cannot drift apart again, and a zero that means "we
// could not look" is no longer spelled the same as a zero that means "nobody
// follows them".
package socialgraph

import "strings"

// Source names where a Reach came from. It travels with the number because the
// sources are not equally good and a caller rendering "1.2k followers" deserves
// to know which one it got.
const (
	// SourceGraph is nagg's own rollup of stored kind-3 events (pubkey_stats).
	// Exact, free, and available only where the nostr module ingests kind 3.
	SourceGraph = "graph"
	// SourceVertex is the Vertex DVM's own follower count, read from the local
	// profile cache. Exact. The cache fills from client-signed profile reads
	// and from the server-key syncer; a deployment with neither stays empty.
	SourceVertex = "vertex"
	// SourceRelays is a live kind-3 scan of the relays nagg already dials,
	// counting distinct authors whose contact list names the target. It needs
	// no credentials and no credits, and it is a LOWER BOUND: relays cap
	// results, the relay set is partial, and a slow relay is dropped. See
	// Reach.Approximate.
	SourceRelays = "relays"
)

// Reach is a follower count and the honesty that has to travel with it.
//
// Known false is the whole point of the type. A provider whose operator has no
// followers and a provider nagg has not managed to ask about are different
// facts, and the app sorts and labels on the difference — the same reason
// aiproviders distinguishes an unknown status from an offline one. Collapsing
// them into 0 is what made every row on two live endpoints report 0 followers
// while looking like a measurement.
type Reach struct {
	Followers uint64
	Follows   uint64
	// Known is false when no source could answer. Followers is then
	// meaningless and must not be rendered as a count.
	Known bool
	// Source is which of the constants above answered; empty when Known is false.
	Source string
	// Approximate is true when Followers is a floor rather than a count, which
	// is every SourceRelays answer. A caller may render it as "174+" but must
	// not present it as exact.
	Approximate bool
}

// Unknown is the zero value spelled out, for callers that would otherwise be
// tempted to return Reach{} and mean "zero followers".
func Unknown() Reach { return Reach{} }

// FromGraph reports an exact count from nagg's own rollup.
func FromGraph(followers, follows uint64) Reach {
	return Reach{Followers: followers, Follows: follows, Known: true, Source: SourceGraph}
}

// FromVertex reports an exact count from the Vertex profile cache.
func FromVertex(followers, follows uint64) Reach {
	return Reach{Followers: followers, Follows: follows, Known: true, Source: SourceVertex}
}

// FromRelays reports a lower bound counted off the relays.
func FromRelays(followers uint64) Reach {
	return Reach{Followers: followers, Known: true, Source: SourceRelays, Approximate: true}
}

// None is a Reach for a subject that HAS no operator identity — a provider
// publishing no pubkey, a mint with no NUT-06 nostr contact. That is a real
// zero, established by the absence of anyone to count, and it must rank
// differently from a pubkey nagg simply failed to resolve.
func None() Reach {
	return Reach{Known: true, Source: SourceGraph}
}

// CompareBest orders two Reach values best-first, and is the shared ranking
// rule so two endpoints cannot disagree about what "more reach" means.
//
// It mirrors the online/unknown/offline ladder: a known positive count leads,
// an unknown sits in the middle, and a known zero comes last. Unknown above
// known-zero on purpose — "we did not manage to ask" is not evidence of
// nobody, and demoting it below a measured zero would punish a provider for
// nagg's own gap.
//
// Returns true when a should sort before b.
func CompareBest(a, b Reach) bool {
	if ra, rb := reachTier(a), reachTier(b); ra != rb {
		return ra < rb
	}
	return a.Followers > b.Followers
}

func reachTier(r Reach) int {
	switch {
	case r.Known && r.Followers > 0:
		return 0
	case !r.Known:
		return 1
	default:
		return 2
	}
}

// NormalizePubkey keeps only a well-formed 64-char hex key, lowercased.
// Anything else — an npub, a display name, a truncated id — would be looked up
// forever and never found.
func NormalizePubkey(raw string) string {
	key := strings.ToLower(strings.TrimSpace(raw))
	if len(key) != 64 {
		return ""
	}
	for _, r := range key {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return ""
		}
	}
	return key
}
