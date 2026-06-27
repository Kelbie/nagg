package appview

import (
	"context"
	"math"
	"net/http"
	"sort"
	"strings"

	"github.com/vertex-lab/nagg/internal/auditor"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/vertex"
)

// DiscoverMint is one mint in the discovery feed: its Nostr review aggregate,
// the operator's Nostr identity + Vertex social reputation, and (when the
// auditor is wired) audit state + supported units. It carries everything a mint
// card renders, so the app builds the "Add Mints" list from ONE response
// instead of the per-mint review + operator-profile N+1 fan-outs layered on a
// direct api.sovran.money auditor call.
type DiscoverMint struct {
	MintURL        string   `json:"mintUrl"`
	Name           string   `json:"name,omitempty"`
	IconURL        string   `json:"iconUrl,omitempty"`
	Description    string   `json:"description,omitempty"`
	SupportedUnits []string `json:"supportedUnits,omitempty"`

	// Nostr reviews (NIP-87 kind-38000). AverageScore is null when no surviving
	// review carried a [n/5]. ReviewCount is the deduped review total (one latest
	// per reviewer); FavouriteCount is the subset posted WITHOUT a score, i.e.
	// pure recommendations/endorsements.
	AverageScore   *float64 `json:"averageScore"`
	ReviewCount    int      `json:"reviewCount"`
	FavouriteCount int      `json:"favouriteCount"`

	// Auditor data, present only when HasAudit is true.
	HasAudit bool   `json:"hasAudit"`
	State    string `json:"state,omitempty"`
	NMints   int    `json:"nMints"`
	NMelts   int    `json:"nMelts"`
	NErrors  int    `json:"nErrors"`

	// Operator Nostr account + Vertex social reputation, present when the mint
	// published a NUT-06 nostr contact nagg could resolve.
	OperatorPubkey string   `json:"operatorPubkey,omitempty"`
	OperatorNpub   string   `json:"operatorNpub,omitempty"`
	Followers      uint64   `json:"followers"`
	Follows        uint64   `json:"follows"`
	VertexRank     float64  `json:"vertexRank"`
	VertexScore    *float64 `json:"vertexScore"`
}

// DiscoverMintsResponse is the discovery feed plus a profiles map (operator
// kind-0 keyed by pubkey), mirroring the feed/thread/reviews responses.
type DiscoverMintsResponse struct {
	Mints    []DiscoverMint         `json:"mints"`
	Profiles map[string]ProfileInfo `json:"profiles"`
}

// discoverReviewScanCap bounds how many kind-38000 review events the discovery
// aggregate scans. It must comfortably exceed the total cashu-mint review count
// so per-mint aggregates match the dedicated reviews endpoint; cashu mint
// reviews number in the low thousands globally, so this is generous and cheap.
const discoverReviewScanCap = 5000

type mintReviewAgg struct {
	display    string
	avg        *float64
	reviews    int
	favourites int
}

func (h *Handler) discoverMints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/mint/discover only", http.StatusMethodNotAllowed)
		return
	}
	ctx := r.Context()
	// limit caps the number of MINTS returned, not the reviews scanned — the
	// review scan must be wide enough that each mint's aggregate is accurate
	// (otherwise a popular mint shows far fewer reviews here than on its own
	// reviews page).
	mintsLimit := intParam(r, "limit", 200)

	// 1) Nostr reviews (kind-38000) → per-mint aggregate, over a wide scan.
	aggByKey, err := h.mintReviewAggregates(ctx, discoverReviewScanCap)
	if err != nil {
		writeError(w, err)
		return
	}

	// 2) Auditor (optional) → mint list with state, units, NUT-06 operator contact.
	auditByKey := map[string]auditor.Mint{}
	if h.auditor != nil {
		if mints, aerr := h.auditor.Mints(ctx); aerr == nil {
			for _, m := range mints {
				auditByKey[normalizeMintURL(m.URL)] = m
			}
		}
	}

	// 3) Union of mint keys from both sources.
	keys := make(map[string]struct{}, len(aggByKey)+len(auditByKey))
	for k := range aggByKey {
		keys[k] = struct{}{}
	}
	for k := range auditByKey {
		keys[k] = struct{}{}
	}

	// 4) Resolve operator pubkeys from the auditor NUT-06 nostr contact.
	operatorByKey := make(map[string]string, len(keys))
	operatorPubkeys := make([]string, 0, len(keys))
	for key := range keys {
		m, ok := auditByKey[key]
		if !ok || m.OperatorContact == "" {
			continue
		}
		pk, err := normalizePubkey(m.OperatorContact)
		if err != nil {
			continue
		}
		operatorByKey[key] = pk
		operatorPubkeys = append(operatorPubkeys, pk)
	}

	// Operator social enrichment in THREE batched reads (kind-0, follow counts,
	// cached Vertex scores) instead of two CH queries + a live Vertex DVM call
	// per operator. The DVM round-trips (which were even failing on credits) blew
	// the client's timeout → "no mints available"; this keeps discovery as cheap
	// as the reviews endpoint (cache-only, no live DVM).
	profiles, perr := h.profileInfos(ctx, operatorPubkeys)
	if perr != nil {
		profiles = map[string]ProfileInfo{}
	}
	followCounts, ferr := h.store.BatchFollowCounts(ctx, operatorPubkeys)
	if ferr != nil {
		followCounts = map[string]chstore.FollowCounts{}
	}
	vertexProfiles, verr := h.store.CachedVertexProfiles(ctx, operatorPubkeys)
	if verr != nil {
		vertexProfiles = map[string]vertex.ProfileResult{}
	}

	// 5) Build a row per mint, enriching from the batched maps (no per-mint CH/DVM).
	mints := make([]DiscoverMint, 0, len(keys))
	for key := range keys {
		row := buildDiscoverMint(key, aggByKey[key], auditByKey, operatorByKey, followCounts, vertexProfiles)
		mints = append(mints, row)
	}

	sortDiscoverMints(mints)
	if mintsLimit > 0 && len(mints) > mintsLimit {
		mints = mints[:mintsLimit]
		profiles = pruneProfilesToMints(mints, profiles)
	}

	writeJSON(w, DiscoverMintsResponse{Mints: mints, Profiles: profiles})
}

// mintReviewAggregates groups all cashu mint reviews by normalized URL and
// computes the per-mint average / review count / favourite count.
func (h *Handler) mintReviewAggregates(ctx context.Context, limit int) (map[string]mintReviewAgg, error) {
	events, err := h.store.QueryEvents(ctx, chstore.EventQueryInput{
		Kinds: []int{mintReviewKind},
		Tags:  []chstore.TagFilter{{Key: "k", Value: cashuMintK}},
		Limit: uint64(limit),
	})
	if err != nil {
		return nil, err
	}
	byMint := map[string][]MintReview{}
	display := map[string]string{}
	for _, event := range events {
		review, ok := mintReviewFromEvent(event)
		if !ok {
			continue
		}
		key := normalizeMintURL(review.MintURL)
		byMint[key] = append(byMint[key], review)
		if _, seen := display[key]; !seen {
			display[key] = review.MintURL
		}
	}
	out := make(map[string]mintReviewAgg, len(byMint))
	for key, reviews := range byMint {
		deduped := dedupeMintReviewsByReviewer(reviews)
		scored := 0
		for _, rv := range deduped {
			if rv.Score != nil {
				scored++
			}
		}
		out[key] = mintReviewAgg{
			display:    display[key],
			avg:        averageMintScore(deduped),
			reviews:    len(deduped),
			favourites: len(deduped) - scored,
		}
	}
	return out, nil
}

// --- ranking ----------------------------------------------------------------
//
// Smart sort: green (auditor-passing) mints ALWAYS rank above everything else,
// then a weighted blend of audit uptime, review score, review count, and
// operator follower count orders within each tier. Counts/followers are
// log-scaled so one huge mint can't dominate, and normalized to ~0..1 so the
// weights are comparable.

const (
	weightUptime      = 0.40 // audit success %
	weightReviewScore = 0.30 // average [n/5] review score
	weightReviewCount = 0.20 // how many reviews/favourites
	weightFollowers   = 0.10 // operator Nostr reach
)

// auditUptime is successes / (successes + errors), 0..1. n_errors is a SEPARATE
// count of failed ops, not a subset of mints/melts, so the denominator includes
// it. 0 when the mint has no audited operations.
func auditUptime(m DiscoverMint) float64 {
	success := m.NMints + m.NMelts
	total := success + m.NErrors
	if total <= 0 {
		return 0
	}
	return float64(success) / float64(total)
}

func isGreenPassing(m DiscoverMint) bool {
	return m.HasAudit && strings.EqualFold(m.State, "OK")
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func discoverRankScore(m DiscoverMint) float64 {
	uptime := auditUptime(m)
	score := scoreOrZero(m.AverageScore) / 5.0
	// log10(1+n): ~100 reviews → 1.0; ~10k followers → 1.0.
	reviewCount := clamp01(math.Log10(1+float64(m.ReviewCount)) / 2.0)
	followers := clamp01(math.Log10(1+float64(m.Followers)) / 4.0)
	return weightUptime*uptime +
		weightReviewScore*score +
		weightReviewCount*reviewCount +
		weightFollowers*followers
}

func sortDiscoverMints(mints []DiscoverMint) {
	sort.Slice(mints, func(i, j int) bool {
		gi, gj := isGreenPassing(mints[i]), isGreenPassing(mints[j])
		if gi != gj {
			return gi // green-passing mints first, always
		}
		si, sj := discoverRankScore(mints[i]), discoverRankScore(mints[j])
		if si != sj {
			return si > sj
		}
		return mints[i].MintURL < mints[j].MintURL // stable tiebreak
	})
}

// pruneProfilesToMints drops operator profiles whose mint fell outside the
// returned slice, so the profiles map stays scoped to what's rendered.
func pruneProfilesToMints(mints []DiscoverMint, profiles map[string]ProfileInfo) map[string]ProfileInfo {
	if len(profiles) == 0 {
		return profiles
	}
	kept := make(map[string]ProfileInfo, len(mints))
	for _, m := range mints {
		if m.OperatorPubkey == "" {
			continue
		}
		if p, ok := profiles[m.OperatorPubkey]; ok {
			kept[m.OperatorPubkey] = p
		}
	}
	return kept
}

func buildDiscoverMint(
	key string,
	agg mintReviewAgg,
	auditByKey map[string]auditor.Mint,
	operatorByKey map[string]string,
	followCounts map[string]chstore.FollowCounts,
	vertexProfiles map[string]vertex.ProfileResult,
) DiscoverMint {
	var row DiscoverMint
	if audit, ok := auditByKey[key]; ok {
		row.MintURL = audit.URL
		row.Name = audit.Name
		row.IconURL = audit.IconURL
		row.Description = audit.Description
		row.SupportedUnits = audit.Units
		row.HasAudit = true
		row.State = audit.State
		row.NMints = audit.NMints
		row.NMelts = audit.NMelts
		row.NErrors = audit.NErrors
	}
	if row.MintURL == "" {
		row.MintURL = agg.display
	}
	row.AverageScore = agg.avg
	row.ReviewCount = agg.reviews
	row.FavouriteCount = agg.favourites

	if pk, ok := operatorByKey[key]; ok {
		row.OperatorPubkey = pk
		row.OperatorNpub = vertex.Npub(pk)
		if counts, ok := followCounts[pk]; ok {
			row.Followers = counts.Followers
			row.Follows = counts.Follows
		}
		if dvm, ok := vertexProfiles[pk]; ok && dvm.PubKey != "" {
			row.VertexRank = dvm.Rank
			row.VertexScore = dvm.Score
		}
	}
	return row
}
