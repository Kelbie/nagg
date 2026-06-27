package appview

import (
	"context"
	"net/http"
	"sort"

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
	limit := intParam(r, "limit", 200)

	// 1) Nostr reviews (kind-38000) → per-mint aggregate.
	aggByKey, err := h.mintReviewAggregates(ctx, limit)
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

	profiles, perr := h.profileInfos(ctx, operatorPubkeys)
	if perr != nil {
		profiles = map[string]ProfileInfo{}
	}

	// 5) Build a row per mint, enriching with operator social reputation.
	mints := make([]DiscoverMint, 0, len(keys))
	for key := range keys {
		row := h.buildDiscoverMint(ctx, key, aggByKey[key], auditByKey, operatorByKey)
		mints = append(mints, row)
	}

	// Best-attested first: most reviews, then highest average, then url (stable;
	// map iteration is random).
	sort.Slice(mints, func(i, j int) bool {
		if mints[i].ReviewCount != mints[j].ReviewCount {
			return mints[i].ReviewCount > mints[j].ReviewCount
		}
		if scoreOrZero(mints[i].AverageScore) != scoreOrZero(mints[j].AverageScore) {
			return scoreOrZero(mints[i].AverageScore) > scoreOrZero(mints[j].AverageScore)
		}
		return mints[i].MintURL < mints[j].MintURL
	})

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

func (h *Handler) buildDiscoverMint(
	ctx context.Context,
	key string,
	agg mintReviewAgg,
	auditByKey map[string]auditor.Mint,
	operatorByKey map[string]string,
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
		if counts, err := h.store.FollowCounts(ctx, pk); err == nil {
			row.Followers = counts.Followers
			row.Follows = counts.Follows
			if dvm, _ := h.vertexProfile(ctx, pk, counts.Followers); dvm.PubKey != "" {
				row.VertexRank = dvm.Rank
				row.VertexScore = dvm.Score
			}
		}
	}
	return row
}
