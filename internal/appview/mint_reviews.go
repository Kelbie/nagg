package appview

import (
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// NIP-87 cashu mint reviews. A review is a kind-38000 event tagged k=38172 (the
// reviewed thing is a cashu mint, vs fedimint 38173) and u=<mintUrl>, with a
// free-text `content` carrying the community [n/5] score convention.
//
// These endpoints compute the per-mint aggregate SERVER-side (average score +
// review count, one latest review per reviewer) so the client makes ONE call per
// mint instead of fetching every review and aggregating N times. The parse /
// per-reviewer dedupe / average mirror the nagg-ts relay-tier fallback exactly,
// so all three tiers produce the same summary.
const (
	mintReviewKind = 38000
	cashuMintK     = "38172"
)

var mintScoreRe = regexp.MustCompile(`\[(\d+(?:\.\d+)?)/5\]`)

// MintReview is one parsed cashu mint review. Score is nil when the content
// carried no [n/5] convention.
type MintReview struct {
	EventID        string   `json:"eventId"`
	ReviewerPubkey string   `json:"reviewerPubkey"`
	MintURL        string   `json:"mintUrl"`
	Score          *float64 `json:"score"`
	Content        string   `json:"content"`
	CreatedAt      int64    `json:"createdAt"`
}

// MintAggregate is the per-mint summary the client renders. AverageScore is null
// when no surviving review carried a score.
type MintAggregate struct {
	MintURL      string   `json:"mintUrl"`
	AverageScore *float64 `json:"averageScore"`
	ReviewCount  int      `json:"reviewCount"`
}

// MintReviewsResponse matches the nagg-ts MintReviewsResponseSchema. Profiles
// bundles each reviewer's kind-0 (name/picture) keyed by pubkey, exactly like
// the feed/thread/notifications responses, so the client renders reviewer
// identity from this one call instead of resolving every pubkey on-device.
type MintReviewsResponse struct {
	Summary  MintAggregate          `json:"summary"`
	Reviews  []MintReview           `json:"reviews"`
	Profiles map[string]ProfileInfo `json:"profiles"`
}

// DiscoverMintsResponse matches the nagg-ts DiscoverMintsResponseSchema.
type DiscoverMintsResponse struct {
	Mints []MintAggregate `json:"mints"`
}

func (h *Handler) mintReviews(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/mint/reviews only", http.StatusMethodNotAllowed)
		return
	}
	mintURL := strings.TrimSpace(r.URL.Query().Get("u"))
	if mintURL == "" {
		http.Error(w, "u (mint url) is required", http.StatusBadRequest)
		return
	}
	limit := intParam(r, "limit", 500)
	events, err := h.store.QueryEvents(r.Context(), chstore.EventQueryInput{
		Kinds: []int{mintReviewKind},
		Tags: []chstore.TagFilter{
			{Key: "k", Value: cashuMintK},
			// Match both the bare and trailing-slash forms; normalize below.
			{Key: "u", Values: mintURLCandidates(mintURL)},
		},
		Limit: uint64(limit),
	})
	if err != nil {
		writeError(w, err)
		return
	}

	target := normalizeMintURL(mintURL)
	parsed := make([]MintReview, 0, len(events))
	for _, event := range events {
		review, ok := mintReviewFromEvent(event)
		if !ok || normalizeMintURL(review.MintURL) != target {
			continue
		}
		parsed = append(parsed, review)
	}
	deduped := dedupeMintReviewsByReviewer(parsed)
	sort.Slice(deduped, func(i, j int) bool {
		if deduped[i].CreatedAt != deduped[j].CreatedAt {
			return deduped[i].CreatedAt > deduped[j].CreatedAt // newest first
		}
		return deduped[i].EventID < deduped[j].EventID
	})

	reviewers := make([]string, 0, len(deduped))
	for _, review := range deduped {
		reviewers = append(reviewers, review.ReviewerPubkey)
	}
	profiles, perr := h.profileInfos(r.Context(), reviewers)
	if perr != nil {
		// Identity is best-effort enrichment; never fail the reviews on it.
		profiles = map[string]ProfileInfo{}
	}

	writeJSON(w, MintReviewsResponse{
		Summary: MintAggregate{
			MintURL:      mintURL,
			AverageScore: averageMintScore(deduped),
			ReviewCount:  len(deduped),
		},
		Reviews:  deduped,
		Profiles: profiles,
	})
}

func (h *Handler) discoverMints(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/mint/discover only", http.StatusMethodNotAllowed)
		return
	}
	limit := intParam(r, "limit", 200)
	events, err := h.store.QueryEvents(r.Context(), chstore.EventQueryInput{
		Kinds: []int{mintReviewKind},
		Tags:  []chstore.TagFilter{{Key: "k", Value: cashuMintK}},
		Limit: uint64(limit),
	})
	if err != nil {
		writeError(w, err)
		return
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

	mints := make([]MintAggregate, 0, len(byMint))
	for key, reviews := range byMint {
		deduped := dedupeMintReviewsByReviewer(reviews)
		mints = append(mints, MintAggregate{
			MintURL:      display[key],
			AverageScore: averageMintScore(deduped),
			ReviewCount:  len(deduped),
		})
	}
	// Best-attested first: most reviews, then highest average, then url for a
	// stable order (map iteration is random).
	sort.Slice(mints, func(i, j int) bool {
		if mints[i].ReviewCount != mints[j].ReviewCount {
			return mints[i].ReviewCount > mints[j].ReviewCount
		}
		if scoreOrZero(mints[i].AverageScore) != scoreOrZero(mints[j].AverageScore) {
			return scoreOrZero(mints[i].AverageScore) > scoreOrZero(mints[j].AverageScore)
		}
		return mints[i].MintURL < mints[j].MintURL
	})

	writeJSON(w, DiscoverMintsResponse{Mints: mints})
}

// --- parsing / aggregation (mirrors nagg-ts facade/mint-reviews.ts) ---------

func normalizeMintURL(url string) string {
	return strings.ToLower(strings.TrimRight(strings.TrimSpace(url), "/"))
}

func mintURLCandidates(url string) []string {
	trimmed := strings.TrimRight(strings.TrimSpace(url), "/")
	return []string{trimmed, trimmed + "/"}
}

func mintTagValue(tags [][]string, key string) (string, bool) {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == key {
			return tag[1], true
		}
	}
	return "", false
}

func parseMintScore(content string) *float64 {
	m := mintScoreRe.FindStringSubmatch(content)
	if m == nil {
		return nil
	}
	n, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return nil
	}
	if n < 0 {
		n = 0
	}
	if n > 5 {
		n = 5
	}
	return &n
}

func mintReviewFromEvent(event chstore.EventView) (MintReview, bool) {
	if event.Kind != mintReviewKind {
		return MintReview{}, false
	}
	if k, ok := mintTagValue(event.Tags, "k"); !ok || k != cashuMintK {
		return MintReview{}, false
	}
	mintURL, ok := mintTagValue(event.Tags, "u")
	if !ok || mintURL == "" {
		return MintReview{}, false
	}
	return MintReview{
		EventID:        event.ID,
		ReviewerPubkey: event.PubKey,
		MintURL:        mintURL,
		Score:          parseMintScore(event.Content),
		Content:        event.Content,
		CreatedAt:      event.CreatedAt.Unix(),
	}, true
}

// dedupeMintReviewsByReviewer keeps the latest review per reviewer, so one
// spammer posting many reviews can't skew a mint's score.
func dedupeMintReviewsByReviewer(reviews []MintReview) []MintReview {
	latest := map[string]MintReview{}
	for _, r := range reviews {
		if cur, ok := latest[r.ReviewerPubkey]; !ok || r.CreatedAt > cur.CreatedAt {
			latest[r.ReviewerPubkey] = r
		}
	}
	out := make([]MintReview, 0, len(latest))
	for _, r := range latest {
		out = append(out, r)
	}
	return out
}

func averageMintScore(reviews []MintReview) *float64 {
	var sum float64
	var n int
	for _, r := range reviews {
		if r.Score != nil {
			sum += *r.Score
			n++
		}
	}
	if n == 0 {
		return nil
	}
	avg := sum / float64(n)
	return &avg
}

func scoreOrZero(s *float64) float64 {
	if s == nil {
		return 0
	}
	return *s
}
