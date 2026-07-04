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
// when no surviving review carried a score. FavouriteCount is the subset of
// deduped reviews posted WITHOUT a score (pure endorsements) — the same field
// the discover endpoint carries, so the two mint surfaces share one contract
// and the app never recomputes the scored/favourite split from reviews[].
type MintAggregate struct {
	MintURL        string   `json:"mintUrl"`
	AverageScore   *float64 `json:"averageScore"`
	ReviewCount    int      `json:"reviewCount"`
	FavouriteCount int      `json:"favouriteCount"`
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

	favourites := 0
	for _, review := range deduped {
		if review.Score == nil {
			favourites++
		}
	}
	writeJSON(w, MintReviewsResponse{
		Summary: MintAggregate{
			MintURL:        mintURL,
			AverageScore:   averageMintScore(deduped),
			ReviewCount:    len(deduped),
			FavouriteCount: favourites,
		},
		Reviews:  deduped,
		Profiles: profiles,
	})
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
