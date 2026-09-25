package appview

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sort"
	"strings"

	"github.com/vertex-lab/nagg/internal/auditor"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/socialgraph"
	"github.com/vertex-lab/nagg/internal/vertex"
)

// DiscoverMint is one mint in the discovery feed: its Nostr review aggregate,
// the operator's Nostr identity + Vertex social reputation, and (when the
// auditor is wired) audit state + supported units. It carries everything a mint
// card renders, so the app builds the "Add Mints" list from ONE response
// instead of the per-mint review + operator-profile N+1 fan-outs layered on a
// direct api.sovran.money auditor call.
type DiscoverMint struct {
	MintURL     string `json:"mintUrl"`
	Name        string `json:"name,omitempty"`
	IconURL     string `json:"iconUrl,omitempty"`
	Description string `json:"description,omitempty"`
	// SupportedUnits is the union of NUT-04 (mint) and NUT-05 (melt) method
	// units — a distilled convenience for list filters.
	SupportedUnits []string `json:"supportedUnits,omitempty"`
	// Nuts is the mint's NUT-06 `nuts` capability map passed through VERBATIM
	// (raw JSON object: {"4":{...},"7":{"supported":true},"17":{...},…}).
	// Deliberately untyped: the app reads whichever NUT entries it cares about
	// (payment methods, state check, P2PK, websockets, …) without a nagg
	// release, and previously had to fetch every mint's /v1/info itself to
	// learn any of this. Also backfilled from stored info for review-only mints.
	Nuts json.RawMessage `json:"nuts,omitempty"`
	// Testnut is true when nagg's weekly probe saw the mint mark a never-paid
	// mint quote as paid — a fake payment backend (internal/mintprobe). False
	// also covers mints not yet probed.
	Testnut bool `json:"testnut"`

	// Nostr reviews (NIP-87 kind-38000). AverageScore is null when no surviving
	// review carried a [n/5]. ReviewCount is the deduped review total (one latest
	// per reviewer); FavouriteCount is the subset posted WITHOUT a score, i.e.
	// pure recommendations/endorsements.
	AverageScore   *float64 `json:"averageScore"`
	ReviewCount    int      `json:"reviewCount"`
	FavouriteCount int      `json:"favouriteCount"`

	// Auditor data, present only when HasAudit is true.
	Uptime24h      *float64 `json:"uptime24h,omitempty"`
	AvgLatencyMs   *float64 `json:"avgLatencyMs,omitempty"`
	AuditSource    string   `json:"auditSource,omitempty"`
	AuditUpdatedAt int64    `json:"auditUpdatedAt,omitempty"`
	HasAudit       bool     `json:"hasAudit"`
	State          string   `json:"state,omitempty"`
	NMints         int      `json:"nMints"`
	NMelts         int      `json:"nMelts"`
	NErrors        int      `json:"nErrors"`

	// Operator Nostr account + Vertex social reputation, present when the mint
	// published a NUT-06 nostr contact nagg could resolve.
	OperatorPubkey string `json:"operatorPubkey,omitempty"`
	OperatorNpub   string `json:"operatorNpub,omitempty"`
	Followers      uint64 `json:"followers"`
	Follows        uint64 `json:"follows"`
	// FollowersKnown says whether Followers is a measurement. It is additive
	// rather than making Followers nullable, because this field is already
	// shipped; false means nagg could not establish the operator's reach and
	// the 0 above is a placeholder, not a count. Every row on this endpoint
	// reported 0 in production precisely because the two were spelled alike.
	FollowersKnown bool `json:"followersKnown"`
	// FollowersSource is which source answered: "graph" (nagg's kind-3 rollup,
	// exact), "vertex" (the Vertex DVM cache, exact), or "relays" (a live
	// kind-3 scan, a LOWER BOUND). Omitted when FollowersKnown is false.
	FollowersSource string   `json:"followersSource,omitempty"`
	VertexRank      float64  `json:"vertexRank"`
	VertexScore     *float64 `json:"vertexScore"`
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
// It goes through Store.MintReviewEvents, NOT QueryEvents: the general reader
// clamps any Limit above 500 down to 50, which silently reduced this scan to
// the 50 newest reviews (23 mints listed, Minibits at 22 of its 91 reviews).
const discoverReviewScanCap = 5000

// TestnutProvider reads the unpaid-quote probe's standing verdict per mint
// (satisfied by *clickhouse.Store). A mint with no verdict yet is absent.
type TestnutProvider interface {
	MintProbeVerdicts(ctx context.Context) (map[string]chstore.MintProbeVerdict, error)
}

// probeVerdicts re-keys the probe verdicts by normalizeMintURL, the key the
// mint surfaces match on (the probe stores its own path-preserving form).
// Without a provider there are no verdicts.
func (h *Handler) probeVerdicts(ctx context.Context) (map[string]chstore.MintProbeVerdict, error) {
	if h.testnuts == nil {
		return map[string]chstore.MintProbeVerdict{}, nil
	}
	stored, err := h.testnuts.MintProbeVerdicts(ctx)
	if err != nil {
		return map[string]chstore.MintProbeVerdict{}, err
	}
	out := make(map[string]chstore.MintProbeVerdict, len(stored))
	for u, verdict := range stored {
		out[normalizeMintURL(u)] = verdict
	}
	return out, nil
}

// testnutFilter is the discover `testnut` query: "" returns every mint, "true"
// only testnuts, "false" only non-testnuts.
type testnutFilter int

const (
	testnutAny testnutFilter = iota
	testnutOnly
	testnutExclude
)

func parseTestnutFilter(raw string) (testnutFilter, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return testnutAny, nil
	case "true":
		return testnutOnly, nil
	case "false":
		return testnutExclude, nil
	}
	return testnutAny, errors.New("testnut must be true or false")
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
	// limit caps the number of MINTS returned, not the reviews scanned — the
	// review scan must be wide enough that each mint's aggregate is accurate
	// (otherwise a popular mint shows far fewer reviews here than on its own
	// reviews page).
	mintsLimit := intParam(r, "limit", 200)
	testnutMode, err := parseTestnutFilter(r.URL.Query().Get("testnut"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

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

	// Filter before metadata/social reads and before applying limit.
	if filter := strings.TrimSpace(r.URL.Query().Get("mint")); filter != "" {
		match := normalizeMintURL(filter)
		for key := range keys {
			if key != match {
				delete(keys, key)
			}
		}
	}

	// Testnut verdicts, keyed like the union. A lookup failure degrades to
	// "no testnuts" for the unfiltered feed, but fails a filtered request
	// rather than silently answering it wrong.
	verdicts, terr := h.probeVerdicts(ctx)
	if terr != nil && testnutMode != testnutAny {
		writeError(w, terr)
		return
	}
	if testnutMode != testnutAny {
		for key := range keys {
			if verdicts[key].Testnut != (testnutMode == testnutOnly) {
				delete(keys, key)
			}
		}
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
	//
	// The kind-0 lookup runs in every deployment — a mint-only one has no
	// firehose for kind 0, so profileInfos' on-demand relay fetch is exactly how
	// operator profiles arrive. The other two read pubkey_stats and the Vertex
	// cache, which the nostr module owns; without it they are skipped rather
	// than issued and discarded (see WithSocialEnrichment).
	profiles, perr := h.profileInfos(ctx, operatorPubkeys)
	if perr != nil {
		profiles = map[string]ProfileInfo{}
	}
	// Operator reach goes through the ONE shared resolver (internal/socialgraph),
	// which /app/ai-providers reads too. It is cache-only and never blocks on a
	// relay, and it answers Unknown rather than 0 for a pubkey it has not
	// resolved — the distinction this endpoint was missing when it reported
	// every mint operator as having 0 followers.
	reach := map[string]socialgraph.Reach{}
	if h.socialReach != nil {
		if resolved, rerr := h.socialReach.Reach(ctx, operatorPubkeys); rerr == nil {
			reach = resolved
		}
	}
	// The Vertex profile cache is NOT gated on the nostr module. Its three
	// tables are declared in every deployment (the DVM plugin registry creates
	// them regardless of module set), and client-signed profile reads fill
	// them without a server key — so a mint deployment can and should read
	// cached operator reputation. Gating it alongside pubkey_stats, which
	// genuinely needs the nostr firehose, is what left vertexRank/vertexScore
	// empty on a deployment documented as supporting them.
	vertexProfiles := map[string]vertex.ProfileResult{}
	if cached, verr := h.store.CachedVertexProfiles(ctx, operatorPubkeys); verr == nil {
		vertexProfiles = cached
	}

	// 5) Build rows; stored NUT-06 info fills metadata for review-only mints.
	mints := make([]DiscoverMint, 0, len(keys))
	for key := range keys {
		row := buildDiscoverMint(key, aggByKey[key], auditByKey, operatorByKey, reach, vertexProfiles)
		row.Testnut = verdicts[key].Testnut
		mints = append(mints, row)
	}

	sortDiscoverMints(mints)
	if mintsLimit > 0 && len(mints) > mintsLimit {
		mints = mints[:mintsLimit]
		profiles = pruneProfilesToMints(mints, profiles)
	}

	// Metadata does not affect ranking, so read snapshots only for returned rows.
	if h.mintInfo != nil {
		for i := range mints {
			row := &mints[i]
			if row.HasAudit {
				continue
			}
			if document, err := h.mintInfo.LatestInfo(ctx, normalizeMintURL(row.MintURL)); err == nil {
				info := auditor.MintFromInfo(document)
				row.Name, row.IconURL, row.Description = info.Name, info.IconURL, info.Description
				row.Nuts, row.SupportedUnits = info.Nuts, info.Units
			}
		}
	}

	writeJSON(w, DiscoverMintsResponse{Mints: mints, Profiles: profiles})
}

// mintReviewAggregates groups all cashu mint reviews by normalized URL and
// computes the per-mint average / review count / favourite count.
func (h *Handler) mintReviewAggregates(ctx context.Context, limit int) (map[string]mintReviewAgg, error) {
	events, err := h.store.MintReviewEvents(ctx, uint64(limit))
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
	if m.Uptime24h != nil {
		uptime = clamp01(*m.Uptime24h / 100)
	}
	score := scoreOrZero(m.AverageScore) / 5.0
	// log10(1+n): ~100 reviews → 1.0; ~10k followers → 1.0.
	reviewCount := clamp01(math.Log10(1+float64(m.ReviewCount)) / 2.0)
	// Unresolved reach contributes nothing, the same as a measured zero. That
	// is deliberate and it is not the conflation this endpoint was fixed for:
	// the blend is additive over evidence, absent evidence adds nothing, and
	// inventing a substitute (an average, a renormalised scale) would be a
	// guess dressed as a measurement. The DIFFERENCE is published instead —
	// followersKnown says whether the 0 is a count — and the shared
	// socialgraph.CompareBest ladder is what ranks on it where reach is the
	// ranking signal rather than one term of four.
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
	reach map[string]socialgraph.Reach,
	vertexProfiles map[string]vertex.ProfileResult,
) DiscoverMint {
	var row DiscoverMint
	if audit, ok := auditByKey[key]; ok {
		row.MintURL = audit.URL
		row.Name = audit.Name
		row.IconURL = audit.IconURL
		row.Description = audit.Description
		row.SupportedUnits = audit.Units
		row.Nuts = audit.Nuts
		row.HasAudit = true
		row.State = audit.State
		row.NMints = audit.NMints
		row.NMelts = audit.NMelts
		row.NErrors = audit.NErrors
		row.Uptime24h = audit.Uptime24h
		row.AvgLatencyMs = audit.AvgLatencyMs
		row.AuditSource = audit.Source
		row.AuditUpdatedAt = audit.UpdatedAt
	}
	if row.MintURL == "" {
		row.MintURL = agg.display
	}
	row.AverageScore = agg.avg
	row.ReviewCount = agg.reviews
	row.FavouriteCount = agg.favourites

	if pk, ok := operatorByKey[key]; !ok {
		// No NUT-06 nostr contact: there is nobody to count, which is an
		// established zero rather than an unresolved one.
		row.FollowersKnown = true
	} else {
		row.OperatorPubkey = pk
		row.OperatorNpub = vertex.Npub(pk)
		if r, ok := reach[pk]; ok && r.Known {
			row.Followers, row.Follows = r.Followers, r.Follows
			row.FollowersKnown, row.FollowersSource = true, r.Source
		}
		if dvm, ok := vertexProfiles[pk]; ok && dvm.PubKey != "" {
			row.VertexRank = dvm.Rank
			row.VertexScore = dvm.Score
		}
	}
	return row
}
