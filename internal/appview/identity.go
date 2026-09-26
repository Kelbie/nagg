package appview

import (
	"context"
	"sort"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/socialgraph"
	"github.com/vertex-lab/nagg/internal/vertex"
)

// Identity is the ONE spelling of a Nostr pubkey across the app-view. Every
// response that names a pubkey — a ranked profile, a mint operator, a
// reviewer, an AI provider's operator — carries the same group under a
// top-level `identities` map, so the app renders score, reach, profile and
// cross-links from any endpoint out of one cache instead of learning a
// different vocabulary per route. It is additive: the older per-route fields
// (`providers[pk].vertex`, `operatorPubkey`/`followers`, `profiles`) stay as
// they were for older builds.
//
// Absence is spelled as null, never as zero. A reach nagg could not resolve,
// a Vertex score it has not cached, a first-event time it has not computed:
// each is null, and a 0 is always a measurement.
type Identity struct {
	Pubkey string `json:"pubkey"`
	Npub   string `json:"npub"`
	// Profile is the kind-0 nagg knows, or null when it knows none.
	Profile *IdentityProfile `json:"profile"`
	Reach   IdentityReach    `json:"reach"`
	Vertex  IdentityVertex   `json:"vertex"`
	// Operates cross-links the pubkey to the cashu mints and AI providers it
	// runs, from the auditor roster's NUT-06 nostr contact and the provider
	// directory. Always present; the lists are empty, never null.
	Operates IdentityOperates `json:"operates"`
	// FirstEventAt is the pubkey's earliest indexed event, Unix seconds. It is
	// only computed where a route already pays for it (/nostr/profile) and
	// null elsewhere.
	FirstEventAt *int64 `json:"firstEventAt"`
}

// IdentityProfile is the distilled kind-0. Empty fields are omitted, the same
// way ProfileFields omits them.
type IdentityProfile struct {
	Name        string `json:"name,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	Picture     string `json:"picture,omitempty"`
	Banner      string `json:"banner,omitempty"`
	About       string `json:"about,omitempty"`
	NIP05       string `json:"nip05,omitempty"`
	// NIP05Valid is present only where the route validated the name
	// (/nostr/profile); list routes carry the claimed name unverified.
	NIP05Valid *bool  `json:"nip05Valid,omitempty"`
	Website    string `json:"website,omitempty"`
	LUD16      string `json:"lud16,omitempty"`
}

// IdentityReach is the operator's follower reach with the honesty that travels
// with it (socialgraph.Reach on the wire). Followers and Follows are null when
// nagg could not resolve them; Source names which source answered and is
// omitted when Followers is null. Follows is also null for a relay-scan
// answer, which only counts followers.
type IdentityReach struct {
	Followers *uint64 `json:"followers"`
	Follows   *uint64 `json:"follows"`
	Source    string  `json:"source,omitempty"`
}

// IdentityVertex is the Vertex DVM reputation: each field null when unknown.
// The object itself is always present.
type IdentityVertex struct {
	Rank      *float64 `json:"rank"`
	Score     *float64 `json:"score"`
	FetchedAt *int64   `json:"fetchedAt"`
}

// IdentityOperates lists what the pubkey runs. Both lists are always present.
type IdentityOperates struct {
	Mints       []string `json:"mints"`
	AIProviders []string `json:"aiProviders"`
}

// identityOptions lets a route hand the builder what it has already resolved,
// so the identities agree with the route's own fields (a fresh DVM result
// must not be contradicted by a stale cache row beside it) and so the builder
// issues no query the route already paid for.
type identityOptions struct {
	// profiles are pre-fetched kind-0 rows keyed by pubkey. Non-nil means the
	// route already read (and backfilled) every requested pubkey, so the
	// builder reads nothing.
	profiles map[string]chstore.K0Row
	// validateNIP05 runs the NIP-05 check, which may cost an HTTP round trip
	// per uncached name. Only the single-subject profile route pays it.
	validateNIP05 bool
	// reach is authoritative pre-resolved reach; non-nil skips socialReach so
	// the identity cannot disagree with the row it sits beside.
	reach map[string]socialgraph.Reach
	// reachFallback answers for pubkeys the resolver left unknown — the
	// nostr module's pubkey_stats rollup, or a row's own published count.
	// Already rendered, so a source that counts only followers leaves
	// follows null rather than inventing a 0.
	reachFallback map[string]IdentityReach
	// vertex overrides the cache per pubkey; the cache is read only for
	// pubkeys absent here. A present override with all-null fields means
	// "the route decided not to publish a score" and is honoured as such.
	vertex map[string]IdentityVertex
	// firstEventAt is Unix seconds per pubkey, where the route computed it.
	firstEventAt map[string]int64
}

// identities builds the identity group for pubkeys with no pre-resolved
// input: batched kind-0 (with the on-demand relay backfill, so a mint-only
// deployment still fills profiles), the shared reach resolver, and the Vertex
// cache — never a live DVM call.
func (h *Handler) identities(ctx context.Context, pubkeys []string) map[string]Identity {
	return h.identitiesWith(ctx, pubkeys, identityOptions{})
}

// identitiesWith is identities with the route's pre-resolved input applied.
func (h *Handler) identitiesWith(ctx context.Context, pubkeys []string, opts identityOptions) map[string]Identity {
	keys := identityKeys(pubkeys)
	out := make(map[string]Identity, len(keys))
	if len(keys) == 0 {
		return out
	}

	rows := opts.profiles
	if rows == nil {
		rows = map[string]chstore.K0Row{}
		if h.store != nil {
			if fetched, err := h.profileRows(ctx, keys); err == nil {
				rows = fetched
			}
		}
	}

	reach := opts.reach
	if reach == nil {
		reach = map[string]socialgraph.Reach{}
		if h.socialReach != nil {
			if resolved, err := h.socialReach.Reach(ctx, keys); err == nil {
				reach = resolved
			}
		}
	}

	cached := map[string]vertex.ProfileResult{}
	if missing := keysWithoutVertex(keys, opts.vertex); len(missing) > 0 && h.store != nil {
		if got, err := h.store.CachedVertexProfiles(ctx, missing); err == nil {
			cached = got
		}
	}

	index := h.operatorIndex(ctx)

	for _, pk := range keys {
		id := Identity{Pubkey: pk, Npub: vertex.Npub(pk), Operates: index.operatesFor(pk)}
		if row, ok := rows[pk]; ok {
			id.Profile = h.identityProfile(ctx, pk, row, opts.validateNIP05)
		}
		if r, ok := reach[pk]; ok && r.Known {
			id.Reach = identityReach(r)
		} else {
			id.Reach = opts.reachFallback[pk]
		}
		if v, ok := opts.vertex[pk]; ok {
			id.Vertex = v
		} else if p, ok := cached[pk]; ok {
			id.Vertex = identityVertexFromProfile(p)
		}
		if at, ok := opts.firstEventAt[pk]; ok {
			id.FirstEventAt = &at
		}
		out[pk] = id
	}
	return out
}

// identityKeys normalises (hex or npub → lowercase hex) and dedupes,
// preserving first-seen order so batched reads are deterministic.
func identityKeys(pubkeys []string) []string {
	seen := make(map[string]struct{}, len(pubkeys))
	out := make([]string, 0, len(pubkeys))
	for _, raw := range pubkeys {
		pk, ok := vertex.NormalizePubkey(raw)
		if !ok {
			continue
		}
		out = appendUniqueString(out, seen, pk)
	}
	return out
}

func keysWithoutVertex(keys []string, overrides map[string]IdentityVertex) []string {
	out := make([]string, 0, len(keys))
	for _, pk := range keys {
		if _, ok := overrides[pk]; !ok {
			out = append(out, pk)
		}
	}
	return out
}

func (h *Handler) identityProfile(ctx context.Context, pubkey string, row chstore.K0Row, validate bool) *IdentityProfile {
	profile := &IdentityProfile{
		Name:        row.Name,
		DisplayName: row.DisplayName,
		Picture:     row.Picture,
		Banner:      row.Banner,
		About:       row.About,
		NIP05:       row.NIP05,
		Website:     row.Website,
		LUD16:       row.LUD16,
	}
	if validate && row.NIP05 != "" {
		fields := h.profileFields(ctx, pubkey, row).fields
		profile.NIP05Valid = fields.NIP05Valid
		if fields.NIP05 == nil {
			// The name resolves to a different pubkey: profileFields
			// withholds it, and so does the identity.
			profile.NIP05 = ""
		}
	}
	return profile
}

// identityReachFromGraph renders an exact pubkey_stats rollup.
func identityReachFromGraph(followers, follows uint64) IdentityReach {
	return identityReach(socialgraph.FromGraph(followers, follows))
}

func identityReach(r socialgraph.Reach) IdentityReach {
	if !r.Known {
		return IdentityReach{}
	}
	followers := r.Followers
	out := IdentityReach{Followers: &followers, Source: r.Source}
	if !r.Approximate {
		follows := r.Follows
		out.Follows = &follows
	}
	return out
}

// identityVertexFromProfile maps a cached or fresh DVM profile. A zero
// ProfileResult (no PubKey) is "nothing known" and maps to all-null fields.
func identityVertexFromProfile(p vertex.ProfileResult) IdentityVertex {
	if p.PubKey == "" {
		return IdentityVertex{}
	}
	rank := p.Rank
	return IdentityVertex{Rank: &rank, Score: p.Score, FetchedAt: p.FetchedAt}
}

// identityVertexFromSearch maps a ranked search/recommended row.
func identityVertexFromSearch(r vertex.SearchResult) IdentityVertex {
	return IdentityVertex{Rank: r.Rank, Score: r.Score, FetchedAt: r.FetchedAt}
}

// identityVertexFromFollower maps a top-follower entry, stamped with the
// parent profile's fetch time since the DVM returns them together.
func identityVertexFromFollower(f vertex.TopFollower, fetchedAt *int64) IdentityVertex {
	rank := f.Rank
	return IdentityVertex{Rank: &rank, Score: f.Score, FetchedAt: fetchedAt}
}

// operatorIndex is the reverse index operator pubkey → what it runs.
type operatorIndex struct {
	mints       map[string][]string
	aiProviders map[string][]string
}

// operatorIndex builds the index from the auditor roster (a NUT-06 nostr
// contact per mint) and the AI provider directory. Both are in-memory
// snapshots on their owners' side, so this is a walk, not a fetch.
func (h *Handler) operatorIndex(ctx context.Context) operatorIndex {
	index := operatorIndex{mints: map[string][]string{}, aiProviders: map[string][]string{}}
	if h.auditor != nil {
		if mints, err := h.auditor.Mints(ctx); err == nil {
			seen := map[string]map[string]struct{}{}
			for _, m := range mints {
				pk, ok := vertex.NormalizePubkey(m.OperatorContact)
				if !ok || m.URL == "" {
					continue
				}
				if seen[pk] == nil {
					seen[pk] = map[string]struct{}{}
				}
				// Verbatim URL, deduped on the normalized key: lowercasing a
				// path ("/Bitcoin") would hand the app a URL its mint does
				// not answer to.
				key := normalizeMintURL(m.URL)
				if _, dup := seen[pk][key]; dup {
					continue
				}
				seen[pk][key] = struct{}{}
				index.mints[pk] = append(index.mints[pk], m.URL)
			}
		}
	}
	if h.aiProviderDir != nil {
		for raw, urls := range h.aiProviderDir.Operators() {
			pk, ok := vertex.NormalizePubkey(raw)
			if !ok {
				continue
			}
			index.aiProviders[pk] = append(index.aiProviders[pk], urls...)
		}
	}
	for _, urls := range index.mints {
		sort.Strings(urls)
	}
	for _, urls := range index.aiProviders {
		sort.Strings(urls)
	}
	return index
}

func (index operatorIndex) operatesFor(pubkey string) IdentityOperates {
	out := IdentityOperates{Mints: []string{}, AIProviders: []string{}}
	if mints := index.mints[pubkey]; len(mints) > 0 {
		out.Mints = append(out.Mints, mints...)
	}
	if providers := index.aiProviders[pubkey]; len(providers) > 0 {
		out.AIProviders = append(out.AIProviders, providers...)
	}
	return out
}
