package appview

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/vertex-lab/nagg/internal/auditor"
	"github.com/vertex-lab/nagg/internal/cache"
	"github.com/vertex-lab/nagg/internal/capabilities"
	"github.com/vertex-lab/nagg/internal/chgate"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/dvm"
	"github.com/vertex-lab/nagg/internal/safego"
	"github.com/vertex-lab/nagg/internal/vertex"
)

type Store interface {
	FollowsFeed(context.Context, []string, int64, uint64, uint64) ([]chstore.EventView, error)
	QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error)
	EventAggregates(context.Context, []string) (map[string]map[string]map[string]uint64, error)
	LatestProfiles(context.Context, []string) (map[string]chstore.ProfileRow, error)
	FollowCounts(context.Context, string) (chstore.FollowCounts, error)
	ProfileFirstEventCreatedAt(context.Context, string) (*time.Time, error)
	CachedVertexProfile(context.Context, string) (vertex.ProfileResult, bool, error)
	CachedVertexProfiles(context.Context, []string) (map[string]vertex.ProfileResult, error)
	SaveVertexProfile(context.Context, vertex.ProfileResult) error
	ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error)
	Notifications(context.Context, chstore.NotificationInput) ([]chstore.NotificationRow, error)
	BatchFollowCounts(context.Context, []string) (map[string]chstore.FollowCounts, error)
	FollowEdges(context.Context, string, []string) (map[string]chstore.FollowEdge, error)
	RankedDirectReplyIDs(context.Context, string, string, int, int) ([]string, error)
	AuthoredReplyChain(context.Context, string, string, int) ([]string, error)
	FollowedReplies(context.Context, string, []string) (map[string]string, error)
	SearchProfiles(context.Context, string, uint64) ([]chstore.ProfileSearchRow, error)
}

// RankedFeedProvider runs the shared ranked-feed ranking pipeline. It is
// satisfied by *graphqlapi.Ranker, which reuses the exact same ranking core as
// the GraphQL rankedEvents resolver. The REST handler decodes the request body
// into the same map shape the GraphQL `rankedEvents(input: ...)` field accepts
// and hands it to RankedEventViews, so both transports produce identical
// ranking for identical input. When nil, the ranked-feed route returns 503.
type RankedFeedProvider interface {
	RankedEventViews(context.Context, any) ([]chstore.EventView, error)
}

type Handler struct {
	store                     Store
	vertex                    VertexClient
	dvm                       *dvm.Registry
	profileSearcher           ProfileSearcher
	userBackfiller            UserFeedBackfiller
	eventBackfiller           EventBackfiller
	profileBackfiller         ProfileBackfiller
	engagementBackfiller      EngagementBackfiller
	threadBackfiller          ThreadBackfiller
	followBackfiller          FollowBackfiller
	dmEnvelopeBackfiller      DMEnvelopeBackfiller
	nip05Validator            *nip05Validator
	rateLimiter               *rateLimiter
	vertexProfileMinFollowers uint64
	viewerPubkey              string
	cache                     cache.Cache
	cacheTTL                  time.Duration
	cacheStaleFor             time.Duration
	ranker                    RankedFeedProvider
	auditor                   AuditorClient
	appLatestVersion          string
	appUpdateMessage          string
	// viewerTouch records "this pubkey is a real Sovran viewer" (relevance
	// tracking for the ingest post cap). Called ONLY on routes where the
	// pubkey is semantically the requesting user — notifications, DM
	// envelopes, thread viewer — never on browsed subjects (profiles, user
	// feeds), so viewing a bot's profile can't mark the bot as a Sovran user.
	// Must be non-blocking; nil disables.
	viewerTouch func(pubkey string)
	// gate bounds how many CH-heavy requests execute concurrently (cache
	// misses only). The shared ClickHouse degrades sharply past a handful of
	// concurrent heavy queries, so excess requests wait here and succeed a little
	// slower instead of stampeding CH and failing. This is the PROCESS-WIDE
	// chgate shared with GraphQL and the background workers, not a REST-only
	// semaphore. nil = unlimited.
	gate *chgate.Gate
}

type VertexClient interface {
	Search(context.Context, vertex.SearchArgs) ([]vertex.SearchResult, bool, error)
	Recommended(context.Context, vertex.RecommendedArgs) ([]vertex.SearchResult, bool, error)
	ProfileRefresh(context.Context, string) (vertex.ProfileResult, error)
}

// ProfileSearcher is the search-only seam the /nostr/search handler depends on.
// It is satisfied by *vertex.SearchProvider (cache-backed: serves Vertex-pagerank
// results from ClickHouse, refreshes the live DVM asynchronously, degrades to
// ErrUnavailable instead of failing). Search needs only ranking; recommended() and
// vertexProfile() keep the wider VertexClient because they also need the live DVM.
type ProfileSearcher interface {
	Search(context.Context, vertex.SearchArgs) ([]vertex.SearchResult, bool, error)
}

type Option func(*Handler)

const defaultVertexProfileMinFollowers uint64 = 500

// WithDVM installs the DVM plugin registry so provider-namespaced envelope
// payloads are keyed by plugin name instead of a hardcoded vendor string.
func WithDVM(reg *dvm.Registry) Option {
	return func(h *Handler) {
		h.dvm = reg
	}
}

// scoreProviderName is the provider namespace for score/graph payloads: the
// first registered DVM plugin, falling back to the Vertex name when the
// registry is unwired (tests).
func (h *Handler) scoreProviderName() string {
	if h.dvm != nil && len(h.dvm.Names()) > 0 {
		return h.dvm.Names()[0]
	}
	return vertex.PluginName
}

func WithVertex(client VertexClient) Option {
	return func(h *Handler) {
		h.vertex = client
	}
}

// WithProfileSearch wires the cache-backed profile-search provider used by
// /nostr/search. Without it the route serves only the local ClickHouse index.
func WithProfileSearch(searcher ProfileSearcher) Option {
	return func(h *Handler) {
		h.profileSearcher = searcher
	}
}

func WithVertexProfileMinFollowers(minFollowers int) Option {
	return func(h *Handler) {
		if minFollowers < 0 {
			minFollowers = 0
		}
		h.vertexProfileMinFollowers = uint64(minFollowers)
	}
}

func WithViewerPubkey(pubkey string) Option {
	return func(h *Handler) {
		normalized, err := normalizePubkey(pubkey)
		if err == nil {
			h.viewerPubkey = normalized
		}
	}
}

func WithNIP05Validation(enabled bool) Option {
	return func(h *Handler) {
		h.nip05Validator = newNIP05Validator(enabled)
	}
}

// WithResponseCache enables the shared Redis response cache for the REST
// app-view routes. A disabled cache is a no-op.
func WithResponseCache(c cache.Cache, defaultTTL, staleFor time.Duration) Option {
	return func(h *Handler) {
		h.cache = c
		h.cacheTTL = defaultTTL
		h.cacheStaleFor = staleFor
	}
}

func WithRateLimit(limit int, window time.Duration) Option {
	return func(h *Handler) {
		h.rateLimiter = newRateLimiter(limit, window)
	}
}

// WithRankedFeed wires the shared ranked-feed ranking pipeline so the REST
// /nostr/feed/ranked route can serve the same ranking the GraphQL rankedEvents
// resolver produces. Without it, the route responds 503.
// WithConcurrencyGate installs the process-wide heavy-query gate (shared with
// GraphQL and background workers). nil leaves the handler ungated.
func WithConcurrencyGate(gate *chgate.Gate) Option {
	return func(h *Handler) {
		h.gate = gate
	}
}

func WithRankedFeed(provider RankedFeedProvider) Option {
	return func(h *Handler) {
		h.ranker = provider
	}
}

// AuditorClient supplies the upstream cashu mint auditor data (mint list, state,
// operation counts, NUT-06 info) that /nostr/mint/discover merges with nagg's
// Nostr review + social data. Satisfied by *auditor.HTTPClient.
type AuditorClient interface {
	Mints(context.Context) ([]auditor.Mint, error)
}

// WithAppVersion backs POST /app/latest-version, letting the app's update check
// read through nagg. An empty version advertises "no update".
func WithAppVersion(version, message string) Option {
	return func(h *Handler) {
		h.appLatestVersion = version
		h.appUpdateMessage = message
	}
}

// WithAuditor wires the cashu mint auditor client. Without it,
// /nostr/mint/discover degrades to Nostr-only discovery (reviews + operator
// social, no audit state / supported-unit data).
func WithAuditor(client AuditorClient) Option {
	return func(h *Handler) {
		h.auditor = client
	}
}

// WithViewerTouch wires the known-viewer recording seam (see the viewerTouch
// field for where it fires). The func must be non-blocking (relevance.Tracker
// throttles and inserts asynchronously).
func WithViewerTouch(touch func(pubkey string)) Option {
	return func(h *Handler) {
		h.viewerTouch = touch
	}
}

func WithUserFeedBackfill(backfiller UserFeedBackfiller) Option {
	return func(h *Handler) {
		h.userBackfiller = backfiller
		h.setOptionalBackfillers(backfiller)
	}
}

func WithAppViewBackfill(backfiller AppViewBackfiller) Option {
	return func(h *Handler) {
		h.userBackfiller = backfiller
		h.eventBackfiller = backfiller
		h.profileBackfiller = backfiller
		h.engagementBackfiller = backfiller
		h.threadBackfiller = backfiller
		h.followBackfiller = backfiller
		if b, ok := any(backfiller).(DMEnvelopeBackfiller); ok {
			h.dmEnvelopeBackfiller = b
		}
	}
}

func New(store Store, opts ...Option) *Handler {
	h := &Handler{
		store:                     store,
		nip05Validator:            newNIP05Validator(true),
		rateLimiter:               newRateLimiter(120, time.Minute),
		vertexProfileMinFollowers: defaultVertexProfileMinFollowers,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *Handler) setOptionalBackfillers(backfiller any) {
	if b, ok := backfiller.(EventBackfiller); ok {
		h.eventBackfiller = b
	}
	if b, ok := backfiller.(ProfileBackfiller); ok {
		h.profileBackfiller = b
	}
	if b, ok := backfiller.(EngagementBackfiller); ok {
		h.engagementBackfiller = b
	}
	if b, ok := backfiller.(ThreadBackfiller); ok {
		h.threadBackfiller = b
	}
	if b, ok := backfiller.(FollowBackfiller); ok {
		h.followBackfiller = b
	}
	if b, ok := backfiller.(DMEnvelopeBackfiller); ok {
		h.dmEnvelopeBackfiller = b
	}
}

type route struct {
	path    string
	handler http.HandlerFunc
	// heavy routes run multi-query ClickHouse aggregations; only these go
	// through the concurrency limiter so a burst can't overwhelm CH.
	heavy bool
}

// routes is the single source of truth for the REST surface. The advertised
// capabilities manifest (capabilities.AppViewRoutes) must mirror it —
// TestCapabilitiesRouteParity enforces that, because the manifest had
// silently drifted to under-report seven live routes.
func (h *Handler) routes() []route {
	return []route{
		{"/nostr/capabilities", h.capabilities, false},
		{"/nostr/feed", h.feed, true},
		{"/nostr/feed/user", h.userFeed, true},
		{"/nostr/feed/ranked", h.rankedFeed, true},
		{"/nostr/notifications", h.notifications, true},
		{"/nostr/events/aggregates", h.eventAggregates, false},
		{"/nostr/thread", h.thread, true},
		{"/nostr/follows", h.follows, false},
		{"/nostr/events", h.events, false},
		{"/nostr/events/query", h.eventsQuery, true},
		{"/nostr/dm/envelopes", h.dmEnvelopes, true},
		{"/nostr/dm/conversation", h.dmConversation, true},
		{"/nostr/follow-status", h.followStatus, false},
		{"/nostr/mint/reviews", h.mintReviews, true},
		{"/nostr/mint/discover", h.discoverMints, true},
		{"/nostr/social-graph", h.socialGraph, true},
		// Exact path; ServeMux routes it ahead of the /nostr/own/ subtree.
		{"/nostr/own/profiles", h.ownProfiles, false},
		{"/nostr/own/", h.ownHistory, true},
		{"/nostr/notifications/seen", h.notificationsSeen, false},
		{"/nostr/profiles", h.profiles, false},
		{"/nostr/profile", h.profile, false},
		{"/nostr/search", h.search, false},
		{"/nostr/recommended", h.recommended, false},
		{"/app/latest-version", h.latestVersion, false},
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	for _, route := range h.routes() {
		next := route.handler
		if route.heavy {
			next = h.limitConcurrency(next)
		}
		wrapped := h.withMiddleware(next)
		mux.HandleFunc(route.path, wrapped)
		// Forward-looking versioned alias; both share the same cache + middleware.
		mux.HandleFunc("/v1"+route.path, wrapped)
	}
}

// limitConcurrency gates a handler on the process-wide chgate so only N
// CH-heavy requests run at once; the rest wait (bounded by the request
// context) rather than piling onto an already-saturated ClickHouse. It wraps
// the raw handler INSIDE the response cache, so cache hits never wait.
// Returns 503 (retryable) if the client's context expires while queued.
func (h *Handler) limitConcurrency(next http.HandlerFunc) http.HandlerFunc {
	return h.gate.Middleware(next)
}

func (h *Handler) capabilities(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, capabilities.ServiceInfo())
}

func (h *Handler) withMiddleware(next http.HandlerFunc) http.HandlerFunc {
	// Capabilities headers and rate limiting always run; the response cache wraps
	// the underlying handler so hits still carry the standard headers.
	handler := next
	if h.cache != nil && h.cache.Enabled() {
		handler = cache.WrapREST(next, h.cache, h.cacheTTL, h.cacheStaleFor)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		capabilities.WriteHeaders(w)
		if !h.rateLimiter.allow(r) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		ctx, timer := withTimer(ctx)
		tw := &timingWriter{ResponseWriter: w, timer: timer, start: time.Now()}
		handler(tw, r.WithContext(ctx))
	}
}

type FeedEvent struct {
	ID        string     `json:"id"`
	Kind      int        `json:"kind"`
	PubKey    string     `json:"pubkey"`
	Content   string     `json:"content"`
	Tags      [][]string `json:"tags"`
	CreatedAt int64      `json:"created_at"`
}

type ProfileInfo struct {
	Name    string `json:"name"`
	Picture string `json:"picture,omitempty"`
}

// eventEndCursor mirrors the GraphQL resolver's cursor format so the REST and
// GraphQL `pageInfo.endCursor` shapes are identical.
func eventEndCursor(events []chstore.EventView) *string {
	if len(events) == 0 {
		return nil
	}
	last := events[len(events)-1]
	cursor := last.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID
	return &cursor
}

type resolvedRootEvent struct {
	ID       string
	Event    chstore.EventView
	HasEvent bool
}

var nostrEventURI = regexp.MustCompile(`nostr:(note1|nevent1)[a-z0-9]{1,512}`)

type ProfileFields struct {
	Name        *string `json:"name,omitempty"`
	DisplayName *string `json:"displayName,omitempty"`
	Picture     *string `json:"picture,omitempty"`
	Image       *string `json:"image,omitempty"`
	Banner      *string `json:"banner,omitempty"`
	About       *string `json:"about,omitempty"`
	NIP05       *string `json:"nip05,omitempty"`
	NIP05Valid  *bool   `json:"nip05Valid,omitempty"`
	Website     *string `json:"website,omitempty"`
	LUD16       *string `json:"lud16,omitempty"`
	LUD06       *string `json:"lud06,omitempty"`
}

// ProvidersEnvelope extends the envelope for pubkey-ranked routes (profile,
// search, recommended): Pubkeys is the complete ranked pubkey list (Order can
// only anchor pubkeys whose kind-0 event is locally indexed), and Providers
// carries provider-namespaced data per pubkey — "vertex" (rank/score/nodes/
// references), "nip05" (validation), "nagg" (first-indexed timestamps) — kept
// out of the generic aggregates because it is float/context-shaped provider
// output, pending the DVM plugin seam.
type ProvidersEnvelope struct {
	Envelope
	Pubkeys   []string                  `json:"pubkeys"`
	Providers map[string]map[string]any `json:"providers,omitempty"`
	FromCache bool                      `json:"fromCache,omitempty"`
}

func (p *ProvidersEnvelope) setProvider(pubkey, provider string, payload map[string]any) {
	if pubkey == "" || len(payload) == 0 {
		return
	}
	if p.Providers == nil {
		p.Providers = map[string]map[string]any{}
	}
	if p.Providers[pubkey] == nil {
		p.Providers[pubkey] = map[string]any{}
	}
	p.Providers[pubkey][provider] = payload
}

func (h *Handler) feed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "GET or POST /nostr/feed only", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Spec       string `json:"spec"`
		Limit      uint64 `json:"limit"`
		Until      int64  `json:"until"`
		Offset     uint64 `json:"offset"`
		UserPubKey string `json:"user_pubkey"`
	}
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		req.Limit = uint64(intParam(r, "limit", 30))
		req.Until = int64(intParam(r, "until", 0))
		req.Offset = uint64(intParam(r, "offset", 0))
	}

	authors := h.authorsFromFeedRequest(req.Spec, req.UserPubKey, r)
	if len(authors) == 0 {
		h.writeFeedEnvelope(w, r, nil, orderByCreatedAt)
		return
	}
	var events []chstore.EventView
	err := recordPhase(r.Context(), "db", func() (e error) {
		events, e = h.store.FollowsFeed(r.Context(), authors, req.Until, req.Limit, req.Offset)
		return
	})
	if err != nil {
		writeError(w, err)
		return
	}
	if h.shouldBackfillAuthoredFeed(events, authors, req.Until, req.Limit, req.Offset) {
		if h.tryBackfillUserFeeds(r.Context(), takeStrings(authors, 10), req.Limit) {
			events, err = h.store.FollowsFeed(r.Context(), authors, req.Until, req.Limit, req.Offset)
			if err != nil {
				writeError(w, err)
				return
			}
		}
	}
	h.writeFeedEnvelope(w, r, events, orderByCreatedAt)
}

func (h *Handler) authorsFromFeedRequest(spec, userPubkey string, r *http.Request) []string {
	if r.Method == http.MethodGet {
		// Accept the plural `pubkeys` and, for parity with the single-viewer
		// routes, fall back to a single `pubkey`/`viewer` author.
		raw := r.URL.Query().Get("pubkeys")
		if strings.TrimSpace(raw) == "" {
			raw = queryViewerParam(r)
		}
		authors := normalizePubkeys(csv(raw))
		if len(authors) == 0 && h.viewerPubkey != "" {
			authors = []string{h.viewerPubkey}
		}
		return authors
	}
	var parsed struct {
		PubKey  string   `json:"pubkey"`
		Viewer  string   `json:"viewer"`
		PubKeys []string `json:"pubkeys"`
	}
	if err := json.Unmarshal([]byte(spec), &parsed); err != nil {
		return nil
	}
	if len(parsed.PubKeys) > 0 {
		return normalizePubkeys(parsed.PubKeys)
	}
	if single := firstNonEmpty(parsed.PubKey, parsed.Viewer); single != "" {
		if pubkey, err := normalizePubkey(single); err == nil {
			return []string{pubkey}
		}
	}
	if userPubkey != "" {
		if pubkey, err := normalizePubkey(userPubkey); err == nil {
			return []string{pubkey}
		}
	}
	if h.viewerPubkey != "" {
		return []string{h.viewerPubkey}
	}
	return nil
}

func (h *Handler) userFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/feed/user only", http.StatusMethodNotAllowed)
		return
	}
	pubkey, err := h.viewerPubkeyOr(queryViewerParam(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	until := int64(intParam(r, "until", 0))
	limitParam := intParam(r, "limit", 50)
	if limitParam < 0 {
		limitParam = 0
	}
	offsetParam := intParam(r, "offset", 0)
	if offsetParam < 0 {
		offsetParam = 0
	}
	limit := uint64(limitParam)
	offset := uint64(offsetParam)
	var events []chstore.EventView
	err = recordPhase(r.Context(), "db", func() (e error) {
		events, e = h.store.FollowsFeed(r.Context(), []string{pubkey}, until, limit, offset)
		return
	})
	if err != nil {
		writeError(w, err)
		return
	}
	if h.shouldBackfillUserFeed(events, until, limit, offset) {
		coldCount := len(events)
		slog.Info("profile.feed.cold", "pubkey", pubkey, "count", coldCount, "limit", limit)
		backfillStart := time.Now()
		if h.tryBackfillUserFeed(r.Context(), pubkey, limit) {
			events, err = h.store.FollowsFeed(r.Context(), []string{pubkey}, until, limit, offset)
			if err != nil {
				writeError(w, err)
				return
			}
			slog.Info("profile.feed.backfill", "pubkey", pubkey, "outcome", "waited",
				"before", coldCount, "after", len(events), "ms", time.Since(backfillStart).Milliseconds())
		} else {
			slog.Info("profile.feed.backfill", "pubkey", pubkey, "outcome", "timeout",
				"before", coldCount, "ms", time.Since(backfillStart).Milliseconds())
		}
	}
	h.writeFeedEnvelope(w, r, events, orderByCreatedAt)
}

func (h *Handler) shouldBackfillUserFeed(events []chstore.EventView, until int64, limit uint64, offset uint64) bool {
	if h.userBackfiller == nil || until != 0 || offset != 0 {
		return false
	}
	if limit == 0 || limit > 100 {
		limit = 50
	}
	return len(events) < int(limit)
}

func (h *Handler) shouldBackfillAuthoredFeed(events []chstore.EventView, authors []string, until int64, limit uint64, offset uint64) bool {
	if h.userBackfiller == nil || len(authors) == 0 || until != 0 || offset != 0 {
		return false
	}
	if limit == 0 || limit > 100 {
		limit = 30
	}
	return len(events) < int(limit)
}

// rankedFeed is the REST counterpart of the GraphQL rankedEvents resolver. It
// decodes the request body into the same input map the GraphQL
// `rankedEvents(input: ...)` field accepts (so Sovran feed recipes produce one
// shape for both transports), runs the shared ranking pipeline via the injected
// RankedFeedProvider, then enriches the ordered events into a FeedResponse using
// the same helpers as /nostr/feed. The ranking order is preserved verbatim.
func (h *Handler) rankedFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST /nostr/feed/ranked only", http.StatusMethodNotAllowed)
		return
	}
	if h.ranker == nil {
		http.Error(w, "ranked feed not configured", http.StatusServiceUnavailable)
		return
	}
	var input map[string]any
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	events, err := h.ranker.RankedEventViews(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	h.writeFeedEnvelope(w, r, events, orderByRank)
}

// NotificationActor is one participant in a grouped notification (a follower /
// reposter / reactor / zapper), carried as a sample so the UI can render an
// avatar cluster without the full member list.
type NotificationActor struct {
	PubKey           string  `json:"pubkey"`
	EventID          string  `json:"eventId"`
	CreatedAt        int64   `json:"createdAt"`
	ActorVertexScore float64 `json:"actorVertexScore,omitempty"`
}

// NotificationEntry is one notification item in kind vocabulary: the
// triggering event's kind says what happened (3 = a latest contact list now
// references you; 6/16 = a kind-6/16 references your event; 7 = a kind-7
// references it; 9735 = a zap receipt references it; 1 = a kind-1 references
// you or your event — the client reads the embedded event's tags to tell
// which). A collapsed group carries the newest member as its representative
// (ID/Kind/Actor), the group Total, and up to three sample Actors; entries
// with no Total are singles. No reason strings anywhere — kinds only.
type NotificationEntry struct {
	ID          string              `json:"id"`
	Kind        int                 `json:"kind"`
	Actor       string              `json:"actor,omitempty"`
	Target      string              `json:"target,omitempty"`
	Total       int                 `json:"total,omitempty"`
	TotalCapped bool                `json:"totalCapped,omitempty"`
	Actors      []NotificationActor `json:"actors,omitempty"`
}

// NotificationsEnvelope is the notifications route response: the generic
// envelope (entry order, triggering + target + profile events, aggregates)
// plus the grouping metadata the client cannot derive from one page, and the
// conservative has-next hint (grouping collapses pages, so the client drives
// load-more; see docs/notifications-flow.md).
type NotificationsEnvelope struct {
	Envelope
	Entries []NotificationEntry `json:"entries"`
	HasNext bool                `json:"hasNext"`
}

// notifications is the REST counterpart of the GraphQL notifications resolver.
// It builds a chstore.NotificationInput from request params (mirroring the
// GraphQL input: viewer, tab, policy, replyScope, since, until, limit), calls
// store.Notifications, then enriches the notification events with the same
// Metrics/Profiles/Quoted hydration the feed uses.
func (h *Handler) notifications(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "GET or POST /nostr/notifications only", http.StatusMethodNotAllowed)
		return
	}
	input, grouped, err := h.parseNotificationRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var (
		entries      []NotificationEntry
		referenced   []chstore.EventView
		actorPubkeys []string
		hasNext      bool
	)
	if grouped {
		// Follows are pulled onto their own small window: a flood of follow
		// candidates (kind-3 republishes from every follower) would otherwise fill
		// the recency window and starve likes/reposts/replies. Follows collapse to
		// one item with an exact count anyway, so a handful of recent followers is
		// all we need; everything else gets the full window.
		// Grouping needs a wide candidate window — many rows collapse into a few
		// items — so fetch well past the page size, then trim to `limit` items in
		// groupNotifications. Saturating this window is what tells us more pages
		// exist (the per-page item count can't, since grouping shrinks it).
		bodyWindow := int(input.Limit) * 6
		if bodyWindow < 120 {
			bodyWindow = 120
		}
		if bodyWindow > 600 {
			bodyWindow = 600
		}
		bodyInput := input
		bodyInput.Limit = uint64(bodyWindow)
		bodyInput.ExcludeReasons = append([]string{"follow"}, input.ExcludeReasons...)
		var bodyRows []chstore.NotificationRow
		err = recordPhase(r.Context(), "db", func() (e error) {
			bodyRows, e = h.store.Notifications(r.Context(), bodyInput)
			return
		})
		if err != nil {
			writeError(w, err)
			return
		}
		windowSaturated := len(bodyRows) >= bodyWindow

		// The follow group is a single collapsed entry pinned to the top of the
		// first page; don't re-fetch/re-emit it while paginating (until > 0) or it
		// would repeat on every scroll page.
		var followRows []chstore.NotificationRow
		if input.Tab != "MENTIONS" && input.Until == 0 {
			followInput := input
			followInput.Reasons = []string{"follow"}
			followInput.ExcludeReasons = nil
			followInput.Limit = 12
			// Follows are kind-3, never replies, so the reply-reference scans (the
			// most expensive part of the notifications query) are pure waste here.
			// "NONE" falls through the store's DIRECT/THREAD switch, skipping the
			// reply joins entirely without changing which follow rows return.
			followInput.ReplyScope = "NONE"
			if fr, ferr := h.store.Notifications(r.Context(), followInput); ferr == nil {
				followRows = fr
			}
		}

		allRows := make([]chstore.NotificationRow, 0, len(followRows)+len(bodyRows))
		allRows = append(allRows, followRows...)
		allRows = append(allRows, bodyRows...)
		entries, referenced, actorPubkeys, hasNext = h.groupNotifications(r.Context(), input, allRows, windowSaturated)
	} else {
		var rows []chstore.NotificationRow
		err = recordPhase(r.Context(), "db", func() (e error) {
			rows, e = h.store.Notifications(r.Context(), input)
			return
		})
		if err != nil {
			writeError(w, err)
			return
		}
		entries = make([]NotificationEntry, 0, len(rows))
		referenced = make([]chstore.EventView, 0, len(rows))
		for _, row := range rows {
			referenced = append(referenced, row.Event)
			entries = append(entries, NotificationEntry{
				ID:    row.Event.ID,
				Kind:  row.Event.Kind,
				Actor: row.ActorPubKey,
				Actors: []NotificationActor{{
					PubKey:           row.ActorPubKey,
					EventID:          row.Event.ID,
					CreatedAt:        row.NotificationCreatedAt.Unix(),
					ActorVertexScore: row.ActorVertexScore,
				}},
			})
		}
		hasNext = len(rows) >= int(input.Limit)
	}

	byID := make(map[string]chstore.EventView, len(referenced))
	for _, event := range referenced {
		byID[event.ID] = event
	}
	order := make([]string, 0, len(entries))
	for _, entry := range entries {
		order = append(order, entry.ID)
	}
	envelope, err := h.assembleEnvelope(r.Context(), order, orderByCreatedAt, referenced, notificationEndCursor(entries, byID))
	if err != nil {
		writeError(w, err)
		return
	}
	// Sample actors beyond the representative are not event authors, so embed
	// their kind-0 profile events explicitly for the avatar cluster.
	if err := h.appendProfileEventsTo(r.Context(), &envelope, actorPubkeys); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, NotificationsEnvelope{
		Envelope: envelope,
		Entries:  entries,
		HasNext:  hasNext,
	})
}

// notificationGroupReasons collapse into a single item. reply / quote / mention
// carry text and always render individually.
func notificationGroupable(reason string) bool {
	switch reason {
	case "follow", "repost", "reaction", "zap":
		return true
	}
	return false
}

// notificationGroupKey is what same-reason notifications collapse on. All
// follows share one group; repost/reaction/zap collapse per target post (or a
// per-reason "profile" bucket when there's no target event, e.g. a profile zap).
func notificationGroupKey(reason, targetID string) string {
	if reason == "follow" {
		return "follow"
	}
	if targetID == "" {
		return reason + ":profile"
	}
	return reason + ":" + targetID
}

type notificationGroupAcc struct {
	rep       chstore.NotificationRow // representative = most-recent member
	targetID  string
	total     int
	actors    []NotificationActor
	actorSeen map[string]struct{}
}

// groupNotifications collapses the (newest-first) candidate rows into grouped
// entries, mirroring how the feed envelope collapses kind-6/16 references. It
// returns the entries, the events to embed (representatives + targets), the
// sample-actor pubkeys to profile-hydrate, and whether more items exist past
// the page. All product semantics live here; the store query stays generic.
func (h *Handler) groupNotifications(ctx context.Context, input chstore.NotificationInput, rows []chstore.NotificationRow, windowSaturated bool) ([]NotificationEntry, []chstore.EventView, []string, bool) {
	order := make([]string, 0, len(rows))
	groups := make(map[string]*notificationGroupAcc, len(rows))
	for _, row := range rows {
		targetID := ""
		if row.Reason == "repost" || row.Reason == "reaction" || row.Reason == "zap" {
			targetID = firstEventTag(row.Event)
		}
		var key string
		if notificationGroupable(row.Reason) {
			key = notificationGroupKey(row.Reason, targetID)
		} else {
			// reply/quote/mention: each is its own single item.
			key = "single:" + row.Event.ID
		}
		acc, ok := groups[key]
		if !ok {
			acc = &notificationGroupAcc{rep: row, targetID: targetID, actorSeen: map[string]struct{}{}}
			groups[key] = acc
			order = append(order, key)
		}
		acc.total++
		if _, seen := acc.actorSeen[row.ActorPubKey]; !seen && len(acc.actors) < 3 && row.ActorPubKey != "" {
			acc.actorSeen[row.ActorPubKey] = struct{}{}
			acc.actors = append(acc.actors, NotificationActor{
				PubKey:           row.ActorPubKey,
				EventID:          row.Event.ID,
				CreatedAt:        row.NotificationCreatedAt.Unix(),
				ActorVertexScore: row.ActorVertexScore,
			})
		}
	}

	// Order items by their representative recency. Rows may arrive as several
	// concatenated windows (e.g. follows fetched separately from everything
	// else), so we can't rely on first-seen order being globally sorted.
	sort.SliceStable(order, func(i, j int) bool {
		return groups[order[i]].rep.NotificationCreatedAt.After(groups[order[j]].rep.NotificationCreatedAt)
	})
	// More items exist if we collapsed past the page, or if the candidate window
	// was saturated (older candidates remain beyond it — they may collapse into
	// the same groups, but the client needs the chance to page for new ones).
	hasNext := len(order) > int(input.Limit) || windowSaturated
	if len(order) > int(input.Limit) {
		order = order[:input.Limit]
	}

	// Exact follower count for the follow group (cheap, reused from /nostr/follows).
	// Under the FOLLOWS policy the follow group is filtered to mutuals, so the
	// all-followers count would be wrong — fall back to the window count there.
	followTotal := -1
	if input.Policy != "FOLLOWS" {
		for _, key := range order {
			if key == "follow" {
				if counts, err := h.store.FollowCounts(ctx, input.Viewer); err == nil {
					followTotal = int(counts.Followers)
				}
				break
			}
		}
	}

	entries := make([]NotificationEntry, 0, len(order))
	referenced := make([]chstore.EventView, 0, len(order)*2)
	for _, key := range order {
		acc := groups[key]
		entry := NotificationEntry{
			ID:     acc.rep.Event.ID,
			Kind:   acc.rep.Event.Kind,
			Actor:  acc.rep.ActorPubKey,
			Target: acc.targetID,
			Actors: acc.actors,
		}
		referenced = append(referenced, acc.rep.Event)
		if notificationGroupable(acc.rep.Reason) && acc.total >= 2 {
			entry.Total = acc.total
			if acc.rep.Reason == "follow" && followTotal >= 0 {
				entry.Total = followTotal
			} else {
				entry.TotalCapped = windowSaturated
			}
		}
		entries = append(entries, entry)
	}

	// Embed target events (like the feed envelope embeds kind-6/16 originals)
	// and collect sample-actor pubkeys so their profiles resolve.
	targetIDs := make([]string, 0, len(entries))
	actorPubkeys := make([]string, 0, len(entries)*3)
	for _, entry := range entries {
		if entry.Target != "" {
			targetIDs = append(targetIDs, entry.Target)
		}
		for _, actor := range entry.Actors {
			actorPubkeys = append(actorPubkeys, actor.PubKey)
		}
	}
	if len(targetIDs) > 0 {
		if targets, err := h.eventsByID(ctx, targetIDs); err == nil {
			for _, entry := range entries {
				if entry.Target == "" {
					continue
				}
				if target, ok := targets[entry.Target]; ok {
					referenced = append(referenced, target)
				}
			}
		}
	}
	return entries, referenced, actorPubkeys, hasNext
}

// notificationEndCursor mirrors eventEndCursor over the last entry's
// representative event, keeping the REST and GraphQL cursor formats identical.
func notificationEndCursor(entries []NotificationEntry, byID map[string]chstore.EventView) *string {
	if len(entries) == 0 {
		return nil
	}
	last, ok := byID[entries[len(entries)-1].ID]
	if !ok {
		return nil
	}
	cursor := last.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID
	return &cursor
}

// parseNotificationRequest builds a chstore.NotificationInput from the request,
// accepting both query params (GET) and a JSON body (POST). Defaults match the
// GraphQL parseNotificationInput: tab ALL, policy STRICT, replyScope THREAD,
// limit 50. The viewer pubkey falls back to the configured viewer. It also
// returns whether the response should be grouped (default true) — the
// followers-detail screen passes grouped=false to read the raw follow list.
func (h *Handler) parseNotificationRequest(r *http.Request) (chstore.NotificationInput, bool, error) {
	input := chstore.NotificationInput{
		Tab:        "ALL",
		Policy:     "STRICT",
		ReplyScope: "THREAD",
		Limit:      50,
	}

	var raw struct {
		PubKey     string `json:"pubkey"`
		Viewer     string `json:"viewer"`
		Tab        string `json:"tab"`
		Policy     string `json:"policy"`
		ReplyScope string `json:"replyScope"`
		Since      int64  `json:"since"`
		Until      int64  `json:"until"`
		Limit      int    `json:"limit"`
		Grouped    *bool  `json:"grouped"`
	}
	grouped := true
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			return input, grouped, err
		}
		if raw.Grouped != nil {
			grouped = *raw.Grouped
		}
		if strings.TrimSpace(raw.PubKey) == "" {
			raw.PubKey = raw.Viewer
		}
	} else {
		q := r.URL.Query()
		raw.PubKey = queryViewerParam(r)
		raw.Tab = q.Get("tab")
		raw.Policy = q.Get("policy")
		raw.ReplyScope = q.Get("replyScope")
		raw.Since = int64(intParam(r, "since", 0))
		raw.Until = int64(intParam(r, "until", 0))
		raw.Limit = intParam(r, "limit", 0)
		grouped = q.Get("grouped") != "false"
	}

	viewer, err := h.viewerPubkeyOr(raw.PubKey)
	if err != nil {
		return input, grouped, fmt.Errorf("notification pubkey: %w", err)
	}
	input.Viewer = strings.ToLower(viewer)
	h.touchViewer(input.Viewer)
	if tab := strings.ToUpper(strings.TrimSpace(raw.Tab)); tab == "ALL" || tab == "MENTIONS" {
		input.Tab = tab
	}
	if policy := strings.ToUpper(strings.TrimSpace(raw.Policy)); policy == "RELAXED" || policy == "MODERATE" || policy == "STRICT" || policy == "FOLLOWS" {
		input.Policy = policy
	}
	if replyScope := strings.ToUpper(strings.TrimSpace(raw.ReplyScope)); replyScope == "DIRECT" || replyScope == "THREAD" {
		input.ReplyScope = replyScope
	}
	input.Since = raw.Since
	input.Until = raw.Until
	if raw.Limit > 0 {
		input.Limit = uint64(raw.Limit)
	}
	return input, grouped, nil
}

func (h *Handler) eventAggregates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST /nostr/events/aggregates only", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		IDs []string `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.IDs) > 100 {
		http.Error(w, "ids max 100", http.StatusBadRequest)
		return
	}
	ids := normalizeHexIDs(req.IDs)
	h.tryBackfillEngagement(r.Context(), ids)
	aggregates, err := h.store.EventAggregates(r.Context(), ids)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, Envelope{
		Order:      []string{},
		OrderBy:    orderByCreatedAt,
		Events:     []FeedEvent{},
		Aggregates: aggregates,
	})
}

func (h *Handler) thread(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/thread only", http.StatusMethodNotAllowed)
		return
	}
	id, err := normalizeEventID(r.URL.Query().Get("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	limit := intParam(r, "limit", 1000)
	root, events, err := h.store.ThreadEvents(r.Context(), id, limit)
	if errors.Is(err, sql.ErrNoRows) {
		if h.tryBackfillThread(r.Context(), id, limit) {
			root, events, err = h.store.ThreadEvents(r.Context(), id, limit)
		}
	} else if err == nil && h.shouldBackfillThread(events, limit) {
		if h.tryBackfillThread(r.Context(), id, limit) {
			root, events, err = h.store.ThreadEvents(r.Context(), id, limit)
		}
	}
	if err != nil {
		writeError(w, err)
		return
	}
	if viewer, verr := normalizePubkey(queryViewerParam(r)); verr == nil {
		h.touchViewer(viewer)
	}
	replies := eventsJSON(events)
	ordering := h.threadOrdering(r.Context(), threadOrderParams{
		rootID:         id,
		author:         root.PubKey,
		replies:        replies,
		sort:           strings.ToLower(strings.TrimSpace(r.URL.Query().Get("sort"))),
		viewer:         queryViewerParam(r),
		offset:         intParam(r, "offset", 0),
		replyLimit:     intParam(r, "replyLimit", 0),
		candidateLimit: intParam(r, "candidateLimit", 0),
		rankedLimit:    intParam(r, "rankedLimit", 0),
	})
	// The envelope's order leads with the thread root, then the server-ranked
	// replies — one shape for feed and thread alike; the client reconstructs
	// the tree from the events' references.
	order := append([]string{root.ID}, ordering.Elements...)
	allEvents := append([]chstore.EventView{*root}, events...)
	envelope, err := h.assembleEnvelope(r.Context(), order, ordering.OrderBy, allEvents, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, envelope)
}

// threadOrderParams carries the inputs that decide a thread's reply render order.
type threadOrderParams struct {
	rootID         string
	author         string
	sort           string
	viewer         string
	replies        []FeedEvent
	offset         int
	replyLimit     int
	candidateLimit int
	rankedLimit    int
}

const (
	// threadRankedSort is the declared aggregation ("rule.metric") the
	// ranked/relevant reply order uses for its ranked tier: unique kind-7
	// referencing actors, the feed's primary signal.
	threadRankedSort        = "k7_e.actors"
	threadCandidateDefault  = 200
	threadRankedTierDefault = 50
	threadAuthorChainDepth  = 8
)

// threadOrdering builds the server-authoritative reply manifest. The default
// ("" / "new") order is the chronological/rank descendant order ThreadEvents
// already returned — unchanged, backward-compatible. "ranked" orders direct
// replies by engagement; "relevant" reproduces the viewer-specific merge the
// client used to assemble from nested GraphQL resolvers (author self-reply chain
// → one followed-tail reply → ranked direct replies → the rest), all from
// precomputed store primitives. Any id without fetched event data is dropped and
// remaining available replies are appended so the page is always complete.
func (h *Handler) threadOrdering(ctx context.Context, p threadOrderParams) OrderingManifest {
	if p.sort != "relevant" && p.sort != "ranked" {
		return eventsOrdering(p.replies, orderByRank)
	}

	var ordered []string
	var err error
	switch p.sort {
	case "relevant":
		ordered, err = h.relevantReplyOrder(ctx, p)
	case "ranked":
		ordered, err = h.store.RankedDirectReplyIDs(ctx, p.rootID, threadRankedSort, candidateLimitOr(p.candidateLimit), 0)
	}
	if err != nil {
		slog.Warn("appview.thread.order failed; using rank fallback", "sort", p.sort, "root", p.rootID, "error", err)
		return eventsOrdering(p.replies, orderByRank)
	}

	available := make(map[string]struct{}, len(p.replies))
	for _, e := range p.replies {
		available[e.ID] = struct{}{}
	}
	elements := make([]string, 0, len(p.replies))
	seen := make(map[string]struct{}, len(p.replies))
	add := func(id string) {
		if id == "" || id == p.rootID {
			return
		}
		if _, ok := available[id]; !ok {
			return
		}
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		elements = append(elements, id)
	}
	for _, id := range ordered {
		add(id)
	}
	// Replies the merge didn't surface (e.g. nested descendants) keep their
	// ThreadEvents order at the tail so the flat list stays complete.
	for _, e := range p.replies {
		add(e.ID)
	}
	elements = pageElements(elements, p.offset, p.replyLimit)
	slog.Info("appview.thread.relevant.merge", "root", p.rootID, "sort", p.sort, "scoped", p.viewer != "", "elements", len(elements))
	return OrderingManifest{OrderBy: orderByRank, Elements: elements}
}

// relevantReplyOrder reproduces the client's mergeRelevantReplyNodes ordering
// server-side using only precomputed store primitives.
func (h *Handler) relevantReplyOrder(ctx context.Context, p threadOrderParams) ([]string, error) {
	candidateLimit := candidateLimitOr(p.candidateLimit)
	rankedLimit := p.rankedLimit
	if rankedLimit <= 0 {
		rankedLimit = threadRankedTierDefault
	}
	merged := make([]string, 0, candidateLimit)

	// 1. The author's self-reply chain from the root.
	authorChain, err := h.store.AuthoredReplyChain(ctx, p.rootID, p.author, threadAuthorChainDepth)
	if err != nil {
		return nil, err
	}
	merged = append(merged, authorChain...)

	// 2. One best reply by someone the viewer follows, to the chain's tail.
	if p.viewer != "" {
		tail := p.rootID
		if len(authorChain) > 0 {
			tail = authorChain[len(authorChain)-1]
		}
		followed, err := h.store.FollowedReplies(ctx, p.viewer, []string{tail})
		if err != nil {
			return nil, err
		}
		if id := followed[tail]; id != "" {
			merged = append(merged, id)
		}
	}

	// 3. Ranked direct replies, then 4. all direct replies (recency).
	ranked, err := h.store.RankedDirectReplyIDs(ctx, p.rootID, threadRankedSort, rankedLimit, 0)
	if err != nil {
		return nil, err
	}
	merged = append(merged, ranked...)
	all, err := h.store.RankedDirectReplyIDs(ctx, p.rootID, "new", candidateLimit, 0)
	if err != nil {
		return nil, err
	}
	merged = append(merged, all...)
	return merged, nil
}

func candidateLimitOr(n int) int {
	if n <= 0 {
		return threadCandidateDefault
	}
	return n
}

// pageElements slices ids to [offset, offset+limit); limit<=0 means "all from
// offset".
func pageElements(ids []string, offset, limit int) []string {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(ids) {
		return []string{}
	}
	ids = ids[offset:]
	if limit > 0 && limit < len(ids) {
		ids = ids[:limit]
	}
	return ids
}

func (h *Handler) follows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/follows only", http.StatusMethodNotAllowed)
		return
	}
	pubkey, err := h.viewerPubkeyOr(queryViewerParam(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.tryBackfillFollows(r.Context(), pubkey)
	counts, err := h.store.FollowCounts(r.Context(), pubkey)
	if err != nil {
		writeError(w, err)
		return
	}
	envelope := inlineEnvelope(nil, orderByCreatedAt, nil, nil)
	if err := h.appendProfileEventsTo(r.Context(), &envelope, []string{pubkey}); err != nil {
		writeError(w, err)
		return
	}
	for _, event := range envelope.Events {
		envelope.Order = append(envelope.Order, event.ID)
	}
	followAggregates(&envelope, pubkey, counts, 0)
	writeJSON(w, envelope)
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/events only", http.StatusMethodNotAllowed)
		return
	}
	requested := normalizeHexIDs(csv(r.URL.Query().Get("ids")))
	eventsByID, err := h.eventsByID(r.Context(), requested)
	if err != nil {
		writeError(w, err)
		return
	}
	order := make([]string, 0, len(eventsByID))
	events := make([]chstore.EventView, 0, len(eventsByID))
	for _, id := range requested {
		event, ok := eventsByID[id]
		if !ok {
			continue
		}
		order = append(order, id)
		events = append(events, event)
	}
	envelope, err := h.assembleEnvelope(r.Context(), order, orderByCreatedAt, events, nil)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, envelope)
}

// dmEnvelopes is the REST app-view counterpart of the GraphQL dmEnvelopes
// resolver, purpose-built for the contacts/DM page: it returns the viewer's
// recent DM envelopes (gift wraps by default), paginated by `until`. nagg never
// decrypts — the client decrypts and buckets by counterparty. Unions
// author=viewer with p-tag=viewer (the one shape the generic events query can't
// OR), dedupes by id, orders createdAt DESC, truncates to limit.
func (h *Handler) dmEnvelopes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "GET or POST /nostr/dm/envelopes only", http.StatusMethodNotAllowed)
		return
	}
	viewer, err := h.viewerPubkeyOr(queryViewerParam(r))
	if err != nil {
		writeError(w, err)
		return
	}
	h.touchViewer(viewer)
	kinds := parseDmKinds(r.URL.Query().Get("kinds"))
	limit := clampDmLimit(intParam(r, "limit", 50))
	until := int64(intParam(r, "until", 0))

	h.tryBackfillDMEnvelopes(r.Context(), viewer, kinds, until, uint64(limit))
	var authored, received []chstore.EventView
	err = recordPhase(r.Context(), "db", func() (e error) {
		authored, e = h.store.QueryEvents(r.Context(), chstore.EventQueryInput{
			PubKeys: []string{viewer}, Kinds: kinds, Until: until, Limit: uint64(limit),
		})
		if e != nil {
			return
		}
		received, e = h.store.QueryEvents(r.Context(), chstore.EventQueryInput{
			Tags: []chstore.TagFilter{{Key: "p", Value: viewer}}, Kinds: kinds, Until: until, Limit: uint64(limit),
		})
		return
	})
	if err != nil {
		writeError(w, err)
		return
	}
	merged := mergeDmEnvelopes(limit, authored, received)
	// PRIVACY: DM envelopes are served bare — no profile hydration, no
	// aggregates. Gift-wrap authors are ephemeral pubkeys; enriching them
	// would leak correlation surface for zero rendering value.
	order := make([]string, 0, len(merged))
	for _, event := range merged {
		order = append(order, event.ID)
	}
	writeJSON(w, inlineEnvelope(order, orderByCreatedAt, merged, eventEndCursor(merged)))
}

// parseDmKinds reads a CSV `kinds` param, defaulting to NIP-04 legacy DMs
// (kind 4) and NIP-17 gift wraps (kind 1059) — matching the GraphQL dmKinds
// default so both transports surface the same conversations.
func parseDmKinds(raw string) []int {
	values := csv(raw)
	kinds := make([]int, 0, len(values))
	for _, v := range values {
		if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
			kinds = append(kinds, n)
		}
	}
	if len(kinds) == 0 {
		return []int{4, 1059}
	}
	return kinds
}

func clampDmLimit(n int) int {
	if n <= 0 {
		return 50
	}
	if n > 200 {
		return 200
	}
	return n
}

// mergeDmEnvelopes merges, dedupes by id, orders createdAt DESC (id DESC
// tiebreak), and truncates to limit — mirroring the GraphQL resolver.
func mergeDmEnvelopes(limit int, lists ...[]chstore.EventView) []chstore.EventView {
	seen := make(map[string]struct{})
	merged := make([]chstore.EventView, 0)
	for _, list := range lists {
		for _, ev := range list {
			if _, ok := seen[ev.ID]; ok {
				continue
			}
			seen[ev.ID] = struct{}{}
			merged = append(merged, ev)
		}
	}
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].CreatedAt.Equal(merged[j].CreatedAt) {
			return merged[i].ID > merged[j].ID
		}
		return merged[i].CreatedAt.After(merged[j].CreatedAt)
	})
	if limit > 0 && len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

func (h *Handler) profiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/profiles only", http.StatusMethodNotAllowed)
		return
	}
	raw := r.URL.Query().Get("pubkeys")
	if strings.TrimSpace(raw) == "" {
		raw = queryViewerParam(r)
	}
	pubkeys := normalizePubkeys(csv(raw))
	envelope := inlineEnvelope(nil, orderByCreatedAt, nil, nil)
	if err := h.appendProfileEventsTo(r.Context(), &envelope, pubkeys); err != nil {
		writeError(w, err)
		return
	}
	for _, event := range envelope.Events {
		envelope.Order = append(envelope.Order, event.ID)
	}
	if counts, err := h.store.BatchFollowCounts(r.Context(), pubkeys); err == nil {
		for _, pubkey := range pubkeys {
			followAggregates(&envelope, pubkey, counts[pubkey], 0)
		}
	}
	writeJSON(w, envelope)
}

func (h *Handler) profile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/profile only", http.StatusMethodNotAllowed)
		return
	}
	pubkey, err := h.viewerPubkeyOr(queryViewerParam(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	h.tryBackfillProfileSummary(ctx, pubkey)
	profiles, err := h.store.LatestProfiles(ctx, []string{pubkey})
	if err != nil {
		writeError(w, err)
		return
	}
	counts, err := h.store.FollowCounts(ctx, pubkey)
	if err != nil {
		writeError(w, err)
		return
	}
	profile := profiles[pubkey]
	createdAt, err := h.localProfileCreatedAt(ctx, pubkey, profile)
	if err != nil {
		writeError(w, err)
		return
	}
	dvmProfile, fromCache := h.vertexProfile(ctx, pubkey, counts.Followers)

	envelope := ProvidersEnvelope{
		Envelope:  inlineEnvelope(nil, orderByCreatedAt, nil, nil),
		Pubkeys:   []string{pubkey},
		FromCache: fromCache,
	}
	followerPubkeys := make([]string, 0, len(dvmProfile.TopFollowers))
	vertexPayload := map[string]any{"rank": dvmProfile.Rank}
	if dvmProfile.Score != nil {
		vertexPayload["score"] = *dvmProfile.Score
	}
	if dvmProfile.Nodes != nil {
		vertexPayload["nodes"] = *dvmProfile.Nodes
	}
	for _, follower := range dvmProfile.TopFollowers {
		fp, ok := vertex.NormalizePubkey(follower.PubKey)
		if !ok {
			continue
		}
		followerPubkeys = append(followerPubkeys, fp)
		payload := map[string]any{"rank": follower.Rank}
		if follower.Score != nil {
			payload["score"] = *follower.Score
		}
		envelope.setProvider(fp, h.scoreProviderName(), payload)
	}
	if len(followerPubkeys) > 0 {
		vertexPayload["references"] = followerPubkeys
	}
	envelope.setProvider(pubkey, h.scoreProviderName(), vertexPayload)
	if createdAt != nil {
		envelope.setProvider(pubkey, "nagg", map[string]any{"firstEventAt": *createdAt})
	}
	if fields := h.profileFields(ctx, pubkey, profile); fields.fields.NIP05Valid != nil {
		envelope.setProvider(pubkey, "nip05", map[string]any{"valid": *fields.fields.NIP05Valid})
	}
	followAggregates(&envelope.Envelope, pubkey, counts, 0)
	if err := h.appendProfileEventsTo(ctx, &envelope.Envelope, append([]string{pubkey}, followerPubkeys...)); err != nil {
		writeError(w, err)
		return
	}
	for _, event := range envelope.Events {
		if event.PubKey == pubkey {
			envelope.Order = append(envelope.Order, event.ID)
			break
		}
	}
	writeJSON(w, envelope)
}

func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/search only", http.StatusMethodNotAllowed)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("query"))
	if len(query) < 3 {
		http.Error(w, "query must be at least 3 characters", http.StatusBadRequest)
		return
	}
	// Clamp up front (matches vertex.NormalizeSearchArgs) so the Vertex cache
	// lookup, the local-fallback slot math, and the echoed limit all agree.
	limit := limitClamp(intParam(r, "limit", 5), 5)
	sortKey := r.URL.Query().Get("sort")

	var results []vertex.SearchResult
	fromCache := false
	seen := make(map[string]struct{})

	// Gold path: the cache-backed Vertex-pagerank provider (shared with the
	// GraphQL profileSearch resolver). It serves ranked results from the ClickHouse
	// cache and refreshes the live DVM asynchronously, so a DVM failure (notably
	// exhausted credits) never fails the request — we fall through to the
	// locally-indexed profiles below. Live DVM ranking is a quality boost, not a
	// hard dependency.
	vertexCount := 0
	if h.profileSearcher != nil {
		vertexRows, vertexFromCache, err := h.profileSearcher.Search(r.Context(), vertex.SearchArgs{
			Query: query,
			Limit: limit,
			Sort:  sortKey,
		})
		if err != nil {
			slog.Warn("vertex profile search failed; falling back to local index",
				"query_len", len(query), "sort", sortKey, "error", err)
		} else {
			fromCache = vertexFromCache
			for _, row := range vertexRows {
				pubkey, ok := vertex.NormalizePubkey(row.PubKey)
				if !ok {
					continue
				}
				if _, dup := seen[pubkey]; dup {
					continue
				}
				row.PubKey = pubkey
				if row.Npub == "" {
					row.Npub = vertex.Npub(pubkey)
				}
				results = append(results, row)
				seen[pubkey] = struct{}{}
				if len(results) >= limit {
					break
				}
			}
			vertexCount = len(results)
		}
	}

	// Fill the remaining slots from the locally-indexed profiles. This is the
	// credit-free path: it serves any indexed profile (including brand-new
	// queries) without touching the live DVM.
	if len(results) < limit {
		localRows, err := h.store.SearchProfiles(r.Context(), query, uint64(limit-len(results)))
		if err != nil {
			writeError(w, err)
			return
		}
		for _, local := range localRows {
			pubkey, ok := vertex.NormalizePubkey(local.Profile.PubKey)
			if !ok {
				continue
			}
			if _, dup := seen[pubkey]; dup {
				continue
			}
			rank := local.Rank
			score := local.Score
			results = append(results, vertex.SearchResult{
				PubKey: pubkey,
				Npub:   vertex.Npub(pubkey),
				Rank:   &rank,
				Score:  &score,
			})
			seen[pubkey] = struct{}{}
			if len(results) >= limit {
				break
			}
		}
	}

	// Seam observability: how the fetch→merge resolved (Vertex pagerank vs local
	// index, cache vs live). query_len only — never the raw search term.
	slog.Debug("appview.search",
		"query_len", len(query), "sort", sortKey, "limit", limit,
		"vertex_count", vertexCount, "local_count", len(results)-vertexCount,
		"from_cache", fromCache)

	envelope, err := h.rankedPubkeysEnvelope(r.Context(), results, fromCache)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, envelope)
}

// rankedPubkeysEnvelope converts a ranked provider result list into the
// providers envelope: the complete ranked pubkey list, each pubkey's locally
// indexed kind-0 event (Order anchors only those), and the provider values.
func (h *Handler) rankedPubkeysEnvelope(ctx context.Context, results []vertex.SearchResult, fromCache bool) (ProvidersEnvelope, error) {
	envelope := ProvidersEnvelope{
		Envelope:  inlineEnvelope(nil, orderByRank, nil, nil),
		Pubkeys:   make([]string, 0, len(results)),
		FromCache: fromCache,
	}
	for _, row := range results {
		envelope.Pubkeys = append(envelope.Pubkeys, row.PubKey)
		payload := map[string]any{}
		if row.Rank != nil {
			payload["rank"] = *row.Rank
		}
		if row.Score != nil {
			payload["score"] = *row.Score
		}
		envelope.setProvider(row.PubKey, h.scoreProviderName(), payload)
	}
	if err := h.appendProfileEventsTo(ctx, &envelope.Envelope, envelope.Pubkeys); err != nil {
		return ProvidersEnvelope{}, err
	}
	if counts, err := h.store.BatchFollowCounts(ctx, envelope.Pubkeys); err == nil {
		for _, pubkey := range envelope.Pubkeys {
			followAggregates(&envelope.Envelope, pubkey, counts[pubkey], 0)
		}
	}
	eventByPubkey := make(map[string]string, len(envelope.Events))
	for _, event := range envelope.Events {
		if event.Kind == 0 {
			eventByPubkey[event.PubKey] = event.ID
		}
	}
	for _, pubkey := range envelope.Pubkeys {
		if id, ok := eventByPubkey[pubkey]; ok {
			envelope.Order = append(envelope.Order, id)
		}
	}
	return envelope, nil
}

func (h *Handler) recommended(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/recommended only", http.StatusMethodNotAllowed)
		return
	}
	if h.vertex == nil {
		http.Error(w, "Vertex DVM proxy not configured", http.StatusServiceUnavailable)
		return
	}
	source := strings.TrimSpace(r.URL.Query().Get("source"))
	limit := intParam(r, "limit", 5)
	sortKey := r.URL.Query().Get("sort")
	if sortKey == "" {
		sortKey = "globalPagerank"
	}
	results, fromCache, err := h.vertex.Recommended(r.Context(), vertex.RecommendedArgs{
		Source: source,
		Limit:  limit,
		Sort:   sortKey,
	})
	if err != nil {
		writeVertexError(w, err)
		return
	}
	envelope, err := h.rankedPubkeysEnvelope(r.Context(), results, fromCache)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, envelope)
}

func (h *Handler) eventsByID(ctx context.Context, ids []string) (map[string]chstore.EventView, error) {
	ids = normalizeHexIDs(ids)
	out := make(map[string]chstore.EventView, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	events, err := h.store.QueryEvents(ctx, chstore.EventQueryInput{IDs: ids, Limit: uint64(len(ids))})
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		out[event.ID] = event
	}
	missing := missingIDs(ids, out)
	if len(missing) > 0 && h.tryBackfillEvents(ctx, missing) {
		events, err = h.store.QueryEvents(ctx, chstore.EventQueryInput{IDs: missing, Limit: uint64(len(missing))})
		if err != nil {
			return nil, err
		}
		for _, event := range events {
			out[event.ID] = event
		}
	}
	return out, nil
}

func (h *Handler) profileInfos(ctx context.Context, pubkeys []string) (map[string]ProfileInfo, error) {
	pubkeys = normalizePubkeys(pubkeys)
	rows, err := h.store.LatestProfiles(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	if missing := missingProfiles(pubkeys, rows); len(missing) > 0 && h.tryBackfillProfiles(ctx, missing) {
		refreshed, err := h.store.LatestProfiles(ctx, missing)
		if err != nil {
			return nil, err
		}
		for pubkey, row := range refreshed {
			rows[pubkey] = row
		}
	}
	out := make(map[string]ProfileInfo, len(rows))
	for pubkey, row := range rows {
		name := row.DisplayName
		if name == "" {
			name = row.Name
		}
		if name == "" && row.Picture == "" {
			continue
		}
		out[pubkey] = ProfileInfo{Name: name, Picture: row.Picture}
	}
	return out, nil
}

func (h *Handler) rootEvents(ctx context.Context, events []chstore.EventView) (map[string]resolvedRootEvent, error) {
	candidates := make(map[string]string, len(events))
	paths := make(map[string]map[string]struct{}, len(events))
	pending := make([]string, 0, len(events))
	seenPending := map[string]struct{}{}

	for _, event := range events {
		rootID := rootEventID(event)
		if rootID == "" {
			continue
		}
		candidates[event.ID] = rootID
		paths[event.ID] = map[string]struct{}{rootID: {}}
		pending = appendUniqueString(pending, seenPending, rootID)
	}
	if len(candidates) == 0 {
		return map[string]resolvedRootEvent{}, nil
	}

	fetched := make(map[string]chstore.EventView, len(pending))
	for depth := 0; depth < 8 && len(pending) > 0; depth++ {
		batch := pending
		pending = nil
		seenPending = map[string]struct{}{}

		toFetch := make([]string, 0, len(batch))
		seenFetch := map[string]struct{}{}
		for _, id := range batch {
			if _, ok := fetched[id]; ok {
				continue
			}
			toFetch = appendUniqueString(toFetch, seenFetch, id)
		}
		if len(toFetch) == 0 {
			break
		}

		eventsByID, err := h.eventsByID(ctx, toFetch)
		if err != nil {
			return nil, err
		}
		for id, event := range eventsByID {
			fetched[id] = event
		}

		for _, source := range events {
			currentID := candidates[source.ID]
			if currentID == "" {
				continue
			}
			current, ok := fetched[currentID]
			if !ok {
				continue
			}
			nextID := rootEventID(current)
			if nextID == "" || nextID == source.ID || nextID == currentID {
				continue
			}
			path := paths[source.ID]
			if _, seen := path[nextID]; seen {
				continue
			}
			path[nextID] = struct{}{}
			candidates[source.ID] = nextID
			pending = appendUniqueString(pending, seenPending, nextID)
		}
	}

	out := make(map[string]resolvedRootEvent, len(candidates))
	for sourceID, rootID := range candidates {
		root := resolvedRootEvent{ID: rootID}
		if event, ok := fetched[rootID]; ok {
			root.Event = event
			root.HasEvent = true
		}
		out[sourceID] = root
	}
	return out, nil
}

func appendUniqueString(values []string, seen map[string]struct{}, value string) []string {
	if value == "" {
		return values
	}
	if _, ok := seen[value]; ok {
		return values
	}
	seen[value] = struct{}{}
	return append(values, value)
}

func (h *Handler) shouldBackfillThread(events []chstore.EventView, limit int) bool {
	if h.threadBackfiller == nil {
		return false
	}
	if limit <= 0 || limit > 2000 {
		limit = 1000
	}
	return len(events)+1 < limit
}

func (h *Handler) tryBackfillUserFeed(ctx context.Context, pubkey string, limit uint64) bool {
	if h.userBackfiller == nil {
		return false
	}
	if hydrator, ok := h.userBackfiller.(UserFeedHydrator); ok {
		completed, err := hydrator.HydrateUserFeed(ctx, pubkey, limit)
		if err != nil {
			slog.Warn("user feed hydration failed", "pubkey", pubkey, "error", err)
			return false
		}
		return completed
	}
	if err := h.userBackfiller.BackfillUserFeed(ctx, pubkey, limit); err != nil {
		slog.Warn("user feed backfill failed", "pubkey", pubkey, "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillUserFeeds(ctx context.Context, pubkeys []string, limit uint64) bool {
	if h.userBackfiller == nil || len(pubkeys) == 0 {
		return false
	}
	if hydrator, ok := h.userBackfiller.(UserFeedsHydrator); ok {
		completed, err := hydrator.HydrateUserFeeds(ctx, pubkeys, limit)
		if err != nil {
			slog.Warn("user feeds hydration failed", "pubkeys", len(pubkeys), "error", err)
			return false
		}
		return completed
	}
	completed := true
	for _, pubkey := range pubkeys {
		if !h.tryBackfillUserFeed(ctx, pubkey, limit) {
			completed = false
		}
	}
	return completed
}

func (h *Handler) tryBackfillEvents(ctx context.Context, ids []string) bool {
	if h.eventBackfiller == nil || len(ids) == 0 {
		return false
	}
	if hydrator, ok := h.eventBackfiller.(EventHydrator); ok {
		completed, err := hydrator.HydrateEvents(ctx, ids)
		if err != nil {
			slog.Warn("event hydration failed", "ids", len(ids), "error", err)
			return false
		}
		return completed
	}
	if err := h.eventBackfiller.BackfillEvents(ctx, ids); err != nil {
		slog.Warn("event backfill failed", "ids", len(ids), "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillProfiles(ctx context.Context, pubkeys []string) bool {
	if h.profileBackfiller == nil || len(pubkeys) == 0 {
		return false
	}
	if hydrator, ok := h.profileBackfiller.(ProfileHydrator); ok {
		completed, err := hydrator.HydrateProfiles(ctx, pubkeys)
		if err != nil {
			slog.Warn("profile hydration failed", "pubkeys", len(pubkeys), "error", err)
			return false
		}
		return completed
	}
	if err := h.profileBackfiller.BackfillProfiles(ctx, pubkeys); err != nil {
		slog.Warn("profile backfill failed", "pubkeys", len(pubkeys), "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillEngagement(ctx context.Context, ids []string) bool {
	if h.engagementBackfiller == nil || len(ids) == 0 {
		return false
	}
	if hydrator, ok := h.engagementBackfiller.(EngagementHydrator); ok {
		completed, err := hydrator.HydrateEngagement(ctx, ids)
		if err != nil {
			slog.Warn("engagement hydration failed", "ids", len(ids), "error", err)
			return false
		}
		return completed
	}
	if err := h.engagementBackfiller.BackfillEngagement(ctx, ids); err != nil {
		slog.Warn("engagement backfill failed", "ids", len(ids), "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillThread(ctx context.Context, id string, limit int) bool {
	if h.threadBackfiller == nil {
		return false
	}
	if hydrator, ok := h.threadBackfiller.(ThreadHydrator); ok {
		completed, err := hydrator.HydrateThread(ctx, id, limit)
		if err != nil {
			slog.Warn("thread hydration failed", "id", id, "error", err)
			return false
		}
		return completed
	}
	if err := h.threadBackfiller.BackfillThread(ctx, id, limit); err != nil {
		slog.Warn("thread backfill failed", "id", id, "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillFollows(ctx context.Context, pubkey string) bool {
	if h.followBackfiller == nil {
		return false
	}
	if hydrator, ok := h.followBackfiller.(FollowHydrator); ok {
		completed, err := hydrator.HydrateFollows(ctx, pubkey)
		if err != nil {
			slog.Warn("follow graph hydration failed", "pubkey", pubkey, "error", err)
			return false
		}
		return completed
	}
	if err := h.followBackfiller.BackfillFollows(ctx, pubkey); err != nil {
		slog.Warn("follow graph backfill failed", "pubkey", pubkey, "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillDMEnvelopes(ctx context.Context, pubkey string, kinds []int, until int64, limit uint64) bool {
	if h.dmEnvelopeBackfiller == nil || pubkey == "" {
		return false
	}
	if hydrator, ok := h.dmEnvelopeBackfiller.(DMEnvelopeHydrator); ok {
		completed, err := hydrator.HydrateDMEnvelopes(ctx, pubkey, kinds, until, limit)
		if err != nil {
			slog.Warn("dm envelope hydration failed", "pubkey", pubkey, "error", err)
			return false
		}
		return completed
	}
	if err := h.dmEnvelopeBackfiller.BackfillDMEnvelopes(ctx, pubkey, kinds, until, limit); err != nil {
		slog.Warn("dm envelope backfill failed", "pubkey", pubkey, "error", err)
		return false
	}
	return true
}

func (h *Handler) tryBackfillEnrichment(ctx context.Context, ids []string, pubkeys []string) bool {
	type result struct {
		completed bool
	}
	tasks := 0
	results := make(chan result, 2)
	if h.engagementBackfiller != nil && len(ids) > 0 {
		tasks++
		go func() {
			defer safego.Recover("appview.backfill")
			results <- result{completed: h.tryBackfillEngagement(ctx, ids)}
		}()
	}
	if h.profileBackfiller != nil && len(pubkeys) > 0 {
		tasks++
		go func() {
			defer safego.Recover("appview.backfill")
			results <- result{completed: h.tryBackfillProfiles(ctx, pubkeys)}
		}()
	}
	completed := true
	for i := 0; i < tasks; i++ {
		if !(<-results).completed {
			completed = false
		}
	}
	return completed
}

func (h *Handler) tryBackfillProfileSummary(ctx context.Context, pubkey string) bool {
	type result struct {
		completed bool
	}
	tasks := 0
	results := make(chan result, 2)
	if h.profileBackfiller != nil {
		tasks++
		go func() {
			defer safego.Recover("appview.backfill")
			results <- result{completed: h.tryBackfillProfiles(ctx, []string{pubkey})}
		}()
	}
	if h.followBackfiller != nil {
		tasks++
		go func() {
			defer safego.Recover("appview.backfill")
			results <- result{completed: h.tryBackfillFollows(ctx, pubkey)}
		}()
	}
	completed := true
	for i := 0; i < tasks; i++ {
		if !(<-results).completed {
			completed = false
		}
	}
	return completed
}

func missingIDs(ids []string, found map[string]chstore.EventView) []string {
	out := make([]string, 0)
	for _, id := range ids {
		if _, ok := found[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

func missingProfiles(pubkeys []string, rows map[string]chstore.ProfileRow) []string {
	out := make([]string, 0)
	for _, pubkey := range pubkeys {
		if row, ok := rows[pubkey]; !ok || row.EventID == "" {
			out = append(out, pubkey)
		}
	}
	return out
}

func takeStrings(values []string, limit int) []string {
	if limit <= 0 || len(values) <= limit {
		return values
	}
	return values[:limit]
}

type profileFieldsResult struct {
	fields   ProfileFields
	conflict bool
}

func (h *Handler) vertexProfile(ctx context.Context, pubkey string, followers uint64) (vertex.ProfileResult, bool) {
	provider := vertex.NewScoreProvider(h.store, h.vertex, h.vertexProfileMinFollowers)
	profile, ok, err := provider.AuthorProfileWithFollowers(ctx, pubkey, followers)
	if err != nil {
		slog.Warn("vertex profile cache read failed", "pubkey", pubkey, "error", err)
		return vertex.ProfileResult{}, false
	}
	return profile, ok
}

func (h *Handler) localProfileCreatedAt(ctx context.Context, pubkey string, localProfile chstore.ProfileRow) (*int64, error) {
	firstEventAt, err := h.store.ProfileFirstEventCreatedAt(ctx, pubkey)
	if err != nil {
		return nil, err
	}
	if firstEventAt != nil {
		return unixPtr(*firstEventAt), nil
	}
	return unixPtr(localProfile.CreatedAt), nil
}

func (h *Handler) profileFields(ctx context.Context, pubkey string, row chstore.ProfileRow) profileFieldsResult {
	fields := ProfileFields{
		Name:        stringPtr(row.Name),
		DisplayName: stringPtr(row.DisplayName),
		Picture:     stringPtr(row.Picture),
		Image:       stringPtr(row.Picture),
		Banner:      stringPtr(row.Banner),
		About:       stringPtr(row.About),
		Website:     stringPtr(row.Website),
		LUD16:       stringPtr(row.LUD16),
		LUD06:       stringPtr(row.LUD06),
	}
	if row.NIP05 == "" {
		return profileFieldsResult{fields: fields}
	}
	if h.nip05Validator == nil || !h.nip05Validator.enabled {
		fields.NIP05 = stringPtr(row.NIP05)
		return profileFieldsResult{fields: fields}
	}
	status := h.nip05Validator.validate(ctx, row.NIP05, pubkey)
	fields.NIP05Valid = &status.valid
	if status.conflict {
		return profileFieldsResult{fields: fields, conflict: true}
	}
	fields.NIP05 = stringPtr(row.NIP05)
	return profileFieldsResult{fields: fields}
}

func eventJSON(event chstore.EventView) FeedEvent {
	return FeedEvent{
		ID:        event.ID,
		Kind:      event.Kind,
		PubKey:    event.PubKey,
		Content:   event.Content,
		Tags:      event.Tags,
		CreatedAt: event.CreatedAt.Unix(),
	}
}

func eventsJSON(events []chstore.EventView) []FeedEvent {
	out := make([]FeedEvent, 0, len(events))
	for _, event := range events {
		out = append(out, eventJSON(event))
	}
	return out
}

func firstEventTag(event chstore.EventView) string {
	for _, tag := range event.Tags {
		if len(tag) >= 2 && tag[0] == "e" && len(tag[1]) == 64 {
			return tag[1]
		}
	}
	return ""
}

func rootEventID(event chstore.EventView) string {
	rootID := markedEventTag(event, "root")
	if rootID == "" {
		rootID = firstPositionalEventTag(event)
	}
	if rootID == "" || rootID == event.ID {
		return ""
	}
	return rootID
}

func markedEventTag(event chstore.EventView, marker string) string {
	for _, tag := range event.Tags {
		if len(tag) < 4 || tag[0] != "e" || tag[3] != marker {
			continue
		}
		if id := cleanEventID(tag[1]); id != "" {
			return id
		}
	}
	return ""
}

func firstPositionalEventTag(event chstore.EventView) string {
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "e" {
			continue
		}
		marker := ""
		if len(tag) >= 4 {
			marker = tag[3]
		}
		if marker == "mention" {
			continue
		}
		if marker != "" && marker != "root" && marker != "reply" {
			continue
		}
		if id := cleanEventID(tag[1]); id != "" {
			return id
		}
	}
	return ""
}

func quotedEventIDs(event chstore.EventView) []string {
	matches := nostrEventURI.FindAllString(event.Content, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]struct{}{}
	for _, match := range matches {
		id, err := normalizeEventID(strings.TrimPrefix(match, "nostr:"))
		if err != nil {
			continue
		}
		out = appendUniqueString(out, seen, id)
	}
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "q" {
			continue
		}
		out = appendUniqueString(out, seen, cleanEventID(tag[1]))
	}
	return out
}

func cleanEventID(value string) string {
	id := strings.ToLower(strings.TrimSpace(value))
	if nostr.IsValid32ByteHex(id) {
		return id
	}
	return ""
}

func normalizePubkey(input string) (string, error) {
	input = strings.TrimSpace(input)
	if nostr.IsValid32ByteHex(input) {
		return input, nil
	}
	prefix, value, err := nip19.Decode(input)
	if err != nil {
		return "", err
	}
	if prefix == "npub" {
		return value.(string), nil
	}
	return "", fmt.Errorf("unsupported pubkey prefix %q", prefix)
}

// touchViewer records a resolved viewer pubkey via the WithViewerTouch seam.
func (h *Handler) touchViewer(pubkey string) {
	if h.viewerTouch != nil && pubkey != "" {
		h.viewerTouch(pubkey)
	}
}

func (h *Handler) viewerPubkeyOr(input string) (string, error) {
	if strings.TrimSpace(input) == "" && h.viewerPubkey != "" {
		return h.viewerPubkey, nil
	}
	return normalizePubkey(input)
}

// queryViewerParam reads the viewer/subject pubkey from the query string,
// accepting every spelling shipped clients use. sovran-app (already in prod)
// sends `pubkey` on some routes and `viewer` on others, so every single-viewer
// route accepts both — plus the first value of the plural `pubkeys` — without the
// client having to change.
func queryViewerParam(r *http.Request) string {
	q := r.URL.Query()
	for _, name := range []string{"pubkey", "viewer"} {
		if v := strings.TrimSpace(q.Get(name)); v != "" {
			return v
		}
	}
	if list := csv(q.Get("pubkeys")); len(list) > 0 {
		return list[0]
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func normalizeEventID(input string) (string, error) {
	input = strings.TrimSpace(input)
	if nostr.IsValid32ByteHex(input) {
		return input, nil
	}
	prefix, value, err := nip19.Decode(input)
	if err != nil {
		return "", err
	}
	switch prefix {
	case "note":
		return value.(string), nil
	case "nevent":
		return value.(nostr.EventPointer).ID, nil
	default:
		return "", fmt.Errorf("unsupported event prefix %q", prefix)
	}
}

func normalizePubkeys(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		pubkey, err := normalizePubkey(value)
		if err == nil {
			out = append(out, pubkey)
		}
	}
	return out
}

func normalizeHexIDs(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		id, err := normalizeEventID(value)
		if err != nil {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func csv(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func intParam(r *http.Request, key string, fallback int) int {
	value := strings.TrimSpace(r.URL.Query().Get(key))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		status = http.StatusGatewayTimeout
	}
	http.Error(w, err.Error(), status)
}

func writeVertexError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, vertex.ErrUnavailable) {
		status = http.StatusServiceUnavailable
	} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "timed out") {
		status = http.StatusGatewayTimeout
	}
	http.Error(w, err.Error(), status)
}

func stringPtr(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func unixPtr(value time.Time) *int64 {
	if value.IsZero() {
		return nil
	}
	seconds := value.Unix()
	return &seconds
}

func limitClamp(value int, fallback int) int {
	if value <= 0 {
		return fallback
	}
	if value > 100 {
		return 100
	}
	return value
}
