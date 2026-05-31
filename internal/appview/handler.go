package appview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/vertex"
)

type Store interface {
	FollowsFeed(context.Context, []string, int64, uint64, uint64) ([]chstore.EventView, error)
	TrendingFeed(context.Context, time.Time, uint64) ([]chstore.EventView, error)
	QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error)
	NoteStats(context.Context, []string) (map[string]chstore.NoteStats, error)
	LatestProfiles(context.Context, []string) (map[string]chstore.ProfileRow, error)
	FollowCounts(context.Context, string) (chstore.FollowCounts, error)
	ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error)
}

type Handler struct {
	store          Store
	vertex         VertexClient
	nip05Validator *nip05Validator
	rateLimiter    *rateLimiter
}

type VertexClient interface {
	Search(context.Context, vertex.SearchArgs) ([]vertex.SearchResult, bool, error)
	Recommended(context.Context, vertex.RecommendedArgs) ([]vertex.SearchResult, bool, error)
	Profile(context.Context, string) (vertex.ProfileResult, bool, error)
}

type Option func(*Handler)

func WithVertex(client VertexClient) Option {
	return func(h *Handler) {
		h.vertex = client
	}
}

func WithNIP05Validation(enabled bool) Option {
	return func(h *Handler) {
		h.nip05Validator = newNIP05Validator(enabled)
	}
}

func WithRateLimit(limit int, window time.Duration) Option {
	return func(h *Handler) {
		h.rateLimiter = newRateLimiter(limit, window)
	}
}

func New(store Store, opts ...Option) *Handler {
	h := &Handler{
		store:          store,
		nip05Validator: newNIP05Validator(true),
		rateLimiter:    newRateLimiter(120, time.Minute),
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/nostr/feed", h.withMiddleware(h.feed))
	mux.HandleFunc("/nostr/feed/user", h.withMiddleware(h.userFeed))
	mux.HandleFunc("/nostr/notes/stats", h.withMiddleware(h.noteStats))
	mux.HandleFunc("/nostr/thread", h.withMiddleware(h.thread))
	mux.HandleFunc("/nostr/follows", h.withMiddleware(h.follows))
	mux.HandleFunc("/nostr/events", h.withMiddleware(h.events))
	mux.HandleFunc("/nostr/profiles", h.withMiddleware(h.profiles))
	mux.HandleFunc("/nostr/profile", h.withMiddleware(h.profile))
	mux.HandleFunc("/nostr/search", h.withMiddleware(h.search))
	mux.HandleFunc("/nostr/recommended", h.withMiddleware(h.recommended))
}

func (h *Handler) withMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.rateLimiter.allow(r) {
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		next(w, r.WithContext(ctx))
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

type FeedItem struct {
	Type            string     `json:"type"`
	Event           *FeedEvent `json:"event,omitempty"`
	RepostEvent     *FeedEvent `json:"repostEvent,omitempty"`
	OriginalEvent   *FeedEvent `json:"originalEvent,omitempty"`
	OriginalEventID string     `json:"originalEventId,omitempty"`
}

type FeedResponse struct {
	Items            []FeedItem                   `json:"items"`
	Metrics          map[string]chstore.NoteStats `json:"metrics"`
	Profiles         map[string]ProfileInfo       `json:"profiles"`
	Quoted           map[string]FeedEvent         `json:"quoted"`
	PaginationUntil  int64                        `json:"paginationUntil"`
	PaginationOffset int                          `json:"paginationOffset"`
}

type EnrichmentResponse struct {
	Metrics  map[string]chstore.NoteStats `json:"metrics"`
	Profiles map[string]ProfileInfo       `json:"profiles"`
	Quoted   map[string]FeedEvent         `json:"quoted"`
}

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

type SearchResult struct {
	ProfileFields
	PubKey    string   `json:"pubkey"`
	Npub      string   `json:"npub"`
	Rank      *float64 `json:"rank,omitempty"`
	Score     *float64 `json:"score"`
	CreatedAt *int64   `json:"created_at,omitempty"`
}

type TopFollower struct {
	ProfileFields
	PubKey string   `json:"pubkey"`
	Npub   string   `json:"npub"`
	Rank   float64  `json:"rank"`
	Score  *float64 `json:"score"`
}

type ProfileResponse struct {
	ProfileFields
	PubKey       string        `json:"pubkey"`
	Npub         string        `json:"npub"`
	Rank         float64       `json:"rank"`
	Score        *float64      `json:"score"`
	Followers    uint64        `json:"followers"`
	Follows      uint64        `json:"follows"`
	CreatedAt    *int64        `json:"created_at"`
	Nodes        *int          `json:"nodes,omitempty"`
	TopFollowers []TopFollower `json:"topFollowers"`
	FromCache    bool          `json:"fromCache"`
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

	authors, trending := authorsFromFeedRequest(req.Spec, r)
	var events []chstore.EventView
	var err error
	if trending || len(authors) == 0 {
		events, err = h.store.TrendingFeed(r.Context(), time.Now().Add(-24*time.Hour), req.Limit)
	} else {
		events, err = h.store.FollowsFeed(r.Context(), authors, req.Until, req.Limit, req.Offset)
	}
	if err != nil {
		writeError(w, err)
		return
	}
	h.writeFeedResponse(w, r, events)
}

func authorsFromFeedRequest(spec string, r *http.Request) ([]string, bool) {
	if r.Method == http.MethodGet {
		kind := r.URL.Query().Get("kind")
		return normalizePubkeys(csv(r.URL.Query().Get("pubkeys"))), kind == "trending"
	}
	var parsed struct {
		ID      string   `json:"id"`
		PubKey  string   `json:"pubkey"`
		PubKeys []string `json:"pubkeys"`
	}
	if err := json.Unmarshal([]byte(spec), &parsed); err != nil {
		return nil, true
	}
	if len(parsed.PubKeys) > 0 {
		return normalizePubkeys(parsed.PubKeys), false
	}
	if parsed.PubKey != "" {
		if pubkey, err := normalizePubkey(parsed.PubKey); err == nil {
			return []string{pubkey}, false
		}
	}
	return nil, true
}

func (h *Handler) userFeed(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/feed/user only", http.StatusMethodNotAllowed)
		return
	}
	pubkey, err := normalizePubkey(r.URL.Query().Get("pubkey"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	events, err := h.store.FollowsFeed(
		r.Context(),
		[]string{pubkey},
		int64(intParam(r, "until", 0)),
		uint64(intParam(r, "limit", 50)),
		uint64(intParam(r, "offset", 0)),
	)
	if err != nil {
		writeError(w, err)
		return
	}
	h.writeFeedResponse(w, r, events)
}

func (h *Handler) writeFeedResponse(w http.ResponseWriter, r *http.Request, events []chstore.EventView) {
	response, err := h.feedResponse(r.Context(), events)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, response)
}

func (h *Handler) feedResponse(ctx context.Context, events []chstore.EventView) (FeedResponse, error) {
	originalIDs := make([]string, 0)
	for _, event := range events {
		if event.Kind == 6 || event.Kind == 16 {
			if id := firstEventTag(event); id != "" {
				originalIDs = append(originalIDs, id)
			}
		}
	}
	originals, err := h.eventsByID(ctx, originalIDs)
	if err != nil {
		return FeedResponse{}, err
	}

	items := make([]FeedItem, 0, len(events))
	metricIDs := make([]string, 0, len(events)+len(originals))
	pubkeys := make([]string, 0, len(events)+len(originals))
	var paginationUntil int64

	for _, event := range events {
		feedEvent := eventJSON(event)
		if paginationUntil == 0 || feedEvent.CreatedAt < paginationUntil {
			paginationUntil = feedEvent.CreatedAt
		}
		pubkeys = append(pubkeys, event.PubKey)

		if event.Kind == 6 || event.Kind == 16 {
			originalID := firstEventTag(event)
			item := FeedItem{Type: "repost", RepostEvent: &feedEvent, OriginalEventID: originalID}
			if original, ok := originals[originalID]; ok {
				originalEvent := eventJSON(original)
				item.OriginalEvent = &originalEvent
				pubkeys = append(pubkeys, original.PubKey)
			}
			if originalID != "" {
				metricIDs = append(metricIDs, originalID)
			}
			items = append(items, item)
			continue
		}

		metricIDs = append(metricIDs, event.ID)
		items = append(items, FeedItem{Type: "note", Event: &feedEvent})
	}

	metrics, err := h.store.NoteStats(ctx, metricIDs)
	if err != nil {
		return FeedResponse{}, err
	}
	profiles, err := h.profileInfos(ctx, pubkeys)
	if err != nil {
		return FeedResponse{}, err
	}

	return FeedResponse{
		Items:            items,
		Metrics:          metrics,
		Profiles:         profiles,
		Quoted:           map[string]FeedEvent{},
		PaginationUntil:  paginationUntil,
		PaginationOffset: len(items),
	}, nil
}

func (h *Handler) noteStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST /nostr/notes/stats only", http.StatusMethodNotAllowed)
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
	stats, err := h.store.NoteStats(r.Context(), ids)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, stats)
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
	root, events, err := h.store.ThreadEvents(r.Context(), id, intParam(r, "limit", 1000))
	if err != nil {
		writeError(w, err)
		return
	}
	allEvents := append([]chstore.EventView{*root}, events...)
	metricIDs := make([]string, 0, len(allEvents))
	pubkeys := make([]string, 0, len(allEvents))
	for _, event := range allEvents {
		metricIDs = append(metricIDs, event.ID)
		pubkeys = append(pubkeys, event.PubKey)
	}
	metrics, err := h.store.NoteStats(r.Context(), metricIDs)
	if err != nil {
		writeError(w, err)
		return
	}
	profiles, err := h.profileInfos(r.Context(), pubkeys)
	if err != nil {
		writeError(w, err)
		return
	}
	out := struct {
		Root     FeedEvent                    `json:"root"`
		Events   []FeedEvent                  `json:"events"`
		Metrics  map[string]chstore.NoteStats `json:"metrics"`
		Profiles map[string]ProfileInfo       `json:"profiles"`
		Quoted   map[string]FeedEvent         `json:"quoted"`
	}{
		Root:     eventJSON(*root),
		Events:   eventsJSON(events),
		Metrics:  metrics,
		Profiles: profiles,
		Quoted:   map[string]FeedEvent{},
	}
	writeJSON(w, out)
}

func (h *Handler) follows(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/follows only", http.StatusMethodNotAllowed)
		return
	}
	pubkey, err := normalizePubkey(r.URL.Query().Get("pubkey"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	counts, err := h.store.FollowCounts(r.Context(), pubkey)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, map[string]any{
		"pubkey":    pubkey,
		"follows":   counts.Follows,
		"followers": counts.Followers,
	})
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/events only", http.StatusMethodNotAllowed)
		return
	}
	eventsByID, err := h.eventsByID(r.Context(), normalizeHexIDs(csv(r.URL.Query().Get("ids"))))
	if err != nil {
		writeError(w, err)
		return
	}
	quoted := make(map[string]FeedEvent, len(eventsByID))
	metricIDs := make([]string, 0, len(eventsByID))
	pubkeys := make([]string, 0, len(eventsByID))
	for id, event := range eventsByID {
		quoted[id] = eventJSON(event)
		metricIDs = append(metricIDs, id)
		pubkeys = append(pubkeys, event.PubKey)
	}
	metrics, err := h.store.NoteStats(r.Context(), metricIDs)
	if err != nil {
		writeError(w, err)
		return
	}
	profiles, err := h.profileInfos(r.Context(), pubkeys)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, EnrichmentResponse{Metrics: metrics, Profiles: profiles, Quoted: quoted})
}

func (h *Handler) profiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/profiles only", http.StatusMethodNotAllowed)
		return
	}
	profiles, err := h.profileInfos(r.Context(), normalizePubkeys(csv(r.URL.Query().Get("pubkeys"))))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, EnrichmentResponse{
		Metrics:  map[string]chstore.NoteStats{},
		Profiles: profiles,
		Quoted:   map[string]FeedEvent{},
	})
}

func (h *Handler) profile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/profile only", http.StatusMethodNotAllowed)
		return
	}
	if h.vertex == nil {
		http.Error(w, "Vertex DVM proxy not configured", http.StatusServiceUnavailable)
		return
	}
	pubkey, err := normalizePubkey(r.URL.Query().Get("pubkey"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	dvmProfile, fromCache, err := h.vertex.Profile(r.Context(), pubkey)
	if err != nil {
		writeVertexError(w, err)
		return
	}
	profiles, err := h.store.LatestProfiles(r.Context(), []string{pubkey})
	if err != nil {
		writeError(w, err)
		return
	}
	counts, err := h.store.FollowCounts(r.Context(), pubkey)
	if err != nil {
		writeError(w, err)
		return
	}
	profile := profiles[pubkey]
	topFollowers, err := h.enrichTopFollowers(r.Context(), dvmProfile.TopFollowers)
	if err != nil {
		writeError(w, err)
		return
	}
	createdAt := unixPtr(profile.CreatedAt)
	if createdAt == nil {
		createdAt = dvmProfile.CreatedAt
	}
	writeJSON(w, ProfileResponse{
		ProfileFields: h.profileFields(r.Context(), pubkey, profile).fields,
		PubKey:        pubkey,
		Npub:          vertex.Npub(pubkey),
		Rank:          dvmProfile.Rank,
		Score:         dvmProfile.Score,
		Followers:     counts.Followers,
		Follows:       counts.Follows,
		CreatedAt:     createdAt,
		Nodes:         dvmProfile.Nodes,
		TopFollowers:  topFollowers,
		FromCache:     fromCache,
	})
}

func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/search only", http.StatusMethodNotAllowed)
		return
	}
	if h.vertex == nil {
		http.Error(w, "Vertex DVM proxy not configured", http.StatusServiceUnavailable)
		return
	}
	query := strings.TrimSpace(r.URL.Query().Get("query"))
	if len(query) < 3 {
		http.Error(w, "query must be at least 3 characters", http.StatusBadRequest)
		return
	}
	limit := intParam(r, "limit", 5)
	sortKey := r.URL.Query().Get("sort")
	results, fromCache, err := h.vertex.Search(r.Context(), vertex.SearchArgs{
		Query: query,
		Limit: limit,
		Sort:  sortKey,
	})
	if err != nil {
		writeVertexError(w, err)
		return
	}
	enriched, err := h.enrichSearchResults(r.Context(), results)
	if err != nil {
		writeError(w, err)
		return
	}
	if sortKey == "" {
		sortKey = "globalPagerank"
	}
	writeJSON(w, map[string]any{
		"query":     query,
		"limit":     limitClamp(limit, 5),
		"sort":      sortKey,
		"results":   enriched,
		"fromCache": fromCache,
	})
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
	enriched, err := h.enrichSearchResults(r.Context(), results)
	if err != nil {
		writeError(w, err)
		return
	}
	if source == "" {
		source = "default"
	}
	writeJSON(w, map[string]any{
		"source":    source,
		"limit":     limitClamp(limit, 5),
		"sort":      sortKey,
		"results":   enriched,
		"fromCache": fromCache,
	})
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
	return out, nil
}

func (h *Handler) profileInfos(ctx context.Context, pubkeys []string) (map[string]ProfileInfo, error) {
	rows, err := h.store.LatestProfiles(ctx, normalizePubkeys(pubkeys))
	if err != nil {
		return nil, err
	}
	out := make(map[string]ProfileInfo, len(rows))
	for pubkey, row := range rows {
		name := row.DisplayName
		if name == "" {
			name = row.Name
		}
		if name == "" {
			continue
		}
		out[pubkey] = ProfileInfo{Name: name, Picture: row.Picture}
	}
	return out, nil
}

type profileFieldsResult struct {
	fields   ProfileFields
	conflict bool
}

func (h *Handler) enrichSearchResults(ctx context.Context, rows []vertex.SearchResult) ([]SearchResult, error) {
	pubkeys := make([]string, 0, len(rows))
	for _, row := range rows {
		pubkeys = append(pubkeys, row.PubKey)
	}
	profiles, err := h.store.LatestProfiles(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make([]SearchResult, 0, len(rows))
	for _, row := range rows {
		fields := h.profileFields(ctx, row.PubKey, profiles[row.PubKey])
		if fields.conflict {
			continue
		}
		createdAt := unixPtr(profiles[row.PubKey].CreatedAt)
		out = append(out, SearchResult{
			ProfileFields: fields.fields,
			PubKey:        row.PubKey,
			Npub:          row.Npub,
			Rank:          row.Rank,
			Score:         row.Score,
			CreatedAt:     createdAt,
		})
	}
	return out, nil
}

func (h *Handler) enrichTopFollowers(ctx context.Context, followers []vertex.TopFollower) ([]TopFollower, error) {
	pubkeys := make([]string, 0, len(followers))
	for _, follower := range followers {
		pubkeys = append(pubkeys, follower.PubKey)
	}
	profiles, err := h.store.LatestProfiles(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make([]TopFollower, 0, len(followers))
	for _, follower := range followers {
		fields := h.profileFields(ctx, follower.PubKey, profiles[follower.PubKey])
		if fields.conflict {
			continue
		}
		out = append(out, TopFollower{
			ProfileFields: fields.fields,
			PubKey:        follower.PubKey,
			Npub:          follower.Npub,
			Rank:          follower.Rank,
			Score:         follower.Score,
		})
	}
	return out, nil
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
