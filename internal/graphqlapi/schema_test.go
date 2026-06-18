package graphqlapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/graphql-go/graphql"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/vertex"
)

const testPubkey = "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"

func testHex(ch string) string {
	return strings.Repeat(ch, 64)
}

type fakeStore struct {
	events                      [][]chstore.EventView
	eventByID                   map[string]chstore.EventView
	latestEvents                map[string][]chstore.EventView
	aggregateRows               [][]chstore.AggregateRow
	calls                       int
	aggregateCalls              int
	referenceAggregateSupported bool
	referenceAggregateRows      map[string][]chstore.AggregateRow
	eventInputs                 []chstore.EventQueryInput
	referenceInputs             []referencingEventsInput
	referenceAggregateInputs    []chstore.ReferenceAggregateInput
	latestInputs                []latestEventsInput
	rankedTargetAggregateInputs []rankedTargetAggregateInput
	aggregateInputs             []chstore.AggregateInput
	pubkeyScoreRows             map[string]chstore.PubkeyScore
	pubkeyScoreInputs           []pubkeyScoreInput
	profileRows                 map[string]chstore.ProfileRow
	profileSearchRows           []chstore.ProfileSearchRow
	profileSearchInputs         []profileSearchInput
	vertexProfileRows           map[string]vertex.ProfileResult
	derivedMetricRows           map[string]map[string]float64
	derivedMetricInputs         []derivedMetricInput
	notificationRows            []chstore.NotificationRow
	notificationInputs          []chstore.NotificationInput
	featureRankRows             []chstore.RankedFeatureRow
	featureRankInputs           []chstore.FeatureRankInput
	directReplyIDs              map[string][]string
	directReplyParents          []string
}

type latestEventsInput struct {
	pubkeys []string
	kinds   []int
	limit   uint64
}

type referencingEventsInput struct {
	input          chstore.EventQueryInput
	tag            chstore.TagFilter
	targets        []string
	limitPerTarget uint64
}

type rankedTargetAggregateInput struct {
	references chstore.AggregateInput
	target     chstore.EventQueryInput
}

type pubkeyScoreInput struct {
	source  string
	pubkeys []string
}

type profileSearchInput struct {
	query string
	limit uint64
}

type derivedMetricInput struct {
	metric   string
	eventIDs []string
}

type fakeProfileSearcher struct {
	rows      []vertex.SearchResult
	fromCache bool
	inputs    []vertex.SearchArgs
	err       error
}

func (s *fakeProfileSearcher) Search(_ context.Context, input vertex.SearchArgs) ([]vertex.SearchResult, bool, error) {
	s.inputs = append(s.inputs, input)
	if s.err != nil {
		return nil, false, s.err
	}
	return s.rows, s.fromCache, nil
}

func (s *fakeStore) EventByID(_ context.Context, id string) (*chstore.EventView, error) {
	if s.eventByID != nil {
		if event, ok := s.eventByID[id]; ok {
			return &event, nil
		}
	}
	return nil, nil
}

func (s *fakeStore) QueryEvents(_ context.Context, input chstore.EventQueryInput) ([]chstore.EventView, error) {
	s.eventInputs = append(s.eventInputs, input)
	if input.Empty {
		s.calls++
		return []chstore.EventView{}, nil
	}
	if len(s.events) == 0 {
		s.calls++
		return nil, nil
	}
	idx := s.calls
	if idx >= len(s.events) {
		idx = len(s.events) - 1
	}
	s.calls++
	out := make([]chstore.EventView, 0, len(s.events[idx]))
	for _, event := range s.events[idx] {
		if eventExcludedByInput(event, input) {
			continue
		}
		out = append(out, event)
	}
	return out, nil
}

func (s *fakeStore) QueryEventsByTagTargets(_ context.Context, input chstore.EventQueryInput, tag chstore.TagFilter, targets []string, limitPerTarget uint64) (map[string][]chstore.EventView, error) {
	s.referenceInputs = append(s.referenceInputs, referencingEventsInput{
		input:          input,
		tag:            tag,
		targets:        append([]string(nil), targets...),
		limitPerTarget: limitPerTarget,
	})
	out := make(map[string][]chstore.EventView, len(targets))
	targetSet := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		out[target] = nil
		targetSet[target] = struct{}{}
	}
	if input.Empty {
		s.calls++
		return out, nil
	}
	if len(s.events) == 0 {
		s.calls++
		return out, nil
	}
	idx := s.calls
	if idx >= len(s.events) {
		idx = len(s.events) - 1
	}
	s.calls++
	for _, event := range s.events[idx] {
		if eventExcludedByInput(event, input) {
			continue
		}
		if len(input.PubKeys) > 0 && !containsString(input.PubKeys, event.PubKey) {
			continue
		}
		for _, eventTag := range event.Tags {
			if len(eventTag) < 2 || eventTag[0] != tag.Key {
				continue
			}
			if _, ok := targetSet[eventTag[1]]; !ok {
				continue
			}
			out[eventTag[1]] = append(out[eventTag[1]], event)
		}
	}
	return out, nil
}

func eventExcludedByInput(event chstore.EventView, input chstore.EventQueryInput) bool {
	if containsString(input.ExcludeIDs, event.ID) || containsString(input.ExcludePubKeys, event.PubKey) {
		return true
	}
	if input.Search != "" && !strings.Contains(strings.ToLower(event.Content), strings.ToLower(input.Search)) {
		return true
	}
	return false
}

func (s *fakeStore) QueryLatestEventsByPubKeys(_ context.Context, pubkeys []string, kinds []int, limit uint64) (map[string][]chstore.EventView, error) {
	s.latestInputs = append(s.latestInputs, latestEventsInput{pubkeys: pubkeys, kinds: kinds, limit: limit})
	out := make(map[string][]chstore.EventView, len(pubkeys))
	for _, pubkey := range pubkeys {
		out[pubkey] = nil
		if s.latestEvents != nil {
			out[pubkey] = s.latestEvents[pubkey]
		}
	}
	return out, nil
}

func (s *fakeStore) AggregateEvents(_ context.Context, input chstore.AggregateInput) ([]chstore.AggregateRow, error) {
	s.aggregateInputs = append(s.aggregateInputs, input)
	if input.Empty {
		s.aggregateCalls++
		return []chstore.AggregateRow{}, nil
	}
	if len(s.aggregateRows) == 0 {
		s.aggregateCalls++
		return nil, nil
	}
	idx := s.aggregateCalls
	if idx >= len(s.aggregateRows) {
		idx = len(s.aggregateRows) - 1
	}
	s.aggregateCalls++
	return s.aggregateRows[idx], nil
}

func (s *fakeStore) AggregateEventReferencesToTargets(_ context.Context, references chstore.AggregateInput, target chstore.EventQueryInput) ([]chstore.AggregateRow, error) {
	s.rankedTargetAggregateInputs = append(s.rankedTargetAggregateInputs, rankedTargetAggregateInput{
		references: references,
		target:     target,
	})
	if references.Empty || target.Empty {
		s.aggregateCalls++
		return []chstore.AggregateRow{}, nil
	}
	if len(s.aggregateRows) == 0 {
		s.aggregateCalls++
		return nil, nil
	}
	idx := s.aggregateCalls
	if idx >= len(s.aggregateRows) {
		idx = len(s.aggregateRows) - 1
	}
	s.aggregateCalls++
	return s.aggregateRows[idx], nil
}

func (s *fakeStore) AggregateEventsByTagTargets(_ context.Context, input chstore.ReferenceAggregateInput) (map[string][]chstore.AggregateRow, bool, error) {
	s.referenceAggregateInputs = append(s.referenceAggregateInputs, input)
	if !s.referenceAggregateSupported {
		return nil, false, nil
	}
	out := make(map[string][]chstore.AggregateRow, len(input.Targets))
	for _, target := range input.Targets {
		out[target] = nil
		if s.referenceAggregateRows != nil {
			out[target] = s.referenceAggregateRows[target]
		}
	}
	return out, true, nil
}

func (s *fakeStore) LatestProfiles(_ context.Context, pubkeys []string) (map[string]chstore.ProfileRow, error) {
	out := make(map[string]chstore.ProfileRow, len(pubkeys))
	for _, pubkey := range pubkeys {
		if s.profileRows == nil {
			continue
		}
		if row, ok := s.profileRows[pubkey]; ok {
			out[pubkey] = row
		}
	}
	return out, nil
}

func (s *fakeStore) SearchProfiles(_ context.Context, query string, limit uint64) ([]chstore.ProfileSearchRow, error) {
	s.profileSearchInputs = append(s.profileSearchInputs, profileSearchInput{query: query, limit: limit})
	return s.profileSearchRows, nil
}

func (s *fakeStore) BatchFollowCounts(_ context.Context, pubkeys []string) (map[string]chstore.FollowCounts, error) {
	out := make(map[string]chstore.FollowCounts, len(pubkeys))
	for _, pubkey := range pubkeys {
		out[pubkey] = chstore.FollowCounts{}
	}
	return out, nil
}

func (s *fakeStore) FollowEdges(_ context.Context, _ string, candidates []string) (map[string]chstore.FollowEdge, error) {
	out := make(map[string]chstore.FollowEdge, len(candidates))
	for _, candidate := range candidates {
		out[candidate] = chstore.FollowEdge{}
	}
	return out, nil
}

func (s *fakeStore) CachedVertexProfiles(_ context.Context, pubkeys []string) (map[string]vertex.ProfileResult, error) {
	out := make(map[string]vertex.ProfileResult, len(pubkeys))
	for _, pubkey := range pubkeys {
		if s.vertexProfileRows == nil {
			continue
		}
		if row, ok := s.vertexProfileRows[pubkey]; ok {
			out[pubkey] = row
		}
	}
	return out, nil
}

func (s *fakeStore) PubkeyScores(_ context.Context, source string, pubkeys []string) (map[string]chstore.PubkeyScore, error) {
	s.pubkeyScoreInputs = append(s.pubkeyScoreInputs, pubkeyScoreInput{
		source:  source,
		pubkeys: append([]string(nil), pubkeys...),
	})
	out := make(map[string]chstore.PubkeyScore, len(pubkeys))
	for _, pubkey := range pubkeys {
		if s.pubkeyScoreRows == nil {
			continue
		}
		if row, ok := s.pubkeyScoreRows[pubkey]; ok {
			out[pubkey] = row
		}
	}
	return out, nil
}

func (s *fakeStore) DerivedMetricValues(_ context.Context, metric string, eventIDs []string) (map[string]float64, error) {
	s.derivedMetricInputs = append(s.derivedMetricInputs, derivedMetricInput{
		metric:   metric,
		eventIDs: append([]string(nil), eventIDs...),
	})
	out := make(map[string]float64, len(eventIDs))
	if s.derivedMetricRows == nil {
		return out, nil
	}
	values := s.derivedMetricRows[metric]
	for _, eventID := range eventIDs {
		if value, ok := values[eventID]; ok {
			out[eventID] = value
		}
	}
	return out, nil
}

func (s *fakeStore) Notifications(_ context.Context, input chstore.NotificationInput) ([]chstore.NotificationRow, error) {
	s.notificationInputs = append(s.notificationInputs, input)
	return s.notificationRows, nil
}

func (s *fakeStore) ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error) {
	return nil, nil, nil
}

func (s *fakeStore) RankedEventsByFeatures(_ context.Context, input chstore.FeatureRankInput) ([]chstore.RankedFeatureRow, error) {
	s.featureRankInputs = append(s.featureRankInputs, input)
	return s.featureRankRows, nil
}

func (s *fakeStore) DirectReplyIDs(_ context.Context, parentID string) ([]string, error) {
	s.directReplyParents = append(s.directReplyParents, parentID)
	return s.directReplyIDs[parentID], nil
}

// forYouTerms mirrors the nagg-ts For-You recipe terms (rank.ts engagementRankTerms
// + vertexAuthorScoreTerm + recencyTerm + contributionQualityTerm).
func forYouTerms() []weightedRankTerm {
	// Engagement terms carry the vertex pubkeyScore gate the For-You recipe sets,
	// so they map to the vertex-real feature columns.
	gated := chstore.EventQueryInput{PubkeyScore: chstore.PubkeyScoreFilter{Source: "vertex"}}
	return []weightedRankTerm{
		// The top-level candidate metric: distinct engagers, identity transform.
		{Kind: weightedRankTermReferences, References: gated, Metric: genericMetric{Name: "actors"}, Weight: 1},
		{Kind: weightedRankTermReferences, References: gated, Metric: genericMetric{Name: "likes"}, Weight: 3, Transform: "LOG1P"},
		{Kind: weightedRankTermReferences, References: gated, Metric: genericMetric{Name: "replies"}, Weight: 2.5, Transform: "LOG1P"},
		{Kind: weightedRankTermReferences, References: gated, Metric: genericMetric{Name: "reposts"}, Weight: 2, Transform: "LOG1P"},
		{Kind: weightedRankTermReferences, References: gated, Metric: genericMetric{Name: "zapSats"}, Weight: 1.5, Transform: "LOG1P"},
		{Kind: weightedRankTermPubkeyScore, PubkeyScore: pubkeyScoreRankTerm{Source: "vertex", Target: "AUTHOR", MinFollowers: 500}, Weight: 0.25},
		{Kind: weightedRankTermCandidateField, CandidateField: "CREATED_AT", Transform: "RECENCY_HALFLIFE", HalfLifeSeconds: 86400, Weight: 1.2},
		{Kind: weightedRankTermDerivedMetric, DerivedMetric: "contribution_quality", Weight: 3},
	}
}

func TestFeatureWeightsFromTerms_RecognizesForYou(t *testing.T) {
	w, halfLife, minFollowers, ok := featureWeightsFromTerms(forYouTerms())
	if !ok {
		t.Fatal("For-You terms must be recognized by the feature mapper")
	}
	if w.Likes != 3 || w.Replies != 2.5 || w.Reposts != 2 || w.ZapSats != 1.5 {
		t.Errorf("engagement weights mismapped: %+v", w)
	}
	if w.Actors != 1 {
		t.Errorf("actors weight = %v, want 1 (the identity candidate metric must map, not bail)", w.Actors)
	}
	if w.AuthorVertexScore != 0.25 || w.Recency != 1.2 || w.ContributionQuality != 3 {
		t.Errorf("scalar weights mismapped: %+v", w)
	}
	if halfLife != 86400 {
		t.Errorf("halfLife = %v, want 86400", halfLife)
	}
	if minFollowers != 500 {
		t.Errorf("minFollowers = %d, want 500", minFollowers)
	}
}

func TestFeatureWeightsFromTerms_BailsOnUnrecognized(t *testing.T) {
	gated := chstore.EventQueryInput{PubkeyScore: chstore.PubkeyScoreFilter{Source: "vertex"}}
	cases := map[string][]weightedRankTerm{
		"empty":                nil,
		"unknown engagement":   {{Kind: weightedRankTermReferences, References: gated, Metric: genericMetric{Name: "bookmarks"}, Transform: "LOG1P"}},
		"non-log1p engagement": {{Kind: weightedRankTermReferences, References: gated, Metric: genericMetric{Name: "likes"}, Transform: "IDENTITY"}},
		// Ungated engagement (counts ALL engagers, e.g. trending) has no real_* column.
		"ungated engagement":   {{Kind: weightedRankTermReferences, Metric: genericMetric{Name: "likes"}, Transform: "LOG1P"}},
		"non-vertex pubkey":    {{Kind: weightedRankTermPubkeyScore, PubkeyScore: pubkeyScoreRankTerm{Source: "other"}}},
		"non-zero fallback":    {{Kind: weightedRankTermPubkeyScore, PubkeyScore: pubkeyScoreRankTerm{Source: "vertex", Fallback: 0.1}}},
		"non-quality derived":  {{Kind: weightedRankTermDerivedMetric, DerivedMetric: "spamminess"}},
	}
	for name, terms := range cases {
		if _, _, _, ok := featureWeightsFromTerms(terms); ok {
			t.Errorf("%s: expected ok=false (fall back to live aggregation)", name)
		}
	}
}

func TestReverseReferenceQuery_DirectRepliesUsesEdgeTable(t *testing.T) {
	parent := chstore.EventView{ID: "parent000000000000000000000000000000000000000000000000000000000a"}
	store := &fakeStore{
		directReplyIDs: map[string][]string{
			parent.ID: {"childA", "childB"},
		},
	}
	r := &resolver{store: store}
	input, err := r.reverseReferenceQuery(context.Background(), parent, map[string]any{
		"via":   map[string]any{"key": "e", "directReplies": true},
		"limit": 50,
	})
	if err != nil {
		t.Fatalf("reverseReferenceQuery error: %v", err)
	}
	if len(store.directReplyParents) != 1 || store.directReplyParents[0] != parent.ID {
		t.Fatalf("DirectReplyIDs not queried for the parent; got %v", store.directReplyParents)
	}
	if len(input.Tags) != 0 {
		t.Errorf("direct-reply query must not use the broad e-tag filter; got tags %+v", input.Tags)
	}
	if len(input.IDs) != 2 || input.IDs[0] != "childA" {
		t.Errorf("expected direct child ids [childA childB], got %v", input.IDs)
	}
}

func TestReverseReferenceQuery_DirectRepliesEmptyShortCircuits(t *testing.T) {
	parent := chstore.EventView{ID: "parent000000000000000000000000000000000000000000000000000000000b"}
	store := &fakeStore{directReplyIDs: map[string][]string{}}
	r := &resolver{store: store}
	input, err := r.reverseReferenceQuery(context.Background(), parent, map[string]any{
		"via": map[string]any{"key": "e", "directReplies": true},
	})
	if err != nil {
		t.Fatalf("reverseReferenceQuery error: %v", err)
	}
	if !input.Empty {
		t.Error("a post with no direct replies must short-circuit to an empty result, not a broad e-tag scan")
	}
}

func TestRankedEvents_RoutesThroughFeatureScan(t *testing.T) {
	store := &fakeStore{
		featureRankRows: []chstore.RankedFeatureRow{
			{EventID: "a", PubKey: "p1", Score: 10},
			{EventID: "b", PubKey: "p2", Score: 5},
		},
		events: [][]chstore.EventView{{
			{ID: "a", PubKey: "p1", Kind: 1},
			{ID: "b", PubKey: "p2", Kind: 1},
		}},
	}
	r := &resolver{store: store, basePool: newBasePoolCache(basePoolTTL, time.Now)}
	views, err := r.rankedEventViews(context.Background(), rankedEventsInput{
		WeightedTerms: forYouTerms(),
		Target:        chstore.EventQueryInput{Kinds: []int{1, 1111}},
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("rankedEventViews error: %v", err)
	}
	if len(store.featureRankInputs) != 1 {
		t.Fatalf("expected the feature scan to be used once, got %d calls", len(store.featureRankInputs))
	}
	if got := store.featureRankInputs[0]; got.Weights.Likes != 3 || got.Limit != basePoolDepth {
		t.Errorf("feature scan input not threaded: %+v", got)
	}
	if len(views) != 2 || views[0].ID != "a" {
		t.Errorf("expected feature-ranked [a,b], got %+v", views)
	}
}

// TestRankedEvents_ScopedTargetSkipsFeatureScan guards the CRITICAL fix: a
// request that scopes candidates by pubkey (e.g. "popular posts by these authors")
// must NOT use the global feature scan — which ignores the author filter — and must
// fall through to the live path that honors it.
func TestRankedEvents_ScopedTargetSkipsFeatureScan(t *testing.T) {
	store := &fakeStore{
		featureRankRows: []chstore.RankedFeatureRow{{EventID: "x", PubKey: "px", Score: 9}},
	}
	r := &resolver{store: store, basePool: newBasePoolCache(basePoolTTL, time.Now)}
	_, err := r.rankedEventViews(context.Background(), rankedEventsInput{
		WeightedTerms: forYouTerms(),
		Target:        chstore.EventQueryInput{Kinds: []int{1, 1111}, PubKeys: []string{"author1"}},
		Limit:         10,
	})
	if err != nil {
		t.Fatalf("rankedEventViews error: %v", err)
	}
	if len(store.featureRankInputs) != 0 {
		t.Errorf("pubkey-scoped target must not hit the global feature scan; got %d calls", len(store.featureRankInputs))
	}
}

type fakeUserBackfiller struct {
	calls  int
	pubkey string
	limit  uint64
}

func (f *fakeUserBackfiller) BackfillUserFeed(_ context.Context, pubkey string, limit uint64) error {
	f.calls++
	f.pubkey = pubkey
	f.limit = limit
	return nil
}

type fakeHydratingUserBackfiller struct {
	fakeUserBackfiller
	completed bool
	hydrated  int
}

func (f *fakeHydratingUserBackfiller) HydrateUserFeed(ctx context.Context, pubkey string, limit uint64) (bool, error) {
	f.hydrated++
	return f.completed, f.BackfillUserFeed(ctx, pubkey, limit)
}

type fakeDMEnvelopeBackfiller struct {
	fakeUserBackfiller
	completed bool
	calls     int
	hydrated  int
	pubkey    string
	kinds     []int
	until     int64
	limit     uint64
}

func (f *fakeDMEnvelopeBackfiller) BackfillDMEnvelopes(_ context.Context, pubkey string, kinds []int, until int64, limit uint64) error {
	f.calls++
	f.pubkey = pubkey
	f.kinds = append([]int(nil), kinds...)
	f.until = until
	f.limit = limit
	return nil
}

func (f *fakeDMEnvelopeBackfiller) HydrateDMEnvelopes(ctx context.Context, pubkey string, kinds []int, until int64, limit uint64) (bool, error) {
	f.hydrated++
	return f.completed, f.BackfillDMEnvelopes(ctx, pubkey, kinds, until, limit)
}

type fakeRelayBackfiller struct {
	fakeUserBackfiller
	completed  bool
	relayCalls int
	labels     []string
	inputs     []chstore.EventQueryInput
}

func (f *fakeRelayBackfiller) BackfillRelayEvents(_ context.Context, input chstore.EventQueryInput, label string) error {
	f.relayCalls++
	f.inputs = append(f.inputs, input)
	f.labels = append(f.labels, label)
	return nil
}

func (f *fakeRelayBackfiller) HydrateRelayEvents(ctx context.Context, input chstore.EventQueryInput, label string) (bool, error) {
	return f.completed, f.BackfillRelayEvents(ctx, input, label)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func TestServiceInfoExposesCapabilities(t *testing.T) {
	store := &fakeStore{}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			serviceInfo {
				graphqlSchemaVersion
				appViewVersion
				capabilities
				appViews { version routes }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	info := data["serviceInfo"].(map[string]any)
	if info["graphqlSchemaVersion"] == "" || info["appViewVersion"] != "v1" {
		t.Fatalf("serviceInfo = %+v", info)
	}
	caps := info["capabilities"].([]any)
	if len(caps) == 0 {
		t.Fatalf("capabilities empty: %+v", info)
	}
}

func TestHandlerWritesCapabilityHeaders(t *testing.T) {
	store := &fakeStore{}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ serviceInfo { appViewVersion } }"}`))

	Handler(schema)(rec, req)

	if got := rec.Header().Get("X-Nagg-Capabilities"); !strings.Contains(got, "graphql.rank.pubkeyScoreTerms") {
		t.Fatalf("capability header = %q", got)
	}
	if got := rec.Header().Get("X-Nagg-App-View-Version"); got != "v1" {
		t.Fatalf("app view version header = %q", got)
	}
}

func TestProfileSearchReturnsSearchAndProfileScores(t *testing.T) {
	searchRank := 0.01
	searchScore := 42.5
	profileScore := 88.25
	followers := uint64(1234)
	follows := uint64(99)
	createdAt := int64(1_720_000_000)
	searcher := &fakeProfileSearcher{
		rows: []vertex.SearchResult{{
			PubKey: testPubkey,
			Npub:   vertex.Npub(testPubkey),
			Rank:   &searchRank,
			Score:  &searchScore,
		}},
		fromCache: true,
	}
	store := &fakeStore{
		profileRows: map[string]chstore.ProfileRow{
			testPubkey: {
				PubKey:      testPubkey,
				Name:        "jack",
				DisplayName: "Jack",
				Picture:     "https://example.test/avatar.png",
				NIP05:       "jack@example.test",
			},
		},
		vertexProfileRows: map[string]vertex.ProfileResult{
			testPubkey: {
				PubKey:    testPubkey,
				Npub:      vertex.Npub(testPubkey),
				Rank:      0.2,
				Score:     &profileScore,
				Followers: &followers,
				Follows:   &follows,
				CreatedAt: &createdAt,
			},
		},
	}
	schema, err := NewSchema(store, WithProfileSearch(searcher))
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query ProfileSearch($input: ProfileSearchInput!) {
			profileSearch(input: $input) {
				query
				limit
				sort
				fromCache
				nodes {
					pubkey
					npub
					rank
					score
					searchRank
					searchScore
					profileRank
					profileScore
					followers
					follows
					name
					displayName
					picture
					nip05
				}
			}
		}`,
		VariableValues: map[string]any{
			"input": map[string]any{"query": "jack", "limit": 1},
		},
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(searcher.inputs) != 1 {
		t.Fatalf("profile search inputs = %+v", searcher.inputs)
	}
	if searcher.inputs[0].Sort != vertex.DefaultSearchSort {
		t.Fatalf("sort = %q, want %q", searcher.inputs[0].Sort, vertex.DefaultSearchSort)
	}
	data := result.Data.(map[string]any)
	connection := data["profileSearch"].(map[string]any)
	if connection["fromCache"] != true {
		t.Fatalf("fromCache = %v", connection["fromCache"])
	}
	nodes := connection["nodes"].([]any)
	if len(nodes) != 1 {
		t.Fatalf("nodes len = %d", len(nodes))
	}
	node := nodes[0].(map[string]any)
	if node["pubkey"] != testPubkey || node["name"] != "jack" {
		t.Fatalf("node = %+v", node)
	}
	if node["searchScore"] != searchScore || node["profileScore"] != profileScore || node["score"] != profileScore {
		t.Fatalf("scores = %+v", node)
	}
	if node["followers"] != int(followers) || node["follows"] != int(follows) {
		t.Fatalf("social counts = %+v", node)
	}
}

func TestProfileSearchReturnsLocalProfileEventsWithoutVertex(t *testing.T) {
	store := &fakeStore{
		profileSearchRows: []chstore.ProfileSearchRow{{
			Profile: chstore.ProfileRow{
				PubKey:      testPubkey,
				Name:        "calle",
				DisplayName: "calle BTC",
				Picture:     "https://example.test/avatar.png",
				NIP05:       "calle@example.test",
			},
			Rank:  100,
			Score: 100,
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			profileSearch(input: {query:"calle", limit:5}) {
				fromCache
				nodes {
					pubkey
					npub
					name
					displayName
					searchScore
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.profileSearchInputs) != 1 {
		t.Fatalf("profile search inputs = %+v", store.profileSearchInputs)
	}
	if store.profileSearchInputs[0].query != "calle" || store.profileSearchInputs[0].limit != 5 {
		t.Fatalf("profile search input = %+v", store.profileSearchInputs[0])
	}
	data := result.Data.(map[string]any)
	connection := data["profileSearch"].(map[string]any)
	if connection["fromCache"] != true {
		t.Fatalf("fromCache = %v", connection["fromCache"])
	}
	nodes := connection["nodes"].([]any)
	if len(nodes) != 1 {
		t.Fatalf("nodes len = %d", len(nodes))
	}
	node := nodes[0].(map[string]any)
	if node["pubkey"] != testPubkey || node["name"] != "calle" || node["displayName"] != "calle BTC" {
		t.Fatalf("node = %+v", node)
	}
	if node["searchScore"] != float64(100) {
		t.Fatalf("searchScore = %+v", node["searchScore"])
	}
}

func TestProfileSearchPreservesVertexOrderBeforeLocalFallback(t *testing.T) {
	localPubkey := testHex("1")
	vertexScore := 99.76
	searcher := &fakeProfileSearcher{
		fromCache: true,
		rows: []vertex.SearchResult{{
			PubKey: testPubkey,
			Npub:   vertex.Npub(testPubkey),
			Score:  &vertexScore,
		}},
	}
	store := &fakeStore{
		profileRows: map[string]chstore.ProfileRow{
			testPubkey: {
				PubKey:      testPubkey,
				Name:        "calle",
				DisplayName: "calle",
				NIP05:       "calle@cashu.me",
			},
		},
		profileSearchRows: []chstore.ProfileSearchRow{{
			Profile: chstore.ProfileRow{
				PubKey:      localPubkey,
				Name:        "calle1cashume",
				DisplayName: "calle",
			},
			Rank:  100,
			Score: 100,
		}},
	}
	schema, err := NewSchema(store, WithProfileSearch(searcher))
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			profileSearch(input: {query:"calle", limit:3}) {
				nodes {
					pubkey
					name
					searchScore
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(searcher.inputs) != 1 {
		t.Fatalf("profile search inputs = %+v", searcher.inputs)
	}
	if len(store.profileSearchInputs) != 1 {
		t.Fatalf("local profile search inputs = %+v", store.profileSearchInputs)
	}
	if store.profileSearchInputs[0].limit != 2 {
		t.Fatalf("local profile search limit = %d, want 2", store.profileSearchInputs[0].limit)
	}
	data := result.Data.(map[string]any)
	connection := data["profileSearch"].(map[string]any)
	nodes := connection["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("nodes len = %d", len(nodes))
	}
	first := nodes[0].(map[string]any)
	second := nodes[1].(map[string]any)
	if first["pubkey"] != testPubkey || first["name"] != "calle" {
		t.Fatalf("first node = %+v", first)
	}
	if second["pubkey"] != localPubkey || second["name"] != "calle1cashume" {
		t.Fatalf("second node = %+v", second)
	}
}

func TestEventsQueryAcceptsContentSearch(t *testing.T) {
	store := &fakeStore{
		events: [][]chstore.EventView{{
			{
				ID:        testHex("a"),
				PubKey:    testPubkey,
				Kind:      0,
				CreatedAt: time.Unix(2, 0),
				Content:   `{"name":"calle"}`,
				Tags:      [][]string{},
				Sig:       testHex("b") + testHex("c"),
			},
			{
				ID:        testHex("d"),
				PubKey:    testHex("e"),
				Kind:      0,
				CreatedAt: time.Unix(1, 0),
				Content:   `{"name":"jack"}`,
				Tags:      [][]string{},
				Sig:       testHex("f") + testHex("a"),
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{kinds:[0], search:"calle", limit:10}) {
				nodes { id kind content }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.eventInputs) != 1 || store.eventInputs[0].Search != "calle" {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 {
		t.Fatalf("nodes len = %d", len(nodes))
	}
	if got := nodes[0].(map[string]any)["content"]; got != `{"name":"calle"}` {
		t.Fatalf("content = %v", got)
	}
}

func TestHandlerUsesConfiguredRequestTimeout(t *testing.T) {
	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query: graphql.NewObject(graphql.ObjectConfig{
			Name: "Query",
			Fields: graphql.Fields{
				"slow": &graphql.Field{
					Type: graphql.String,
					Resolve: func(p graphql.ResolveParams) (any, error) {
						select {
						case <-time.After(50 * time.Millisecond):
							return "done", nil
						case <-p.Context.Done():
							return nil, p.Context.Err()
						}
					},
				},
			},
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ slow }"}`))

	Handler(schema, WithRequestTimeout(time.Nanosecond))(rec, req)

	if body := rec.Body.String(); !strings.Contains(body, "context deadline exceeded") {
		t.Fatalf("body = %s", body)
	}
}

func TestAuthoredReplyChainRecursesThroughDirectAuthorReplies(t *testing.T) {
	rootID := testHex("1")
	replyID := testHex("2")
	secondReplyID := testHex("3")
	otherParentID := testHex("4")
	author := testPubkey
	otherPubkey := testHex("a")
	root := chstore.EventView{
		ID:        rootID,
		PubKey:    author,
		Kind:      1,
		CreatedAt: time.Unix(100, 0).UTC(),
	}
	firstReply := chstore.EventView{
		ID:        replyID,
		PubKey:    author,
		Kind:      1,
		CreatedAt: time.Unix(110, 0).UTC(),
		Tags:      [][]string{{"e", rootID, "", "reply"}},
	}
	nonAuthorReply := chstore.EventView{
		ID:        testHex("5"),
		PubKey:    otherPubkey,
		Kind:      1,
		CreatedAt: time.Unix(105, 0).UTC(),
		Tags:      [][]string{{"e", rootID, "", "reply"}},
	}
	decoyNestedReply := chstore.EventView{
		ID:        testHex("6"),
		PubKey:    author,
		Kind:      1,
		CreatedAt: time.Unix(106, 0).UTC(),
		Tags:      [][]string{{"e", rootID, "", "root"}, {"e", otherParentID, "", "reply"}},
	}
	secondReply := chstore.EventView{
		ID:        secondReplyID,
		PubKey:    author,
		Kind:      1,
		CreatedAt: time.Unix(120, 0).UTC(),
		Tags:      [][]string{{"e", rootID, "", "root"}, {"e", replyID, "", "reply"}},
	}
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{rootID: root},
		events: [][]chstore.EventView{
			{nonAuthorReply, decoyNestedReply, firstReply},
			{secondReply},
			{},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query($id: String!) {
			event(id: $id) {
				authoredReplyChain(input: { maxDepth: 8, maxBranchFanout: 32 }) {
					nodes { id pubkey }
				}
			}
		}`,
		VariableValues: map[string]any{"id": rootID},
		Context:        context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	event := data["event"].(map[string]any)
	chain := event["authoredReplyChain"].(map[string]any)
	nodes := chain["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("nodes = %+v, want 2 author replies", nodes)
	}
	if got := nodes[0].(map[string]any)["id"]; got != replyID {
		t.Fatalf("first chain id = %v, want %s", got, replyID)
	}
	if got := nodes[1].(map[string]any)["id"]; got != secondReplyID {
		t.Fatalf("second chain id = %v, want %s", got, secondReplyID)
	}
	if len(store.referenceInputs) < 2 {
		t.Fatalf("reference inputs = %d, want at least 2", len(store.referenceInputs))
	}
	if got := store.referenceInputs[0].limitPerTarget; got != 32 {
		t.Fatalf("fanout limit = %d, want 32", got)
	}
	if got := store.referenceInputs[0].input.PubKeys; len(got) != 1 || got[0] != author {
		t.Fatalf("query pubkeys = %+v, want author only", got)
	}
}

func TestEventsQueryBackfillsAuthorWhenFirstPageShort(t *testing.T) {
	store := &fakeStore{
		events: [][]chstore.EventView{
			nil,
			{{
				ID:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "hello",
				Tags:      [][]string{},
				Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			}},
		},
	}
	backfiller := &fakeUserBackfiller{}
	schema, err := NewSchema(store, WithUserFeedBackfill(backfiller))
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				pubkeys:["` + testPubkey + `"],
				kinds:[1,6,16],
				limit:20
			}) { nodes { id kind pubkey content } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if backfiller.calls != 1 || backfiller.pubkey != testPubkey || backfiller.limit != 20 {
		t.Fatalf("backfill call = %+v", backfiller)
	}
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2", store.calls)
	}
	data := result.Data.(map[string]any)
	events := data["events"].(map[string]any)
	nodes := events["nodes"].([]any)
	if len(nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(nodes))
	}
	node := nodes[0].(map[string]any)
	if node["content"] != "hello" {
		t.Fatalf("node = %+v", node)
	}
}

func TestEventsQueryAcceptsShuffle(t *testing.T) {
	store := &fakeStore{
		events: [][]chstore.EventView{{}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				kinds:[1]
				limit:2
				shuffle:{seed:"viewer-seed", counter:7, strength:0.25}
			}) {
				nodes { id }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
	shuffle := store.eventInputs[0].Shuffle
	if shuffle.Seed != "viewer-seed" || shuffle.Counter != 7 || shuffle.Strength != 0.25 {
		t.Fatalf("shuffle = %+v", shuffle)
	}
}

func TestEventsQueryReturnsIndexedDataWhenHydrationIsSlow(t *testing.T) {
	store := &fakeStore{
		events: [][]chstore.EventView{
			nil,
			{{
				ID:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "eventually available",
				Tags:      [][]string{},
				Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			}},
		},
	}
	backfiller := &fakeHydratingUserBackfiller{completed: false}
	schema, err := NewSchema(store, WithUserFeedBackfill(backfiller))
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				pubkeys:["` + testPubkey + `"],
				kinds:[1,6,16],
				limit:20
			}) { nodes { id kind pubkey content } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if backfiller.hydrated != 1 || backfiller.calls != 1 || backfiller.pubkey != testPubkey || backfiller.limit != 20 {
		t.Fatalf("hydration call = %+v", backfiller)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", store.calls)
	}
	data := result.Data.(map[string]any)
	events := data["events"].(map[string]any)
	nodes := events["nodes"].([]any)
	if len(nodes) != 0 {
		t.Fatalf("nodes len = %d, want 0", len(nodes))
	}
}

func TestEventsQueryHydratesRelaySafeTagRange(t *testing.T) {
	store := &fakeStore{events: [][]chstore.EventView{{}}}
	backfiller := &fakeRelayBackfiller{completed: true}
	schema, err := NewSchema(store, WithUserFeedBackfill(backfiller))
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				kinds:[1],
				tags:[{key:"p", value:"` + testPubkey + `"}],
				since:1710000000,
				until:1710000100,
				limit:20
			}) { nodes { id } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if backfiller.relayCalls != 1 || len(backfiller.inputs) != 1 || backfiller.labels[0] != "events" {
		t.Fatalf("relay hydration = calls %d labels %+v inputs %+v", backfiller.relayCalls, backfiller.labels, backfiller.inputs)
	}
	input := backfiller.inputs[0]
	if len(input.Tags) != 1 || input.Tags[0].Key != "p" || input.Tags[0].Value != testPubkey {
		t.Fatalf("hydrated tags = %+v", input.Tags)
	}
	if input.Since != 1_710_000_000 || input.Until != 1_710_000_100 || input.Limit != 20 {
		t.Fatalf("hydrated bounds = %+v", input)
	}
	if backfiller.calls != 0 {
		t.Fatalf("legacy author backfill should not run when generic hydration is available: %+v", backfiller)
	}
}

func TestEventQueryHydratesByID(t *testing.T) {
	id := testHex("a")
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			id: {
				ID:     id,
				PubKey: testPubkey,
				Kind:   1,
			},
		},
	}
	backfiller := &fakeRelayBackfiller{completed: true}
	schema, err := NewSchema(store, WithUserFeedBackfill(backfiller))
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			event(id:"` + id + `") { id kind pubkey }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if backfiller.relayCalls != 1 || backfiller.labels[0] != "event" {
		t.Fatalf("relay hydration = calls %d labels %+v inputs %+v", backfiller.relayCalls, backfiller.labels, backfiller.inputs)
	}
	if len(backfiller.inputs[0].IDs) != 1 || backfiller.inputs[0].IDs[0] != id || backfiller.inputs[0].Limit != 1 {
		t.Fatalf("hydrated event input = %+v", backfiller.inputs[0])
	}
}

func TestAggregateEventsHydratesRelaySafeInput(t *testing.T) {
	store := &fakeStore{}
	backfiller := &fakeRelayBackfiller{completed: true}
	schema, err := NewSchema(store, WithUserFeedBackfill(backfiller))
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			aggregateEvents(input:{
				dataset:"TAGS",
				groupBy:["TAG_VALUE"],
				kinds:[7],
				tags:[{key:"e", value:"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],
				since:1710000000,
				limit:10
			}) { rows { dimensions metrics } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if backfiller.relayCalls != 1 || backfiller.labels[0] != "aggregateEvents" {
		t.Fatalf("relay hydration = calls %d labels %+v inputs %+v", backfiller.relayCalls, backfiller.labels, backfiller.inputs)
	}
	input := backfiller.inputs[0]
	if len(input.Kinds) != 1 || input.Kinds[0] != 7 || input.Since != 1_710_000_000 || input.Limit != 10 {
		t.Fatalf("hydrated aggregate input = %+v", input)
	}
}

func TestRelayHydrationBudgetCapsGraphQLRequest(t *testing.T) {
	backfiller := &fakeRelayBackfiller{completed: true}
	ctx := context.WithValue(context.Background(), relayHydrationBudgetKey{}, &relayHydrationBudget{remaining: 1})
	resolver := &resolver{relayEventBackfiller: backfiller}
	query := chstore.EventQueryInput{Kinds: []int{1}, Limit: 20}

	if !resolver.hydrateRelayEventQuery(ctx, query, "first") {
		t.Fatal("first hydration should run")
	}
	if resolver.hydrateRelayEventQuery(ctx, query, "second") {
		t.Fatal("second hydration should be budget-capped")
	}
	if backfiller.relayCalls != 1 || backfiller.labels[0] != "first" {
		t.Fatalf("relay hydration = calls %d labels %+v", backfiller.relayCalls, backfiller.labels)
	}
}

func TestDMEnvelopesQueryHydratesViewerInbox(t *testing.T) {
	store := &fakeStore{}
	backfiller := &fakeDMEnvelopeBackfiller{}
	schema, err := NewSchema(store, WithUserFeedBackfill(backfiller))
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			dmEnvelopes(input:{
				viewer:"` + testPubkey + `",
				kinds:[1059],
				until:1710000000,
				limit:25
			}) { nodes { id kind pubkey } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if backfiller.hydrated != 1 || backfiller.calls != 1 || backfiller.pubkey != testPubkey || backfiller.until != 1_710_000_000 || backfiller.limit != 25 {
		t.Fatalf("dm hydration = %+v", backfiller)
	}
	if len(backfiller.kinds) != 1 || backfiller.kinds[0] != 1059 {
		t.Fatalf("dm kinds = %+v", backfiller.kinds)
	}
	if len(store.eventInputs) != 2 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
	if len(store.eventInputs[0].PubKeys) != 1 || store.eventInputs[0].PubKeys[0] != testPubkey {
		t.Fatalf("authored input = %+v", store.eventInputs[0])
	}
	if len(store.eventInputs[1].Tags) != 1 || store.eventInputs[1].Tags[0].Key != "p" || store.eventInputs[1].Tags[0].Value != testPubkey {
		t.Fatalf("received input = %+v", store.eventInputs[1])
	}
}

func TestEventReferencesResolveByGenericTagPredicate(t *testing.T) {
	sourceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	quoteID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		events: [][]chstore.EventView{
			{{
				ID:        sourceID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "quoting",
				Tags:      [][]string{{"q", quoteID}},
				Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			}},
			{{
				ID:        quoteID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "quoted",
				Tags:      [][]string{},
				Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			}},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{ids:["` + sourceID + `"], limit:1}) {
				nodes {
					id
					references(input:{tags:[{key:"q"}], limit:1}) {
						nodes { id content }
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	refs := nodes[0].(map[string]any)["references"].(map[string]any)["nodes"].([]any)
	if len(refs) != 1 || refs[0].(map[string]any)["id"] != quoteID {
		t.Fatalf("refs = %+v", refs)
	}
}

func TestEventSelectedReferencesPrefersMarkedRootAndBatches(t *testing.T) {
	rootID := testHex("a")
	parentID := testHex("b")
	replyID := testHex("c")
	secondReplyID := testHex("d")
	store := &fakeStore{
		events: [][]chstore.EventView{
			{
				{
					ID:        replyID,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "reply",
					Tags:      [][]string{{"e", parentID, "", "reply"}, {"e", rootID, "", "root"}},
					Sig:       strings.Repeat("e", 128),
				},
				{
					ID:        secondReplyID,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_001, 0),
					Content:   "reply two",
					Tags:      [][]string{{"e", rootID, "", "root"}},
					Sig:       strings.Repeat("f", 128),
				},
			},
			{{
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("a", 128),
			}},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{kinds:[1], limit:2}) {
				nodes {
					id
					selectedReferences(input:{
						selectors:[{key:"e", marker:"root"}]
						fallback:{key:"e", excludeMarkers:["mention"]}
						maxDepth:8
						limit:1
					}) {
						nodes { id content }
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("nodes = %+v", nodes)
	}
	for _, node := range nodes {
		refs := node.(map[string]any)["selectedReferences"].(map[string]any)["nodes"].([]any)
		if len(refs) != 1 || refs[0].(map[string]any)["id"] != rootID {
			t.Fatalf("refs = %+v", refs)
		}
	}
	if len(store.eventInputs) != 2 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
	if got := store.eventInputs[1].IDs; len(got) != 1 || got[0] != rootID {
		t.Fatalf("selected reference fetch ids = %+v", got)
	}
}

func TestEventSelectedReferencesWalksFallbackChain(t *testing.T) {
	rootID := testHex("a")
	parentID := testHex("b")
	replyID := testHex("c")
	store := &fakeStore{
		events: [][]chstore.EventView{
			{{
				ID:        replyID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_002, 0),
				Content:   "reply",
				Tags:      [][]string{{"e", parentID, "", "reply"}},
				Sig:       strings.Repeat("e", 128),
			}},
			{{
				ID:        parentID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "parent",
				Tags:      [][]string{{"e", rootID, "", "root"}},
				Sig:       strings.Repeat("f", 128),
			}},
			{{
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("a", 128),
			}},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{ids:["` + replyID + `"], limit:1}) {
				nodes {
					id
					selectedReferences(input:{
						selectors:[{key:"e", marker:"root"}]
						fallback:{key:"e", excludeMarkers:["mention"]}
						maxDepth:8
						limit:1
					}) {
						nodes { id content }
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	refs := nodes[0].(map[string]any)["selectedReferences"].(map[string]any)["nodes"].([]any)
	if len(refs) != 1 || refs[0].(map[string]any)["id"] != rootID {
		t.Fatalf("refs = %+v", refs)
	}
	if len(store.eventInputs) != 3 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
}

func TestEventSelectedReferencesIgnoresMentionFallback(t *testing.T) {
	mentionID := testHex("a")
	eventID := testHex("b")
	store := &fakeStore{
		events: [][]chstore.EventView{
			{{
				ID:        eventID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "root with mention",
				Tags:      [][]string{{"e", mentionID, "", "mention"}},
				Sig:       strings.Repeat("e", 128),
			}},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{ids:["` + eventID + `"], limit:1}) {
				nodes {
					id
					selectedReferences(input:{
						selectors:[{key:"e", marker:"root"}]
						fallback:{key:"e", excludeMarkers:["mention"]}
						maxDepth:8
						limit:1
					}) {
						nodes { id }
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	refs := nodes[0].(map[string]any)["selectedReferences"].(map[string]any)["nodes"].([]any)
	if len(refs) != 0 {
		t.Fatalf("refs = %+v", refs)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
}

func TestEventAggregateReferencedByCountsDistinctGenericSources(t *testing.T) {
	sourceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	reactionID1 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	reactionID2 := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	reactionPubkey := "1111111111111111111111111111111111111111111111111111111111111111"
	store := &fakeStore{
		events: [][]chstore.EventView{
			{{
				ID:        sourceID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "target",
				Tags:      [][]string{},
				Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			}},
			{
				{
					ID:        reactionID1,
					PubKey:    reactionPubkey,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_001, 0),
					Content:   "+",
					Tags:      [][]string{{"e", sourceID}},
					Sig:       "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
				},
				{
					ID:        reactionID2,
					PubKey:    reactionPubkey,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "+",
					Tags:      [][]string{{"e", sourceID}},
					Sig:       "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
				},
			},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{ids:["` + sourceID + `"], limit:1}) {
				nodes {
					aggregateReferencedBy(input:{
						via:{key:"e"}
						events:{kinds:[7], limit:20}
						metrics:[{name:"pubkeys", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}]
					}) {
						rows { metrics }
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	rows := nodes[0].(map[string]any)["aggregateReferencedBy"].(map[string]any)["rows"].([]any)
	metrics := rows[0].(map[string]any)["metrics"].(map[string]uint64)
	if metrics["pubkeys"] != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}
}

func TestEventAggregateReferencedByBatchesAcrossSiblingEvents(t *testing.T) {
	sourceID1 := testHex("a")
	sourceID2 := testHex("b")
	reactionID1 := testHex("c")
	reactionID2 := testHex("d")
	reactionID3 := testHex("e")
	reactionPubkey1 := testHex("1")
	reactionPubkey2 := testHex("2")
	store := &fakeStore{
		events: [][]chstore.EventView{
			{
				{
					ID:        sourceID1,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_000, 0),
					Content:   "source one",
					Tags:      [][]string{},
					Sig:       strings.Repeat("a", 128),
				},
				{
					ID:        sourceID2,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_001, 0),
					Content:   "source two",
					Tags:      [][]string{},
					Sig:       strings.Repeat("b", 128),
				},
			},
			{
				{
					ID:        reactionID1,
					PubKey:    reactionPubkey1,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "+",
					Tags:      [][]string{{"e", sourceID1}},
					Sig:       strings.Repeat("c", 128),
				},
				{
					ID:        reactionID2,
					PubKey:    reactionPubkey1,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_003, 0),
					Content:   "+",
					Tags:      [][]string{{"e", sourceID1}},
					Sig:       strings.Repeat("d", 128),
				},
				{
					ID:        reactionID3,
					PubKey:    reactionPubkey2,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_004, 0),
					Content:   "+",
					Tags:      [][]string{{"e", sourceID2}},
					Sig:       strings.Repeat("e", 128),
				},
			},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{kinds:[1], limit:2}) {
				nodes {
					id
					aggregateReferencedBy(input:{
						via:{key:"e"}
						events:{kinds:[7], limit:20}
						metrics:[{name:"pubkeys", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}]
					}) {
						rows { metrics }
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("nodes = %+v", nodes)
	}
	gotByID := map[string]uint64{}
	for _, rawNode := range nodes {
		node := rawNode.(map[string]any)
		rows := node["aggregateReferencedBy"].(map[string]any)["rows"].([]any)
		if len(rows) != 1 {
			t.Fatalf("rows for %s = %+v", node["id"], rows)
		}
		metrics := rows[0].(map[string]any)["metrics"].(map[string]uint64)
		gotByID[node["id"].(string)] = metrics["pubkeys"]
	}
	if gotByID[sourceID1] != 1 || gotByID[sourceID2] != 1 {
		t.Fatalf("metrics = %+v", gotByID)
	}
	if len(store.referenceInputs) != 1 {
		t.Fatalf("reference inputs = %+v", store.referenceInputs)
	}
	if got := store.referenceInputs[0].targets; len(got) != 2 || got[0] != sourceID1 || got[1] != sourceID2 {
		t.Fatalf("targets = %+v", got)
	}
}

func TestSelectedReferencesShareNestedRelationBatches(t *testing.T) {
	replyID1 := testHex("a")
	replyID2 := testHex("b")
	rootID1 := testHex("c")
	rootID2 := testHex("d")
	reactionID1 := testHex("e")
	reactionID2 := testHex("f")
	reactionPubkey := testHex("1")
	store := &fakeStore{
		events: [][]chstore.EventView{
			{
				{
					ID:        replyID1,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "reply one",
					Tags:      [][]string{{"e", rootID1}},
					Sig:       strings.Repeat("a", 128),
				},
				{
					ID:        replyID2,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_001, 0),
					Content:   "reply two",
					Tags:      [][]string{{"e", rootID2}},
					Sig:       strings.Repeat("b", 128),
				},
			},
			{
				{
					ID:        rootID1,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_000, 0),
					Content:   "root one",
					Tags:      [][]string{},
					Sig:       strings.Repeat("c", 128),
				},
				{
					ID:        rootID2,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_000, 0),
					Content:   "root two",
					Tags:      [][]string{},
					Sig:       strings.Repeat("d", 128),
				},
			},
			{
				{
					ID:        reactionID1,
					PubKey:    reactionPubkey,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_003, 0),
					Content:   "+",
					Tags:      [][]string{{"e", rootID1}},
					Sig:       strings.Repeat("e", 128),
				},
				{
					ID:        reactionID2,
					PubKey:    reactionPubkey,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_004, 0),
					Content:   "+",
					Tags:      [][]string{{"e", rootID2}},
					Sig:       strings.Repeat("f", 128),
				},
			},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{ids:["` + replyID1 + `","` + replyID2 + `"], limit:2}) {
				nodes {
					id
					selectedReferences(input:{fallback:{key:"e"}, limit:1}) {
						nodes {
							id
							aggregateReferencedBy(input:{
								via:{key:"e"}
								events:{kinds:[7], limit:20}
								metrics:[{name:"pubkeys", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}]
							}) {
								rows { metrics }
							}
						}
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.referenceInputs) != 1 {
		t.Fatalf("reference inputs = %+v", store.referenceInputs)
	}
	if got := store.referenceInputs[0].targets; len(got) != 2 || got[0] != rootID1 || got[1] != rootID2 {
		t.Fatalf("targets = %+v", got)
	}
}

func TestEventAggregateReferencedByUsesAggregateStoreFastPath(t *testing.T) {
	sourceID1 := testHex("a")
	sourceID2 := testHex("b")
	store := &fakeStore{
		referenceAggregateSupported: true,
		referenceAggregateRows: map[string][]chstore.AggregateRow{
			sourceID1: {{
				Dimensions: map[string]string{},
				Metrics:    map[string]uint64{"pubkeys": 2},
			}},
			sourceID2: {{
				Dimensions: map[string]string{},
				Metrics:    map[string]uint64{"pubkeys": 3},
			}},
		},
		events: [][]chstore.EventView{{
			{
				ID:        sourceID1,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "source one",
				Tags:      [][]string{},
				Sig:       strings.Repeat("a", 128),
			},
			{
				ID:        sourceID2,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "source two",
				Tags:      [][]string{},
				Sig:       strings.Repeat("b", 128),
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{kinds:[1], limit:2}) {
				nodes {
					id
					aggregateReferencedBy(input:{
						via:{key:"e"}
						events:{kinds:[7], limit:500}
						metrics:[{name:"pubkeys", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}]
					}) {
						rows { metrics }
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	gotByID := map[string]uint64{}
	for _, rawNode := range nodes {
		node := rawNode.(map[string]any)
		rows := node["aggregateReferencedBy"].(map[string]any)["rows"].([]any)
		metrics := rows[0].(map[string]any)["metrics"].(map[string]uint64)
		gotByID[node["id"].(string)] = metrics["pubkeys"]
	}
	if gotByID[sourceID1] != 2 || gotByID[sourceID2] != 3 {
		t.Fatalf("metrics = %+v", gotByID)
	}
	if len(store.referenceAggregateInputs) != 1 {
		t.Fatalf("reference aggregate inputs = %+v", store.referenceAggregateInputs)
	}
	if len(store.referenceInputs) != 0 {
		t.Fatalf("fallback reference inputs = %+v", store.referenceInputs)
	}
	input := store.referenceAggregateInputs[0]
	if input.Tag.Key != "e" || input.LimitPerTarget != 500 {
		t.Fatalf("aggregate input = %+v", input)
	}
	if got := input.Targets; len(got) != 2 || got[0] != sourceID1 || got[1] != sourceID2 {
		t.Fatalf("targets = %+v", got)
	}
}

func TestAggregateEventsAcceptsTimeBounds(t *testing.T) {
	store := &fakeStore{
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
				Metrics:    map[string]uint64{"unique_pubkeys": 2},
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			aggregateEvents(input:{
				dataset:"TAGS"
				kinds:[7]
				tags:[{key:"e"}]
				since:1710000000
				until:1710086400
				groupBy:["TAG_VALUE"]
				metrics:["UNIQUE_PUBKEYS"]
				limit:5
				shuffle:{seed:"agg-seed", counter:3, strength:0.5}
			}) {
				rows { dimensions metrics }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.aggregateInputs) != 1 {
		t.Fatalf("aggregate inputs len = %d", len(store.aggregateInputs))
	}
	input := store.aggregateInputs[0]
	if input.Since != 1_710_000_000 || input.Until != 1_710_086_400 {
		t.Fatalf("time bounds = %d/%d", input.Since, input.Until)
	}
	if input.Shuffle.Seed != "agg-seed" || input.Shuffle.Counter != 3 || input.Shuffle.Strength != 0.5 {
		t.Fatalf("shuffle = %+v", input.Shuffle)
	}
}

func TestRankedEventsUsesRecentReferencesAndPreservesRank(t *testing.T) {
	topID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	secondID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": topID},
				Metrics:    map[string]uint64{"unique_pubkeys": 3},
			},
			{
				Dimensions: map[string]string{"tag_value": secondID},
				Metrics:    map[string]uint64{"unique_pubkeys": 2},
			},
		}},
		events: [][]chstore.EventView{{
			{
				ID:        secondID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "second",
				Tags:      [][]string{},
				Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			},
			{
				ID:        topID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "top",
				Tags:      [][]string{},
				Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			rankedEvents(input:{
				references:{kinds:[7], since:1710000000}
				via:{key:"e"}
				target:{kinds:[1]}
				metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
				limit:2
			}) {
				nodes { id content }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.aggregateInputs) != 1 {
		t.Fatalf("aggregate inputs len = %d", len(store.aggregateInputs))
	}
	if len(store.rankedTargetAggregateInputs) != 0 {
		t.Fatalf("unexpected ranked target aggregate inputs = %+v", store.rankedTargetAggregateInputs)
	}
	aggregateInput := store.aggregateInputs[0]
	if aggregateInput.Dataset != "TAGS" || aggregateInput.Since != 1_710_000_000 {
		t.Fatalf("aggregate input = %+v", aggregateInput)
	}
	if len(aggregateInput.Tags) != 1 || aggregateInput.Tags[0].Key != "e" {
		t.Fatalf("aggregate tags = %+v", aggregateInput.Tags)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}
	if got := store.eventInputs[0].IDs; len(got) != 2 || got[0] != topID || got[1] != secondID {
		t.Fatalf("target ids = %+v", got)
	}
	if got := store.eventInputs[0].Kinds; len(got) != 1 || got[0] != 1 {
		t.Fatalf("target kinds = %+v", got)
	}
	data := result.Data.(map[string]any)
	nodes := data["rankedEvents"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 || nodes[0].(map[string]any)["id"] != topID || nodes[1].(map[string]any)["id"] != secondID {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestRankedEventsPropagatesPubkeyScoreFilterToEngagementReferences(t *testing.T) {
	topID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	secondID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": topID},
				Metrics:    map[string]uint64{"unique_pubkeys": 3},
			},
			{
				Dimensions: map[string]string{"tag_value": secondID},
				Metrics:    map[string]uint64{"unique_pubkeys": 2},
			},
		}},
		events: [][]chstore.EventView{{
			{
				ID:        topID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "top",
				Tags:      [][]string{},
				Sig:       strings.Repeat("c", 128),
			},
			{
				ID:        secondID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "second",
				Tags:      [][]string{},
				Sig:       strings.Repeat("d", 128),
			},
		}},
		referenceAggregateSupported: true,
		referenceAggregateRows: map[string][]chstore.AggregateRow{
			topID: {{
				Metrics: map[string]uint64{"value": 2},
			}},
			secondID: {{
				Metrics: map[string]uint64{"value": 1},
			}},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			rankedEvents(input:{
				references:{kinds:[7,9735,6,16,1,1111], pubkeyScore:{source:"vertex"}}
				via:{key:"e"}
				target:{kinds:[1,1111]}
				metric:{name:"actors", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
				terms:[{
					references:{kinds:[7], limit:500, pubkeyScore:{source:"vertex"}}
					via:{key:"e"}
					metric:{name:"likes", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
					weight:3.0
					transform:"LOG1P"
				}]
				limit:2
			}) {
				nodes { id content }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.aggregateInputs) != 1 {
		t.Fatalf("aggregate inputs len = %d", len(store.aggregateInputs))
	}
	if got := store.aggregateInputs[0].PubkeyScore; got.Source != "vertex" || got.MinFollowers != 0 {
		t.Fatalf("aggregate pubkey score filter = %+v", got)
	}
	if len(store.referenceAggregateInputs) != 2 {
		t.Fatalf("reference aggregate inputs = %+v", store.referenceAggregateInputs)
	}
	for _, input := range store.referenceAggregateInputs {
		if got := input.Events.PubkeyScore; got.Source != "vertex" || got.MinFollowers != 0 {
			t.Fatalf("reference pubkey score filter = %+v", got)
		}
	}
}

func TestRankedEventsCachesBasePoolAcrossRequests(t *testing.T) {
	topID := strings.Repeat("a", 64)
	secondID := strings.Repeat("b", 64)
	newStore := func() *fakeStore {
		return &fakeStore{
			aggregateRows: [][]chstore.AggregateRow{{
				{Dimensions: map[string]string{"tag_value": topID}, Metrics: map[string]uint64{"unique_pubkeys": 3}},
				{Dimensions: map[string]string{"tag_value": secondID}, Metrics: map[string]uint64{"unique_pubkeys": 2}},
			}},
			events: [][]chstore.EventView{{
				{ID: topID, PubKey: testPubkey, Kind: 1, CreatedAt: time.Unix(1_710_000_001, 0), Content: "top", Tags: [][]string{}, Sig: strings.Repeat("c", 128)},
				{ID: secondID, PubKey: testPubkey, Kind: 1, CreatedAt: time.Unix(1_710_000_000, 0), Content: "second", Tags: [][]string{}, Sig: strings.Repeat("d", 128)},
			}},
			referenceAggregateSupported: true,
			referenceAggregateRows: map[string][]chstore.AggregateRow{
				topID:    {{Metrics: map[string]uint64{"value": 2}}},
				secondID: {{Metrics: map[string]uint64{"value": 1}}},
			},
		}
	}

	query := `query {
		rankedEvents(input:{
			references:{kinds:[7,9735,6,16,1,1111], pubkeyScore:{source:"vertex"}}
			via:{key:"e"}
			target:{kinds:[1,1111]}
			metric:{name:"actors", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
			terms:[{
				references:{kinds:[7], limit:500}
				via:{key:"e"}
				metric:{name:"likes", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
				weight:3.0
				transform:"LOG1P"
			}]
			limit:2
		}) {
			nodes { id content }
		}
	}`

	store := newStore()
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		result := graphql.Do(graphql.Params{Schema: schema, RequestString: query, Context: context.Background()})
		if len(result.Errors) > 0 {
			t.Fatalf("request %d graphql errors = %+v", i, result.Errors)
		}
		nodes := result.Data.(map[string]any)["rankedEvents"].(map[string]any)["nodes"].([]any)
		if len(nodes) != 2 || nodes[0].(map[string]any)["id"] != topID {
			t.Fatalf("request %d nodes = %+v", i, nodes)
		}
	}

	// Two identical viewer-free requests must share one cached base pool: the
	// expensive reference aggregation and per-candidate term aggregation run once.
	if len(store.aggregateInputs) != 1 {
		t.Fatalf("expected base pool computed once, got %d reference aggregations", len(store.aggregateInputs))
	}
	if len(store.referenceAggregateInputs) != 2 {
		t.Fatalf("expected term aggregation computed once (2 candidate inputs), got %d", len(store.referenceAggregateInputs))
	}

	// A fresh schema (fresh cache) recomputes — proving the cache is the reason,
	// and that it is instance-scoped rather than a shared global.
	store2 := newStore()
	schema2, err := NewSchema(store2)
	if err != nil {
		t.Fatal(err)
	}
	if result := graphql.Do(graphql.Params{Schema: schema2, RequestString: query, Context: context.Background()}); len(result.Errors) > 0 {
		t.Fatalf("fresh-schema graphql errors = %+v", result.Errors)
	}
	if len(store2.aggregateInputs) != 1 {
		t.Fatalf("fresh schema expected 1 aggregation, got %d", len(store2.aggregateInputs))
	}
}

func TestRankedEventsAggregatesInsideDerivedTargetFilters(t *testing.T) {
	topID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	followedAuthor := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		latestEvents: map[string][]chstore.EventView{
			testPubkey: {{
				ID:        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				PubKey:    testPubkey,
				Kind:      3,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Tags:      [][]string{{"p", followedAuthor}},
			}},
		},
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": topID},
				Metrics:    map[string]uint64{"unique_pubkeys": 3},
			},
		}},
		events: [][]chstore.EventView{{
			{
				ID:        topID,
				PubKey:    followedAuthor,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "popular followed post",
				Tags:      [][]string{},
				Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			rankedEvents(input:{
				references:{kinds:[7], since:1710000000}
				via:{key:"e"}
				target:{
					kinds:[1]
					pubkeysFrom:[{
						latestEventTags:{
							pubkey:"` + testPubkey + `"
							kinds:[3]
							tag:{key:"p"}
							limit:1
							maxValues:10
						}
					}]
				}
				metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
				limit:1
			}) {
				nodes { id pubkey content }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.aggregateInputs) != 0 {
		t.Fatalf("unexpected global aggregate inputs = %+v", store.aggregateInputs)
	}
	if len(store.rankedTargetAggregateInputs) != 1 {
		t.Fatalf("ranked target aggregate inputs len = %d", len(store.rankedTargetAggregateInputs))
	}
	aggregateInput := store.rankedTargetAggregateInputs[0]
	if len(aggregateInput.references.Tags) != 1 || aggregateInput.references.Tags[0].Key != "e" {
		t.Fatalf("reference tags = %+v", aggregateInput.references.Tags)
	}
	if len(aggregateInput.target.PubKeys) != 1 || aggregateInput.target.PubKeys[0] != followedAuthor {
		t.Fatalf("target pubkeys = %+v", aggregateInput.target.PubKeys)
	}
	if len(aggregateInput.target.Kinds) != 1 || aggregateInput.target.Kinds[0] != 1 {
		t.Fatalf("target kinds = %+v", aggregateInput.target.Kinds)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}
	eventInput := store.eventInputs[0]
	if len(eventInput.IDs) != 1 || eventInput.IDs[0] != topID {
		t.Fatalf("event ids = %+v", eventInput.IDs)
	}
	if len(eventInput.PubKeys) != 1 || eventInput.PubKeys[0] != followedAuthor {
		t.Fatalf("event pubkeys = %+v", eventInput.PubKeys)
	}
	data := result.Data.(map[string]any)
	nodes := data["rankedEvents"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["id"] != topID {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestEventsQueryResolvesPubkeysFromLatestEventTags(t *testing.T) {
	authorA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	authorB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		latestEvents: map[string][]chstore.EventView{
			testPubkey: {{
				ID:        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				PubKey:    testPubkey,
				Kind:      3,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Tags:      [][]string{{"p", authorB}, {"p", authorA}, {"t", "not-a-pubkey"}},
			}},
		},
		events: [][]chstore.EventView{{
			{
				ID:        "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
				PubKey:    authorA,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "from derived pubkey",
				Tags:      [][]string{},
				Sig:       "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				pubkeysFrom:[{
					latestEventTags:{
						pubkey:"` + testPubkey + `"
						kinds:[3]
						tag:{key:"p"}
						limit:1
						maxValues:10
					}
				}]
				kinds:[1]
				limit:20
			}) { nodes { id pubkey content } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.latestInputs) != 1 {
		t.Fatalf("latest inputs len = %d", len(store.latestInputs))
	}
	if got := store.latestInputs[0]; len(got.pubkeys) != 1 || got.pubkeys[0] != testPubkey || got.kinds[0] != 3 || got.limit != 1 {
		t.Fatalf("latest input = %+v", got)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}
	if got := store.eventInputs[0].PubKeys; len(got) != 2 || got[0] != authorA || got[1] != authorB {
		t.Fatalf("derived pubkeys = %+v", got)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["pubkey"] != authorA {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestEventsQueryWithEmptyPubkeySourceDoesNotBroaden(t *testing.T) {
	store := &fakeStore{}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				pubkeysFrom:[{
					latestEventTags:{
						pubkey:"` + testPubkey + `"
						kinds:[3]
						tag:{key:"p"}
					}
				}]
				kinds:[1]
				limit:20
			}) { nodes { id } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.eventInputs) != 0 {
		t.Fatalf("empty pubkey source should not query events, got %+v", store.eventInputs)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 0 {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestReferencedByResolvesPubkeysFromSourceEventAuthor(t *testing.T) {
	rootID := "1111111111111111111111111111111111111111111111111111111111111111"
	replyID := "2222222222222222222222222222222222222222222222222222222222222222"
	rootAuthor := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherAuthor := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    rootAuthor,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       testHex("f") + testHex("f"),
			},
		},
		events: [][]chstore.EventView{{
			{
				ID:        replyID,
				PubKey:    rootAuthor,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "thread continuation",
				Tags:      [][]string{{"e", rootID}},
				Sig:       testHex("e") + testHex("e"),
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			event(id:"` + rootID + `") {
				referencedBy(input:{
					via:{key:"e"}
					events:{
						kinds:[1]
						pubkeysFrom:[{sourceEventAuthor:true}]
						limit:20
					}
					limit:20
				}) { nodes { id pubkey } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}
	eventInput := store.eventInputs[0]
	if len(eventInput.PubKeys) != 1 || eventInput.PubKeys[0] != rootAuthor {
		t.Fatalf("source author pubkeys = %+v", eventInput.PubKeys)
	}
	if len(eventInput.Tags) != 1 || eventInput.Tags[0].Key != "e" || eventInput.Tags[0].Value != rootID {
		t.Fatalf("reply target tag = %+v", eventInput.Tags)
	}
	if eventInput.Empty {
		t.Fatal("source author query should not remain empty after adding source pubkey")
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["referencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["pubkey"] != rootAuthor {
		t.Fatalf("nodes = %+v (other author %s)", nodes, otherAuthor)
	}
}

func TestRankedReferencedByRanksCandidateEventsByGenericReferences(t *testing.T) {
	rootID := "1111111111111111111111111111111111111111111111111111111111111111"
	replyAID := "2222222222222222222222222222222222222222222222222222222222222222"
	replyBID := "3333333333333333333333333333333333333333333333333333333333333333"
	authorA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	authorB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       "44444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444",
			},
		},
		latestEvents: map[string][]chstore.EventView{
			testPubkey: {{
				ID:        "5555555555555555555555555555555555555555555555555555555555555555",
				PubKey:    testPubkey,
				Kind:      3,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Tags:      [][]string{{"p", authorA}, {"p", authorB}},
			}},
		},
		events: [][]chstore.EventView{
			{
				{
					ID:        replyBID,
					PubKey:    authorB,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_003, 0),
					Content:   "less ranked candidate",
					Tags:      [][]string{{"e", rootID}},
					Sig:       "66666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666",
				},
				{
					ID:        replyAID,
					PubKey:    authorA,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "top ranked candidate",
					Tags:      [][]string{{"e", rootID}},
					Sig:       "77777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777",
				},
			},
			{
				{
					ID:        replyAID,
					PubKey:    authorA,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "top ranked candidate",
					Tags:      [][]string{{"e", rootID}},
					Sig:       "77777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777",
				},
			},
		},
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": replyAID},
				Metrics:    map[string]uint64{"unique_pubkeys": 7},
			},
			{
				Dimensions: map[string]string{"tag_value": replyBID},
				Metrics:    map[string]uint64{"unique_pubkeys": 2},
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			event(id:"` + rootID + `") {
				rankedReferencedBy(input:{
					via:{key:"e"}
					events:{
						kinds:[1]
						pubkeysFrom:[{
							latestEventTags:{
								pubkey:"` + testPubkey + `"
								kinds:[3]
								tag:{key:"p"}
							}
						}]
						limit:10
					}
					rank:{
						references:{kinds:[7]}
						via:{key:"e"}
						metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
					}
					limit:1
				}) { nodes { id content } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}
	if len(store.referenceInputs) != 1 {
		t.Fatalf("reference inputs len = %d", len(store.referenceInputs))
	}
	candidateInput := store.referenceInputs[0].input
	if got := candidateInput.PubKeys; len(got) != 2 || got[0] != authorA || got[1] != authorB {
		t.Fatalf("candidate pubkeys = %+v", got)
	}
	if got := store.referenceInputs[0].tag.Key; got != "e" {
		t.Fatalf("candidate tag key = %q", got)
	}
	if got := store.referenceInputs[0].targets; len(got) != 1 || got[0] != rootID {
		t.Fatalf("candidate targets = %+v", got)
	}
	if len(store.aggregateInputs) != 1 {
		t.Fatalf("aggregate inputs len = %d", len(store.aggregateInputs))
	}
	aggregateInput := store.aggregateInputs[0]
	if aggregateInput.Dataset != "TAGS" || len(aggregateInput.Tags) != 1 || aggregateInput.Tags[0].Key != "e" {
		t.Fatalf("aggregate input = %+v", aggregateInput)
	}
	if got := aggregateInput.Tags[0].Values; len(got) != 2 || !containsString(got, replyAID) || !containsString(got, replyBID) {
		t.Fatalf("aggregate tag values = %+v", got)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["id"] != replyAID {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestRankedReferencedByCapsLargeLimitInsteadOfDefaultingToOne(t *testing.T) {
	rootID := testHex("1")
	replyAID := testHex("2")
	replyBID := testHex("3")
	replyA := chstore.EventView{
		ID:        replyAID,
		PubKey:    testHex("a"),
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_002, 0),
		Content:   "reply a",
		Tags:      [][]string{{"e", rootID}},
		Sig:       strings.Repeat("a", 128),
	}
	replyB := chstore.EventView{
		ID:        replyBID,
		PubKey:    testHex("b"),
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_003, 0),
		Content:   "reply b",
		Tags:      [][]string{{"e", rootID}},
		Sig:       strings.Repeat("b", 128),
	}
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
		},
		events: [][]chstore.EventView{
			{replyA, replyB},
			{replyB, replyA},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			event(id:"` + rootID + `") {
				rankedReferencedBy(input:{
					via:{key:"e"}
					events:{kinds:[1], limit:10}
					rank:{
						references:{kinds:[7], limit:500}
						via:{key:"e"}
						metric:{name:"likes", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
					}
					limit:100
				}) { nodes { id } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("nodes len = %d, nodes = %+v", len(nodes), nodes)
	}
}

func TestRankedReferencedBySupportsWeightedTermsAndCandidateBoosts(t *testing.T) {
	rootID := testHex("1")
	replyAID := testHex("2")
	replyBID := testHex("3")
	authorA := testHex("a")
	authorB := testHex("b")
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
		},
		latestEvents: map[string][]chstore.EventView{
			testPubkey: {{
				ID:        testHex("5"),
				PubKey:    testPubkey,
				Kind:      3,
				CreatedAt: time.Unix(1_710_000_004, 0),
				Tags:      [][]string{{"p", authorA}},
				Sig:       strings.Repeat("5", 128),
			}},
		},
		events: [][]chstore.EventView{{
			{
				ID:        replyBID,
				PubKey:    authorB,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_003, 0),
				Content:   "more likes",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("6", 128),
			},
			{
				ID:        replyAID,
				PubKey:    authorA,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_002, 0),
				Content:   "followed author",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("7", 128),
			},
		}},
		referenceAggregateSupported: true,
		referenceAggregateRows: map[string][]chstore.AggregateRow{
			replyAID: {{
				Metrics: map[string]uint64{"likes": 1},
			}},
			replyBID: {{
				Metrics: map[string]uint64{"likes": 3},
			}},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			event(id:"` + rootID + `") {
				rankedReferencedBy(input:{
					via:{key:"e"}
					events:{kinds:[1], limit:10}
					rank:{
						references:{kinds:[7], limit:500}
						via:{key:"e"}
						metric:{name:"likes", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
						weight:3.0
						transform:"LOG1P"
						candidatePubkeyBoosts:[{
							pubkeysFrom:[{
								latestEventTags:{
									pubkey:"` + testPubkey + `"
									kinds:[3]
									tag:{key:"p"}
								}
							}]
							weight:6.0
						}]
					}
					limit:2
				}) { nodes { id } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 || nodes[0].(map[string]any)["id"] != replyAID || nodes[1].(map[string]any)["id"] != replyBID {
		t.Fatalf("nodes = %+v", nodes)
	}
	if len(store.latestInputs) != 1 {
		t.Fatalf("latest inputs = %+v", store.latestInputs)
	}
	if len(store.referenceAggregateInputs) != 1 {
		t.Fatalf("reference aggregate inputs = %+v", store.referenceAggregateInputs)
	}
	input := store.referenceAggregateInputs[0]
	if input.Tag.Key != "e" || input.OrderBy != "likes" || input.First != 1 {
		t.Fatalf("reference aggregate input = %+v", input)
	}
	if got := input.Targets; len(got) != 2 || !containsString(got, replyAID) || !containsString(got, replyBID) {
		t.Fatalf("targets = %+v", got)
	}
	if len(input.Metrics) != 1 || input.Metrics[0].Name != "likes" || input.Metrics[0].DistinctField != "PUBKEY" {
		t.Fatalf("metrics = %+v", input.Metrics)
	}
	if len(store.aggregateInputs) != 0 {
		t.Fatalf("legacy aggregate inputs = %+v", store.aggregateInputs)
	}
}

func TestRankedReferencedBySupportsPubkeyScoreTermWithFollowerThreshold(t *testing.T) {
	rootID := testHex("1")
	replyAID := testHex("2")
	replyBID := testHex("3")
	authorA := testHex("a")
	authorB := testHex("b")
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
		},
		events: [][]chstore.EventView{{
			{
				ID:        replyBID,
				PubKey:    authorB,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_003, 0),
				Content:   "high score but below threshold",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("6", 128),
			},
			{
				ID:        replyAID,
				PubKey:    authorA,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_002, 0),
				Content:   "eligible cached score",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("7", 128),
			},
		}},
		pubkeyScoreRows: map[string]chstore.PubkeyScore{
			authorA: {PubKey: authorA, Source: "vertex", Score: 20, Followers: 500},
			authorB: {PubKey: authorB, Source: "vertex", Score: 90, Followers: 499},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			event(id:"` + rootID + `") {
				rankedReferencedBy(input:{
					via:{key:"e"}
					events:{kinds:[1], limit:10}
					rank:{
						references:{kinds:[7], limit:500}
						via:{key:"e"}
						metric:{name:"likes", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
						terms:[{
							pubkeyScore:{source:"vertex", target:"AUTHOR", minFollowers:500}
							weight:1.0
						}]
					}
					limit:2
				}) { nodes { id } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 || nodes[0].(map[string]any)["id"] != replyAID {
		t.Fatalf("nodes = %+v", nodes)
	}
	if len(store.pubkeyScoreInputs) != 1 {
		t.Fatalf("pubkey score inputs = %+v", store.pubkeyScoreInputs)
	}
	scoreInput := store.pubkeyScoreInputs[0]
	if scoreInput.source != "vertex" || len(scoreInput.pubkeys) != 2 {
		t.Fatalf("score input = %+v", scoreInput)
	}
}

func TestRankedReferencedByUsesDerivedMetricRankTerm(t *testing.T) {
	rootID := testHex("1")
	replyAID := testHex("2")
	replyBID := testHex("3")
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
		},
		events: [][]chstore.EventView{{
			{
				ID:        replyAID,
				PubKey:    testHex("a"),
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_002, 0),
				Content:   "low contribution",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("7", 128),
			},
			{
				ID:        replyBID,
				PubKey:    testHex("b"),
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "high contribution",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("6", 128),
			},
		}},
		derivedMetricRows: map[string]map[string]float64{
			"contribution_quality": {
				replyAID: 0.2,
				replyBID: 0.9,
			},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			event(id:"` + rootID + `") {
				rankedReferencedBy(input:{
					via:{key:"e"}
					events:{kinds:[1], limit:10}
					rank:{
						references:{kinds:[7], limit:500}
						via:{key:"e"}
						metric:{name:"likes", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
						terms:[{
							derivedMetric:"contribution_quality"
							weight:10
						}]
					}
					limit:2
				}) { nodes { id } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 || nodes[0].(map[string]any)["id"] != replyBID {
		t.Fatalf("nodes = %+v", nodes)
	}
	if len(store.derivedMetricInputs) != 1 {
		t.Fatalf("derived metric inputs = %+v", store.derivedMetricInputs)
	}
	input := store.derivedMetricInputs[0]
	if input.metric != "contribution_quality" || len(input.eventIDs) != 2 {
		t.Fatalf("derived metric input = %+v", input)
	}
}

func TestEventsAcceptsDerivedTagExcludeValues(t *testing.T) {
	store := &fakeStore{
		events: [][]chstore.EventView{{
			{
				ID:        testHex("1"),
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "untagged still eligible",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				kinds:[1]
				tags:[{key:"topic", dataset:"DERIVED_TAGS", excludeValues:["crypto"]}]
				limit:10
			}) { nodes { id } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
	tags := store.eventInputs[0].Tags
	if len(tags) != 1 {
		t.Fatalf("tags = %+v", tags)
	}
	tag := tags[0]
	if tag.Key != "topic" || tag.Dataset != "DERIVED_TAGS" || len(tag.ExcludeValues) != 1 || tag.ExcludeValues[0] != "crypto" {
		t.Fatalf("tag filter = %+v", tag)
	}
}

func TestEventsAcceptsNegativeEventFilters(t *testing.T) {
	hiddenID := testHex("1")
	hiddenPubkey := testHex("2")
	visibleID := testHex("3")
	store := &fakeStore{
		events: [][]chstore.EventView{{
			{
				ID:        hiddenID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "hidden by id",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
			{
				ID:        testHex("4"),
				PubKey:    hiddenPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "hidden by pubkey",
				Tags:      [][]string{},
				Sig:       strings.Repeat("2", 128),
			},
			{
				ID:        visibleID,
				PubKey:    testHex("5"),
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_002, 0),
				Content:   "visible",
				Tags:      [][]string{},
				Sig:       strings.Repeat("3", 128),
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				kinds:[1]
				excludeIds:["` + hiddenID + `"]
				excludePubkeys:["` + hiddenPubkey + `"]
				limit:10
			}) { nodes { id } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["id"] != visibleID {
		t.Fatalf("nodes = %+v", nodes)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
	input := store.eventInputs[0]
	if len(input.ExcludeIDs) != 1 || input.ExcludeIDs[0] != hiddenID {
		t.Fatalf("exclude ids = %+v", input.ExcludeIDs)
	}
	if len(input.ExcludePubKeys) != 1 || input.ExcludePubKeys[0] != hiddenPubkey {
		t.Fatalf("exclude pubkeys = %+v", input.ExcludePubKeys)
	}
}

func TestNotificationsReturnsPolicyFilteredConnection(t *testing.T) {
	eventID := testHex("8")
	actorPubkey := testHex("a")
	store := &fakeStore{
		notificationRows: []chstore.NotificationRow{
			{
				Event: chstore.EventView{
					ID:        eventID,
					PubKey:    actorPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_000, 0),
					Content:   "mention",
					Tags:      [][]string{{"p", testPubkey}},
					Sig:       strings.Repeat("8", 128),
					UpdatedAt: time.Unix(1_710_000_001, 0),
				},
				Reason:           "mention",
				ActorVertexScore: 77,
			},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			notifications(input:{
				pubkey:"` + testPubkey + `"
				tab:MENTIONS
				policy:STRICT
				replyScope:DIRECT
				since:1710000000
				limit:2
			}) {
				nodes {
					reason
					actorVertexScore
					event { id pubkey kind }
				}
				pageInfo { hasNextPage endCursor }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	connection := data["notifications"].(map[string]any)
	nodes := connection["nodes"].([]any)
	if len(nodes) != 1 {
		t.Fatalf("nodes = %+v", nodes)
	}
	node := nodes[0].(map[string]any)
	if node["reason"] != "mention" || node["actorVertexScore"] != float64(77) {
		t.Fatalf("notification = %+v", node)
	}
	event := node["event"].(map[string]any)
	if event["id"] != eventID || event["pubkey"] != actorPubkey || event["kind"] != 1 {
		t.Fatalf("event = %+v", event)
	}
	pageInfo := connection["pageInfo"].(map[string]any)
	if pageInfo["hasNextPage"] != false || pageInfo["endCursor"] == "" {
		t.Fatalf("pageInfo = %+v", pageInfo)
	}
	if len(store.notificationInputs) != 1 {
		t.Fatalf("notification inputs = %+v", store.notificationInputs)
	}
	input := store.notificationInputs[0]
	if input.Viewer != testPubkey || input.Tab != "MENTIONS" || input.Policy != "STRICT" || input.ReplyScope != "DIRECT" || input.Since != 1_710_000_000 || input.Limit != 2 {
		t.Fatalf("notification input = %+v", input)
	}
}

func TestNotificationsDefaultsToAllStrict(t *testing.T) {
	store := &fakeStore{}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema:        schema,
		RequestString: `query { notifications(input:{pubkey:"` + testPubkey + `"}) { nodes { reason } } }`,
		Context:       context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.notificationInputs) != 1 {
		t.Fatalf("notification inputs = %+v", store.notificationInputs)
	}
	input := store.notificationInputs[0]
	if input.Tab != "ALL" || input.Policy != "STRICT" || input.ReplyScope != "THREAD" || input.Limit != 50 {
		t.Fatalf("notification input defaults = %+v", input)
	}
}

func TestRankedReferencedBySupportsWeightedSumMetricAndOffset(t *testing.T) {
	rootID := testHex("1")
	replyAID := testHex("2")
	replyBID := testHex("3")
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
		},
		events: [][]chstore.EventView{{
			{
				ID:        replyAID,
				PubKey:    testHex("a"),
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_002, 0),
				Content:   "low zaps",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("a", 128),
			},
			{
				ID:        replyBID,
				PubKey:    testHex("b"),
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_003, 0),
				Content:   "high zaps",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("b", 128),
			},
		}},
		referenceAggregateSupported: true,
		referenceAggregateRows: map[string][]chstore.AggregateRow{
			replyAID: {{
				Metrics: map[string]uint64{"zapSats": 10},
			}},
			replyBID: {{
				Metrics: map[string]uint64{"zapSats": 100},
			}},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			event(id:"` + rootID + `") {
				rankedReferencedBy(input:{
					via:{key:"e"}
					events:{kinds:[1], limit:10}
					rank:{
						references:{kinds:[9735], limit:500}
						via:{key:"e"}
						metric:{name:"zapSats", op:"SUM", derived:"nip57.amount_sats"}
					}
					limit:1
					offset:1
				}) { nodes { id } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["id"] != replyAID {
		t.Fatalf("nodes = %+v", nodes)
	}
	if len(store.referenceAggregateInputs) != 1 {
		t.Fatalf("reference aggregate inputs = %+v", store.referenceAggregateInputs)
	}
	metric := store.referenceAggregateInputs[0].Metrics[0]
	if metric.Name != "zapSats" || metric.Op != "SUM" || metric.Derived != "nip57.amount_sats" {
		t.Fatalf("metric = %+v", metric)
	}
	if len(store.aggregateInputs) != 0 {
		t.Fatalf("legacy aggregate inputs = %+v", store.aggregateInputs)
	}
}

func TestRankedReferencedByBatchesAcrossSiblingEvents(t *testing.T) {
	rootAID := "1111111111111111111111111111111111111111111111111111111111111111"
	rootBID := "2222222222222222222222222222222222222222222222222222222222222222"
	replyAID := "3333333333333333333333333333333333333333333333333333333333333333"
	replyBID := "4444444444444444444444444444444444444444444444444444444444444444"
	authorA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	authorB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		latestEvents: map[string][]chstore.EventView{
			testPubkey: {{
				ID:        "5555555555555555555555555555555555555555555555555555555555555555",
				PubKey:    testPubkey,
				Kind:      3,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Tags:      [][]string{{"p", authorA}, {"p", authorB}},
			}},
		},
		events: [][]chstore.EventView{
			{
				{
					ID:        rootAID,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_010, 0),
					Content:   "root a",
					Tags:      [][]string{},
					Sig:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				},
				{
					ID:        rootBID,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_009, 0),
					Content:   "root b",
					Tags:      [][]string{},
					Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				},
			},
			{
				{
					ID:        replyAID,
					PubKey:    authorA,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_008, 0),
					Content:   "reply a",
					Tags:      [][]string{{"e", rootAID}},
					Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				},
				{
					ID:        replyBID,
					PubKey:    authorB,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_007, 0),
					Content:   "reply b",
					Tags:      [][]string{{"e", rootBID}},
					Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
				},
			},
			{
				{
					ID:        replyAID,
					PubKey:    authorA,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_008, 0),
					Content:   "reply a",
					Tags:      [][]string{{"e", rootAID}},
					Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				},
				{
					ID:        replyBID,
					PubKey:    authorB,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_007, 0),
					Content:   "reply b",
					Tags:      [][]string{{"e", rootBID}},
					Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
				},
			},
		},
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": replyAID},
				Metrics:    map[string]uint64{"unique_pubkeys": 10},
			},
			{
				Dimensions: map[string]string{"tag_value": replyBID},
				Metrics:    map[string]uint64{"unique_pubkeys": 9},
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{ids:["` + rootAID + `","` + rootBID + `"], limit:2}) {
				nodes {
					id
					rankedReferencedBy(input:{
						via:{key:"e"}
						events:{
							kinds:[1]
							pubkeysFrom:[{
								latestEventTags:{
									pubkey:"` + testPubkey + `"
									kinds:[3]
									tag:{key:"p"}
								}
							}]
							limit:10
						}
						rank:{
							references:{kinds:[7]}
							via:{key:"e"}
							metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
						}
						limit:1
					}) { nodes { id } }
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.latestInputs) != 1 {
		t.Fatalf("latest inputs len = %d", len(store.latestInputs))
	}
	if len(store.referenceInputs) != 1 {
		t.Fatalf("reference inputs len = %d", len(store.referenceInputs))
	}
	if got := store.referenceInputs[0].targets; len(got) != 2 || got[0] != rootAID || got[1] != rootBID {
		t.Fatalf("reference targets = %+v", got)
	}
	if store.referenceInputs[0].limitPerTarget != 10 {
		t.Fatalf("reference limit per target = %d", store.referenceInputs[0].limitPerTarget)
	}
	if len(store.aggregateInputs) != 1 {
		t.Fatalf("aggregate inputs len = %d", len(store.aggregateInputs))
	}
	if len(store.eventInputs) != 2 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}

	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("nodes len = %d", len(nodes))
	}
	replyA := nodes[0].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	replyB := nodes[1].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(replyA) != 1 || replyA[0].(map[string]any)["id"] != replyAID {
		t.Fatalf("replyA = %+v", replyA)
	}
	if len(replyB) != 1 || replyB[0].(map[string]any)["id"] != replyBID {
		t.Fatalf("replyB = %+v", replyB)
	}
}
