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
	topicRows                   []chstore.TopicRow
	availableTopicInputs        []chstore.EventQueryInput
	trendingRows                []chstore.TrendingClusterRow
	trendingInputs              []chstore.TrendingInput
	notificationRows            []chstore.NotificationRow
	notificationInputs          []chstore.NotificationInput
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

func (s *fakeStore) AvailableTopics(_ context.Context, input chstore.EventQueryInput) ([]chstore.TopicRow, error) {
	s.availableTopicInputs = append(s.availableTopicInputs, input)
	return s.topicRows, nil
}

func (s *fakeStore) TrendingClusters(_ context.Context, input chstore.TrendingInput) ([]chstore.TrendingClusterRow, error) {
	s.trendingInputs = append(s.trendingInputs, input)
	return s.trendingRows, nil
}

func (s *fakeStore) Notifications(_ context.Context, input chstore.NotificationInput) ([]chstore.NotificationRow, error) {
	s.notificationInputs = append(s.notificationInputs, input)
	return s.notificationRows, nil
}

func (s *fakeStore) ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error) {
	return nil, nil, nil
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

func TestAvailableTopicsReturnsDerivedTopicRows(t *testing.T) {
	store := &fakeStore{
		topicRows: []chstore.TopicRow{
			{Value: "crypto.bitcoin", Parent: "crypto", Label: "Bitcoin", IsDefault: true, Count: 42},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			availableTopics(input:{kinds:[1], limit:25}) {
				value
				parent
				label
				isDefault
				count
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	topics := data["availableTopics"].([]any)
	if len(topics) != 1 {
		t.Fatalf("topics = %+v", topics)
	}
	topic := topics[0].(map[string]any)
	if topic["value"] != "crypto.bitcoin" || topic["label"] != "Bitcoin" || topic["count"] != 42 {
		t.Fatalf("topic = %+v", topic)
	}
	if len(store.availableTopicInputs) != 1 {
		t.Fatalf("available topic inputs = %+v", store.availableTopicInputs)
	}
	input := store.availableTopicInputs[0]
	if len(input.Kinds) != 1 || input.Kinds[0] != 1 || input.Limit != 25 {
		t.Fatalf("available topic input = %+v", input)
	}
}

func TestTrendingReturnsClustersWithSampleEvents(t *testing.T) {
	clusterID := "cluster-h24-crypto"
	sampleID := testHex("9")
	store := &fakeStore{
		events: [][]chstore.EventView{{
			{
				ID:        sampleID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "sample",
				Tags:      [][]string{},
				Sig:       strings.Repeat("9", 128),
			},
		}},
		trendingRows: []chstore.TrendingClusterRow{
			{
				ID:          clusterID,
				Window:      "H24",
				StartedAt:   time.Unix(1_710_000_000, 0),
				Category:    "crypto",
				Subcategory: "bitcoin",
				Title:       "Bitcoin relay chatter",
				Description: "Clustered Bitcoin notes",
				EventCount:  12,
				Score:       4.5,
				ComputedAt:  time.Unix(1_710_000_100, 0),
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
			trending(input:{window:H24, category:"crypto", limit:2}) {
				id
				window
				category
				subcategory
				title
				eventCount
				score
				sampleEvents(limit:1) { nodes { id } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	clusters := data["trending"].([]any)
	if len(clusters) != 1 {
		t.Fatalf("clusters = %+v", clusters)
	}
	cluster := clusters[0].(map[string]any)
	if cluster["id"] != clusterID || cluster["title"] != "Bitcoin relay chatter" || cluster["eventCount"] != 12 {
		t.Fatalf("cluster = %+v", cluster)
	}
	nodes := cluster["sampleEvents"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["id"] != sampleID {
		t.Fatalf("sample nodes = %+v", nodes)
	}
	if len(store.trendingInputs) != 1 {
		t.Fatalf("trending inputs = %+v", store.trendingInputs)
	}
	input := store.trendingInputs[0]
	if input.Window != "H24" || input.Category != "crypto" || input.Limit != 2 {
		t.Fatalf("trending input = %+v", input)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("sample event inputs = %+v", store.eventInputs)
	}
	sampleInput := store.eventInputs[0]
	if sampleInput.Limit != 1 || len(sampleInput.Tags) != 1 {
		t.Fatalf("sample input = %+v", sampleInput)
	}
	tag := sampleInput.Tags[0]
	if tag.Key != "cluster" || tag.Value != clusterID || tag.Dataset != "DERIVED_TAGS" {
		t.Fatalf("sample tag = %+v", tag)
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
				viewer:"` + testPubkey + `"
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
		RequestString: `query { notifications(input:{viewer:"` + testPubkey + `"}) { nodes { reason } } }`,
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
