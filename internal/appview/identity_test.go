package appview

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vertex-lab/nagg/internal/aiproviders"
	"github.com/vertex-lab/nagg/internal/auditor"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/mintinfo"
	"github.com/vertex-lab/nagg/internal/socialgraph"
	"github.com/vertex-lab/nagg/internal/vertex"
)

const (
	identityA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	identityB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func identityProviders(pubkey, baseURL string) stubProviders {
	return stubProviders{ready: true, directory: aiproviders.Directory{Providers: []aiproviders.Provider{
		{BaseURL: baseURL, Name: "ai", Pubkey: pubkey, Mints: []string{}, Status: aiproviders.StatusOnline},
	}}}
}

// TestIdentitiesBuilder pins the builder's contract: absence is null (never
// 0), a route's fresh figure beats the cache, both cross-link lists are
// always present, and every spelling of a pubkey lands on one lowercase key.
func TestIdentitiesBuilder(t *testing.T) {
	store := fakeStore{
		profiles:       map[string]chstore.K0Row{identityA: {PubKey: identityA, Name: "alice", DisplayName: "Alice", NIP05: "alice@example.com", LUD16: "alice@ln.example"}},
		cachedVertexOK: true,
		cachedVertex:   vertexProfile(identityA, 0.42, 0.77),
	}
	h := New(store, WithNIP05Validation(false),
		WithAuditor(fakeAuditor{mints: []auditor.Mint{
			{URL: "https://Mint.example/Bitcoin", OperatorContact: vertex.Npub(identityA)},
			{URL: "https://mint.example/bitcoin/", OperatorContact: identityA}, // one mint, two spellings
			{URL: "https://other.example", OperatorContact: identityA},
			{URL: "https://anonymous.example"},
		}}),
		WithAIProviders(identityProviders(identityA, "https://ai.example")),
		WithSocialReach(stubReach{identityA: socialgraph.FromGraph(10, 3)}))

	t.Run("resolved identity", func(t *testing.T) {
		ids := h.identities(context.Background(), []string{strings.ToUpper(identityA), vertex.Npub(identityA), identityA})
		if len(ids) != 1 {
			t.Fatalf("three spellings of one pubkey produced %d identities", len(ids))
		}
		id := ids[identityA]
		if id.Pubkey != identityA || id.Npub != vertex.Npub(identityA) {
			t.Fatalf("keys = %q / %q", id.Pubkey, id.Npub)
		}
		if id.Profile == nil || id.Profile.DisplayName != "Alice" || id.Profile.NIP05 != "alice@example.com" || id.Profile.LUD16 != "alice@ln.example" {
			t.Fatalf("profile = %+v", id.Profile)
		}
		if id.Profile.NIP05Valid != nil {
			t.Fatalf("nip05Valid = %v without validation; must be omitted, not false", *id.Profile.NIP05Valid)
		}
		if id.Reach.Followers == nil || *id.Reach.Followers != 10 || id.Reach.Follows == nil || *id.Reach.Follows != 3 || id.Reach.Source != socialgraph.SourceGraph {
			t.Fatalf("reach = %+v", id.Reach)
		}
		if id.Vertex.Rank == nil || *id.Vertex.Rank != 0.42 || id.Vertex.Score == nil || *id.Vertex.Score != 0.77 {
			t.Fatalf("vertex from cache = %+v", id.Vertex)
		}
		wantMints := []string{"https://Mint.example/Bitcoin", "https://other.example"}
		if strings.Join(id.Operates.Mints, ",") != strings.Join(wantMints, ",") {
			t.Fatalf("operates.mints = %v, want the verbatim URL once per mint: %v", id.Operates.Mints, wantMints)
		}
		if len(id.Operates.AIProviders) != 1 || id.Operates.AIProviders[0] != "https://ai.example" {
			t.Fatalf("operates.aiProviders = %v", id.Operates.AIProviders)
		}
		if id.FirstEventAt != nil {
			t.Fatalf("firstEventAt = %d on a list read; it is only computed by /nostr/profile", *id.FirstEventAt)
		}
	})

	t.Run("unknown is null, never zero", func(t *testing.T) {
		unknownStore := fakeStore{}
		bare := New(unknownStore, WithNIP05Validation(false), WithSocialReach(stubReach{}))
		raw, err := json.Marshal(bare.identities(context.Background(), []string{identityB})[identityB])
		if err != nil {
			t.Fatal(err)
		}
		body := string(raw)
		for _, want := range []string{
			`"profile":null`,
			`"reach":{"followers":null,"follows":null}`,
			`"vertex":{"rank":null,"score":null,"fetchedAt":null}`,
			`"operates":{"mints":[],"aiProviders":[]}`,
			`"firstEventAt":null`,
		} {
			if !strings.Contains(body, want) {
				t.Fatalf("identity JSON lacks %s: %s", want, body)
			}
		}
	})

	t.Run("route override beats the cache", func(t *testing.T) {
		fresh := IdentityVertex{Rank: f(0.99)}
		ids := h.identitiesWith(context.Background(), []string{identityA}, identityOptions{vertex: map[string]IdentityVertex{identityA: fresh}})
		if got := ids[identityA].Vertex; got.Rank == nil || *got.Rank != 0.99 || got.Score != nil {
			t.Fatalf("vertex = %+v, want the override %+v", got, fresh)
		}
		// An override that withholds the score is honoured too: the identity
		// must not resurrect a cache row the route decided not to publish.
		ids = h.identitiesWith(context.Background(), []string{identityA}, identityOptions{vertex: map[string]IdentityVertex{identityA: {}}})
		if got := ids[identityA].Vertex; got.Rank != nil || got.Score != nil {
			t.Fatalf("withheld vertex = %+v, want all null", got)
		}
	})

	t.Run("relay reach counts followers only", func(t *testing.T) {
		relays := New(fakeStore{}, WithNIP05Validation(false), WithSocialReach(stubReach{identityB: socialgraph.FromRelays(174)}))
		got := relays.identities(context.Background(), []string{identityB})[identityB].Reach
		if got.Followers == nil || *got.Followers != 174 || got.Follows != nil || got.Source != socialgraph.SourceRelays {
			t.Fatalf("relay reach = %+v, want 174 followers, null follows", got)
		}
	})

	t.Run("fallback fills only what the resolver left unknown", func(t *testing.T) {
		fallback := map[string]IdentityReach{
			identityA: identityReachFromGraph(1, 1),
			identityB: identityReachFromGraph(7, 2),
		}
		ids := h.identitiesWith(context.Background(), []string{identityA, identityB}, identityOptions{reachFallback: fallback})
		if got := ids[identityA].Reach; *got.Followers != 10 {
			t.Fatalf("resolved reach overwritten by the fallback: %+v", got)
		}
		if got := ids[identityB].Reach; got.Followers == nil || *got.Followers != 7 || *got.Follows != 2 {
			t.Fatalf("unresolved reach did not take the fallback: %+v", got)
		}
	})

	t.Run("nil store and nothing wired", func(t *testing.T) {
		ids := New(nil).identities(context.Background(), []string{identityA, "not a pubkey"})
		if len(ids) != 1 || ids[identityA].Profile != nil || ids[identityA].Reach.Followers != nil {
			t.Fatalf("identities without a store = %+v", ids)
		}
	})
}

func TestDiscoverCarriesOperatorIdentities(t *testing.T) {
	store := mintReviewStore{fakeStore: fakeStore{
		profiles:       map[string]chstore.K0Row{identityA: {PubKey: identityA, DisplayName: "Op Account", Picture: "https://op/pic.png"}},
		cachedVertexOK: true,
		cachedVertex:   vertexProfile(identityA, 0.42, 0.77),
	}}
	handler := New(store, WithNIP05Validation(false),
		WithAuditor(fakeAuditor{mints: []auditor.Mint{
			{URL: "https://m1", State: "OK", OperatorContact: identityA},
			{URL: "https://m2", State: "OK", OperatorContact: identityA},
			{URL: "https://m3", State: "OK", OperatorContact: identityB},
		}}),
		WithAIProviders(identityProviders(identityA, "https://ai.example")),
		WithSocialReach(stubReach{identityA: socialgraph.FromGraph(1234, 56)}))

	rec := httptest.NewRecorder()
	handler.discoverMints(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/discover?limit=2", nil))
	var resp DiscoverMintsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Mints) != 2 {
		t.Fatalf("mints = %d", len(resp.Mints))
	}
	// Identities are scoped to the returned rows, like profiles.
	if len(resp.Identities) != 1 {
		t.Fatalf("identities = %d for one distinct returned operator: %+v", len(resp.Identities), resp.Identities)
	}
	id, ok := resp.Identities[identityA]
	if !ok {
		t.Fatalf("identities[%s] missing", identityA)
	}
	row := resp.Mints[0]
	if row.OperatorPubkey != identityA || row.Followers != 1234 {
		t.Fatalf("flat operator fields changed: %+v", row)
	}
	if id.Profile == nil || id.Profile.DisplayName != "Op Account" {
		t.Fatalf("profile = %+v", id.Profile)
	}
	if id.Reach.Followers == nil || *id.Reach.Followers != row.Followers || *id.Reach.Follows != row.Follows || id.Reach.Source != row.FollowersSource {
		t.Fatalf("identity reach %+v disagrees with the row %+v", id.Reach, row)
	}
	if id.Vertex.Rank == nil || *id.Vertex.Rank != row.VertexRank || *id.Vertex.Score != *row.VertexScore {
		t.Fatalf("identity vertex %+v disagrees with the row %v/%v", id.Vertex, row.VertexRank, row.VertexScore)
	}
	if strings.Join(id.Operates.Mints, ",") != "https://m1,https://m2" || strings.Join(id.Operates.AIProviders, ",") != "https://ai.example" {
		t.Fatalf("operates = %+v", id.Operates)
	}
}

func TestAIProvidersCarriesOperatorIdentities(t *testing.T) {
	h := New(nil, WithAIProviders(stubProviders{directory: fixtureDirectory(), ready: true}))
	rec := httptest.NewRecorder()
	h.aiProviders(rec, httptest.NewRequest(http.MethodGet, "/app/ai-providers", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	var resp AIProvidersResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Providers) != 3 {
		t.Fatalf("rows changed: %d", len(resp.Providers))
	}
	const op = "aa3f3bf381ac923afcf5a3c16fb2957de94057de84df0c3e84a44c57fa031482"
	if len(resp.Identities) != 1 {
		t.Fatalf("identities = %d; only the row that names an operator has one: %+v", len(resp.Identities), resp.Identities)
	}
	id := resp.Identities[op]
	if id.Npub != vertex.Npub(op) || len(id.Operates.AIProviders) != 1 || id.Operates.AIProviders[0] != "https://ai.redsh1ft.com" {
		t.Fatalf("identity = %+v", id)
	}
	// Without the shared resolver the row's own count answers; a row counts
	// followers only, so follows stays null rather than reading as 0.
	if id.Reach.Followers == nil || *id.Reach.Followers != 1234 || id.Reach.Source != socialgraph.SourceGraph || id.Reach.Follows != nil {
		t.Fatalf("reach = %+v", id.Reach)
	}
	if id.Operates.Mints == nil || id.Profile != nil {
		t.Fatalf("no store: mints must still be [] and profile null: %+v", id)
	}
}

func TestProfileCarriesIdentity(t *testing.T) {
	score := 55.5
	firstEventAt := time.Unix(1_600_000_000, 0)
	store := &profilePolicySpyStore{fakeStore: fakeStore{
		profiles: map[string]chstore.K0Row{testPubkey: {PubKey: testPubkey, Name: "sovran", DisplayName: "Sovran"}},
		counts:   chstore.PubkeyStats{Follows: 30, Followers: 500},
		cachedVertex: vertex.ProfileResult{
			PubKey: testPubkey, Npub: vertex.Npub(testPubkey), Rank: 0.01, Score: &score,
			TopFollowers: []vertex.TopFollower{{PubKey: identityA, Rank: 0.5, Score: f(9)}},
		},
		cachedVertexOK: true,
		firstEventAt:   &firstEventAt,
	}}
	handler := New(store, WithVertex(fakeVertex{profileErr: errors.New("vertex unavailable"), refreshCalls: new(int)}), WithNIP05Validation(false),
		WithAuditor(fakeAuditor{mints: []auditor.Mint{{URL: "https://m1", OperatorContact: testPubkey}}}))

	rec := httptest.NewRecorder()
	handler.profile(rec, httptest.NewRequest(http.MethodGet, "/nostr/profile?pubkey="+testPubkey, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp ProvidersEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	id, ok := resp.Identities[testPubkey]
	if !ok {
		t.Fatalf("identities lacks the subject: %+v", resp.Identities)
	}
	if id.Profile == nil || id.Profile.Name != "sovran" {
		t.Fatalf("profile = %+v", id.Profile)
	}
	if id.Vertex.Score == nil || *id.Vertex.Score != score || *id.Vertex.Rank != 0.01 {
		t.Fatalf("vertex = %+v, want the same figures as providers.vertex %+v", id.Vertex, vertexOf(resp, testPubkey))
	}
	if id.Reach.Followers == nil || *id.Reach.Followers != 500 || *id.Reach.Follows != 30 || id.Reach.Source != socialgraph.SourceGraph {
		t.Fatalf("reach = %+v, want the pubkey_stats fallback", id.Reach)
	}
	if id.FirstEventAt == nil || *id.FirstEventAt != firstEventAt.Unix() {
		t.Fatalf("firstEventAt = %v", id.FirstEventAt)
	}
	if len(id.Operates.Mints) != 1 || id.Operates.Mints[0] != "https://m1" {
		t.Fatalf("operates = %+v", id.Operates)
	}
	// The referenced top follower gets an identity carrying its own rank.
	follower, ok := resp.Identities[identityA]
	if !ok || follower.Vertex.Rank == nil || *follower.Vertex.Rank != 0.5 || *follower.Vertex.Score != 9 {
		t.Fatalf("top follower identity = %+v ok=%v", follower, ok)
	}
}

// Below the follower threshold the route withholds Vertex from providers;
// the identity must say the same thing rather than reading the cache.
func TestProfileIdentityAgreesWhenVertexWithheld(t *testing.T) {
	score := 42.5
	store := &profilePolicySpyStore{fakeStore: fakeStore{
		profiles:       map[string]chstore.K0Row{testPubkey: {PubKey: testPubkey, Name: "sovran"}},
		counts:         chstore.PubkeyStats{Follows: 7, Followers: 499},
		cachedVertex:   vertex.ProfileResult{PubKey: testPubkey, Rank: 0.99, Score: &score},
		cachedVertexOK: true,
	}}
	handler := New(store, WithVertex(fakeVertex{refreshCalls: new(int)}), WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	handler.profile(rec, httptest.NewRequest(http.MethodGet, "/nostr/profile?pubkey="+testPubkey, nil))
	var resp ProvidersEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if got := resp.Identities[testPubkey].Vertex; got.Rank != nil || got.Score != nil {
		t.Fatalf("identity vertex = %+v leaked the cache past the follower threshold", got)
	}
}

func TestSearchCarriesIdentities(t *testing.T) {
	rank, score := 0.01, 42.5
	handler := New(
		fakeStore{
			profiles: map[string]chstore.K0Row{testPubkey: {PubKey: testPubkey, DisplayName: "Sovran", EventID: strings.Repeat("f", 64), RawJSON: `{"display_name":"Sovran"}`}},
			counts:   chstore.PubkeyStats{Followers: 12, Follows: 4},
		},
		WithProfileSearch(fakeVertex{search: []vertex.SearchResult{
			{PubKey: testPubkey, Npub: vertex.Npub(testPubkey), Rank: &rank, Score: &score},
			{PubKey: identityB, Npub: vertex.Npub(identityB)}, // ranked, no local kind-0
		}}),
		WithNIP05Validation(false),
	)
	rec := httptest.NewRecorder()
	handler.search(rec, httptest.NewRequest(http.MethodGet, "/nostr/search?query=sovran&limit=2", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp ProvidersEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Identities) != 2 {
		t.Fatalf("identities = %d, want one per ranked pubkey: %+v", len(resp.Identities), resp.Identities)
	}
	id := resp.Identities[testPubkey]
	if id.Vertex.Score == nil || *id.Vertex.Score != score || *id.Vertex.Rank != rank {
		t.Fatalf("vertex = %+v, want the ranked row's figures", id.Vertex)
	}
	if id.Profile == nil || id.Profile.DisplayName != "Sovran" {
		t.Fatalf("profile = %+v", id.Profile)
	}
	if id.Reach.Followers == nil || *id.Reach.Followers != 12 {
		t.Fatalf("reach = %+v, want the pubkey_stats fallback", id.Reach)
	}
	other := resp.Identities[identityB]
	if other.Profile != nil || other.Vertex.Rank != nil || other.Npub != vertex.Npub(identityB) {
		t.Fatalf("unindexed ranked pubkey = %+v", other)
	}
}

func TestMintInfoCarriesOperator(t *testing.T) {
	history := keyedHistory{"https://stored.example": json.RawMessage(`{"name":"Stored","contact":[{"method":"nostr","info":"` + identityB + `"}]}`)}
	handler := New(mintReviewStore{fakeStore: fakeStore{
		profiles: map[string]chstore.K0Row{identityA: {PubKey: identityA, Name: "alice"}},
	}}, WithNIP05Validation(false),
		WithAuditor(fakeAuditor{mints: []auditor.Mint{{URL: "https://audited.example", Name: "Audited", OperatorContact: vertex.Npub(identityA)}}}),
		WithMintHistory(history), WithTestnutMints(fakeTestnuts{}))

	rec := httptest.NewRecorder()
	handler.mintInfos(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/info?u=https%3A%2F%2Faudited.example&u=https%3A%2F%2Fstored.example&u=https%3A%2F%2Fnobody.example", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}
	var resp MintInfoResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Mints[0].OperatorPubkey != identityA || resp.Mints[1].OperatorPubkey != identityB {
		t.Fatalf("operatorPubkey = %q / %q", resp.Mints[0].OperatorPubkey, resp.Mints[1].OperatorPubkey)
	}
	if strings.Count(rec.Body.String(), `"operatorPubkey"`) != 2 {
		t.Fatalf("operatorPubkey must be omitted when the mint publishes none: %s", rec.Body)
	}
	if len(resp.Identities) != 2 || resp.Identities[identityA].Profile == nil || resp.Identities[identityA].Profile.Name != "alice" {
		t.Fatalf("identities = %+v", resp.Identities)
	}
	if got := resp.Identities[identityA].Operates.Mints; len(got) != 1 || got[0] != "https://audited.example" {
		t.Fatalf("operates.mints = %v", got)
	}
}

func TestMintReviewsCarriesIdentities(t *testing.T) {
	const op = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	store := mintReviewStore{
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{
			identityA: {PubKey: identityA, DisplayName: "Alice"},
			op:        {PubKey: op, DisplayName: "Operator"},
		}},
		events: []chstore.EventView{
			reviewEvent("1", identityA, mintA, "great [5/5]", 100),
			reviewEvent("2", identityB, mintA, "ok [3/5]", 90),
		},
	}
	handler := New(store, WithNIP05Validation(false),
		WithAuditor(fakeAuditor{mints: []auditor.Mint{{URL: mintA, OperatorContact: op}}}),
		WithSocialReach(stubReach{identityA: socialgraph.FromVertex(5, 1)}))

	rec := httptest.NewRecorder()
	handler.mintReviews(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/reviews?u="+mintA, nil))
	var resp MintReviewsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Identities) != 3 {
		t.Fatalf("identities = %d, want both reviewers and the operator: %+v", len(resp.Identities), resp.Identities)
	}
	if alice := resp.Identities[identityA]; alice.Profile == nil || alice.Profile.DisplayName != "Alice" || alice.Reach.Followers == nil || *alice.Reach.Followers != 5 || alice.Reach.Source != socialgraph.SourceVertex {
		t.Fatalf("alice = %+v", alice)
	}
	if bob := resp.Identities[identityB]; bob.Profile != nil || bob.Reach.Followers != nil {
		t.Fatalf("bob (no kind-0, unresolved reach) = %+v", bob)
	}
	if operator := resp.Identities[op]; operator.Profile == nil || operator.Profile.DisplayName != "Operator" || len(operator.Operates.Mints) != 1 {
		t.Fatalf("operator = %+v", operator)
	}
	if _, ok := resp.Profiles[op]; ok {
		t.Fatal("profiles is the reviewers' map and must not grow the operator")
	}
}

// keyedHistory answers a stored NUT-06 document per mint URL, and nothing for
// the rest.
type keyedHistory map[string]json.RawMessage

func (k keyedHistory) LatestInfo(_ context.Context, mint string) (json.RawMessage, error) {
	if doc, ok := k[mint]; ok {
		return doc, nil
	}
	return nil, errors.New("no snapshot")
}

func (keyedHistory) History(context.Context, string, bool) (*mintinfo.History, bool, error) {
	return nil, false, nil
}

func (keyedHistory) GlobalChanges(context.Context, int) (*mintinfo.GlobalChanges, error) {
	return nil, nil
}
