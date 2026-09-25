package aiproviders

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/nagg/internal/relayquery"
	"github.com/vertex-lab/nagg/internal/socialgraph"
)

var providersNow = time.Unix(1_790_000_000, 0).UTC() // fixed "now" for deterministic statuses

// insecureClient lets the tests point an https-only service at httptest TLS
// servers. Production stays https-only on purpose: a .onion base is
// unreachable without Tor and an http base would put a bearer Cashu token on
// the wire in the clear.
func insecureClient() *http.Client {
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}}
}

// catalogBody is a node's /v1/models answer: n models, of which the first
// `sealed` carry the "tinfoil-" prefix a client seals on, and the next
// `teeOnly` declare a tinfoil upstream WITHOUT the prefix — the live shape
// that makes the two counts differ.
func catalogBody(n, sealed, teeOnly int) string {
	body := `{"data":[`
	for i := 0; i < n; i++ {
		id, upstream := fmt.Sprintf("model-%d", i), "openrouter"
		switch {
		case i < sealed:
			id, upstream = fmt.Sprintf("tinfoil-model-%d", i), "tinfoil"
		case i < sealed+teeOnly:
			upstream = "tinfoil"
		}
		if i > 0 {
			body += ","
		}
		body += fmt.Sprintf(
			`{"id":%q,"enabled":true,"context_length":200000,"upstream_provider_id":%q,"architecture":{"output_modalities":["text"]},"sats_pricing":{"completion":1,"max_cost":10}}`,
			id, upstream)
	}
	return body + `]}`
}

// node is a Routstr provider: /v1/info and /v1/models, switchable off to
// simulate an outage rather than a removal.
type node struct {
	*httptest.Server
	up      atomic.Bool
	name    string
	models  int
	sealed  int
	teeOnly int
}

func newNode(t *testing.T, name string, models, sealed, teeOnly int) *node {
	t.Helper()
	n := &node{name: name, models: models, sealed: sealed, teeOnly: teeOnly}
	n.up.Store(true)
	n.Server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !n.up.Load() {
			// A dead node does not answer politely; it does not answer.
			hijacked, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = hijacked.Close()
			}
			return
		}
		switch r.URL.Path {
		case "/v1/info":
			fmt.Fprintf(w, `{"name":%q,"mints":["https://mint.example/Bitcoin"]}`, n.name)
		case "/v1/models":
			fmt.Fprint(w, catalogBody(n.models, n.sealed, n.teeOnly))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(n.Close)
	return n
}

func newTestService(t *testing.T, cfg Config, relays RelayFetcher, followers FollowerCounter) *Service {
	t.Helper()
	s := NewService(cfg, relays, followers, nil)
	s.http = insecureClient()
	s.now = func() time.Time { return providersNow }
	return s
}

func followers(n uint64) *uint64 { return &n }

// graph/relays build a published row's reach fields the way render does.
func byURL(d Directory, url string) (Provider, bool) {
	for _, p := range d.Providers {
		if p.BaseURL == url {
			return p, true
		}
	}
	return Provider{}, false
}

// TestSortIsServerSideAndTotal pins the published order: online, then unknown,
// then offline; sealed models first; followers descending; and a total
// tiebreak so two equal rows never swap between requests.
func TestSortIsServerSideAndTotal(t *testing.T) {
	providers := []Provider{
		{BaseURL: "https://z-offline", Status: StatusOffline, Followers: followers(9999), EncryptedModelCount: 5},
		{BaseURL: "https://b-online-plain", Status: StatusOnline, Followers: followers(500)},
		{BaseURL: "https://a-online-plain", Status: StatusOnline, Followers: followers(500)},
		{BaseURL: "https://c-online-sealed", Status: StatusOnline, Followers: followers(1), EncryptedModelCount: 9, TEEModelCount: 13},
		// Declared enclave hosting with nothing the client will seal. The
		// boost is for end-to-end encryption, so this row must rank purely on
		// followers alongside the plaintext providers.
		{BaseURL: "https://f-online-tee-only", Status: StatusOnline, Followers: followers(600), TEEModelCount: 13},
		{BaseURL: "https://d-unknown", Status: StatusUnknown, Followers: followers(100_000)},
		{BaseURL: "https://e-online-popular", Status: StatusOnline, Followers: followers(800)},
	}
	Sort(providers)

	want := []string{
		"https://c-online-sealed",   // online + sealed beats every plaintext peer
		"https://e-online-popular",  // then followers, descending
		"https://f-online-tee-only", // a TEE claim alone earns no boost
		"https://a-online-plain",    // equal followers → base URL, a total order
		"https://b-online-plain",
		"https://d-unknown", // unknown outranks offline however popular it is
		"https://z-offline",
	}
	for i, url := range want {
		if providers[i].BaseURL != url {
			t.Fatalf("position %d = %q, want %q (full order: %v)", i, providers[i].BaseURL, url, urls(providers))
		}
	}

	// Sorting the already-sorted slice must not move anything: the app sees
	// the same order on every request, not a churn of equal rows.
	before := urls(providers)
	Sort(providers)
	for i := range before {
		if providers[i].BaseURL != before[i] {
			t.Fatalf("re-sorting churned the order: %v then %v", before, urls(providers))
		}
	}
}

func urls(providers []Provider) []string {
	out := make([]string, 0, len(providers))
	for _, p := range providers {
		out = append(out, p.BaseURL)
	}
	return out
}

// TestUnknownIsDistinctFromOffline is the contract's sharpest edge: the app
// sorts and labels on the difference, so "not established" must never render
// as "we watched it fail".
func TestUnknownIsDistinctFromOffline(t *testing.T) {
	cfg := Config{MaxAge: time.Hour, CatalogMinAge: 30 * time.Minute}.withDefaults()

	never := (&record{baseURL: "https://never.example"}).render(providersNow, cfg.MaxAge)
	if never.Status != StatusUnknown {
		t.Fatalf("never-probed status = %q, want unknown", never.Status)
	}
	if never.CheckedAt != nil {
		t.Fatalf("never-probed carried checkedAt %v; nothing has been established", never.CheckedAt)
	}
	if never.Mints == nil {
		t.Fatal("mints must be an empty list, never null: the payment path reads empty as any-mint")
	}

	failed := (&record{baseURL: "https://down.example", probedAt: providersNow.Add(-time.Minute)}).render(providersNow, cfg.MaxAge)
	if failed.Status != StatusOffline || failed.CheckedAt == nil {
		t.Fatalf("failed probe = %q checkedAt=%v, want offline with a timestamp", failed.Status, failed.CheckedAt)
	}
	if failed.LatencyMs != 0 {
		t.Fatalf("offline row reported latency %dms", failed.LatencyMs)
	}

	// A probe older than MaxAge stops standing for anything. This is what takes
	// the directory to unknown when the sweep worker dies, instead of leaving a
	// stale "online" asserted forever.
	agedOut := (&record{
		baseURL:   "https://stale.example",
		probedAt:  providersNow.Add(-2 * time.Hour),
		reachable: true,
		latency:   120 * time.Millisecond,
	}).render(providersNow, cfg.MaxAge)
	if agedOut.Status != StatusUnknown || agedOut.CheckedAt != nil || agedOut.LatencyMs != 0 {
		t.Fatalf("aged-out probe = %+v, want a clean unknown", agedOut)
	}

	fresh := (&record{
		baseURL:   "https://up.example",
		probedAt:  providersNow.Add(-time.Minute),
		reachable: true,
		latency:   340 * time.Millisecond,
	}).render(providersNow, cfg.MaxAge)
	if fresh.Status != StatusOnline || fresh.LatencyMs != 340 {
		t.Fatalf("fresh probe = %+v, want online with latency", fresh)
	}
}

// TestFailedProbeKeepsProviderListed is the rule that a failure marks a
// provider offline rather than deleting it: dropping the row would read to the
// app as "this provider does not exist", when what nagg knows is "it did not
// answer".
func TestFailedProbeKeepsProviderListed(t *testing.T) {
	good := newNode(t, "steady", 4, 0, 0)
	flaky := newNode(t, "flaky", 564, 9, 4)

	s := newTestService(t, Config{
		Seeds:         []string{good.URL, flaky.URL},
		Interval:      5 * time.Minute,
		Timeout:       2 * time.Second,
		CatalogMinAge: 30 * time.Minute,
		MaxAge:        2 * time.Hour,
	}, nil, nil)
	s.RunOnce(context.Background())

	directory, ok := s.Directory()
	if !ok || len(directory.Providers) != 2 {
		t.Fatalf("first sweep: ok=%v providers=%d", ok, len(directory.Providers))
	}
	before, _ := byURL(directory, flaky.URL)
	// The live redsh1ft shape: 564 priced models, 9 the client will seal, 13
	// the node declares as enclave-hosted. The two counts must not collapse
	// into one, or the directory claims 13 while the picker badges 9.
	if before.Status != StatusOnline || before.ModelCount != 564 || before.EncryptedModelCount != 9 || before.TEEModelCount != 13 {
		t.Fatalf("flaky before outage = %+v, want online 564 models / 9 sealed / 13 TEE", before)
	}
	if before.Name != "flaky" || len(before.Mints) != 1 {
		t.Fatalf("node /v1/info did not fill name and mints: %+v", before)
	}

	flaky.up.Store(false)
	s.RunOnce(context.Background())

	directory, _ = s.Directory()
	if len(directory.Providers) != 2 {
		t.Fatalf("a failed probe dropped the provider: %v", urls(directory.Providers))
	}
	after, listed := byURL(directory, flaky.URL)
	if !listed || after.Status != StatusOffline {
		t.Fatalf("flaky after outage = %+v, want offline and still listed", after)
	}
	// Counts survive the outage: what a provider serves when it is up is still
	// the best answer to "how big is it" while it is down.
	if after.ModelCount != 564 || after.EncryptedModelCount != 9 || after.TEEModelCount != 13 {
		t.Fatalf("outage erased the catalog counts: %+v", after)
	}
	// The healthy node must outrank it.
	if directory.Providers[0].BaseURL != good.URL {
		t.Fatalf("offline provider ranked above the healthy one: %v", urls(directory.Providers))
	}
	if directory.TTLSeconds != 300 {
		t.Fatalf("ttlSeconds = %d, want the sweep interval in seconds", directory.TTLSeconds)
	}
}

// TestDirectoryWarmsBeforeItServes covers the other 503: configured but with
// nothing discovered yet.
func TestDirectoryWarmsBeforeItServes(t *testing.T) {
	s := newTestService(t, Config{}, nil, nil)
	if _, ok := s.Directory(); ok {
		t.Fatal("an empty service must report not-ready so the route 503s")
	}
	s.absorb(found{baseURL: "https://seeded.example"})
	directory, ok := s.Directory()
	if !ok || len(directory.Providers) != 1 || directory.Providers[0].Status != StatusUnknown {
		t.Fatalf("a discovered-but-unprobed provider should serve as unknown: %+v", directory)
	}
}

// stubRelays replays fixed announcements, so discovery is tested without a relay.
type stubRelays struct {
	events []relayquery.Event
	err    error
}

func (s stubRelays) Query(context.Context, map[string]any, time.Duration) ([]relayquery.Event, error) {
	return s.events, s.err
}

func announcement(pubkey string, tags nostr.Tags, content string) relayquery.Event {
	return relayquery.Event{Event: &nostr.Event{Kind: AnnouncementKind, PubKey: pubkey, Tags: tags, Content: content}}
}

const (
	signerA = "aa3f3bf381ac923afcf5a3c16fb2957de94057de84df0c3e84a44c57fa031482"
	signerB = "d5637c99179be3051f2363d394c9ff2cd840a48881525417983ce5fb347e455b"
)

// TestDiscoveryMergesBothShapesAndFillsGaps covers the two announcement forms
// in the wild plus the merge rule: later sources fill gaps, they do not
// overwrite, because none of them is a complete record on its own.
func TestDiscoveryMergesBothShapesAndFillsGaps(t *testing.T) {
	s := newTestService(t, Config{
		Relays: []string{"wss://relay.example"},
		Seeds:  []string{"https://seed.example/v1/"},
	}, stubRelays{events: []relayquery.Event{
		// `u`-tag shape: endpoints on tags, name on a tag, signer is the operator.
		announcement(signerA, nostr.Tags{{"u", "https://tagged.example/"}, {"name", "Tagged Node"}}, ""),
		// JSON-content shape: the row carries its own key and the mints.
		announcement(signerB, nil, `{"providers":[{"endpoint_url":"https://json.example","name":"Json Node","mint_urls":["https://mint.example/Bitcoin"]}]}`),
		// Unusable rows: onion and plain http can never be paid from the app.
		announcement(signerB, nostr.Tags{{"u", "http://plain.example"}, {"u", "http://xyz.onion"}}, ""),
		// A kind-38421 event that is not a Routstr announcement at all.
		announcement(signerB, nostr.Tags{{"d", "lnproxy-v1"}}, "not json"),
	}}, nil)

	s.discover(context.Background())

	directory, ok := s.Directory()
	if !ok {
		t.Fatal("discovery produced nothing")
	}
	if len(directory.Providers) != 3 {
		t.Fatalf("providers = %v, want the seed plus the two usable announcements", urls(directory.Providers))
	}
	tagged, listed := byURL(directory, "https://tagged.example")
	if !listed || tagged.Name != "Tagged Node" || tagged.Pubkey != signerA {
		t.Fatalf("u-tag announcement = %+v", tagged)
	}
	jsonRow, listed := byURL(directory, "https://json.example")
	if !listed || jsonRow.Name != "Json Node" || jsonRow.Pubkey != signerB || len(jsonRow.Mints) != 1 {
		t.Fatalf("json announcement = %+v", jsonRow)
	}
	// The seed is normalized, not listed twice under two spellings.
	if _, listed := byURL(directory, "https://seed.example"); !listed {
		t.Fatalf("seed url was not normalized: %v", urls(directory.Providers))
	}

	// A later announcement must not overwrite a name that is already known.
	s.absorb(found{baseURL: "https://tagged.example", name: "Renamed", mints: []string{"https://mint.example/Bitcoin"}})
	directory, _ = s.Directory()
	tagged, _ = byURL(directory, "https://tagged.example")
	if tagged.Name != "Tagged Node" || len(tagged.Mints) != 1 {
		t.Fatalf("merge overwrote instead of filling gaps: %+v", tagged)
	}
}

// TestRelayFailureLeavesSeedsStanding: relays are the registry, but a picker
// that empties itself when a relay is unreachable is worse than one that falls
// back to the nodes nagg is configured to pay.
func TestRelayFailureLeavesSeedsStanding(t *testing.T) {
	s := newTestService(t, Config{
		Relays: []string{"wss://relay.example"},
		Seeds:  []string{"https://seed.example"},
	}, stubRelays{err: context.DeadlineExceeded}, nil)
	s.discover(context.Background())
	directory, ok := s.Directory()
	if !ok || len(directory.Providers) != 1 {
		t.Fatalf("relay outage emptied the directory: ok=%v %v", ok, urls(directory.Providers))
	}
}

// stubFollowers stands in for the shared reach resolver.
type stubFollowers struct {
	counts map[string]socialgraph.Reach
	err    error
}

func (s stubFollowers) Reach(_ context.Context, _ []string) (map[string]socialgraph.Reach, error) {
	return s.counts, s.err
}

func TestFollowerCountsRankAndDegrade(t *testing.T) {
	quiet := newNode(t, "quiet", 2, 0, 0)
	loud := newNode(t, "loud", 2, 0, 0)
	cfg := Config{Seeds: []string{quiet.URL, loud.URL}, Timeout: 2 * time.Second}

	s := newTestService(t, cfg, nil, stubFollowers{counts: map[string]socialgraph.Reach{}})
	s.absorb(found{baseURL: normalizeBaseURL(loud.URL), pubkey: signerA})
	s.followers = stubFollowers{counts: map[string]socialgraph.Reach{signerA: socialgraph.FromGraph(1234, 7)}}
	s.RunOnce(context.Background())

	directory, _ := s.Directory()
	row, _ := byURL(directory, normalizeBaseURL(loud.URL))
	if row.Followers == nil || *row.Followers != 1234 || row.FollowersSource != socialgraph.SourceGraph {
		t.Fatalf("followers = %v/%q, want the resolved count and its source", row.Followers, row.FollowersSource)
	}
	if directory.Providers[0].BaseURL != normalizeBaseURL(loud.URL) {
		t.Fatalf("follower count did not break the tie: %v", urls(directory.Providers))
	}
	// The seed with no operator pubkey has nobody to count: a real zero, not
	// an unresolved one, so it publishes 0 rather than null.
	quietRow, _ := byURL(directory, normalizeBaseURL(quiet.URL))
	if quietRow.Followers == nil || *quietRow.Followers != 0 {
		t.Fatalf("a provider with no pubkey = %v, want a known zero", quietRow.Followers)
	}

	// A resolver that fails must not erase a count it already established, and
	// must not invent one for the rows it never answered.
	s.followers = stubFollowers{err: context.DeadlineExceeded}
	s.RunOnce(context.Background())
	directory, ok := s.Directory()
	if !ok || len(directory.Providers) != 2 {
		t.Fatalf("a failed reach read broke the sweep: ok=%v %v", ok, urls(directory.Providers))
	}
	row, _ = byURL(directory, normalizeBaseURL(loud.URL))
	if row.Followers == nil || *row.Followers != 1234 {
		t.Fatalf("a failed reach read erased an established count: %v", row.Followers)
	}
}

// TestCatalogReadIsRateLimited proves the expensive request is not the status
// probe: /v1/models is three quarters of a megabyte on the largest live node,
// so /v1/info carries status between catalog reads.
func TestCatalogReadIsRateLimited(t *testing.T) {
	var infoHits, catalogHits atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/info":
			infoHits.Add(1)
			fmt.Fprint(w, `{"name":"chatty","mints":[]}`)
		case "/v1/models":
			catalogHits.Add(1)
			fmt.Fprint(w, catalogBody(3, 1, 0))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	s := newTestService(t, Config{Seeds: []string{server.URL}, CatalogMinAge: 30 * time.Minute}, nil, nil)
	for range 3 {
		s.RunOnce(context.Background())
	}
	if infoHits.Load() != 3 || catalogHits.Load() != 1 {
		t.Fatalf("info=%d catalog=%d, want a catalog read only on the first sweep", infoHits.Load(), catalogHits.Load())
	}

	// Past CatalogMinAge the counts are re-established.
	s.now = func() time.Time { return providersNow.Add(time.Hour) }
	s.RunOnce(context.Background())
	if catalogHits.Load() != 2 {
		t.Fatalf("catalog reads = %d, want a refresh once CatalogMinAge elapsed", catalogHits.Load())
	}
}

// TestNodeWithoutInfoRouteStaysOnline: older nodes 404 /v1/info. That is a
// node saying nothing, not a node being down, and the sweep must stop paying
// for the request that can only fail.
func TestNodeWithoutInfoRouteStaysOnline(t *testing.T) {
	var infoHits atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/info" {
			infoHits.Add(1)
			http.Error(w, `{"detail":"Not found"}`, http.StatusNotFound)
			return
		}
		fmt.Fprint(w, catalogBody(5, 2, 1))
	}))
	defer server.Close()

	s := newTestService(t, Config{Seeds: []string{server.URL}, CatalogMinAge: 30 * time.Minute}, nil, nil)
	s.RunOnce(context.Background())
	s.RunOnce(context.Background())

	directory, _ := s.Directory()
	row := directory.Providers[0]
	if row.Status != StatusOnline || row.ModelCount != 5 || row.EncryptedModelCount != 2 || row.TEEModelCount != 3 {
		t.Fatalf("legacy node = %+v, want online with both counts from the catalog", row)
	}
	if infoHits.Load() != 1 {
		t.Fatalf("/v1/info asked %d times; a node that 404s it should be asked once", infoHits.Load())
	}
}

func TestNormalizeBaseURLCollapsesSpellings(t *testing.T) {
	for _, raw := range []string{
		"https://node.example", "https://node.example/", "https://node.example/v1",
		"https://node.example/v1/", " https://node.example//",
	} {
		if got := normalizeBaseURL(raw); got != "https://node.example" {
			t.Fatalf("normalizeBaseURL(%q) = %q", raw, got)
		}
	}
	for _, raw := range []string{"", "http://node.example", "http://xyz.onion", "https://xyz.onion", "wss://relay.example", "https://"} {
		if got := normalizeBaseURL(raw); got != "" {
			t.Fatalf("normalizeBaseURL(%q) = %q, want dropped", raw, got)
		}
	}
}

func TestDecodeNpub(t *testing.T) {
	// The npub the live node routstr.otrta.me publishes on /v1/info.
	const npub = "npub18kpn83drge7x9vz4cuhh7xta79sl4tfq55se4e554yj90s8y3f7qa49nps"
	hex := decodeNpub(npub)
	if len(hex) != 64 {
		t.Fatalf("decodeNpub(%q) = %q, want 64 hex chars", npub, hex)
	}
	if got := decodeNpub(signerA); got != signerA {
		t.Fatalf("a bare hex key must pass through, got %q", got)
	}
	for _, raw := range []string{"", "not-an-npub", "nprofile1qqs"} {
		if got := decodeNpub(raw); got != "" {
			t.Fatalf("decodeNpub(%q) = %q, want empty", raw, got)
		}
	}
}
