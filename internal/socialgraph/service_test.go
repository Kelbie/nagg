package socialgraph

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/nagg/internal/relayquery"
)

const (
	alice = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	bob   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	carol = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

var socialNow = time.Unix(1_790_000_000, 0).UTC()

type stubSource struct {
	counts map[string]Reach
	err    error
	asked  [][]string
	mu     sync.Mutex
}

func (s *stubSource) Followers(_ context.Context, pubkeys []string) (map[string]Reach, error) {
	s.mu.Lock()
	s.asked = append(s.asked, append([]string(nil), pubkeys...))
	s.mu.Unlock()
	return s.counts, s.err
}

type stubScanner struct {
	counts map[string]uint64
	errs   map[string]error
	mu     sync.Mutex
	calls  []string
}

func (s *stubScanner) ScanFollowers(_ context.Context, pubkey string) (uint64, error) {
	s.mu.Lock()
	s.calls = append(s.calls, pubkey)
	s.mu.Unlock()
	if err, ok := s.errs[pubkey]; ok {
		return 0, err
	}
	n, ok := s.counts[pubkey]
	if !ok {
		return 0, errors.New("no answer")
	}
	return n, nil
}

func newTestService(t *testing.T, stats StatsSource, vertexCache VertexSource, relays RelayScanner) *Service {
	t.Helper()
	s := NewService(Config{}, stats, vertexCache, relays, nil)
	s.now = func() time.Time { return socialNow }
	return s
}

// TestReachNeverBlocksAndNeverInventsAZero is the defect this package exists
// for. Two live endpoints reported every operator as having 0 followers,
// because the only source they read is empty on a mint deployment and the
// emptiness was rendered as the number.
func TestReachNeverBlocksAndNeverInventsAZero(t *testing.T) {
	scanner := &stubScanner{counts: map[string]uint64{alice: 174}}
	s := newTestService(t, nil, nil, scanner)

	// First read: nothing is resolved, so nothing is claimed.
	got, err := s.Reach(context.Background(), []string{alice, bob})
	if err != nil {
		t.Fatal(err)
	}
	for _, pubkey := range []string{alice, bob} {
		if got[pubkey].Known {
			t.Fatalf("%s answered before anything was resolved: %+v", pubkey[:8], got[pubkey])
		}
	}
	if len(scanner.calls) != 0 {
		t.Fatal("a read hit the relays; request paths must never pay for a scan")
	}

	s.RunOnce(context.Background())

	got, _ = s.Reach(context.Background(), []string{alice, bob})
	if !got[alice].Known || got[alice].Followers != 174 {
		t.Fatalf("alice = %+v, want a resolved 174", got[alice])
	}
	if got[alice].Source != SourceRelays || !got[alice].Approximate {
		t.Fatalf("a relay count must name its source and admit it is a floor: %+v", got[alice])
	}
	// Bob's scan failed. He stays unknown rather than becoming a zero, and is
	// retried rather than cached as a failure.
	if got[bob].Known {
		t.Fatalf("a failed scan was recorded as an answer: %+v", got[bob])
	}
	scanner.counts[bob] = 3
	s.RunOnce(context.Background())
	got, _ = s.Reach(context.Background(), []string{bob})
	if !got[bob].Known || got[bob].Followers != 3 {
		t.Fatalf("bob was not retried: %+v", got[bob])
	}
}

// TestSourcePrecedenceStopsAtTheFirstExactAnswer: the exact sources are
// batched table reads and the relay scan costs seconds per pubkey, so a
// pubkey the graph can answer must never reach the relays.
func TestSourcePrecedenceStopsAtTheFirstExactAnswer(t *testing.T) {
	stats := &stubSource{counts: map[string]Reach{alice: FromGraph(1234, 56)}}
	vertexCache := &stubSource{counts: map[string]Reach{bob: FromVertex(99, 12)}}
	scanner := &stubScanner{counts: map[string]uint64{carol: 7}}
	s := newTestService(t, stats, vertexCache, scanner)

	s.Reach(context.Background(), []string{alice, bob, carol})
	s.RunOnce(context.Background())
	got, _ := s.Reach(context.Background(), []string{alice, bob, carol})

	if got[alice].Source != SourceGraph || got[alice].Followers != 1234 || got[alice].Approximate {
		t.Fatalf("alice = %+v, want the exact graph count", got[alice])
	}
	if got[bob].Source != SourceVertex || got[bob].Followers != 99 || got[bob].Approximate {
		t.Fatalf("bob = %+v, want the exact Vertex count", got[bob])
	}
	if got[carol].Source != SourceRelays || got[carol].Followers != 7 {
		t.Fatalf("carol = %+v, want the relay fallback", got[carol])
	}
	// Only the pubkey neither exact source answered may cost a scan.
	if len(scanner.calls) != 1 || scanner.calls[0] != carol {
		t.Fatalf("relay scans = %v, want only carol", scanner.calls)
	}
	// And the second source is asked only about what the first could not answer.
	if len(vertexCache.asked) != 1 || len(vertexCache.asked[0]) != 2 {
		t.Fatalf("vertex was asked %v; it should skip what the graph resolved", vertexCache.asked)
	}
}

// TestGraphOutageFallsThroughInsteadOfPublishingZero: a deployment without the
// nostr module has no pubkey_stats. That must degrade to the next source, not
// to a confident zero.
func TestGraphOutageFallsThroughInsteadOfPublishingZero(t *testing.T) {
	stats := &stubSource{err: errors.New("no such table")}
	scanner := &stubScanner{counts: map[string]uint64{alice: 174}}
	s := newTestService(t, stats, nil, scanner)

	s.Reach(context.Background(), []string{alice})
	s.RunOnce(context.Background())
	got, _ := s.Reach(context.Background(), []string{alice})
	if !got[alice].Known || got[alice].Followers != 174 || got[alice].Source != SourceRelays {
		t.Fatalf("alice = %+v, want the relay fallback after the graph failed", got[alice])
	}

	// With no fallback wired either, the honest answer is Unknown.
	bare := newTestService(t, stats, nil, nil)
	bare.Reach(context.Background(), []string{alice})
	bare.RunOnce(context.Background())
	got, _ = bare.Reach(context.Background(), []string{alice})
	if got[alice].Known {
		t.Fatalf("with no usable source the answer must be Unknown, got %+v", got[alice])
	}
}

func TestExpiredAnswersAreReResolved(t *testing.T) {
	scanner := &stubScanner{counts: map[string]uint64{alice: 10}}
	s := NewService(Config{TTL: time.Hour}, nil, nil, scanner, nil)
	now := socialNow
	s.now = func() time.Time { return now }

	s.Reach(context.Background(), []string{alice})
	s.RunOnce(context.Background())
	if got, _ := s.Reach(context.Background(), []string{alice}); got[alice].Followers != 10 {
		t.Fatalf("first resolve = %+v", got[alice])
	}

	// Inside the TTL nothing is re-scanned.
	s.RunOnce(context.Background())
	if len(scanner.calls) != 1 {
		t.Fatalf("scans = %d inside the TTL, want 1", len(scanner.calls))
	}

	now = socialNow.Add(2 * time.Hour)
	scanner.counts[alice] = 20
	s.RunOnce(context.Background())
	got, _ := s.Reach(context.Background(), []string{alice})
	if got[alice].Followers != 20 {
		t.Fatalf("expired answer was not refreshed: %+v", got[alice])
	}
}

func TestMaxTrackedBoundsTheCrawl(t *testing.T) {
	scanner := &stubScanner{counts: map[string]uint64{}}
	s := NewService(Config{MaxTracked: 2}, nil, nil, scanner, nil)
	s.now = func() time.Time { return socialNow }
	s.Reach(context.Background(), []string{alice, bob, carol})
	s.mu.Lock()
	pending := len(s.pending)
	s.mu.Unlock()
	if pending != 2 {
		t.Fatalf("queued %d pubkeys, want the cap of 2: announcements are free to publish", pending)
	}
}

func TestReachIgnoresUnusablePubkeys(t *testing.T) {
	scanner := &stubScanner{counts: map[string]uint64{}}
	s := newTestService(t, nil, nil, scanner)
	got, _ := s.Reach(context.Background(), []string{"", "npub1xyz", "NOTHEX", alice[:40]})
	if len(got) != 0 {
		t.Fatalf("unusable keys produced answers: %+v", got)
	}
	s.mu.Lock()
	pending := len(s.pending)
	s.mu.Unlock()
	if pending != 0 {
		t.Fatal("queued a string that is not a pubkey; no amount of scanning resolves it")
	}
}

// TestCompareBestPutsUnknownBetween is the ranking half of the unknown/zero
// distinction, and the rule both endpoints share.
func TestCompareBestPutsUnknownBetween(t *testing.T) {
	popular := FromGraph(1000, 0)
	quiet := FromGraph(0, 0)
	unresolved := Unknown()

	if !CompareBest(popular, unresolved) {
		t.Fatal("a known count must outrank an unresolved one")
	}
	if !CompareBest(unresolved, quiet) {
		t.Fatal("an unresolved count must outrank a measured zero: not asking is not evidence of nobody")
	}
	if !CompareBest(popular, quiet) || CompareBest(quiet, popular) {
		t.Fatal("known counts must order by size")
	}
	if CompareBest(unresolved, unresolved) {
		t.Fatal("equal values must not claim an order")
	}
	// None() is a measured zero — a subject with nobody to count — so it ranks
	// with the zeros, not with the unknowns.
	if CompareBest(None(), unresolved) {
		t.Fatal("a subject with no operator identity is a known zero, not an unknown")
	}
}

func TestNormalizePubkey(t *testing.T) {
	if got := NormalizePubkey("  " + carol + "  "); got != carol {
		t.Fatalf("NormalizePubkey = %q", got)
	}
	for _, raw := range []string{"", "npub1abc", carol[:63], carol + "d", "gggggggg"} {
		if got := NormalizePubkey(raw); got != "" {
			t.Fatalf("NormalizePubkey(%q) = %q, want dropped", raw, got)
		}
	}
}

// fakeRelays replays kind-3 contact lists per relay, so the scan's counting
// rule and its failure rule are tested without a relay.
type fakeRelays struct {
	mu      sync.Mutex
	byRelay map[string][]relayquery.Event
	errs    map[string]error
	filter  map[string]any
}

func (f *fakeRelays) QueryOne(_ context.Context, relay string, filter map[string]any, _ time.Duration) ([]relayquery.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.filter = filter
	if err, ok := f.errs[relay]; ok {
		return nil, err
	}
	return f.byRelay[relay], nil
}

func contactList(author string) relayquery.Event {
	return relayquery.Event{Event: &nostr.Event{Kind: 3, PubKey: author}}
}

func scannerFor(relays *fakeRelays, names ...string) relayScanner {
	return relayScanner{client: relays, relays: names, timeout: time.Second}
}

// TestRelayScanCountsDistinctAuthors: the same contact list served by three
// relays is one follower, not three.
func TestRelayScanCountsDistinctAuthors(t *testing.T) {
	relays := &fakeRelays{byRelay: map[string][]relayquery.Event{
		"wss://a": {contactList(alice), contactList(bob), {}}, // a nil event must not count
		"wss://b": {contactList(alice)},                       // the same author again
	}}
	n, err := scannerFor(relays, "wss://a", "wss://b").ScanFollowers(context.Background(), carol)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("followers = %d, want 2 distinct authors", n)
	}
	if kinds, _ := relays.filter["kinds"].([]int); len(kinds) != 1 || kinds[0] != 3 {
		t.Fatalf("filter = %v, want kind 3", relays.filter)
	}
	if tagged, _ := relays.filter["#p"].([]string); len(tagged) != 1 || tagged[0] != carol {
		t.Fatalf("filter = %v, want a #p tag naming the target", relays.filter)
	}
}

// TestRelayScanSeparatesNobodyFromNobodyAnswered is the second defect this
// package hit in production. The shared fan-out Query reports an error
// whenever it ends up with zero events, so "a relay answered and nobody
// follows this pubkey" and "every relay fell over" arrived identical — and on
// a real relay set, where something is always failing, every unfollowed
// operator stuck at unresolved forever and was rescanned every pass.
func TestRelayScanSeparatesNobodyFromNobodyAnswered(t *testing.T) {
	// One relay answers with nothing while the other fails. Somebody looked:
	// the floor is a measured zero.
	partial := &fakeRelays{
		byRelay: map[string][]relayquery.Event{"wss://up": nil},
		errs:    map[string]error{"wss://down": errors.New("bad handshake")},
	}
	n, err := scannerFor(partial, "wss://up", "wss://down").ScanFollowers(context.Background(), carol)
	if err != nil || n != 0 {
		t.Fatalf("partial failure over an empty result = %d, %v; one relay answered, so zero is the answer", n, err)
	}

	// A partial failure with real events still yields a floor from the relays
	// that did answer.
	mixed := &fakeRelays{
		byRelay: map[string][]relayquery.Event{"wss://up": {contactList(alice)}},
		errs:    map[string]error{"wss://down": errors.New("timeout")},
	}
	if n, err := scannerFor(mixed, "wss://up", "wss://down").ScanFollowers(context.Background(), carol); err != nil || n != 1 {
		t.Fatalf("partial scan = %d, %v; the relays that answered still count", n, err)
	}

	// Nobody answered: that is not a zero, and the pubkey must stay unresolved.
	down := &fakeRelays{errs: map[string]error{
		"wss://a": errors.New("bad handshake"),
		"wss://b": errors.New("timeout"),
	}}
	if _, err := scannerFor(down, "wss://a", "wss://b").ScanFollowers(context.Background(), carol); err == nil {
		t.Fatal("a total relay failure returned a count; it must error so the pubkey stays unknown")
	}

	// No relays configured at all is the same kind of nothing.
	if _, err := scannerFor(&fakeRelays{}).ScanFollowers(context.Background(), carol); err == nil {
		t.Fatal("scanning with no relays returned a count")
	}
}

// TestRelayScanZeroIsAnAnswer closes the loop through the service: a measured
// zero resolves and stops being rescanned, where an unresolved one does not.
func TestRelayScanZeroIsAnAnswer(t *testing.T) {
	relays := &fakeRelays{byRelay: map[string][]relayquery.Event{"wss://up": nil}}
	s := newTestService(t, nil, nil, scannerFor(relays, "wss://up"))
	s.Reach(context.Background(), []string{alice})
	s.RunOnce(context.Background())
	got, _ := s.Reach(context.Background(), []string{alice})
	if !got[alice].Known || got[alice].Followers != 0 || got[alice].Source != SourceRelays {
		t.Fatalf("alice = %+v, want a measured zero rather than an unresolved one", got[alice])
	}
}
