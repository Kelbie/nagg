package mintinfo

import (
	"context"
	"strings"
	"testing"
	"time"
)

// --- fakes ------------------------------------------------------------------

type fakeStore struct {
	snaps map[string]map[string][]byte // url -> hash -> canonical doc
	obs   []Observation
}

func newFakeStore() *fakeStore {
	return &fakeStore{snaps: map[string]map[string][]byte{}}
}

func (f *fakeStore) PutSnapshot(_ context.Context, url, hash string, doc []byte, _ time.Time) error {
	if f.snaps[url] == nil {
		f.snaps[url] = map[string][]byte{}
	}
	f.snaps[url][hash] = append([]byte(nil), doc...)
	return nil
}

func (f *fakeStore) PutObservation(_ context.Context, o Observation) error {
	f.obs = append(f.obs, o)
	return nil
}

func (f *fakeStore) LastMintObservations(_ context.Context) (map[string]LastObservation, error) {
	out := map[string]LastObservation{}
	for _, o := range f.obs {
		cur := out[o.MintURL]
		if o.CheckedAt.After(cur.CheckedAt) {
			cur.CheckedAt = o.CheckedAt // max checked, any poll (the due clock)
		}
		out[o.MintURL] = cur
	}
	reachAt := map[string]time.Time{}
	for _, o := range f.obs {
		if o.Reachable && o.Hash != "" && (reachAt[o.MintURL].IsZero() || o.CheckedAt.After(reachAt[o.MintURL])) {
			reachAt[o.MintURL] = o.CheckedAt
			cur := out[o.MintURL]
			cur.Hash = o.Hash // last reachable hash (the change basis)
			out[o.MintURL] = cur
		}
	}
	return out, nil
}

func (f *fakeStore) MintObservations(_ context.Context, url string) ([]Observation, error) {
	var out []Observation
	for _, o := range f.obs {
		if o.MintURL == url {
			out = append(out, o)
		}
	}
	return out, nil
}

func (f *fakeStore) MintSnapshots(_ context.Context, url string, hashes []string) (map[string]Snapshot, error) {
	out := map[string]Snapshot{}
	for _, h := range hashes {
		if doc, ok := f.snaps[url][h]; ok {
			out[h] = Snapshot{Hash: h, Document: append([]byte(nil), doc...)}
		}
	}
	return out, nil
}

type fakeFetcher struct {
	bodies map[string][]byte
	fail   map[string]bool
}

func (f *fakeFetcher) Info(_ context.Context, url string) ([]byte, bool) {
	if f.fail[url] {
		return nil, false
	}
	b, ok := f.bodies[url]
	return b, ok
}

type fakeLister struct{ urls []string }

func (l fakeLister) MintURLs(_ context.Context) ([]string, error) { return l.urls, nil }

func at(clock *time.Time) func() time.Time { return func() time.Time { return *clock } }

// --- tests ------------------------------------------------------------------

func TestNormalizeMintURLPreservesPathCase(t *testing.T) {
	cases := map[string]string{
		"https://mint.minibits.cash/Bitcoin":     "https://mint.minibits.cash/Bitcoin", // path case kept — it's the fetch target
		"https://Mint.Minibits.Cash/Bitcoin/":    "https://mint.minibits.cash/Bitcoin", // host lowered, slash trimmed
		"  https://nofees.testnut.cashu.space  ": "https://nofees.testnut.cashu.space",
		"HTTPS://8333.SPACE:3338/":               "https://8333.space:3338",
		"":                                       "",
	}
	for in, want := range cases {
		if got := NormalizeMintURL(in); got != want {
			t.Errorf("NormalizeMintURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCanonicalizeStripsTimeAndIsOrderStable(t *testing.T) {
	a := []byte(`{"name":"m","time":1,"nuts":{"7":{"supported":true}}}`)
	b := []byte(`{"nuts":{"7":{"supported":true}},"name":"m","time":99999}`)
	ca, err := CashuNUT06.Canonicalize(a)
	if err != nil {
		t.Fatalf("canonicalize a: %v", err)
	}
	cb, err := CashuNUT06.Canonicalize(b)
	if err != nil {
		t.Fatalf("canonicalize b: %v", err)
	}
	if strings.Contains(string(ca), "time") {
		t.Fatalf("volatile time not stripped: %s", ca)
	}
	if Hash(ca) != Hash(cb) {
		t.Fatalf("reordered keys + changed time must hash equal:\n%s\n%s", ca, cb)
	}
}

func TestSnapshotterStoresOnChangeAndObservesEveryPoll(t *testing.T) {
	ctx := context.Background()
	clock := time.Unix(1_700_000_000, 0).UTC()
	store := newFakeStore()
	fetch := &fakeFetcher{bodies: map[string][]byte{"https://m": []byte(`{"version":"0.1","time":1}`)}, fail: map[string]bool{}}
	s := NewSnapshotter(store, fakeLister{[]string{"https://m"}}, fetch, CashuNUT06, Config{MinAge: time.Hour}, nil)
	s.now = at(&clock)

	// 1) first poll — new snapshot + changed observation.
	if _, err := s.RunOnce(ctx); err != nil {
		t.Fatalf("run 1: %v", err)
	}
	if got := len(store.snaps["https://m"]); got != 1 {
		t.Fatalf("snapshots after first poll = %d, want 1", got)
	}
	if len(store.obs) != 1 || !store.obs[0].Changed || !store.obs[0].Reachable {
		t.Fatalf("first observation = %+v, want changed+reachable", store.obs[0])
	}

	// 2) not due yet — skipped, no observation.
	clock = clock.Add(30 * time.Minute)
	stats, _ := s.RunOnce(ctx)
	if stats.Skipped != 1 || stats.Due != 0 || len(store.obs) != 1 {
		t.Fatalf("within MinAge must skip: stats=%+v obs=%d", stats, len(store.obs))
	}

	// 3) due, only the volatile time changed — observed, NOT a new snapshot.
	clock = clock.Add(2 * time.Hour)
	fetch.bodies["https://m"] = []byte(`{"version":"0.1","time":2}`)
	if _, err := s.RunOnce(ctx); err != nil {
		t.Fatalf("run 3: %v", err)
	}
	if got := len(store.snaps["https://m"]); got != 1 {
		t.Fatalf("time-only change must not snapshot: snapshots = %d, want 1", got)
	}
	if len(store.obs) != 2 || store.obs[1].Changed {
		t.Fatalf("time-only change must observe unchanged: obs=%d changed=%v", len(store.obs), store.obs[1].Changed)
	}

	// 4) real change — new snapshot.
	clock = clock.Add(2 * time.Hour)
	fetch.bodies["https://m"] = []byte(`{"version":"0.2","time":3}`)
	if _, err := s.RunOnce(ctx); err != nil {
		t.Fatalf("run 4: %v", err)
	}
	if got := len(store.snaps["https://m"]); got != 2 {
		t.Fatalf("real change must snapshot: snapshots = %d, want 2", got)
	}
	if !store.obs[2].Changed {
		t.Fatalf("real change must observe changed")
	}
}

func TestSnapshotterUnreachableDoesNotFalseChangeOnRecovery(t *testing.T) {
	ctx := context.Background()
	clock := time.Unix(1_700_000_000, 0).UTC()
	store := newFakeStore()
	fetch := &fakeFetcher{bodies: map[string][]byte{"https://m": []byte(`{"version":"0.1","time":1}`)}, fail: map[string]bool{}}
	s := NewSnapshotter(store, fakeLister{[]string{"https://m"}}, fetch, CashuNUT06, Config{MinAge: time.Hour}, nil)
	s.now = at(&clock)

	s.RunOnce(ctx) // snapshot v1

	clock = clock.Add(2 * time.Hour)
	fetch.fail["https://m"] = true
	s.RunOnce(ctx) // unreachable
	if !store.obs[1].Reachable == false || store.obs[1].Changed {
		t.Fatalf("unreachable poll must be reachable=false, changed=false: %+v", store.obs[1])
	}

	clock = clock.Add(2 * time.Hour)
	fetch.fail["https://m"] = false // recovers, SAME config
	s.RunOnce(ctx)
	if len(store.snaps["https://m"]) != 1 || store.obs[2].Changed {
		t.Fatalf("recovery to same config must not re-snapshot: snaps=%d changed=%v",
			len(store.snaps["https://m"]), store.obs[2].Changed)
	}
}

func TestReaderBuildsInitialPlusNewestFirstRevisions(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	d1 := []byte(`{"version":"Nutshell/0.15.0","nuts":{"7":{"supported":true}}}`)
	d2 := []byte(`{"version":"Nutshell/0.16.0","nuts":{"7":{"supported":true},"17":{"supported":true}}}`)
	c1, _ := CashuNUT06.Canonicalize(d1)
	c2, _ := CashuNUT06.Canonicalize(d2)
	h1, h2 := Hash(c1), Hash(c2)
	store.PutSnapshot(ctx, "https://m", h1, c1, time.Now())
	store.PutSnapshot(ctx, "https://m", h2, c2, time.Now())

	t0 := time.Unix(1_700_000_000, 0).UTC()
	store.PutObservation(ctx, Observation{MintURL: "https://m", CheckedAt: t0, Hash: h1, Changed: true, Reachable: true})
	store.PutObservation(ctx, Observation{MintURL: "https://m", CheckedAt: t0.Add(24 * time.Hour), Hash: h1, Reachable: true})
	store.PutObservation(ctx, Observation{MintURL: "https://m", CheckedAt: t0.Add(48 * time.Hour), Hash: h2, Changed: true, Reachable: true})

	reader := NewReader(store, CashuNUT06)
	h, found, err := reader.History(ctx, "https://m", true)
	if err != nil || !found {
		t.Fatalf("history: found=%v err=%v", found, err)
	}
	if h.Initial == nil || h.Initial.Hash != h1 || h.Initial.At != t0.Unix() {
		t.Fatalf("initial = %+v, want hash %s at %d", h.Initial, h1, t0.Unix())
	}
	if len(h.Revisions) != 1 {
		t.Fatalf("revisions = %d, want 1 (one real change)", len(h.Revisions))
	}
	rev := h.Revisions[0]
	if rev.At != t0.Add(48*time.Hour).Unix() || rev.PreviousLastSeenAt != t0.Add(24*time.Hour).Unix() {
		t.Fatalf("revision timestamps = at %d prev %d", rev.At, rev.PreviousLastSeenAt)
	}
	joined := strings.Join(rev.Summary, " | ")
	if !strings.Contains(joined, "version: Nutshell/0.15.0 → Nutshell/0.16.0") {
		t.Fatalf("summary missing version bump: %q", joined)
	}
	if !strings.Contains(joined, "NUT-17 enabled") {
		t.Fatalf("summary missing NUT-17: %q", joined)
	}
	if h.CurrentHash != h2 || h.UnchangedSince != t0.Add(48*time.Hour).Unix() {
		t.Fatalf("current = %s unchangedSince = %d", h.CurrentHash, h.UnchangedSince)
	}
	if h.CheckCount != 3 || h.LastCheckedAt != t0.Add(48*time.Hour).Unix() {
		t.Fatalf("checkCount=%d lastChecked=%d", h.CheckCount, h.LastCheckedAt)
	}
	if len(h.Observations) != 3 {
		t.Fatalf("includeObservations must return the full log, got %d", len(h.Observations))
	}
	if string(rev.Patch) == "[]" || len(rev.Patch) == 0 {
		t.Fatalf("revision patch must be a non-empty RFC 6902 array: %s", rev.Patch)
	}
}

func TestReaderUnknownMintNotFound(t *testing.T) {
	reader := NewReader(newFakeStore(), CashuNUT06)
	_, found, err := reader.History(context.Background(), "https://never-seen", false)
	if err != nil || found {
		t.Fatalf("unknown mint: found=%v err=%v, want not found", found, err)
	}
}
