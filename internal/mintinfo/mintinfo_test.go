package mintinfo

import (
	"context"
	"sort"
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

func (f *fakeStore) RecentlyChangedMintURLs(_ context.Context, limit int) ([]string, error) {
	type agg struct {
		hashes map[string]struct{}
		last   time.Time
	}
	m := map[string]*agg{}
	for _, o := range f.obs {
		if !o.Reachable || o.Hash == "" {
			continue
		}
		a := m[o.MintURL]
		if a == nil {
			a = &agg{hashes: map[string]struct{}{}}
			m[o.MintURL] = a
		}
		a.hashes[o.Hash] = struct{}{}
		if o.CheckedAt.After(a.last) {
			a.last = o.CheckedAt
		}
	}
	type row struct {
		url  string
		last time.Time
	}
	var rows []row
	for u, a := range m {
		if len(a.hashes) >= 2 {
			rows = append(rows, row{u, a.last})
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].last.After(rows[j].last) })
	var out []string
	for _, r := range rows {
		out = append(out, r.url)
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeStore) MintInfoStats(_ context.Context) (MintInfoStats, error) {
	tracked := map[string]struct{}{}
	reach := map[string]struct{}{}
	for _, o := range f.obs {
		tracked[o.MintURL] = struct{}{}
		if o.Reachable {
			reach[o.MintURL] = struct{}{}
		}
	}
	return MintInfoStats{Tracked: len(tracked), Reachable: len(reach)}, nil
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

func TestGlobalChangesFlattensAndSortsNewestFirst(t *testing.T) {
	ctx := context.Background()
	store := newFakeStore()
	put := func(url string, docs ...[]byte) []string {
		hashes := make([]string, 0, len(docs))
		for _, d := range docs {
			c, _ := CashuNUT06.Canonicalize(d)
			h := Hash(c)
			store.PutSnapshot(ctx, url, h, c, time.Now())
			hashes = append(hashes, h)
		}
		return hashes
	}
	t0 := time.Unix(1_700_000_000, 0).UTC()
	obs := func(url, hash string, at time.Time) {
		store.PutObservation(ctx, Observation{MintURL: url, CheckedAt: at, Hash: hash, Reachable: true})
	}

	// Mint A: v1 -> v2 at t1 (a change). Mint B: only v1 (no change). Mint C:
	// v1 -> v2 at t2 (a change, more recent than A's).
	ha := put("https://a", []byte(`{"name":"Alpha","version":"1.0"}`), []byte(`{"name":"Alpha","version":"1.1"}`))
	put("https://b", []byte(`{"name":"Bravo","version":"1.0"}`))
	hc := put("https://c", []byte(`{"name":"Charlie","version":"2.0"}`), []byte(`{"name":"Charlie","version":"2.1","nuts":{"17":{"supported":true}}}`))
	hb, _ := CashuNUT06.Canonicalize([]byte(`{"name":"Bravo","version":"1.0"}`))
	obs("https://a", ha[0], t0)
	obs("https://a", ha[1], t0.Add(24*time.Hour)) // t1
	obs("https://b", Hash(hb), t0)
	obs("https://c", hc[0], t0)
	obs("https://c", hc[1], t0.Add(48*time.Hour)) // t2 (newest)

	g, err := NewReader(store, CashuNUT06).GlobalChanges(ctx, 100)
	if err != nil {
		t.Fatalf("GlobalChanges: %v", err)
	}
	if g.TrackedMints != 3 || g.ReachableMints != 3 {
		t.Fatalf("stats: tracked=%d reachable=%d, want 3/3", g.TrackedMints, g.ReachableMints)
	}
	if g.TotalChanges != 2 || len(g.Changes) != 2 {
		t.Fatalf("changes = %d (total %d), want 2 (B contributes none)", len(g.Changes), g.TotalChanges)
	}
	// Newest first: C's change (t2) before A's (t1).
	if g.Changes[0].MintURL != "https://c" || g.Changes[0].At != t0.Add(48*time.Hour).Unix() {
		t.Fatalf("changes[0] = %s @ %d, want https://c @ %d", g.Changes[0].MintURL, g.Changes[0].At, t0.Add(48*time.Hour).Unix())
	}
	if g.Changes[1].MintURL != "https://a" {
		t.Fatalf("changes[1] = %s, want https://a", g.Changes[1].MintURL)
	}
	if g.Changes[0].Name != "Charlie" {
		t.Fatalf("changes[0].Name = %q, want Charlie", g.Changes[0].Name)
	}
	if len(g.Changes[0].Summary) == 0 || string(g.Changes[0].Patch) == "[]" {
		t.Fatalf("changes[0] must carry a summary + patch: %+v", g.Changes[0])
	}
}

func TestReaderUnknownMintNotFound(t *testing.T) {
	reader := NewReader(newFakeStore(), CashuNUT06)
	_, found, err := reader.History(context.Background(), "https://never-seen", false)
	if err != nil || found {
		t.Fatalf("unknown mint: found=%v err=%v, want not found", found, err)
	}
}
