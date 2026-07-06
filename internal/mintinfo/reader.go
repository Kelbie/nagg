package mintinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/wI2L/jsondiff"
)

// ReaderStore is the per-mint read seam (implemented by *clickhouse.Store).
type ReaderStore interface {
	MintObservations(ctx context.Context, mintURL string) ([]Observation, error)
	MintSnapshots(ctx context.Context, mintURL string, hashes []string) (map[string]Snapshot, error)
}

// GlobalStore adds the ecosystem-wide reads the global changelog needs.
type GlobalStore interface {
	ReaderStore
	// RecentlyChangedMintURLs returns mints that have had at least one real
	// revision (≥2 distinct reachable content hashes), most-recent-change first.
	RecentlyChangedMintURLs(ctx context.Context, limit int) ([]string, error)
	// MintInfoStats is the roster summary for the changelog header.
	MintInfoStats(ctx context.Context) (MintInfoStats, error)
}

// MintInfoStats is the tracked/reachable roster summary.
type MintInfoStats struct {
	Tracked   int
	Reachable int
}

// GlobalChange is one mint's info change, for the ecosystem-wide feed. It is a
// per-mint Revision tagged with which mint it belongs to.
type GlobalChange struct {
	MintURL            string          `json:"mintUrl"`
	Name               string          `json:"name"`
	At                 int64           `json:"at"`
	PreviousLastSeenAt int64           `json:"previousLastSeenAt"`
	Hash               string          `json:"hash"`
	Summary            []string        `json:"summary"`
	Patch              json.RawMessage `json:"patch"`
}

// GlobalChanges is the ecosystem-wide changelog: recent revisions across all
// tracked mints, newest first, plus roster stats for the header.
type GlobalChanges struct {
	TrackedMints   int            `json:"trackedMints"`
	ReachableMints int            `json:"reachableMints"`
	TotalChanges   int            `json:"totalChanges"`
	Changes        []GlobalChange `json:"changes"`
}

// SnapshotView is the initial full document in a history response.
type SnapshotView struct {
	At       int64           `json:"at"`
	Hash     string          `json:"hash"`
	Document json.RawMessage `json:"document"`
}

// Revision is one change: an RFC 6902 patch against the previous document, plus
// a server-rendered human summary and the two timestamps that bound it.
type Revision struct {
	At                 int64           `json:"at"`                 // when the changed check ran
	PreviousLastSeenAt int64           `json:"previousLastSeenAt"` // last time the old value was confirmed live
	Hash               string          `json:"hash"`
	Summary            []string        `json:"summary"`
	Patch              json.RawMessage `json:"patch"`
}

// ObservationView is one poll, surfaced only when observations are requested.
type ObservationView struct {
	At        int64  `json:"at"`
	Hash      string `json:"hash"`
	Changed   bool   `json:"changed"`
	Reachable bool   `json:"reachable"`
}

// History is the full response for one mint: initial document, newest-first
// revisions, and top-level check metadata. "We checked, nothing changed" lives
// in lastCheckedAt/checkCount/unchangedSince, not in empty rows.
type History struct {
	MintURL        string            `json:"mintUrl"`
	NormalizedURL  string            `json:"normalizedUrl"`
	Name           string            `json:"name"`
	CurrentHash    string            `json:"currentHash"`
	FirstSeenAt    int64             `json:"firstSeenAt"`
	LastCheckedAt  int64             `json:"lastCheckedAt"`
	CheckCount     int               `json:"checkCount"`
	UnchangedSince int64             `json:"unchangedSince"`
	Initial        *SnapshotView     `json:"initial"`
	Revisions      []Revision        `json:"revisions"`
	Observations   []ObservationView `json:"observations"`
}

// Reader assembles mint info history at read time — per mint (History) and
// ecosystem-wide (GlobalChanges).
type Reader struct {
	store  GlobalStore
	source Source
}

func NewReader(store GlobalStore, source Source) *Reader {
	return &Reader{store: store, source: source}
}

// globalChangeMintCap bounds how many changed mints a single global feed read
// fans out over. Far above any realistic count of mints that have ever revised.
const globalChangeMintCap = 500

// GlobalChanges builds the ecosystem-wide changelog: every mint's revisions,
// flattened and sorted newest first, capped at limit. A per-mint read failing
// is skipped rather than failing the whole aggregate — it's a best-effort
// monitoring view, and the per-mint endpoint surfaces individual errors.
func (r *Reader) GlobalChanges(ctx context.Context, limit int) (*GlobalChanges, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	stats, err := r.store.MintInfoStats(ctx)
	if err != nil {
		return nil, err
	}
	mints, err := r.store.RecentlyChangedMintURLs(ctx, globalChangeMintCap)
	if err != nil {
		return nil, err
	}

	var all []GlobalChange
	for _, mintURL := range mints {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		h, found, err := r.History(ctx, mintURL, false)
		if err != nil || !found || h == nil {
			continue
		}
		for _, rev := range h.Revisions {
			all = append(all, GlobalChange{
				MintURL:            h.MintURL,
				Name:               h.Name,
				At:                 rev.At,
				PreviousLastSeenAt: rev.PreviousLastSeenAt,
				Hash:               rev.Hash,
				Summary:            rev.Summary,
				Patch:              rev.Patch,
			})
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].At != all[j].At {
			return all[i].At > all[j].At
		}
		return all[i].MintURL < all[j].MintURL
	})
	total := len(all)
	if len(all) > limit {
		all = all[:limit]
	}
	if all == nil {
		all = []GlobalChange{}
	}
	return &GlobalChanges{
		TrackedMints:   stats.Tracked,
		ReachableMints: stats.Reachable,
		TotalChanges:   total,
		Changes:        all,
	}, nil
}

// run is one contiguous stretch of identical-hash observations.
type run struct {
	hash    string
	firstAt int64 // detected_at
	lastAt  int64 // last time this hash was confirmed live
}

// History returns the mint's info history. found is false when the mint has
// never been observed (the caller returns 404). A mint observed but never
// reachable returns found=true with a nil Initial and empty revisions.
func (r *Reader) History(ctx context.Context, rawMintURL string, includeObservations bool) (history *History, found bool, err error) {
	norm := NormalizeMintURL(rawMintURL)
	obs, err := r.store.MintObservations(ctx, norm)
	if err != nil {
		return nil, false, err
	}
	if len(obs) == 0 {
		return nil, false, nil
	}

	out := &History{
		MintURL:       rawMintURL,
		NormalizedURL: norm,
		CheckCount:    len(obs),
		LastCheckedAt: obs[len(obs)-1].CheckedAt.Unix(),
		Revisions:     []Revision{},
	}

	// Collapse consecutive same-hash reachable observations into runs.
	var runs []run
	for _, o := range obs {
		if !o.Reachable || o.Hash == "" {
			continue
		}
		at := o.CheckedAt.Unix()
		if n := len(runs); n > 0 && runs[n-1].hash == o.Hash {
			runs[n-1].lastAt = at
			continue
		}
		runs = append(runs, run{hash: o.Hash, firstAt: at, lastAt: at})
	}
	if includeObservations {
		out.Observations = observationViews(obs)
	}
	if len(runs) == 0 {
		return out, true, nil // observed, never reachable
	}

	// Fetch the documents for the distinct run hashes.
	hashes := make([]string, 0, len(runs))
	seen := map[string]struct{}{}
	for _, rn := range runs {
		if _, ok := seen[rn.hash]; ok {
			continue
		}
		seen[rn.hash] = struct{}{}
		hashes = append(hashes, rn.hash)
	}
	snaps, err := r.store.MintSnapshots(ctx, norm, hashes)
	if err != nil {
		return nil, false, err
	}

	initial := runs[0]
	out.FirstSeenAt = initial.firstAt
	out.CurrentHash = runs[len(runs)-1].hash
	out.UnchangedSince = runs[len(runs)-1].firstAt
	if doc, ok := snaps[initial.hash]; ok {
		out.Initial = &SnapshotView{At: initial.firstAt, Hash: initial.hash, Document: doc.Document}
	}
	if doc, ok := snaps[out.CurrentHash]; ok {
		out.Name = extractName(doc.Document)
	}

	// Diff each run against its predecessor; emit newest-first.
	prevDoc := docBytes(snaps, initial.hash)
	revisions := make([]Revision, 0, len(runs)-1)
	for i := 1; i < len(runs); i++ {
		curDoc := docBytes(snaps, runs[i].hash)
		rev := Revision{
			At:                 runs[i].firstAt,
			PreviousLastSeenAt: runs[i-1].lastAt,
			Hash:               runs[i].hash,
			Summary:            []string{},
			Patch:              json.RawMessage("[]"),
		}
		if prevDoc != nil && curDoc != nil {
			if patch, derr := jsondiff.CompareJSON(prevDoc, curDoc, jsondiff.Invertible(), jsondiff.Ignores("/time")); derr == nil {
				if encoded, merr := json.Marshal(patch); merr == nil {
					rev.Patch = encoded
					rev.Summary = summarize(encoded)
				}
			}
		}
		revisions = append(revisions, rev)
		if curDoc != nil {
			prevDoc = curDoc
		}
	}
	// Newest first.
	for i, j := 0, len(revisions)-1; i < j; i, j = i+1, j-1 {
		revisions[i], revisions[j] = revisions[j], revisions[i]
	}
	out.Revisions = revisions
	return out, true, nil
}

// extractName pulls the NUT-06 "name" field from a canonical document, for
// display in the changelog. Empty when absent.
func extractName(doc json.RawMessage) string {
	var fields struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(doc, &fields); err != nil {
		return ""
	}
	return fields.Name
}

func docBytes(snaps map[string]Snapshot, hash string) []byte {
	if s, ok := snaps[hash]; ok {
		return []byte(s.Document)
	}
	return nil
}

func observationViews(obs []Observation) []ObservationView {
	out := make([]ObservationView, 0, len(obs))
	for _, o := range obs {
		out = append(out, ObservationView{
			At: o.CheckedAt.Unix(), Hash: o.Hash, Changed: o.Changed, Reachable: o.Reachable,
		})
	}
	return out
}

// --- human summary rendering ------------------------------------------------

type patchOp struct {
	Op    string          `json:"op"`
	Path  string          `json:"path"`
	Value json.RawMessage `json:"value"`
}

// summarize renders an invertible RFC 6902 patch into human sentences ("version:
// Nutshell/0.15.0 → Nutshell/0.16.0", "NUT-17 enabled"). The old values come
// from the invertible patch's `test` ops. Unknown fields fall back to a generic
// phrasing rather than being dropped — the patch itself carries the full detail.
func summarize(patchJSON []byte) []string {
	var ops []patchOp
	if err := json.Unmarshal(patchJSON, &ops); err != nil {
		return []string{}
	}
	olds := map[string]json.RawMessage{}
	for _, op := range ops {
		if op.Op == "test" {
			olds[op.Path] = op.Value
		}
	}
	out := []string{}
	for _, op := range ops {
		switch op.Op {
		case "test":
			continue
		case "add":
			out = append(out, labelAdd(op.Path, op.Value))
		case "remove":
			out = append(out, labelRemove(op.Path))
		case "replace":
			out = append(out, fmt.Sprintf("%s: %s → %s", fieldLabel(op.Path), renderVal(olds[op.Path]), renderVal(op.Value)))
		default:
			out = append(out, fmt.Sprintf("%s %s", op.Op, fieldLabel(op.Path)))
		}
	}
	return out
}

func labelAdd(path string, value json.RawMessage) string {
	if nut, ok := nutNumber(path); ok {
		return fmt.Sprintf("NUT-%s enabled", nut)
	}
	return fmt.Sprintf("%s added: %s", fieldLabel(path), renderVal(value))
}

func labelRemove(path string) string {
	if nut, ok := nutNumber(path); ok {
		return fmt.Sprintf("NUT-%s removed", nut)
	}
	return fmt.Sprintf("%s removed", fieldLabel(path))
}

// nutNumber extracts the NUT number from a top-level /nuts/<n> path.
func nutNumber(path string) (string, bool) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 2 && parts[0] == "nuts" {
		return parts[1], true
	}
	return "", false
}

var fieldNames = map[string]string{
	"version":          "version",
	"name":             "name",
	"description":      "description",
	"description_long": "long description",
	"motd":             "message of the day",
	"icon_url":         "icon",
	"contact":          "contact",
	"urls":             "URLs",
	"tos_url":          "terms of service",
	"pubkey":           "pubkey",
}

// fieldLabel turns a JSON pointer into a human field name.
func fieldLabel(path string) string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return "document"
	}
	parts := strings.Split(trimmed, "/")
	if parts[0] == "nuts" && len(parts) >= 2 {
		if len(parts) == 2 {
			return "NUT-" + parts[1]
		}
		return "NUT-" + parts[1] + " " + strings.Join(parts[2:], " ")
	}
	if name, ok := fieldNames[parts[0]]; ok {
		if len(parts) > 1 {
			return name + " " + strings.Join(parts[1:], " ")
		}
		return name
	}
	return strings.Join(parts, " ")
}

const maxRenderLen = 60

// renderVal makes a patch value human-readable: bare string for JSON strings,
// compact-and-truncated JSON otherwise.
func renderVal(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "∅"
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return truncate(s)
	}
	return truncate(string(raw))
}

func truncate(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxRenderLen {
		return s[:maxRenderLen-1] + "…"
	}
	return s
}
