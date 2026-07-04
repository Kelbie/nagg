package ingest

import (
	"errors"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

// errPostCapped marks an event dropped by the per-author daily post cap. It is
// counted into the periodic summary (Flush) rather than logged per event.
var errPostCapped = errors.New("author over daily post cap")

// postCapCounterMaxEntries bounds the per-author counter map. Overflow FAILS
// OPEN: authors beyond the bound are not counted (so never capped) until the
// day rolls over — never drop events because bookkeeping ran out of room.
// Sized ~3× the busiest observed day (~60k distinct non-exempt authors).
const postCapCounterMaxEntries = 200_000

// postCapCounter tracks accepted capped-kind events per (author, UTC day). The
// day is the INGESTION day (wall clock), not the event's author-claimed
// created_at, so backdated timestamps can't dodge the cap. All access is from
// the single batching goroutine — no locking.
type postCapCounter struct {
	day    string
	counts map[string]int

	// summary-log state: events dropped since the last summary, and how many
	// distinct authors hit the cap today.
	droppedSinceLog    uint64
	cappedAuthorsToday int

	// now is a test seam; nil means time.Now.
	now func() time.Time
}

// overPostCap reports whether this event must be dropped, and does the
// counting for events it lets through.
func (p *Pipeline) overPostCap(event *nostr.Event) bool {
	limit := p.cfg.PostCapPerDay
	if limit <= 0 || event == nil {
		return false
	}
	if _, capped := postCapKinds[event.Kind]; !capped {
		return false
	}
	if p.exempt != nil && p.exempt(event.PubKey) {
		return false
	}

	nowFn := p.cap.now
	if nowFn == nil {
		nowFn = time.Now
	}
	day := nowFn().UTC().Format("2006-01-02")
	if p.cap.day != day {
		p.cap.day = day
		p.cap.counts = make(map[string]int, 4096)
		p.cap.cappedAuthorsToday = 0
	}

	count, tracked := p.cap.counts[event.PubKey]
	if count >= limit {
		p.cap.droppedSinceLog++
		return true
	}
	if !tracked && len(p.cap.counts) >= postCapCounterMaxEntries {
		// Fail open past the bound (see postCapCounterMaxEntries).
		return false
	}
	p.cap.counts[event.PubKey] = count + 1
	if count+1 == limit {
		p.cap.cappedAuthorsToday++
	}
	return false
}
