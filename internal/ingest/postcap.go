package ingest

import (
	"errors"
	"time"

	"github.com/nbd-wtf/go-nostr"

	"github.com/vertex-lab/nagg/internal/rules"
)

// errCapped marks an event dropped by a per-author ingest cap rule. It is
// counted into the periodic summary (Flush) rather than logged per event.
var errCapped = errors.New("author over ingest cap")

// capCounterMaxEntries bounds each rule's per-author counter map. Overflow
// FAILS OPEN: authors beyond the bound are not counted (so never capped) until
// the window rolls over — never drop events because bookkeeping ran out of
// room. Sized ~3× the busiest observed day (~60k distinct non-exempt authors).
const capCounterMaxEntries = 200_000

// capCounter tracks accepted events per (author, window bucket) for one cap
// rule. The bucket is derived from the INGESTION time (wall clock), not the
// event's author-claimed created_at, so backdated timestamps can't dodge the
// cap. All access is from the single batching goroutine — no locking.
//
// Window == 0 declares a lifetime cap; the in-process counter approximates it
// (it resets on restart) — a durable lifetime counter is a follow-up, so
// lifetime rules currently under-enforce rather than over-drop.
type capCounter struct {
	rule   rules.Cap
	bucket string
	counts map[string]int

	// summary-log state: events dropped since the last summary, and how many
	// distinct authors hit the cap in the current bucket.
	droppedSinceLog     uint64
	cappedAuthorsBucket int
}

func newCapCounters(caps []rules.Cap) []*capCounter {
	out := make([]*capCounter, 0, len(caps))
	for _, c := range caps {
		if c.Max <= 0 {
			continue
		}
		out = append(out, &capCounter{rule: c})
	}
	return out
}

// bucketKey identifies the rule's current window. Duration-window rules
// bucket by truncated wall clock (a 24h window is the UTC day, matching the
// previous per-day semantics); lifetime rules share one process-lived bucket.
func (c *capCounter) bucketKey(now time.Time) string {
	if c.rule.Window <= 0 {
		return "lifetime"
	}
	return now.UTC().Truncate(c.rule.Window).Format(time.RFC3339)
}

// admit reports whether ev passes this cap rule evaluated at `at` — the wall
// clock on the firehose path (ingestion time, so backdated created_at can't
// dodge the cap) and the event's own created_at on the history-walk path (the
// walk delivers years in hours; wall bucketing would collapse an author's
// whole history into one window, while event-day bucketing mirrors what the
// live firehose would have admitted at the time). Counts events it admits.
func (c *capCounter) admit(ev *nostr.Event, at time.Time, exempt func(pubkey string) bool) bool {
	if !kindIn(c.rule.Kinds, ev.Kind) {
		return true
	}
	if c.rule.ExemptKnownViewers && exempt != nil && exempt(ev.PubKey) {
		return true
	}

	bucket := c.bucketKey(at)
	if c.bucket != bucket {
		c.bucket = bucket
		c.counts = make(map[string]int, 4096)
		c.cappedAuthorsBucket = 0
	}

	count, tracked := c.counts[ev.PubKey]
	if count >= c.rule.Max {
		c.droppedSinceLog++
		return false
	}
	if !tracked && len(c.counts) >= capCounterMaxEntries {
		// Fail open past the bound (see capCounterMaxEntries).
		return true
	}
	c.counts[ev.PubKey] = count + 1
	if count+1 == c.rule.Max {
		c.cappedAuthorsBucket++
	}
	return true
}

// overCap reports whether this event must be dropped by any declared cap
// rule, and does the counting for events it lets through.
func (p *Pipeline) overCap(event *nostr.Event) bool {
	if event == nil || len(p.caps) == 0 {
		return false
	}
	nowFn := p.capNow
	if nowFn == nil {
		nowFn = time.Now
	}
	now := nowFn()

	for _, c := range p.caps {
		if !c.admit(event, now, p.exempt) {
			return true
		}
	}
	return false
}

func kindIn(kinds []int, kind int) bool {
	for _, k := range kinds {
		if k == kind {
			return true
		}
	}
	return false
}
