package ingest

import (
	"errors"

	"github.com/nbd-wtf/go-nostr"

	"github.com/vertex-lab/nagg/internal/rules"
)

// errUnaddressed marks an event dropped by an addressee-gate rule: none of
// its p tags address the exemption universe (known viewers + their follows).
// Counted into the periodic summary (Flush) rather than logged per event.
var errUnaddressed = errors.New("no exempt addressee")

// gateCounter carries one addressee-gate rule plus its summary-log state.
// All access is from the single batching goroutine — no locking.
type gateCounter struct {
	rule            rules.AddresseeGate
	droppedSinceLog uint64
}

func newGateCounters(gates []rules.AddresseeGate) []*gateCounter {
	out := make([]*gateCounter, 0, len(gates))
	for _, g := range gates {
		out = append(out, &gateCounter{rule: g})
	}
	return out
}

// unaddressed reports whether an event must be dropped by an addressee-gate
// rule: its kind is gated and NO p tag addresses an exempt pubkey. With no
// exemption source configured the gate FAILS OPEN — before the tracker's
// first refresh, or with relevance disabled, everything ingests (dropping
// events because bookkeeping isn't ready is never acceptable; the tracker's
// Exempt has the same nil-set semantics).
func (p *Pipeline) unaddressed(event *nostr.Event) bool {
	if event == nil || len(p.gates) == 0 || p.exempt == nil {
		return false
	}
	for _, g := range p.gates {
		if !kindIn(g.rule.Kinds, event.Kind) {
			continue
		}
		if eventAddressesExempt(event, p.exempt) {
			continue
		}
		g.droppedSinceLog++
		return true
	}
	return false
}

func eventAddressesExempt(event *nostr.Event, exempt func(pubkey string) bool) bool {
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "p" || len(tag[1]) != 64 {
			continue
		}
		if exempt(tag[1]) {
			return true
		}
	}
	return false
}
