package ingest

import (
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/rules"
)

// backfillFilter applies the live firehose's ingest rules to walked history:
// per-author caps (bucketed by the EVENT's created_at day, not the wall clock
// — see capCounter.admit) and addressee gates, with the same exemption source
// (relevance.Tracker.Exempt) and the same fail-open semantics: a nil
// exemption source caps everyone and gates nobody, exactly like Pipeline.
// The counters here are the Backfiller's own — the Pipeline's cap state is
// confined to its batching goroutine and must not be shared.
type backfillFilter struct {
	caps   []*capCounter
	gates  []rules.AddresseeGate
	exempt func(pubkey string) bool
}

func (f *backfillFilter) keep(rec chstore.EventRecord) bool {
	ev := rec.Event
	if ev == nil {
		return false
	}
	at := time.Unix(int64(ev.CreatedAt), 0)
	for _, c := range f.caps {
		if !c.admit(ev, at, f.exempt) {
			return false
		}
	}
	if f.exempt != nil {
		for _, g := range f.gates {
			if kindIn(g.Kinds, ev.Kind) && !eventAddressesExempt(ev, f.exempt) {
				return false
			}
		}
	}
	return true
}
