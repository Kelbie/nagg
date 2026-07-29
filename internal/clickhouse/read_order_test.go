package clickhouse

import (
	"strings"
	"testing"
	"time"
)

// DescendantEvents collects its BFS results in a map, so without an explicit
// sort the returned slice order is Go map-iteration order — different on every
// call. The thread endpoint renders this slice directly for the default sort
// and the tail-append, so the order must be a total one.
func TestSortEventsByRecencyIsDeterministic(t *testing.T) {
	at := func(sec int64) time.Time { return time.Unix(sec, 0) }
	events := []EventView{
		{ID: "b", CreatedAt: at(100)},
		{ID: "a", CreatedAt: at(100)}, // same timestamp: id DESC tiebreak
		{ID: "d", CreatedAt: at(300)},
		{ID: "c", CreatedAt: at(200)},
	}
	sortEventsByRecency(events)
	got := make([]string, 0, len(events))
	for _, e := range events {
		got = append(got, e.ID)
	}
	want := "d,c,b,a"
	if strings.Join(got, ",") != want {
		t.Fatalf("order = %v, want %s", got, want)
	}
}

// The max-content-length filter caps TEXT kinds only: a kind-6/16 repost's
// content is the reposted event's JSON (NIP-18), so a raw length test on it
// would delete every repost from the feed.
func TestMaxContentLengthWhereScopesToTextKinds(t *testing.T) {
	clause, args := maxContentLengthWhere("e", 280)
	if clause != "(e.kind NOT IN (1, 1111) OR lengthUTF8(e.content) <= ?)" {
		t.Fatalf("clause = %q", clause)
	}
	if len(args) != 1 || args[0].(uint64) != 280 {
		t.Fatalf("args = %v, want [280]", args)
	}
	if clause, args := maxContentLengthWhere("e", 0); clause != "" || args != nil {
		t.Fatalf("zero max must be a no-op, got %q %v", clause, args)
	}
}
