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
