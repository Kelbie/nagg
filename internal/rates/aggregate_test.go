package rates

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestRatesAggregate(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, tt := range []struct {
		name       string
		prices     []float64
		want       float64
		samples    int
		confidence string
	}{
		{"10x outlier", []float64{100, 100, 101, 1000}, 100, 3, "high"},
		{"single", []float64{100}, 100, 1, "single-source"},
		{"two", []float64{100, 102}, 101, 2, "medium"},
		{"zero MAD floor", []float64{100, 100, 100.4, 110}, 100, 3, "high"},
		{"invalid prices", []float64{0, -1, math.NaN(), math.Inf(1)}, 0, 0, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var obs []Observation
			for i, p := range tt.prices {
				obs = append(obs, Observation{Price: p, At: now, Source: fmt.Sprint(i), Kind: NostrNote})
			}
			r, ok := Aggregate(obs, now, 6*time.Hour, nil)
			if ok != (tt.samples > 0) || r.Price != tt.want || r.Samples != tt.samples || r.Confidence != tt.confidence {
				t.Fatalf("got %+v, %v", r, ok)
			}
		})
	}
}

func TestRatesAggregateAgeAndLimits(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, tt := range []struct {
		name string
		at   time.Time
		ok   bool
	}{
		{"stale", now.Add(-6*time.Hour - time.Second), false},
		{"boundary", now.Add(-6 * time.Hour), true},
		{"future", now.Add(time.Second), false},
		{"unset", time.Time{}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := Aggregate([]Observation{{Price: 100, At: tt.at}}, now, 6*time.Hour, nil)
			if ok != tt.ok {
				t.Fatalf("ok=%v", ok)
			}
		})
	}
	var obs []Observation
	// Oldest arrives first, and must not displace any of the five latest notes.
	for i := 6; i >= 0; i-- {
		obs = append(obs, Observation{Price: 100, At: now.Add(-time.Duration(i) * time.Minute), Source: "bot", Kind: NostrNote})
	}
	r, ok := Aggregate(obs, now, 6*time.Hour, nil)
	if !ok || r.Samples != 5 || len(r.Sources) != 1 || r.Confidence != "medium" || r.At != now.Unix() {
		t.Fatalf("got %+v", r)
	}
	for i := range obs {
		obs[i].Kind = HTTPJSON
	}
	r, _ = Aggregate(obs, now, 6*time.Hour, nil)
	if r.Samples != 1 {
		t.Fatalf("HTTP counted %d samples", r.Samples)
	}
}

func TestRatesPlausibilityReanchor(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, tt := range []struct {
		name  string
		price float64
		age   time.Duration
		ok    bool
	}{
		{"jump blocked", 121, time.Hour, false},
		{"fall blocked", 79, time.Hour, false},
		{"20 percent allowed", 120, time.Hour, true},
		{"24h boundary", 200, 24 * time.Hour, false},
		{"reanchor", 200, 24*time.Hour + time.Second, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			last := Rate{Price: 100, At: now.Add(-tt.age).Unix()}
			_, ok := Aggregate([]Observation{{Price: tt.price, At: now}}, now, 6*time.Hour, &last)
			if ok != tt.ok {
				t.Fatalf("ok=%v", ok)
			}
		})
	}
}
