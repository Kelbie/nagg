package rates

import (
	"math"
	"sort"
	"time"
)

type Rate struct {
	Price      float64  `json:"price"`
	At         int64    `json:"at"`
	Samples    int      `json:"samples"`
	Sources    []string `json:"sources"`
	Confidence string   `json:"confidence"`
}

// Aggregate handles one currency, without mutating input or advancing the
// timestamp on retained data. The caller keeps the last good rate on rejection.
func Aggregate(observations []Observation, now time.Time, maxAge time.Duration, last *Rate) (Rate, bool) {
	if maxAge <= 0 {
		maxAge = 6 * time.Hour
	}
	obs := append([]Observation(nil), observations...)
	sort.SliceStable(obs, func(i, j int) bool { return obs[i].At.After(obs[j].At) })
	counts := map[string]int{}
	var eligible []Observation
	var prices []float64
	for _, o := range obs {
		if !validPrice(o.Price) || o.At.IsZero() || o.At.After(now) || now.Sub(o.At) > maxAge {
			continue
		}
		limit := 1
		if o.Kind == NostrNote {
			limit = 5
		}
		if counts[o.Source] >= limit {
			continue
		}
		counts[o.Source]++
		eligible = append(eligible, o)
		prices = append(prices, o.Price)
	}
	if len(prices) == 0 {
		return Rate{}, false
	}
	center := median(prices)
	deviations := make([]float64, len(prices))
	for i, p := range prices {
		deviations[i] = math.Abs(p - center)
	}
	threshold := math.Max(3*1.4826*median(deviations), .005*center)
	var survivors []float64
	sources := map[string]bool{}
	var at int64
	for _, o := range eligible {
		if math.Abs(o.Price-center) > threshold {
			continue
		}
		survivors = append(survivors, o.Price)
		sources[o.Source] = true
		if o.At.Unix() > at {
			at = o.At.Unix()
		}
	}
	if len(survivors) == 0 {
		return Rate{}, false
	}
	price := median(survivors)
	if last != nil && validPrice(last.Price) && now.Sub(time.Unix(last.At, 0)) <= 24*time.Hour && math.Abs(price-last.Price)/last.Price > .2 {
		return Rate{}, false
	}
	r := Rate{Price: price, At: at, Samples: len(survivors), Confidence: "single-source"}
	for source := range sources {
		r.Sources = append(r.Sources, source)
	}
	sort.Strings(r.Sources)
	if r.Samples >= 3 && len(sources) >= 2 {
		r.Confidence = "high"
	} else if r.Samples >= 2 {
		r.Confidence = "medium"
	}
	return r, true
}

func median(values []float64) float64 {
	sort.Float64s(values)
	i := len(values) / 2
	if len(values)%2 == 1 {
		return values[i]
	}
	return values[i-1]/2 + values[i]/2
}
