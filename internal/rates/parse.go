package rates

import (
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Observation struct {
	Currency string
	Price    float64
	At       time.Time
	Source   string
	Kind     Kind
}

var pricePattern = regexp.MustCompile(`(?m)^1 BTC = ([0-9][0-9,]*(?:\.[0-9]+)?) ([A-Z]{3})\b`)
var satsPattern = regexp.MustCompile(`\b1 ([A-Z]{3}) = ([^\s/]+) sats\b`)

// ParseBitagentNote leaves At unset: only the signed event supplies its time.
func ParseBitagentNote(content string) (Observation, bool) {
	m := pricePattern.FindStringSubmatch(content)
	if m == nil {
		return Observation{}, false
	}
	price, ok := parsePrice(m[1])
	if !ok {
		return Observation{}, false
	}
	for _, sats := range satsPattern.FindAllStringSubmatch(content, -1) {
		n, valid := parsePrice(sats[2])
		if !valid || sats[1] != m[2] || math.Abs(1e8/n-price)/price > .02 {
			return Observation{}, false
		}
	}
	return Observation{Currency: m[2], Price: price}, true
}

func parsePrice(s string) (float64, bool) {
	p, err := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64)
	return p, err == nil && validPrice(p)
}

func validPrice(p float64) bool { return p > 0 && !math.IsNaN(p) && !math.IsInf(p, 0) }
