package rates

import (
	"strings"
	"testing"
)

func TestRatesParseBitagentNote(t *testing.T) {
	for _, tt := range []struct {
		name, content, currency string
		price                   float64
		ok                      bool
	}{
		{"CHF suffix", "1 BTC = 62577.6 CHF / 1 CHF = 1598 sats / #bitcoin 966452 2sat/vB", "CHF", 62577.6, true},
		{"EUR older", "1 BTC = 66639 EUR / 1 EUR = 1501 sats / #bitcoin", "EUR", 66639, true},
		{"USD separators", "1 BTC = 77,242 USD / 1 USD = 1,295 sats / #bitcoin 966452 2sat/vB", "USD", 77242, true},
		{"price only", "1 BTC = 57,274.25 GBP", "GBP", 57274.25, true},
		{"multiline", "update\n1 BTC = 62577.6 CHF", "CHF", 62577.6, true},
		{"garbage", "bitcoin is great", "", 0, false},
		{"embedded", "fake 1 BTC = 60000 USD", "", 0, false},
		{"zero", "1 BTC = 0 USD", "", 0, false},
		{"negative", "1 BTC = -10 USD", "", 0, false},
		{"nan", "1 BTC = NaN USD", "", 0, false},
		{"overflow", "1 BTC = " + strings.Repeat("9", 400) + " USD", "", 0, false},
		{"mismatch", "1 BTC = 62577.6 CHF / 1 CHF = 10 sats", "", 0, false},
		{"inverse zero", "1 BTC = 62577.6 CHF / 1 CHF = 0 sats", "", 0, false},
		{"inverse negative", "1 BTC = 62577.6 CHF / 1 CHF = -1598 sats", "", 0, false},
		{"wrong currency", "1 BTC = 62577.6 CHF / 1 USD = 1598 sats", "", 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o, ok := ParseBitagentNote(tt.content)
			if ok != tt.ok || ok && (o.Currency != tt.currency || o.Price != tt.price || !o.At.IsZero()) {
				t.Fatalf("got %+v, %v", o, ok)
			}
		})
	}
}
