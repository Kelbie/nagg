// Package mintprobe checks whether each known Cashu mint hands out ecash for
// quotes nobody paid. Once per MinAge (default weekly) per mint, it requests a
// NUT-04 mint quote for every advertised mint method, never pays it, watches
// the quote's state, and — if the mint reports it paid anyway — asks the mint
// to sign outputs for it. A mint that marks an unpaid quote paid is running a
// fake payment backend: a testnut. /nostr/mint/discover exposes that verdict as
// its `testnut` flag and filter.
//
// Like internal/mintinfo it sits outside the rules registry: it is polled HTTP
// against mints, not a Nostr primitive. It reuses mintinfo's work-list and info
// fetcher so both pollers watch the same roster.
package mintprobe

import (
	"encoding/json"
	"strings"
	"time"
)

// Status is the terminal state of one probe.
type Status string

const (
	// StatusInfoUnreachable: the mint's /v1/info could not be read, so no
	// methods were known to probe. Recorded so the due-gate clock still ticks.
	StatusInfoUnreachable Status = "info_unreachable"
	// StatusNoMethods: /v1/info advertised no enabled NUT-04 mint method.
	StatusNoMethods Status = "no_methods"
	// StatusQuoteFailed: the mint refused or failed the quote request.
	StatusQuoteFailed Status = "quote_failed"
	// StatusUnpaid: the quote stayed unpaid — a real payment backend.
	StatusUnpaid Status = "unpaid"
	// StatusPaidNotIssued: the unpaid quote was marked paid, but minting
	// against it failed.
	StatusPaidNotIssued Status = "paid_not_issued"
	// StatusIssued: the unpaid quote was marked paid and the mint signed
	// outputs for it — ecash credited for nothing.
	StatusIssued Status = "issued"
)

// Verdict reports whether the probe got far enough to say anything about the
// mint's payment backend: a quote was created and its state observed.
func (s Status) Verdict() bool {
	return s == StatusUnpaid || s == StatusPaidNotIssued || s == StatusIssued
}

// Method is one NUT-04 mint method/unit pair a mint advertises.
type Method struct {
	Method    string
	Unit      string
	MinAmount uint64
	MaxAmount uint64
}

// Result is one probe of one mint method. Method and Unit are empty for the
// mint-level statuses (info_unreachable, no_methods).
type Result struct {
	MintURL  string
	Method   string
	Unit     string
	Amount   uint64
	ProbedAt time.Time
	Status   Status
	// Paid: the mint reported the never-paid quote as paid.
	Paid bool
	// Issued: the mint returned a signature for every requested output.
	Issued bool
	// Error is a short reason when the probe stopped early.
	Error string
}

// MintMethods reads the enabled NUT-04 mint methods from a NUT-06 info
// document, deduplicated by method+unit. nil when NUT-04 is absent, disabled
// or unparseable.
func MintMethods(info []byte) []Method {
	var doc struct {
		Nuts map[string]json.RawMessage `json:"nuts"`
	}
	if err := json.Unmarshal(info, &doc); err != nil {
		return nil
	}
	raw, ok := doc.Nuts["4"]
	if !ok {
		return nil
	}
	var nut4 struct {
		Disabled bool `json:"disabled"`
		Methods  []struct {
			Method    string  `json:"method"`
			Unit      string  `json:"unit"`
			MinAmount *uint64 `json:"min_amount"`
			MaxAmount *uint64 `json:"max_amount"`
		} `json:"methods"`
	}
	if err := json.Unmarshal(raw, &nut4); err != nil || nut4.Disabled {
		return nil
	}
	seen := map[string]struct{}{}
	var out []Method
	for _, m := range nut4.Methods {
		method := strings.ToLower(strings.TrimSpace(m.Method))
		unit := strings.ToLower(strings.TrimSpace(m.Unit))
		if method == "" || unit == "" {
			continue
		}
		key := method + "/" + unit
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		entry := Method{Method: method, Unit: unit}
		if m.MinAmount != nil {
			entry.MinAmount = *m.MinAmount
		}
		if m.MaxAmount != nil {
			entry.MaxAmount = *m.MaxAmount
		}
		out = append(out, entry)
	}
	return out
}

// probeAmount is the smallest amount the method accepts: its min_amount, or 1.
func probeAmount(m Method) uint64 {
	if m.MinAmount > 1 {
		return m.MinAmount
	}
	return 1
}

// splitAmount decomposes amount into the power-of-two denominations Cashu
// keysets sign, one output per set bit.
func splitAmount(amount uint64) []uint64 {
	var out []uint64
	for bit := uint64(1); amount > 0; bit <<= 1 {
		if amount&bit != 0 {
			out = append(out, bit)
			amount &^= bit
		}
	}
	return out
}
