package mintprobe

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
)

// maxBody caps every mint response read; NUT-04 bodies are a few hundred bytes.
const maxBody = 1 << 20

// Prober runs one probe against one mint method.
type Prober interface {
	Probe(ctx context.Context, mintURL string, m Method) Result
}

// HTTPProber speaks NUT-04 (plus NUT-20 quote locking for methods that require
// it) directly to each mint.
type HTTPProber struct {
	client *http.Client
	// paidPolls is how many times an unpaid quote's state is re-read, paidWait
	// apart, before concluding it will stay unpaid. Fake backends settle within
	// a second or two; real ones never do.
	paidPolls int
	paidWait  time.Duration
	now       func() time.Time
}

// NewHTTPProber builds a prober with a per-request timeout and the unpaid-quote
// re-check schedule.
func NewHTTPProber(timeout time.Duration, paidPolls int, paidWait time.Duration) *HTTPProber {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if paidPolls < 0 {
		paidPolls = 0
	}
	return &HTTPProber{
		client:    &http.Client{Timeout: timeout},
		paidPolls: paidPolls,
		paidWait:  paidWait,
		now:       time.Now,
	}
}

// quoteResponse covers every NUT-04 quote shape: bolt11's `state`, the legacy
// `paid` boolean, and the amount_paid/amount_issued counters of bolt12 and
// onchain quotes.
type quoteResponse struct {
	Quote        string `json:"quote"`
	State        string `json:"state"`
	Paid         *bool  `json:"paid"`
	Amount       uint64 `json:"amount"`
	AmountPaid   uint64 `json:"amount_paid"`
	AmountIssued uint64 `json:"amount_issued"`
}

func (q quoteResponse) paid() bool {
	switch strings.ToUpper(q.State) {
	case "PAID", "ISSUED":
		return true
	}
	return (q.Paid != nil && *q.Paid) || q.AmountPaid > 0
}

// mintable is what the quote can still be minted for.
func (q quoteResponse) mintable(requested uint64) uint64 {
	if q.AmountPaid > 0 {
		if q.AmountPaid > q.AmountIssued {
			return q.AmountPaid - q.AmountIssued
		}
		return 0
	}
	if strings.EqualFold(q.State, "ISSUED") {
		return 0
	}
	if q.Amount > 0 {
		return q.Amount
	}
	return requested
}

func (p *HTTPProber) Probe(ctx context.Context, mintURL string, m Method) Result {
	amount := probeAmount(m)
	res := Result{MintURL: mintURL, Method: m.Method, Unit: m.Unit, Amount: amount, ProbedAt: p.now().UTC()}
	if m.MaxAmount > 0 && amount > m.MaxAmount {
		res.Status, res.Error = StatusQuoteFailed, "min_amount exceeds max_amount"
		return res
	}

	// bolt11 quotes may be unlocked; every newer method (bolt12, onchain)
	// requires a NUT-20 pubkey, so lock those and sign the mint request.
	var lockKey *btcec.PrivateKey
	body := map[string]any{"amount": amount, "unit": m.Unit}
	if m.Method != "bolt11" {
		key, err := btcec.NewPrivateKey()
		if err != nil {
			res.Status, res.Error = StatusQuoteFailed, "generate lock key"
			return res
		}
		lockKey = key
		body["pubkey"] = hex.EncodeToString(key.PubKey().SerializeCompressed())
	}

	base := strings.TrimRight(mintURL, "/")
	method := url.PathEscape(m.Method)
	var quote quoteResponse
	if err := p.postJSON(ctx, base+"/v1/mint/quote/"+method, body, &quote); err != nil {
		res.Status, res.Error = StatusQuoteFailed, err.Error()
		return res
	}
	if quote.Quote == "" {
		res.Status, res.Error = StatusQuoteFailed, "quote response has no id"
		return res
	}

	for i := 0; !quote.paid() && i < p.paidPolls; i++ {
		if !sleep(ctx, p.paidWait) {
			res.Status, res.Error = StatusUnpaid, "cancelled"
			return res
		}
		var next quoteResponse
		if err := p.getJSON(ctx, base+"/v1/mint/quote/"+method+"/"+url.PathEscape(quote.Quote), &next); err != nil {
			continue
		}
		next.Quote = quote.Quote
		quote = next
	}
	if !quote.paid() {
		res.Status = StatusUnpaid
		return res
	}
	res.Paid = true

	if err := p.mint(ctx, base, method, m.Unit, quote.Quote, quote.mintable(amount), lockKey); err != nil {
		res.Status, res.Error = StatusPaidNotIssued, err.Error()
		return res
	}
	res.Status, res.Issued = StatusIssued, true
	return res
}

// mint asks the mint to sign outputs for the quote. The blinded messages are
// random curve points rather than hash_to_curve(secret) blindings: the mint
// cannot tell the difference, and the probe only needs to know it signs — the
// resulting ecash is never unblinded or held.
func (p *HTTPProber) mint(ctx context.Context, base, method, unit, quoteID string, amount uint64, lockKey *btcec.PrivateKey) error {
	if amount == 0 {
		return fmt.Errorf("quote has nothing left to mint")
	}
	keysetID, err := p.activeKeyset(ctx, base, unit)
	if err != nil {
		return err
	}

	type output struct {
		Amount uint64 `json:"amount"`
		ID     string `json:"id"`
		B      string `json:"B_"`
	}
	denominations := splitAmount(amount)
	outputs := make([]output, 0, len(denominations))
	for _, denom := range denominations {
		point, err := btcec.NewPrivateKey()
		if err != nil {
			return fmt.Errorf("generate output")
		}
		outputs = append(outputs, output{Amount: denom, ID: keysetID, B: hex.EncodeToString(point.PubKey().SerializeCompressed())})
	}

	body := map[string]any{"quote": quoteID, "outputs": outputs}
	if lockKey != nil {
		// NUT-20: sign sha256(quote_id || B_0 || … || B_n).
		msg := quoteID
		for _, o := range outputs {
			msg += o.B
		}
		digest := sha256.Sum256([]byte(msg))
		sig, err := schnorr.Sign(lockKey, digest[:])
		if err != nil {
			return fmt.Errorf("sign mint request")
		}
		body["signature"] = hex.EncodeToString(sig.Serialize())
	}

	var resp struct {
		Signatures []struct {
			Amount uint64 `json:"amount"`
			C      string `json:"C_"`
		} `json:"signatures"`
	}
	if err := p.postJSON(ctx, base+"/v1/mint/"+method, body, &resp); err != nil {
		return err
	}
	if len(resp.Signatures) != len(outputs) {
		return fmt.Errorf("mint returned %d signatures for %d outputs", len(resp.Signatures), len(outputs))
	}
	for i, sig := range resp.Signatures {
		if sig.C == "" || sig.Amount != outputs[i].Amount {
			return fmt.Errorf("mint returned an unusable signature")
		}
	}
	return nil
}

// activeKeyset returns an active keyset id for the unit.
func (p *HTTPProber) activeKeyset(ctx context.Context, base, unit string) (string, error) {
	var resp struct {
		Keysets []struct {
			ID     string `json:"id"`
			Unit   string `json:"unit"`
			Active *bool  `json:"active"`
		} `json:"keysets"`
	}
	if err := p.getJSON(ctx, base+"/v1/keysets", &resp); err != nil {
		return "", err
	}
	for _, ks := range resp.Keysets {
		if ks.ID != "" && strings.EqualFold(ks.Unit, unit) && (ks.Active == nil || *ks.Active) {
			return ks.ID, nil
		}
	}
	return "", fmt.Errorf("no active %s keyset", unit)
}

func (p *HTTPProber) postJSON(ctx context.Context, target string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode request")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("bad url")
	}
	req.Header.Set("Content-Type", "application/json")
	return p.do(req, out)
}

func (p *HTTPProber) getJSON(ctx context.Context, target string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("bad url")
	}
	return p.do(req, out)
}

// do performs the request and decodes a 200 body into out. Errors are short and
// carry the mint's NUT-00 `detail` when it sent one; they are stored verbatim.
func (p *HTTPProber) do(req *http.Request, out any) error {
	resp, err := p.client.Do(req)
	if err != nil {
		if req.Context().Err() != nil {
			return fmt.Errorf("cancelled")
		}
		return fmt.Errorf("request failed")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	if err != nil {
		return fmt.Errorf("read failed")
	}
	if resp.StatusCode != http.StatusOK {
		var mintErr struct {
			Detail string `json:"detail"`
		}
		if json.Unmarshal(body, &mintErr) == nil && mintErr.Detail != "" {
			return fmt.Errorf("http %d: %s", resp.StatusCode, truncate(mintErr.Detail, 200))
		}
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("invalid json")
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// sleep waits d or until ctx ends; false means the context ended.
func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
