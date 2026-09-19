package mintprobe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/schnorr"
)

// fakeMint is a minimal NUT-04 mint. With fakeBackend it marks quotes paid
// after paidAfter state reads (0 = at creation), like a testnut; otherwise
// quotes stay UNPAID forever, like a real Lightning backend.
type fakeMint struct {
	fakeBackend bool
	paidAfter   int
	mintFails   bool

	mu      sync.Mutex
	reads   map[string]int
	pubkeys map[string]string
	minted  int
}

func newFakeMint() *fakeMint {
	return &fakeMint{reads: map[string]int{}, pubkeys: map[string]string{}}
}

func (f *fakeMint) paid(id string) bool {
	return f.fakeBackend && f.reads[id] >= f.paidAfter
}

func (f *fakeMint) quoteBody(method, id string) map[string]any {
	paid := f.paid(id)
	if method == "bolt11" {
		state := "UNPAID"
		if paid {
			state = "PAID"
		}
		return map[string]any{"quote": id, "state": state, "amount": 1, "unit": "sat"}
	}
	amountPaid := 0
	if paid {
		amountPaid = 1
	}
	return map[string]any{"quote": id, "amount": 1, "unit": "sat", "amount_paid": amountPaid, "amount_issued": 0}
}

func (f *fakeMint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/"), "/")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/keysets":
		_ = json.NewEncoder(w).Encode(map[string]any{"keysets": []map[string]any{
			{"id": "00old", "unit": "sat", "active": false},
			{"id": "00ad268c4d1f5826", "unit": "sat", "active": true},
		}})
	case r.Method == http.MethodPost && len(parts) == 3 && parts[0] == "mint" && parts[1] == "quote":
		var req struct {
			Amount uint64 `json:"amount"`
			Unit   string `json:"unit"`
			Pubkey string `json:"pubkey"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if parts[2] != "bolt11" && req.Pubkey == "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"detail":"pubkey required","code":20009}`))
			return
		}
		id := parts[2] + "-q" + string(rune('0'+len(f.reads)))
		f.reads[id] = 0
		f.pubkeys[id] = req.Pubkey
		_ = json.NewEncoder(w).Encode(f.quoteBody(parts[2], id))
	case r.Method == http.MethodGet && len(parts) == 4 && parts[0] == "mint" && parts[1] == "quote":
		f.reads[parts[3]]++
		_ = json.NewEncoder(w).Encode(f.quoteBody(parts[2], parts[3]))
	case r.Method == http.MethodPost && len(parts) == 2 && parts[0] == "mint":
		var req struct {
			Quote   string `json:"quote"`
			Outputs []struct {
				Amount uint64 `json:"amount"`
				ID     string `json:"id"`
				B      string `json:"B_"`
			} `json:"outputs"`
			Signature string `json:"signature"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if f.mintFails || !f.paid(req.Quote) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"detail":"quote not paid","code":20001}`))
			return
		}
		if pk := f.pubkeys[req.Quote]; pk != "" && !validNUT20(pk, req.Quote, req.Signature, req.Outputs) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"detail":"bad signature","code":20008}`))
			return
		}
		sigs := make([]map[string]any, 0, len(req.Outputs))
		for _, o := range req.Outputs {
			if o.ID != "00ad268c4d1f5826" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			sigs = append(sigs, map[string]any{"amount": o.Amount, "id": o.ID, "C_": "02" + strings.Repeat("ab", 32)})
		}
		f.minted++
		_ = json.NewEncoder(w).Encode(map[string]any{"signatures": sigs})
	default:
		http.NotFound(w, r)
	}
}

func validNUT20(pubHex, quote, sigHex string, outputs []struct {
	Amount uint64 `json:"amount"`
	ID     string `json:"id"`
	B      string `json:"B_"`
}) bool {
	pubBytes, err := hex.DecodeString(pubHex)
	if err != nil {
		return false
	}
	pub, err := btcec.ParsePubKey(pubBytes)
	if err != nil {
		return false
	}
	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil {
		return false
	}
	sig, err := schnorr.ParseSignature(sigBytes)
	if err != nil {
		return false
	}
	msg := quote
	for _, o := range outputs {
		msg += o.B
	}
	digest := sha256.Sum256([]byte(msg))
	return sig.Verify(digest[:], pub)
}

func TestProbeDetectsTestnutAcrossMethods(t *testing.T) {
	mint := newFakeMint()
	mint.fakeBackend = true
	mint.paidAfter = 1 // settles on the first re-read, like CDK's fake wallet
	srv := httptest.NewServer(mint)
	defer srv.Close()

	prober := NewHTTPProber(time.Second, 3, time.Millisecond)
	for _, method := range []string{"bolt11", "bolt12"} {
		res := prober.Probe(context.Background(), srv.URL, Method{Method: method, Unit: "sat"})
		if res.Status != StatusIssued || !res.Paid || !res.Issued || res.Error != "" {
			t.Fatalf("%s: %+v", method, res)
		}
	}
	if mint.minted != 2 {
		t.Fatalf("minted = %d, want 2", mint.minted)
	}
}

func TestProbeRealBackendStaysUnpaid(t *testing.T) {
	mint := newFakeMint()
	srv := httptest.NewServer(mint)
	defer srv.Close()

	res := NewHTTPProber(time.Second, 2, time.Millisecond).Probe(context.Background(), srv.URL, Method{Method: "bolt11", Unit: "sat"})
	if res.Status != StatusUnpaid || res.Paid || res.Issued {
		t.Fatalf("real backend: %+v", res)
	}
	if mint.minted != 0 {
		t.Fatalf("minted against an unpaid quote")
	}
}

func TestProbePaidButMintFails(t *testing.T) {
	mint := newFakeMint()
	mint.fakeBackend, mint.mintFails = true, true
	srv := httptest.NewServer(mint)
	defer srv.Close()

	res := NewHTTPProber(time.Second, 0, 0).Probe(context.Background(), srv.URL, Method{Method: "bolt11", Unit: "sat"})
	if res.Status != StatusPaidNotIssued || !res.Paid || res.Issued || !strings.Contains(res.Error, "quote not paid") {
		t.Fatalf("paid-not-issued: %+v", res)
	}
}

func TestProbeQuoteRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"detail":"minting disabled","code":11000}`))
	}))
	defer srv.Close()

	res := NewHTTPProber(time.Second, 0, 0).Probe(context.Background(), srv.URL, Method{Method: "bolt11", Unit: "sat"})
	if res.Status != StatusQuoteFailed || res.Error != "http 400: minting disabled" || res.Status.Verdict() {
		t.Fatalf("refused: %+v", res)
	}
}

func TestMintMethods(t *testing.T) {
	info := []byte(`{"nuts":{"4":{"methods":[
		{"method":"bolt11","unit":"sat","min_amount":10,"max_amount":100000},
		{"method":"BOLT11","unit":"SAT"},
		{"method":"bolt12","unit":"sat"},
		{"method":"bolt11","unit":"usd"},
		{"method":"","unit":"sat"}
	],"disabled":false}}}`)
	got := MintMethods(info)
	want := []Method{
		{Method: "bolt11", Unit: "sat", MinAmount: 10, MaxAmount: 100000},
		{Method: "bolt12", Unit: "sat"},
		{Method: "bolt11", Unit: "usd"},
	}
	if len(got) != len(want) {
		t.Fatalf("methods = %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("method %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	if MintMethods([]byte(`{"nuts":{"4":{"methods":[{"method":"bolt11","unit":"sat"}],"disabled":true}}}`)) != nil {
		t.Fatal("disabled NUT-04 should yield no methods")
	}
	if MintMethods([]byte(`{"nuts":{}}`)) != nil || MintMethods([]byte(`not json`)) != nil {
		t.Fatal("missing NUT-04 should yield no methods")
	}
	if probeAmount(got[0]) != 10 || probeAmount(got[1]) != 1 {
		t.Fatal("probe amount should be min_amount or 1")
	}
}

func TestSplitAmount(t *testing.T) {
	got := splitAmount(13)
	if len(got) != 3 || got[0] != 1 || got[1] != 4 || got[2] != 8 {
		t.Fatalf("split(13) = %v", got)
	}
}

// --- runner -----------------------------------------------------------------

type fakeStore struct {
	last    map[string]time.Time
	written []Result
}

func (s *fakeStore) PutProbeResults(_ context.Context, results []Result) error {
	s.written = append(s.written, results...)
	return nil
}

func (s *fakeStore) LastMintProbes(context.Context) (map[string]time.Time, error) {
	return s.last, nil
}

type staticMints []string

func (m staticMints) MintURLs(context.Context) ([]string, error) { return m, nil }

type staticInfo map[string]string

func (i staticInfo) Info(_ context.Context, mint string) ([]byte, bool) {
	doc, ok := i[mint]
	return []byte(doc), ok
}

type recordingProber struct{ calls []string }

func (p *recordingProber) Probe(_ context.Context, mint string, m Method) Result {
	p.calls = append(p.calls, mint+" "+m.Method+"/"+m.Unit)
	return Result{Method: m.Method, Unit: m.Unit, Status: StatusIssued, Paid: true, Issued: true}
}

func TestRunnerProbesDueMintsOncePerMinAge(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	store := &fakeStore{last: map[string]time.Time{
		"https://fresh": now.Add(-6 * 24 * time.Hour),
		"https://stale": now.Add(-8 * 24 * time.Hour),
	}}
	info := staticInfo{
		"https://stale": `{"nuts":{"4":{"methods":[{"method":"bolt11","unit":"sat"},{"method":"bolt12","unit":"sat"}]}}}`,
		"https://fresh": `{"nuts":{"4":{"methods":[{"method":"bolt11","unit":"sat"}]}}}`,
		"https://new":   `{"nuts":{"4":{"disabled":true}}}`,
	}
	prober := &recordingProber{}
	runner := NewRunner(store, staticMints{"https://fresh", "https://stale", "https://new", "https://down"}, info, prober, Config{Throttle: time.Nanosecond}, nil)
	runner.now = func() time.Time { return now }

	stats, err := runner.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Due != 3 || stats.Skipped != 1 || stats.Probes != 2 || stats.Paid != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	if strings.Join(prober.calls, ",") != "https://stale bolt11/sat,https://stale bolt12/sat" {
		t.Fatalf("probes = %v", prober.calls)
	}
	statuses := map[string]Status{}
	for _, r := range store.written {
		statuses[r.MintURL+" "+r.Method] = r.Status
	}
	if statuses["https://new "] != StatusNoMethods || statuses["https://down "] != StatusInfoUnreachable || statuses["https://stale bolt12"] != StatusIssued {
		t.Fatalf("written = %+v", store.written)
	}
}
