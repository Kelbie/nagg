package appview

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/vertex-lab/nagg/internal/auditor"
)

func mintInfoRequest(t *testing.T, handler *Handler, mints ...string) (int, []MintInfo) {
	t.Helper()
	query := url.Values{}
	for _, m := range mints {
		query.Add("u", m)
	}
	rec := httptest.NewRecorder()
	handler.mintInfos(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/info?"+query.Encode(), nil))
	if rec.Code != http.StatusOK {
		return rec.Code, nil
	}
	var resp MintInfoResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	return rec.Code, resp.Mints
}

func TestMintInfoVerdictsAndMetadata(t *testing.T) {
	history := &discoverHistory{document: json.RawMessage(`{"name":"Stored name","nuts":{"4":{"methods":[{"unit":"usd"}]}}}`)}
	handler := New(mintReviewStore{}, WithNIP05Validation(false),
		WithAuditor(fakeAuditor{mints: []auditor.Mint{
			{URL: "https://real.example", Name: "Real", Units: []string{"sat"}},
		}}),
		WithMintHistory(history),
		// The probe keys by its own path-preserving URL form.
		WithTestnutMints(fakeTestnuts{
			urls: []string{"https://NoFee.Testnut.example/"},
			real: []string{"https://real.example"},
		}))

	code, rows := mintInfoRequest(t, handler,
		"https://real.example/", "https://nofee.testnut.example", "https://REAL.example", " ")
	if code != http.StatusOK || len(rows) != 2 {
		t.Fatalf("code=%d rows=%+v, want the two distinct mints", code, rows)
	}

	real, testnut := rows[0], rows[1]
	if real.MintURL != "https://real.example/" || !real.Known || real.Testnut ||
		real.ProbedAt != fakeProbedAt.Unix() || real.Name != "Real" || len(real.SupportedUnits) != 1 {
		t.Fatalf("real row = %+v", real)
	}
	// Not in the auditor roster: metadata comes from the stored NUT-06 info.
	if !testnut.Known || !testnut.Testnut || testnut.ProbedAt != fakeProbedAt.Unix() ||
		testnut.Name != "Stored name" || len(testnut.SupportedUnits) != 1 || testnut.SupportedUnits[0] != "usd" {
		t.Fatalf("testnut row = %+v", testnut)
	}
}

func TestMintInfoUnknownMintIsNotAVerdict(t *testing.T) {
	handler := New(mintReviewStore{}, WithNIP05Validation(false),
		WithMintHistory(&discoverHistory{}), WithTestnutMints(fakeTestnuts{}))

	rec := httptest.NewRecorder()
	handler.mintInfos(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/info?u=https%3A%2F%2Fprivate.example", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	// probedAt must be absent, not 0: the app only caches a verdict that has one.
	if body := rec.Body.String(); strings.Contains(body, "probedAt") ||
		!strings.Contains(body, `"known":false`) || !strings.Contains(body, `"testnut":false`) {
		t.Fatalf("body = %s", body)
	}
}

func TestMintInfoRejectsBadRequests(t *testing.T) {
	handler := New(mintReviewStore{}, WithNIP05Validation(false))
	if code, _ := mintInfoRequest(t, handler); code != http.StatusBadRequest {
		t.Fatalf("no urls = %d, want 400", code)
	}
	many := make([]string, mintInfoMaxURLs+1)
	for i := range many {
		many[i] = "https://m" + string(rune('a'+i%26)) + string(rune('a'+i/26)) + ".example"
	}
	if code, _ := mintInfoRequest(t, handler, many...); code != http.StatusBadRequest {
		t.Fatalf("over cap = %d, want 400", code)
	}
	rec := httptest.NewRecorder()
	handler.mintInfos(rec, httptest.NewRequest(http.MethodPost, "/nostr/mint/info?u=https%3A%2F%2Fm", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST = %d, want 405", rec.Code)
	}
}

// The app caches these verdicts, so a failed lookup must not read as "real mint".
func TestMintInfoVerdictLookupFailure(t *testing.T) {
	handler := New(mintReviewStore{}, WithNIP05Validation(false),
		WithTestnutMints(fakeTestnuts{err: errors.New("clickhouse down")}))
	if code, _ := mintInfoRequest(t, handler, "https://real.example"); code == http.StatusOK {
		t.Fatal("answered without verdicts")
	}
}
