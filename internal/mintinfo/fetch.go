package mintinfo

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// InfoFetcher fetches one mint's raw info document. ok is false when the mint is
// unreachable or returned an unusable body — the poll is still recorded, just as
// an unreachable observation.
type InfoFetcher interface {
	Info(ctx context.Context, mintURL string) (raw []byte, ok bool)
}

// maxInfoBody caps the /v1/info read. NUT-06 responses are a few KB; the cap
// guards against a hostile or misbehaving endpoint streaming forever.
const maxInfoBody = 1 << 20

// HTTPFetcher polls each mint's own info endpoint directly (Source.InfoPath),
// borrowing the auditor client's defensive posture: a tight timeout so a slow
// mint degrades to "unreachable" quickly, and a capped body reader.
type HTTPFetcher struct {
	client *http.Client
	source Source
	logger *slog.Logger
}

// NewHTTPFetcher builds a direct-poll fetcher for the source.
func NewHTTPFetcher(source Source, timeout time.Duration, logger *slog.Logger) *HTTPFetcher {
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &HTTPFetcher{
		client: &http.Client{Timeout: timeout},
		source: source,
		logger: logger,
	}
}

func (f *HTTPFetcher) Info(ctx context.Context, mintURL string) ([]byte, bool) {
	url := strings.TrimRight(mintURL, "/") + f.source.InfoPath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		f.logger.Debug("mintinfo: bad request url", "mint", mintURL, "error", err)
		return nil, false
	}
	resp, err := f.client.Do(req)
	if err != nil {
		f.logger.Debug("mintinfo: fetch failed", "mint", mintURL, "error", err)
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		f.logger.Debug("mintinfo: non-200", "mint", mintURL, "status", resp.StatusCode)
		return nil, false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxInfoBody))
	if err != nil {
		f.logger.Debug("mintinfo: read failed", "mint", mintURL, "error", err)
		return nil, false
	}
	if !json.Valid(body) {
		f.logger.Debug("mintinfo: invalid json", "mint", mintURL)
		return nil, false
	}
	return body, true
}
