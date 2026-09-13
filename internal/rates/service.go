package rates

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/vertex-lab/nagg/internal/relayquery"
)

const fetchTimeout = 8 * time.Second
const maxHTTPBody = 64 << 10

type NostrFetcher interface {
	Query(context.Context, map[string]any, time.Duration) ([]relayquery.Event, error)
}

type HTTPFetcher interface {
	Fetch(context.Context, string) ([]byte, error)
}

type JSONFetcher struct{ client *http.Client }

func NewHTTPFetcher() *JSONFetcher {
	return &JSONFetcher{client: &http.Client{Timeout: fetchTimeout}}
}

func (f *JSONFetcher) Fetch(ctx context.Context, url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.New("invalid HTTP request")
	}
	res, err := f.client.Do(req)
	if err != nil {
		return nil, errors.New("HTTP request failed")
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, errors.New("HTTP status not OK")
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, maxHTTPBody+1))
	if err != nil {
		return nil, errors.New("HTTP body read failed")
	}
	if len(body) > maxHTTPBody {
		return nil, errors.New("HTTP body too large")
	}
	return body, nil
}

type Config struct {
	Interval time.Duration
	MaxAge   time.Duration
	StaleFor time.Duration
	Sources  []Source
}

type SourceHealth struct {
	ID                  string `json:"id"`
	Kind                Kind   `json:"kind"`
	Currency            string `json:"currency"`
	OK                  bool   `json:"ok"`
	LastSuccessAt       *int64 `json:"lastSuccessAt"`
	LastErrorAt         *int64 `json:"lastErrorAt"`
	ConsecutiveFailures int    `json:"consecutiveFailures"`
	LastError           string `json:"lastError"`
}

type Snapshot struct {
	Version   int             `json:"version"`
	Base      string          `json:"base"`
	UpdatedAt int64           `json:"updatedAt"`
	Degraded  bool            `json:"degraded"`
	Rates     map[string]Rate `json:"rates"`
	Sources   []SourceHealth  `json:"sources"`
}

type Service struct {
	cfg       Config
	nostr     NostrFetcher
	http      HTTPFetcher
	logger    *slog.Logger
	now       func() time.Time
	passMu    sync.Mutex
	mu        sync.RWMutex
	good      map[string]Rate
	stale     map[string]bool
	health    []SourceHealth
	updatedAt int64
}

func NewService(cfg Config, nostr NostrFetcher, httpFetcher HTTPFetcher, logger *slog.Logger) *Service {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 6 * time.Hour
	}
	if cfg.StaleFor <= 0 {
		cfg.StaleFor = 24 * time.Hour
	}
	if logger == nil {
		logger = slog.Default()
	}
	cfg.Sources = append([]Source(nil), cfg.Sources...)
	s := &Service{cfg: cfg, nostr: nostr, http: httpFetcher, logger: logger, now: time.Now, good: map[string]Rate{}, stale: map[string]bool{}}
	for _, src := range cfg.Sources {
		s.health = append(s.health, SourceHealth{ID: src.ID, Kind: src.Kind, Currency: src.Currency})
	}
	return s
}

// Run performs an immediate pass, then hourly by default. Source failures are
// recorded independently and never terminate the worker.
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		s.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunOnce serializes passes but never holds the snapshot lock during IO.
func (s *Service) RunOnce(ctx context.Context) {
	s.passMu.Lock()
	defer s.passMu.Unlock()
	type document struct {
		body []byte
		err  error
	}
	documents := map[string]document{}
	observations := map[string][]Observation{}
	results := make([]string, len(s.cfg.Sources))
	for i, src := range s.cfg.Sources {
		if ctx.Err() != nil {
			return
		}
		var obs []Observation
		var failure string
		switch src.Kind {
		case NostrNote:
			obs, failure = s.fetchNotes(ctx, src)
		case HTTPJSON:
			doc, exists := documents[src.URL]
			if !exists {
				if s.http == nil {
					doc.err = errors.New("disabled")
				} else {
					doc.body, doc.err = s.http.Fetch(ctx, src.URL)
				}
				documents[src.URL] = doc
			}
			if doc.err != nil {
				failure = "HTTP fetch failed"
			} else {
				if o, ok := parseHTTP(doc.body, src, s.now()); ok {
					obs = []Observation{o}
				} else {
					failure = "invalid HTTP observation"
				}
			}
		default:
			failure = "unsupported source kind"
		}
		var fresh []Observation
		now := s.now()
		for _, o := range obs {
			if !o.At.IsZero() && !o.At.After(now) && now.Sub(o.At) <= s.cfg.MaxAge {
				fresh = append(fresh, o)
			}
		}
		if len(fresh) == 0 && failure == "" {
			failure = "no fresh observations"
		}
		results[i] = failure
		observations[src.Currency] = append(observations[src.Currency], fresh...)
	}
	if ctx.Err() != nil {
		return
	}
	now := s.now()
	s.mu.Lock()
	for i, failure := range results {
		h := &s.health[i]
		h.OK = failure == ""
		at := now.Unix()
		if h.OK {
			h.LastSuccessAt = &at
			h.ConsecutiveFailures = 0
			h.LastError = ""
		} else {
			h.LastErrorAt = &at
			h.ConsecutiveFailures++
			// Fixed categories, never upstream error strings or URLs.
			h.LastError = failure
		}
	}
	for currency := range s.good {
		s.stale[currency] = true
	}
	for currency, obs := range observations {
		var last *Rate
		if old, ok := s.good[currency]; ok {
			last = &old
		}
		if r, ok := Aggregate(obs, now, s.cfg.MaxAge, last); ok {
			s.good[currency] = r
			s.stale[currency] = false
			if r.At > s.updatedAt {
				s.updatedAt = r.At
			}
		}
	}
	s.mu.Unlock()
	snap, warm := s.Snapshot()
	sourceAttrs := make([]slog.Attr, 0, len(snap.Sources))
	for _, h := range snap.Sources {
		sourceAttrs = append(sourceAttrs, slog.Group(h.ID+"."+h.Currency,
			"ok", h.OK, "consecutiveFailures", h.ConsecutiveFailures, "lastError", h.LastError))
	}
	s.logger.Info("rates.pass", "warm", warm, "currencies", len(snap.Rates), "degraded", snap.Degraded,
		"sources", slog.GroupValue(sourceAttrs...))
}

func (s *Service) fetchNotes(ctx context.Context, src Source) ([]Observation, string) {
	if s.nostr == nil {
		return nil, "Nostr fetch failed"
	}
	events, err := s.nostr.Query(ctx, map[string]any{"kinds": []int{1}, "authors": []string{src.Pubkey}, "limit": 10}, fetchTimeout)
	if err != nil && len(events) == 0 {
		return nil, "Nostr fetch failed"
	}
	seen := map[string]bool{}
	var out []Observation
	for _, result := range events {
		e := result.Event
		// Relays can ignore filters and return duplicates. Query verifies
		// signatures; enforce author/kind and dedupe before counting votes.
		if e == nil || e.Kind != 1 || e.PubKey != src.Pubkey || seen[e.ID] {
			continue
		}
		seen[e.ID] = true
		o, ok := ParseBitagentNote(e.Content)
		if !ok || o.Currency != src.Currency {
			continue
		}
		o.At, o.Source, o.Kind = time.Unix(int64(e.CreatedAt), 0), src.ID, src.Kind
		out = append(out, o)
	}
	return out, ""
}

func parseHTTP(body []byte, src Source, now time.Time) (Observation, bool) {
	if len(body) > maxHTTPBody {
		return Observation{}, false
	}
	var doc map[string]json.RawMessage
	if json.Unmarshal(body, &doc) != nil {
		return Observation{}, false
	}
	var price float64
	if json.Unmarshal(doc[src.JSONKey], &price) != nil || !validPrice(price) {
		return Observation{}, false
	}
	at := now
	if raw, exists := doc["time"]; exists {
		var unix int64
		if json.Unmarshal(raw, &unix) != nil || unix <= 0 {
			return Observation{}, false
		}
		at = time.Unix(unix, 0)
	}
	return Observation{Currency: src.Currency, Price: price, At: at, Source: src.ID, Kind: src.Kind}, true
}

// Snapshot returns an independent copy. Expiry is evaluated at read time, even
// if the worker has stopped. Empty/expired currencies are omitted; no remaining
// usable currency means the endpoint must return 503.
func (s *Service) Snapshot() (Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now()
	out := Snapshot{Version: 1, Base: "BTC", UpdatedAt: s.updatedAt, Rates: map[string]Rate{}, Sources: make([]SourceHealth, len(s.health))}
	for i, h := range s.health {
		out.Sources[i] = h
		if h.LastSuccessAt != nil {
			at := *h.LastSuccessAt
			out.Sources[i].LastSuccessAt = &at
		}
		if h.LastErrorAt != nil {
			at := *h.LastErrorAt
			out.Sources[i].LastErrorAt = &at
		}
		if h.ConsecutiveFailures >= 3 {
			out.Degraded = true
		}
		if _, exists := s.good[h.Currency]; !exists {
			out.Degraded = true
		}
	}
	for currency, rate := range s.good {
		age := now.Sub(time.Unix(rate.At, 0))
		if age > s.cfg.StaleFor {
			out.Degraded = true
			continue
		}
		rate.Sources = append([]string(nil), rate.Sources...)
		out.Rates[currency] = rate
		if s.stale[currency] || age > s.cfg.MaxAge || len(rate.Sources) == 1 {
			out.Degraded = true
		}
	}
	return out, len(out.Rates) > 0
}
