package cache

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"golang.org/x/sync/singleflight"
)

// maxBodyBytes caps how much of a GraphQL request body we buffer for keying.
const maxBodyBytes = 1 << 20 // 1 MiB

// capturedResult is the result of computing a response once, shared across
// concurrent identical requests via singleflight.
type capturedResult struct {
	status      int
	contentType string
	body        []byte
}

// capture is an http.ResponseWriter that records the handler's response instead
// of sending it, so the middleware can both cache and forward it.
type capture struct {
	header http.Header
	status int
	wrote  bool
	buf    bytes.Buffer
}

func newCapture() *capture { return &capture{header: make(http.Header), status: http.StatusOK} }

func (c *capture) Header() http.Header { return c.header }

func (c *capture) WriteHeader(status int) {
	if !c.wrote {
		c.status = status
		c.wrote = true
	}
}

func (c *capture) Write(b []byte) (int, error) { return c.buf.Write(b) }

// WrapGraphQL caches successful, error-free POST responses from a GraphQL
// handler. Non-POST requests and requests with a refresh hint bypass the cache.
func WrapGraphQL(next http.HandlerFunc, c Cache, defaultTTL time.Duration) http.HandlerFunc {
	if c == nil || !c.Enabled() {
		return next
	}
	var group singleflight.Group
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next(w, r)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
		_ = r.Body.Close()
		if err != nil {
			r.Body = io.NopCloser(bytes.NewReader(body))
			next(w, r)
			return
		}

		var req struct {
			Query         string         `json:"query"`
			OperationName string         `json:"operationName"`
			Variables     map[string]any `json:"variables"`
		}
		if uerr := json.Unmarshal(body, &req); uerr != nil || req.Query == "" {
			r.Body = io.NopCloser(bytes.NewReader(body))
			next(w, r)
			return
		}

		key := GraphQLKey(req.Query, req.OperationName, req.Variables, findViewer(req.Variables))
		ttl := graphqlTTL(req.Query, defaultTTL)

		if !wantsRefresh(r) {
			if cached, ok := c.Get(r.Context(), key); ok {
				writeResponse(w, http.StatusOK, "application/json", cached, "hit")
				return
			}
		}

		v, _, _ := group.Do(key, func() (any, error) {
			rec := newCapture()
			r.Body = io.NopCloser(bytes.NewReader(body))
			next(rec, r)
			res := capturedResult{status: rec.status, contentType: rec.header.Get("Content-Type"), body: rec.buf.Bytes()}
			if res.status == http.StatusOK && !hasGraphQLErrors(res.body) {
				c.Set(r.Context(), key, res.body, ttl)
			}
			return res, nil
		})
		res := v.(capturedResult)
		writeResponse(w, res.status, res.contentType, res.body, "miss")
	}
}

// WrapREST caches successful GET responses from an app-view handler. Non-GET
// requests and refresh hints bypass the cache.
func WrapREST(next http.HandlerFunc, c Cache, defaultTTL time.Duration) http.HandlerFunc {
	if c == nil || !c.Enabled() {
		return next
	}
	var group singleflight.Group
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			next(w, r)
			return
		}
		viewer := ""
		if pk := r.URL.Query().Get("pubkey"); isHex64(pk) {
			viewer = strings.ToLower(pk)
		}
		key := RESTKey(r.Method, r.URL.Path, r.URL.RawQuery, viewer)
		ttl := restTTL(r.URL.Path, defaultTTL)

		if !wantsRefresh(r) {
			if cached, ok := c.Get(r.Context(), key); ok {
				writeResponse(w, http.StatusOK, "application/json", cached, "hit")
				return
			}
		}

		v, _, _ := group.Do(key, func() (any, error) {
			rec := newCapture()
			next(rec, r)
			res := capturedResult{status: rec.status, contentType: rec.header.Get("Content-Type"), body: rec.buf.Bytes()}
			if res.status == http.StatusOK {
				c.Set(r.Context(), key, res.body, ttl)
			}
			return res, nil
		})
		res := v.(capturedResult)
		writeResponse(w, res.status, res.contentType, res.body, "miss")
	}
}

func writeResponse(w http.ResponseWriter, status int, contentType string, body []byte, cacheState string) {
	if contentType == "" {
		contentType = "application/json"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Nagg-Cache", cacheState)
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// wantsRefresh reports whether the caller asked to bypass and repopulate the
// cache, via ?refresh=1 or a Cache-Control: no-cache header.
func wantsRefresh(r *http.Request) bool {
	switch strings.ToLower(r.URL.Query().Get("refresh")) {
	case "1", "true", "yes":
		return true
	}
	return strings.Contains(strings.ToLower(r.Header.Get("Cache-Control")), "no-cache")
}

// hasGraphQLErrors reports whether a GraphQL response body carries a non-empty
// top-level errors array (or is unparseable), in which case it is not cached.
func hasGraphQLErrors(body []byte) bool {
	var resp struct {
		Errors []json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return true
	}
	return len(resp.Errors) > 0
}

// graphqlTTL picks a TTL based on the dominant root field in the query. DM and
// notification data is kept short-lived; static metadata is long-lived.
func graphqlTTL(query string, def time.Duration) time.Duration {
	switch {
	case strings.Contains(query, "dmEnvelopes"), strings.Contains(query, "dmConversation"):
		return 10 * time.Second
	case strings.Contains(query, "followStatus"):
		return 300 * time.Second
	case strings.Contains(query, "notifications"):
		return 15 * time.Second
	case strings.Contains(query, "ownProfiles"), strings.Contains(query, "profileSearch"):
		return 120 * time.Second
	case strings.Contains(query, "trending"):
		return 60 * time.Second
	case strings.Contains(query, "serviceInfo"):
		return 600 * time.Second
	case strings.Contains(query, "rankedEvents"), strings.Contains(query, "events"):
		return 20 * time.Second
	default:
		return def
	}
}

// restTTL picks a TTL based on the app-view route path.
func restTTL(path string, def time.Duration) time.Duration {
	switch {
	case strings.HasSuffix(path, "/profile"), strings.HasSuffix(path, "/profiles"),
		strings.HasSuffix(path, "/search"), strings.HasSuffix(path, "/recommended"):
		return 120 * time.Second
	case strings.HasSuffix(path, "/follows"):
		return 300 * time.Second
	case strings.HasSuffix(path, "/thread"):
		return 30 * time.Second
	case strings.Contains(path, "/feed"), strings.HasSuffix(path, "/notes/stats"),
		strings.HasSuffix(path, "/events"):
		return 20 * time.Second
	default:
		return def
	}
}
