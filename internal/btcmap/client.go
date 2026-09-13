// Package btcmap proxies the public BTC Map v4 place API without storage.
package btcmap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

const DefaultURL = "https://api.btcmap.org"
const fetchTimeout = 8 * time.Second
const maxBodyBytes = 16 << 20 // the full BTC Map places list is ~5.3 MB today

// Defaults cover the app's strict place schema and every field its detail
// sheet reads. Upstream otherwise returns only id when fields is omitted.
const ListFields = "id,lat,lon,icon,comments,boosted_until,deleted_at,updated_at"
const DetailFields = ListFields + ",name,address,description,phone,website,twitter,facebook,instagram,email,opening_hours,created_at,verified_at,osm_id,osm_url,required_app_url,osm:contact:instagram,osm:contact:twitter,osm:contact:facebook,osm:contact:phone,osm:contact:website,osm:contact:email,osm:payment:onchain,osm:payment:lightning,osm:payment:lightning_contactless,osm:payment:bitcoin,osm:payment:uri,osm:payment:coinos,osm:payment:pouch,osm:amenity,osm:category,osm:survey:date,osm:check_date,osm:check_date:currency:XBT"

var placeID = regexp.MustCompile(`^(?:(?:node|way|relation):)?[0-9]+$`)

type Client struct {
	base string
	http *http.Client
}

func NewClient(base string) *Client {
	return &Client{base: strings.TrimRight(base, "/"), http: &http.Client{
		Timeout: fetchTimeout,
		// A public data proxy must not follow an upstream redirect to an
		// unrelated host or forward callers' authorization/cookie headers.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
}

type Error struct{ Status int }

func (e *Error) Error() string { return http.StatusText(e.Status) }

// Fetch returns validated JSON unchanged, preserving colon-keyed osm fields.
// Empty id selects the list. Only documented sync/field query parameters are
// forwarded; refresh belongs to nagg's response-cache middleware.
func (c *Client) Fetch(ctx context.Context, id string, query url.Values) (json.RawMessage, error) {
	if id != "" && (len(id) > 128 || !placeID.MatchString(id)) {
		return nil, &Error{Status: http.StatusBadRequest}
	}
	params := url.Values{}
	for _, key := range []string{"fields", "updated_since", "include_deleted", "limit"} {
		if values, ok := query[key]; ok {
			params[key] = append([]string(nil), values...)
		}
	}
	path := "/v4/places"
	fields := ListFields
	if id != "" {
		path += "/" + id
		fields = DetailFields
	}
	if !params.Has("fields") {
		params.Set("fields", fields)
	}
	if id == "" && !params.Has("include_deleted") {
		params.Set("include_deleted", "false")
	}
	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path+"?"+params.Encode(), nil)
	if err != nil {
		return nil, &Error{Status: http.StatusBadGateway}
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fetchError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		status := http.StatusBadGateway
		switch resp.StatusCode {
		case http.StatusBadRequest, http.StatusNotFound, http.StatusTooManyRequests:
			status = resp.StatusCode
		case http.StatusGatewayTimeout:
			status = http.StatusGatewayTimeout
		}
		return nil, &Error{Status: status}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes+1))
	if err != nil {
		return nil, fetchError(err)
	}
	trimmed := bytes.TrimSpace(body)
	first := byte('[')
	if id != "" {
		first = '{'
	}
	if len(body) > maxBodyBytes || len(trimmed) == 0 || trimmed[0] != first || !json.Valid(body) {
		return nil, &Error{Status: http.StatusBadGateway}
	}
	return body, nil
}

func fetchError(err error) error {
	var netErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
		return &Error{Status: http.StatusGatewayTimeout}
	}
	return &Error{Status: http.StatusBadGateway}
}
