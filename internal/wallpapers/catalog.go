// Package wallpapers builds the Sovran catalog from verified admin events.
// It has no dependency on event storage or the nostr module.
package wallpapers

import (
	"encoding/json"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/nagg/internal/relayquery"
)

const DefaultAdminPubkey = "1e53e900c3bbc5ead295215efe27b2c8d5fbd15fb3dd810da3063674cb7213b2"
const albumNamespace = "money.sovran.wallpaper"
const catalogTag = "wallpaper-catalog"

type Catalog struct {
	Wallpapers  []Wallpaper `json:"wallpapers"`
	Albums      []Album     `json:"albums"`
	LastUpdated int64       `json:"lastUpdated"` // successful refresh, Unix milliseconds
}

type Wallpaper struct {
	EventID        string            `json:"eventId"`
	ThemeName      string            `json:"themeName"`
	DisplayName    string            `json:"displayName"`
	BlossomURL     string            `json:"blossomUrl"`
	ThumbURL       string            `json:"thumbUrl"`
	SHA256         string            `json:"sha256"`
	FileSize       int64             `json:"fileSize"`
	Dimensions     string            `json:"dimensions"`
	AlbumSlug      string            `json:"albumSlug"`
	Palette        map[string]string `json:"palette"`
	DominantColors []json.RawMessage `json:"dominantColors"`
	GradientColors []json.RawMessage `json:"gradientColors"`
	CreatedAt      int64             `json:"createdAt"`
}

type Album struct {
	Slug           string `json:"slug"`
	DisplayName    string `json:"displayName"`
	Description    string `json:"description"`
	SortOrder      int64  `json:"sortOrder"`
	Topic          string `json:"topic"`
	CoverThemeName string `json:"coverThemeName,omitempty"`
}

// buildCatalog rechecks relay filters (Query verifies signatures, not filters).
// Only declared catalog fields escape this boundary; newest events win, with
// lexicographically lowest IDs breaking timestamp ties deterministically.
func buildCatalog(events []relayquery.Event, admin string, now int64) (Catalog, bool) {
	c := Catalog{Wallpapers: []Wallpaper{}, Albums: []Album{}}
	byTheme := map[string]Wallpaper{}
	var albumEvent *nostr.Event
	for _, result := range events {
		e := result.Event
		if e == nil || e.PubKey != admin || int64(e.CreatedAt) < 0 || int64(e.CreatedAt) > now {
			continue
		}
		if e.Kind == 30078 && tag(e, "d") == catalogTag {
			if albumEvent == nil || e.CreatedAt > albumEvent.CreatedAt || (e.CreatedAt == albumEvent.CreatedAt && e.ID < albumEvent.ID) {
				albumEvent = e
			}
		}
		if e.Kind != 1063 || !hasTag(e, "t", "wallpaper") {
			continue
		}
		w, ok := parseWallpaper(e)
		if !ok {
			continue
		}
		old, exists := byTheme[w.ThemeName]
		if !exists || w.CreatedAt > old.CreatedAt || (w.CreatedAt == old.CreatedAt && w.EventID < old.EventID) {
			byTheme[w.ThemeName] = w
		}
	}
	for _, w := range byTheme {
		c.Wallpapers = append(c.Wallpapers, w)
	}
	sort.Slice(c.Wallpapers, func(i, j int) bool {
		a, b := c.Wallpapers[i], c.Wallpapers[j]
		if a.CreatedAt != b.CreatedAt {
			return a.CreatedAt > b.CreatedAt
		}
		return a.EventID < b.EventID
	})
	if albumEvent != nil {
		var doc struct {
			Albums []json.RawMessage `json:"albums"`
		}
		if json.Unmarshal([]byte(albumEvent.Content), &doc) != nil || doc.Albums == nil || len(doc.Albums) > 1000 {
			return c, false
		}
		for _, raw := range doc.Albums {
			a := Album{Topic: "Other"}
			if json.Unmarshal(raw, &a) != nil || a.Slug == "" || a.DisplayName == "" ||
				!bounded(a.Slug, 64) || !bounded(a.DisplayName, 128) || !bounded(a.Description, 1024) ||
				!bounded(a.Topic, 64) || !bounded(a.CoverThemeName, 64) || a.SortOrder > 1<<53-1 || a.SortOrder < -(1<<53-1) {
				continue
			}
			c.Albums = append(c.Albums, a)
		}
	}
	if len(c.Albums) == 0 {
		seen := map[string]bool{}
		for _, w := range c.Wallpapers {
			if seen[w.AlbumSlug] {
				continue
			}
			seen[w.AlbumSlug] = true
			r, n := utf8.DecodeRuneInString(w.AlbumSlug)
			c.Albums = append(c.Albums, Album{Slug: w.AlbumSlug, DisplayName: strings.ToUpper(string(r)) + w.AlbumSlug[n:], Topic: "Other"})
		}
	}
	// Empty relay results can also mean a timeout. Never replace a usable
	// snapshot with an empty pass or reset its expiry on such a pass.
	return c, len(c.Wallpapers) > 0 && len(c.Wallpapers) <= 10000 && len(c.Albums) <= 1000
}

func parseWallpaper(e *nostr.Event) (Wallpaper, bool) {
	w := Wallpaper{EventID: e.ID, ThemeName: tag(e, "theme_name"), DisplayName: tag(e, "title"),
		BlossomURL: tag(e, "url"), ThumbURL: tag(e, "thumb"), SHA256: tag(e, "x"), Dimensions: tag(e, "dim"),
		AlbumSlug: "uncategorized", CreatedAt: int64(e.CreatedAt), Palette: map[string]string{},
		DominantColors: []json.RawMessage{}, GradientColors: []json.RawMessage{}}
	if w.DisplayName == "" {
		w.DisplayName = w.ThemeName
	}
	if w.ThumbURL == "" {
		w.ThumbURL = w.BlossomURL
	}
	for _, t := range e.Tags {
		if len(t) >= 3 && t[0] == "l" && t[2] == albumNamespace && t[1] != "" {
			w.AlbumSlug = t[1]
			break
		}
	}
	if size, err := strconv.ParseInt(tag(e, "size"), 10, 64); err == nil && size >= 0 && size <= 1<<53-1 {
		w.FileSize = size
	}
	if w.ThemeName == "" || !bounded(w.ThemeName, 64) || !bounded(w.DisplayName, 128) ||
		!httpURL(w.BlossomURL) || !httpURL(w.ThumbURL) || !bounded(w.SHA256, 128) ||
		!bounded(w.Dimensions, 32) || !bounded(w.AlbumSlug, 64) {
		return w, false
	}
	var palette map[string]string
	if json.Unmarshal([]byte(tag(e, "palette")), &palette) == nil {
		for key, color := range palette {
			if bounded(key, 8) && colorPattern.MatchString(color) {
				w.Palette[key] = color
			}
		}
	}
	w.DominantColors = parseColors(tag(e, "dominant_colors"), false)
	w.GradientColors = parseColors(tag(e, "gradient_colors"), true)
	return w, true
}

var colorPattern = regexp.MustCompile(`^#[0-9a-fA-F]{3,8}$`)

func parseColors(raw string, gradient bool) []json.RawMessage {
	out := []json.RawMessage{}
	var entries []map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &entries) != nil {
		return out
	}
	for _, entry := range entries {
		var hex string
		if json.Unmarshal(entry["hex"], &hex) != nil || !colorPattern.MatchString(hex) {
			continue
		}
		clean := map[string]any{"hex": hex}
		if gradient {
			var position string
			var hsb map[string]json.RawMessage
			if json.Unmarshal(entry["position"], &position) != nil || (position != "light" && position != "mid" && position != "dark") || json.Unmarshal(entry["hsb"], &hsb) != nil {
				continue
			}
			values, ok := colorNumbers(hsb, "hue", "saturation", "brightness")
			if !ok {
				continue
			}
			clean["position"], clean["hsb"] = position, values
		} else {
			values, ok := colorNumbers(entry, "hue", "saturation", "lightness")
			if !ok {
				continue
			}
			for key, value := range values {
				clean[key] = value
			}
		}
		encoded, _ := json.Marshal(clean)
		out = append(out, encoded)
		if len(out) == 32 {
			break
		}
	}
	return out
}

func colorNumbers(entry map[string]json.RawMessage, keys ...string) (map[string]float64, bool) {
	out := map[string]float64{}
	for _, key := range keys {
		var value *float64
		if json.Unmarshal(entry[key], &value) != nil || value == nil {
			return nil, false
		}
		out[key] = *value
	}
	return out, true
}

func tag(e *nostr.Event, key string) string {
	for _, t := range e.Tags {
		if len(t) >= 2 && t[0] == key {
			return t[1]
		}
	}
	return ""
}

func hasTag(e *nostr.Event, key, value string) bool {
	for _, t := range e.Tags {
		if len(t) >= 2 && t[0] == key && t[1] == value {
			return true
		}
	}
	return false
}

// The app's Zod string limits count JavaScript UTF-16 code units.
func bounded(s string, limit int) bool {
	n := 0
	for _, r := range s {
		n += utf16.RuneLen(r)
		if n > limit {
			return false
		}
	}
	return true
}

func httpURL(s string) bool {
	u, err := url.Parse(s)
	return err == nil && bounded(s, 2048) && u.Hostname() != "" && (u.Scheme == "https" || u.Scheme == "http")
}
