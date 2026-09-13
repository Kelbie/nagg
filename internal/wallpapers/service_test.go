package wallpapers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/nagg/internal/relayquery"
)

func wallpaperEvent(id string, at int64) *nostr.Event {
	return &nostr.Event{ID: id, PubKey: DefaultAdminPubkey, Kind: 1063, CreatedAt: nostr.Timestamp(at), Tags: nostr.Tags{
		{"t", "wallpaper"}, {"theme_name", "sunset"}, {"title", "Sunset"}, {"url", "https://example.com/full.jpg"},
		{"thumb", "https://example.com/thumb.jpg"}, {"x", "hash"}, {"size", "1024"}, {"dim", "100x200"},
		{"l", "wrong", "another.namespace"}, {"l", "nature", albumNamespace},
		{"palette", `{"50":"#fff","900":"#000000","bad":"not a color"}`},
		{"dominant_colors", `[{"hex":"#fff","hue":1,"saturation":2,"lightness":3,"unknown":"secret"}]`},
		{"gradient_colors", `[{"hex":"#000","position":"dark","hsb":{"hue":1,"saturation":2,"brightness":3,"unknown":"secret"}}]`},
		{"private", "secret"},
	}}
}

func TestWallpaperCatalogContract(t *testing.T) {
	old, latest := wallpaperEvent("b", 100), wallpaperEvent("a", 200)
	album := &nostr.Event{ID: "album", PubKey: DefaultAdminPubkey, Kind: 30078, CreatedAt: 150,
		Tags: nostr.Tags{{"d", catalogTag}}, Content: `{"albums":[{"slug":"nature","displayName":"Nature","description":"Outdoors","sortOrder":2,"topic":"Art","coverThemeName":"sunset","unknown":"secret","author":{"pubkey":"other"}}]}`}
	wrongAuthor := wallpaperEvent("other", 300)
	wrongAuthor.PubKey = "other"
	wrongKind := wallpaperEvent("other-kind", 300)
	wrongKind.Kind = 1
	future := wallpaperEvent("future", 501)
	wrongTag := wallpaperEvent("wrong-tag", 300)
	wrongTag.Tags[0][1] = "other"
	olderAlbum := *album
	olderAlbum.CreatedAt = 100
	olderAlbum.Content = `{"albums":[{"slug":"old","displayName":"Old"}]}`
	events := []relayquery.Event{{Event: old}, {Event: latest}, {Event: album}, {Event: wrongAuthor}, {Event: wrongKind}, {Event: future}, {Event: wrongTag}, {Event: &olderAlbum}, {Event: latest}, {}}
	c, ok := buildCatalog(events, DefaultAdminPubkey, 500)
	if !ok || len(c.Wallpapers) != 1 || len(c.Albums) != 1 {
		t.Fatalf("catalog %+v, ok=%v", c, ok)
	}
	w := c.Wallpapers[0]
	if w.EventID != "a" || w.CreatedAt != 200 || w.ThemeName != "sunset" || w.DisplayName != "Sunset" || w.BlossomURL != "https://example.com/full.jpg" || w.ThumbURL != "https://example.com/thumb.jpg" || w.SHA256 != "hash" || w.FileSize != 1024 || w.Dimensions != "100x200" || w.AlbumSlug != "nature" || len(w.Palette) != 2 || len(w.DominantColors) != 1 || len(w.GradientColors) != 1 {
		t.Fatalf("wallpaper %+v", w)
	}
	if c.Albums[0] != (Album{Slug: "nature", DisplayName: "Nature", Description: "Outdoors", SortOrder: 2, Topic: "Art", CoverThemeName: "sunset"}) {
		t.Fatalf("album %+v", c.Albums)
	}
	b, err := json.Marshal(c)
	if err != nil || strings.Contains(string(b), "secret") || strings.Contains(string(b), "unknown") || strings.Contains(string(b), "author") {
		t.Fatalf("unexpected JSON %s, %v", b, err)
	}
}

func TestWallpaperFallbacksAndMalformedMetadata(t *testing.T) {
	e := wallpaperEvent("a", 1)
	e.Tags = nostr.Tags{{"t", "wallpaper"}, {"theme_name", "plain"}, {"url", "https://example.com/a"}, {"size", "-1"}, {"palette", `[]`}, {"dominant_colors", `[{"hex":"#fff","hue":null}]`}, {"gradient_colors", `bad json`}, {"l"}}
	c, ok := buildCatalog([]relayquery.Event{{Event: e}}, DefaultAdminPubkey, 2)
	if !ok || len(c.Albums) != 1 {
		t.Fatalf("catalog %+v", c)
	}
	w := c.Wallpapers[0]
	if w.DisplayName != "plain" || w.ThumbURL != w.BlossomURL || w.AlbumSlug != "uncategorized" || w.FileSize != 0 || w.Palette == nil || len(w.DominantColors) != 0 || len(w.GradientColors) != 0 || c.Albums[0].Topic != "Other" {
		t.Fatalf("fallbacks %+v", c)
	}
	for _, bad := range []nostr.Tag{{"url", "javascript:bad"}, {"theme_name", strings.Repeat("x", 65)}, {"theme_name", strings.Repeat("🌅", 33)}, {"thumb", "file:///bad"}} {
		t.Run(bad[0], func(t *testing.T) {
			broken := *e
			broken.Tags = append(nostr.Tags{bad}, e.Tags...)
			if _, ok := buildCatalog([]relayquery.Event{{Event: &broken}}, DefaultAdminPubkey, 2); ok {
				t.Fatal("accepted malformed wallpaper")
			}
		})
	}
}

func TestWallpaperCatalogOrderingAndMalformedAlbum(t *testing.T) {
	a, b, c := wallpaperEvent("a", 2), wallpaperEvent("b", 2), wallpaperEvent("c", 1)
	c.Tags = append(nostr.Tags{{"theme_name", "other"}}, c.Tags...)
	for _, events := range [][]relayquery.Event{
		{{Event: b}, {Event: c}, {Event: a}},
		{{Event: a}, {Event: c}, {Event: b}},
	} {
		catalog, ok := buildCatalog(events, DefaultAdminPubkey, 10)
		if !ok || len(catalog.Wallpapers) != 2 || catalog.Wallpapers[0].EventID != "a" || catalog.Wallpapers[1].EventID != "c" {
			t.Fatalf("unstable order %+v", catalog)
		}
	}
	album := &nostr.Event{PubKey: DefaultAdminPubkey, Kind: 30078, CreatedAt: 1, Tags: nostr.Tags{{"d", catalogTag}}}
	for _, content := range []string{`bad`, `null`, `{}`, `{"albums":null}`, `{"albums":"wrong"}`} {
		album.Content = content
		if _, ok := buildCatalog([]relayquery.Event{{Event: a}, {Event: album}}, DefaultAdminPubkey, 10); ok {
			t.Fatalf("accepted malformed album: %s", content)
		}
	}
	album.Content = `{"albums":[{"slug":"bad","displayName":5},{"slug":"unsafe","displayName":"Unsafe","sortOrder":9007199254740992},{"slug":"nature","displayName":"Nature"}]}`
	catalog, ok := buildCatalog([]relayquery.Event{{Event: a}, {Event: album}}, DefaultAdminPubkey, 10)
	if !ok || len(catalog.Albums) != 1 || catalog.Albums[0].Topic != "Other" {
		t.Fatalf("album defaults/validation %+v", catalog)
	}
}

type fakeFetcher struct {
	events []relayquery.Event
	err    error
	calls  int
}

func (f *fakeFetcher) Query(_ context.Context, filter map[string]any, _ time.Duration) ([]relayquery.Event, error) {
	f.calls++
	var result []relayquery.Event
	for _, e := range f.events {
		if e.Event.Kind == filter["kinds"].([]int)[0] {
			result = append(result, e)
		}
	}
	return result, f.err
}

func TestWallpaperSnapshotExpiryAndFailedRefresh(t *testing.T) {
	now := time.Unix(1000, 0)
	f := &fakeFetcher{events: []relayquery.Event{{Event: wallpaperEvent("a", 1)}}}
	s := NewService(Config{}, f, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s.now = func() time.Time { return now }
	if _, ok := s.Snapshot(); ok {
		t.Fatal("cold snapshot available")
	}
	s.RunOnce(context.Background())
	first, ok := s.Snapshot()
	if !ok {
		t.Fatal("not warm")
	}
	var catalog Catalog
	if json.Unmarshal(first, &catalog) != nil || catalog.LastUpdated != now.UnixMilli() {
		t.Fatal("missing refresh timestamp")
	}
	first[0] = '!'
	if body, _ := s.Snapshot(); body[0] != '{' {
		t.Fatal("snapshot mutated")
	}
	now = now.Add(23 * time.Hour)
	f.err = errors.New("upstream private detail")
	s.RunOnce(context.Background())
	if _, ok := s.Snapshot(); !ok {
		t.Fatal("failed to serve stale")
	}
	f.err = nil
	f.events = nil
	s.RunOnce(context.Background())
	now = now.Add(time.Hour)
	if _, ok := s.Snapshot(); ok {
		t.Fatal("empty/failed pass extended expiry")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := f.calls
	s.Run(ctx)
	if f.calls != calls {
		t.Fatal("canceled Run queried")
	}
}

func TestWallpaperRelaySignatureAndFilters(t *testing.T) {
	key := nostr.GeneratePrivateKey()
	file := wallpaperEvent("", 100)
	if err := file.Sign(key); err != nil {
		t.Fatal(err)
	}
	album := &nostr.Event{Kind: 30078, CreatedAt: 100, Tags: nostr.Tags{{"d", catalogTag}}, Content: `{"albums":[{"slug":"nature","displayName":"Nature"}]}`}
	if err := album.Sign(key); err != nil {
		t.Fatal(err)
	}
	tampered := *file
	tampered.Content = "tampered"
	tampered.Tags = append(nostr.Tags{{"theme_name", "unverified"}}, file.Tags...)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		var req []json.RawMessage
		if err := conn.ReadJSON(&req); err != nil {
			t.Error(err)
			return
		}
		var id string
		_ = json.Unmarshal(req[1], &id)
		var filter struct {
			Authors []string `json:"authors"`
			Kinds   []int    `json:"kinds"`
			D       []string `json:"#d"`
			T       []string `json:"#t"`
		}
		if err := json.Unmarshal(req[2], &filter); err != nil {
			t.Error(err)
			return
		}
		if len(filter.Authors) != 1 || filter.Authors[0] != file.PubKey || len(filter.Kinds) != 1 {
			t.Error("wrong filter")
			return
		}
		event := file
		if filter.Kinds[0] == 30078 {
			event = album
			if len(filter.D) != 1 || filter.D[0] != catalogTag {
				t.Error("missing catalog d filter")
			}
		} else if len(filter.T) != 1 || filter.T[0] != "wallpaper" {
			t.Error("missing wallpaper t filter")
		}
		_ = conn.WriteJSON([]any{"EVENT", id, &tampered})
		_ = conn.WriteJSON([]any{"EVENT", id, event})
		_ = conn.WriteJSON([]any{"EOSE", id})
	}))
	defer server.Close()
	s := NewService(Config{AdminPubkey: file.PubKey}, relayquery.Client{Relays: []string{"ws" + strings.TrimPrefix(server.URL, "http")}}, nil)
	s.RunOnce(context.Background())
	body, ok := s.Snapshot()
	if !ok {
		t.Fatal("signed catalog not warm")
	}
	var c Catalog
	if json.Unmarshal(body, &c) != nil || len(c.Wallpapers) != 1 || c.Wallpapers[0].EventID != file.ID {
		t.Fatalf("unexpected catalog %s", body)
	}
}
