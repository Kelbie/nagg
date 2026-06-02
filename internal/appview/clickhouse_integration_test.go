package appview

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nbd-wtf/go-nostr"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/vertex"
)

func TestClickHouseAppViewIntegration(t *testing.T) {
	addr := strings.TrimSpace(os.Getenv("NAGG_INTEGRATION_CLICKHOUSE_ADDR"))
	if addr == "" {
		t.Skip("set NAGG_INTEGRATION_CLICKHOUSE_ADDR to run ClickHouse-backed app-view integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	username := envFallback("NAGG_INTEGRATION_CLICKHOUSE_USERNAME", "NAGG_CLICKHOUSE_USERNAME", "default")
	password := envFallback("NAGG_INTEGRATION_CLICKHOUSE_PASSWORD", "NAGG_CLICKHOUSE_PASSWORD", "")
	database := fmt.Sprintf("nagg_it_%d", time.Now().UnixNano())
	createIntegrationDatabase(t, ctx, addr, username, password, database)

	store, err := chstore.Open(ctx, chstore.Config{
		Addr:     addr,
		Database: database,
		Username: username,
		Password: password,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	seedAppViewEvents(t, ctx, store)
	if err := store.Backfill(ctx); err != nil {
		t.Fatal(err)
	}
	assertProfileStoreHelpers(t, ctx, store)

	mux := http.NewServeMux()
	New(store, WithNIP05Validation(false), WithRateLimit(1_000, time.Minute)).Register(mux)

	var feed FeedResponse
	requestJSON(t, mux, http.MethodGet, "/nostr/feed?kind=trending&limit=5", nil, &feed)
	if len(feed.Items) == 0 || feed.Items[0].Type != "note" || feed.Items[0].Event.ID != integrationRootID {
		t.Fatalf("trending feed first item = %+v", feed.Items)
	}
	assertRootStats(t, feed.Metrics[integrationRootID])
	if profile := feed.Profiles[integrationAlice]; profile.Name != "Alice" {
		t.Fatalf("alice profile = %+v", profile)
	}

	var userFeed FeedResponse
	requestJSON(t, mux, http.MethodGet, "/nostr/feed/user?pubkey="+integrationAlice+"&limit=5", nil, &userFeed)
	if len(userFeed.Items) == 0 || userFeed.Items[0].Type != "note" || userFeed.Items[0].Event.ID != integrationRootID {
		t.Fatalf("user feed first item = %+v", userFeed.Items)
	}

	var stats map[string]chstore.NoteStats
	requestJSON(
		t,
		mux,
		http.MethodPost,
		"/nostr/notes/stats",
		map[string][]string{"ids": {integrationRootID}},
		&stats,
	)
	assertRootStats(t, stats[integrationRootID])

	var follows struct {
		PubKey    string `json:"pubkey"`
		Follows   uint64 `json:"follows"`
		Followers uint64 `json:"followers"`
	}
	requestJSON(t, mux, http.MethodGet, "/nostr/follows?pubkey="+integrationAlice, nil, &follows)
	if follows.PubKey != integrationAlice || follows.Follows != 1 || follows.Followers != 1 {
		t.Fatalf("follows = %+v", follows)
	}

	var thread struct {
		Root   FeedEvent   `json:"root"`
		Events []FeedEvent `json:"events"`
	}
	requestJSON(t, mux, http.MethodGet, "/nostr/thread?id="+integrationRootID+"&limit=100", nil, &thread)
	if thread.Root.ID != integrationRootID {
		t.Fatalf("thread root = %+v", thread.Root)
	}
	if !containsEvent(thread.Events, integrationReplyID) {
		t.Fatalf("thread events missing reply: %+v", thread.Events)
	}

	var enrichment EnrichmentResponse
	requestJSON(t, mux, http.MethodGet, "/nostr/events?ids="+integrationRootID, nil, &enrichment)
	if enrichment.Quoted[integrationRootID].ID != integrationRootID {
		t.Fatalf("quoted root = %+v", enrichment.Quoted)
	}
	assertRootStats(t, enrichment.Metrics[integrationRootID])
}

func assertProfileStoreHelpers(t *testing.T, ctx context.Context, store *chstore.Store) {
	t.Helper()

	firstAt, err := store.ProfileFirstEventCreatedAt(ctx, integrationAlice)
	if err != nil {
		t.Fatal(err)
	}
	if firstAt == nil {
		t.Fatal("expected first local event timestamp")
	}

	score := 42.5
	followers := uint64(500)
	profile := vertex.ProfileResult{
		PubKey:    integrationAlice,
		Npub:      vertex.Npub(integrationAlice),
		Rank:      0.01,
		Score:     &score,
		Followers: &followers,
		TopFollowers: []vertex.TopFollower{{
			PubKey: integrationBob,
			Npub:   vertex.Npub(integrationBob),
			Rank:   0.02,
		}},
	}
	if err := store.SaveVertexProfile(ctx, profile); err != nil {
		t.Fatal(err)
	}
	cached, ok, err := store.CachedVertexProfile(ctx, integrationAlice)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || cached.PubKey != integrationAlice || cached.Score == nil || *cached.Score != score {
		t.Fatalf("cached profile = %+v ok=%v", cached, ok)
	}
	if cached.Followers == nil || *cached.Followers != followers || len(cached.TopFollowers) != 1 {
		t.Fatalf("cached profile fields = %+v", cached)
	}
}

func createIntegrationDatabase(t *testing.T, ctx context.Context, addr string, username string, password string, database string) {
	t.Helper()

	adminDatabase := envFallback("NAGG_INTEGRATION_CLICKHOUSE_ADMIN_DATABASE", "NAGG_CLICKHOUSE_DATABASE", "default")
	conn, err := ch.Open(&ch.Options{
		Addr: []string{addr},
		Auth: ch.Auth{
			Database: adminDatabase,
			Username: username,
			Password: password,
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "CREATE DATABASE "+database); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = conn.Exec(dropCtx, "DROP DATABASE IF EXISTS "+database)
		_ = conn.Close()
	})
}

func seedAppViewEvents(t *testing.T, ctx context.Context, store *chstore.Store) {
	t.Helper()

	now := time.Now().UTC().Add(-time.Minute)
	events := []*nostr.Event{
		event(integrationProfileAliceID, integrationAlice, 0, now.Add(-6*time.Second), nil, `{"name":"Alice","display_name":"Alice","picture":"https://example.test/alice.png"}`),
		event(integrationProfileBobID, integrationBob, 0, now.Add(-5*time.Second), nil, `{"name":"Bob"}`),
		event(integrationAliceFollowsID, integrationAlice, 3, now.Add(-4*time.Second), nostr.Tags{{"p", integrationBob}}, ""),
		event(integrationBobFollowsID, integrationBob, 3, now.Add(-3*time.Second), nostr.Tags{{"p", integrationAlice}}, ""),
		event(integrationRootID, integrationAlice, 1, now.Add(-2*time.Second), nil, "root note"),
		event(integrationReplyID, integrationBob, 1, now.Add(-time.Second), nostr.Tags{{"e", integrationRootID, "", "root"}}, "reply"),
		event(integrationLikeID, integrationBob, 7, now, nostr.Tags{{"e", integrationRootID}}, "+"),
		event(integrationRepostID, integrationBob, 6, now.Add(time.Second), nostr.Tags{{"e", integrationRootID}}, ""),
		event(integrationZapID, integrationCarol, 9735, now.Add(2*time.Second), nostr.Tags{
			{"e", integrationRootID},
			{"description", fmt.Sprintf(`{"tags":[["amount","123000"],["e","%s"]]}`, integrationRootID)},
			{"bolt11", "lnbc999u1test"},
		}, ""),
	}

	records := make([]chstore.EventRecord, 0, len(events))
	for _, event := range events {
		records = append(records, chstore.EventRecord{
			Event: event,
			Relay: "wss://integration.test",
			Seen:  now.Add(3 * time.Second),
		})
	}
	if err := store.InsertEvents(ctx, records); err != nil {
		t.Fatal(err)
	}
}

func requestJSON(t *testing.T, mux *http.ServeMux, method string, path string, body any, out any) {
	t.Helper()

	var rawBody *bytes.Reader
	if body == nil {
		rawBody = bytes.NewReader(nil)
	} else {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rawBody = bytes.NewReader(payload)
	}

	req := httptest.NewRequest(method, path, rawBody)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("%s %s status = %d body = %s", method, path, rec.Code, rec.Body.String())
	}
	if err := json.NewDecoder(rec.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

func assertRootStats(t *testing.T, stats chstore.NoteStats) {
	t.Helper()

	if stats.LikeCount != 1 || stats.RepostCount != 1 || stats.ReplyCount != 1 || stats.SatsZapped != 123 {
		t.Fatalf("root stats = %+v", stats)
	}
}

func containsEvent(events []FeedEvent, id string) bool {
	for _, event := range events {
		if event.ID == id {
			return true
		}
	}
	return false
}

func event(id string, pubkey string, kind int, createdAt time.Time, tags nostr.Tags, content string) *nostr.Event {
	return &nostr.Event{
		ID:        id,
		PubKey:    pubkey,
		CreatedAt: nostr.Timestamp(createdAt.Unix()),
		Kind:      kind,
		Tags:      tags,
		Content:   content,
		Sig:       strings.Repeat("f", 128),
	}
}

func envFallback(primary string, secondary string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(primary)); value != "" {
		return value
	}
	if value := strings.TrimSpace(os.Getenv(secondary)); value != "" {
		return value
	}
	return fallback
}

const (
	integrationAlice = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	integrationBob   = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	integrationCarol = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	integrationRootID         = "1111111111111111111111111111111111111111111111111111111111111111"
	integrationReplyID        = "2222222222222222222222222222222222222222222222222222222222222222"
	integrationLikeID         = "3333333333333333333333333333333333333333333333333333333333333333"
	integrationRepostID       = "4444444444444444444444444444444444444444444444444444444444444444"
	integrationZapID          = "5555555555555555555555555555555555555555555555555555555555555555"
	integrationProfileAliceID = "6666666666666666666666666666666666666666666666666666666666666666"
	integrationProfileBobID   = "7777777777777777777777777777777777777777777777777777777777777777"
	integrationAliceFollowsID = "8888888888888888888888888888888888888888888888888888888888888888"
	integrationBobFollowsID   = "9999999999999999999999999999999999999999999999999999999999999999"
)
