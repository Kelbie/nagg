package rates

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestRatesSources(t *testing.T) {
	sources := LoadSources("", true, nil)
	if len(sources) != 7 {
		t.Fatalf("registry has %d valid sources", len(sources))
	}
	for _, s := range sources {
		if s.Kind == NostrNote && len(s.Pubkey) != 64 {
			t.Fatalf("not normalized: %+v", s)
		}
	}
	if got := len(LoadSources("", false, nil)); got != 3 {
		t.Fatalf("HTTP disabled: %d", got)
	}
	extra, _ := json.Marshal([]Source{{ID: "gbpbot", Currency: "GBP", Kind: NostrNote, Pubkey: strings.ToUpper(sources[0].Pubkey), Priority: -1}})
	withExtra := LoadSources(string(extra), true, nil)
	if len(withExtra) != 8 || withExtra[0].ID != "gbpbot" || withExtra[0].Pubkey != sources[0].Pubkey {
		t.Fatalf("extra not normalized/merged: %+v", withExtra)
	}
	var log bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&log, nil))
	for _, bad := range []string{`{"secret":"private"}`, `[{"id":"private\n","currency":"GBP","kind":"nostr-note","pubkey":"private"}]`} {
		if len(LoadSources(bad, true, logger)) != 7 {
			t.Fatal("invalid extra changed registry")
		}
	}
	if log.Len() == 0 || strings.Contains(log.String(), "private") {
		t.Fatalf("unsafe/missing warning: %s", log.String())
	}
	duplicate, _ := json.Marshal([]Source{sources[0]})
	if len(LoadSources(string(duplicate), true, logger)) != 7 {
		t.Fatal("duplicate source counted twice")
	}
}
