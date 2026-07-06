package mintinfo

import (
	"context"
	"log/slog"
	"strings"

	"github.com/vertex-lab/nagg/internal/auditor"
)

// MintLister is the snapshot work-list: which mints to watch.
type MintLister interface {
	MintURLs(ctx context.Context) ([]string, error)
}

// AuditorClient is the auditor half of the work-list.
type AuditorClient interface {
	Mints(ctx context.Context) ([]auditor.Mint, error)
}

// MintSource is the Nostr half of the work-list (kind-38000 u-tags).
type MintSource interface {
	CashuMintURLs(ctx context.Context) ([]string, error)
}

// WorkList composes the snapshot work-list from the two sources nagg already
// reads — the auditor's mint list and the NIP-87 ecash recommendations — exactly
// the union discoverMints builds. Both are normalized and deduped so a mint
// referenced several ways collapses to one watch target. Either source failing
// is logged, not fatal: a partial list still makes progress.
type WorkList struct {
	auditor AuditorClient
	store   MintSource
	logger  *slog.Logger
}

func NewWorkList(auditorClient AuditorClient, store MintSource, logger *slog.Logger) *WorkList {
	if logger == nil {
		logger = slog.Default()
	}
	return &WorkList{auditor: auditorClient, store: store, logger: logger}
}

func (w *WorkList) MintURLs(ctx context.Context) ([]string, error) {
	seen := map[string]struct{}{}
	var out []string
	add := func(raw string) {
		norm := NormalizeMintURL(raw)
		if norm == "" {
			return
		}
		if _, ok := seen[norm]; ok {
			return
		}
		seen[norm] = struct{}{}
		out = append(out, norm)
	}

	if w.auditor != nil {
		if mints, err := w.auditor.Mints(ctx); err != nil {
			w.logger.Warn("mintinfo: auditor work-list failed", "error", err)
		} else {
			for _, m := range mints {
				add(m.URL)
			}
		}
	}
	if w.store != nil {
		if urls, err := w.store.CashuMintURLs(ctx); err != nil {
			w.logger.Warn("mintinfo: recommendation work-list failed", "error", err)
		} else {
			for _, u := range urls {
				add(u)
			}
		}
	}
	return out, nil
}

// NormalizeMintURL is the canonical mint key: lowercase, no trailing slash. It
// matches appview.normalizeMintURL so the same mint keys across reviews,
// discover, and info history.
func NormalizeMintURL(url string) string {
	return strings.ToLower(strings.TrimRight(strings.TrimSpace(url), "/"))
}
