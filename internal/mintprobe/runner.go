package mintprobe

import (
	"context"
	"log/slog"
	"time"

	"github.com/vertex-lab/nagg/internal/mintinfo"
)

// Store is the write + due-gate seam (implemented by *clickhouse.Store).
type Store interface {
	PutProbeResults(ctx context.Context, results []Result) error
	// LastMintProbes is each mint's most recent probe time, any status.
	LastMintProbes(ctx context.Context) (map[string]time.Time, error)
}

// Config parameterizes the runner.
type Config struct {
	// Interval is how often RunOnce re-checks for due mints.
	Interval time.Duration
	// MinAge is the minimum time between probes of the same mint. Default 7 days.
	MinAge time.Duration
	// Throttle is the delay between consecutive mints in one pass. Default 5s.
	Throttle time.Duration
}

func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = time.Hour
	}
	if c.MinAge <= 0 {
		c.MinAge = 7 * 24 * time.Hour
	}
	if c.Throttle <= 0 {
		c.Throttle = 5 * time.Second
	}
	return c
}

// Stats summarizes one pass.
type Stats struct {
	Due     int // mints probed this pass
	Skipped int // not yet due
	Probes  int // method probes run
	Paid    int // probes whose unpaid quote was marked paid
}

// Runner walks the shared mint work-list and probes every advertised mint
// method of each due mint. It mirrors mintinfo.Snapshotter: Run loops, RunOnce
// does one pass, errors are logged and never fatal.
type Runner struct {
	store  Store
	mints  mintinfo.MintLister
	info   mintinfo.InfoFetcher
	prober Prober
	cfg    Config
	now    func() time.Time
	logger *slog.Logger
}

func NewRunner(store Store, mints mintinfo.MintLister, info mintinfo.InfoFetcher, prober Prober, cfg Config, logger *slog.Logger) *Runner {
	if logger == nil {
		logger = slog.Default()
	}
	return &Runner{
		store:  store,
		mints:  mints,
		info:   info,
		prober: prober,
		cfg:    cfg.withDefaults(),
		now:    time.Now,
		logger: logger,
	}
}

// Run executes a pass immediately, then every Interval until the context ends.
func (r *Runner) Run(ctx context.Context) {
	if r == nil || r.store == nil || r.mints == nil || r.info == nil || r.prober == nil {
		return
	}
	for {
		stats, err := r.RunOnce(ctx)
		if err != nil {
			r.logger.Error("mintprobe: pass failed", "error", err)
		} else {
			r.logger.Info("mintprobe: pass complete",
				"due", stats.Due, "skipped", stats.Skipped, "probes", stats.Probes, "paid", stats.Paid)
		}
		if !sleep(ctx, r.cfg.Interval) {
			return
		}
	}
}

// RunOnce probes every mint whose last probe is older than MinAge.
func (r *Runner) RunOnce(ctx context.Context) (Stats, error) {
	targets, err := r.mints.MintURLs(ctx)
	if err != nil {
		return Stats{}, err
	}
	last, err := r.store.LastMintProbes(ctx)
	if err != nil {
		return Stats{}, err
	}

	var stats Stats
	for _, mintURL := range targets {
		if ctx.Err() != nil {
			return stats, ctx.Err()
		}
		if at, ok := last[mintURL]; ok && r.now().Sub(at) < r.cfg.MinAge {
			stats.Skipped++
			continue
		}
		if stats.Due > 0 && !sleep(ctx, r.cfg.Throttle) {
			return stats, ctx.Err()
		}
		stats.Due++
		results := r.probeMint(ctx, mintURL)
		for _, res := range results {
			if res.Method != "" {
				stats.Probes++
			}
			if res.Paid {
				stats.Paid++
			}
		}
		if ctx.Err() != nil {
			// A shutdown mid-probe would record cancellations as verdicts.
			return stats, ctx.Err()
		}
		if err := r.store.PutProbeResults(ctx, results); err != nil {
			r.logger.Warn("mintprobe: store failed", "mint", mintURL, "error", err)
		}
	}
	return stats, nil
}

// probeMint reads the mint's current methods from /v1/info and probes each.
// A mint with nothing to probe still gets one mint-level row so the due gate
// moves and it is not retried every Interval.
func (r *Runner) probeMint(ctx context.Context, mintURL string) []Result {
	raw, ok := r.info.Info(ctx, mintURL)
	if !ok {
		return []Result{{MintURL: mintURL, ProbedAt: r.now().UTC(), Status: StatusInfoUnreachable}}
	}
	methods := MintMethods(raw)
	if len(methods) == 0 {
		return []Result{{MintURL: mintURL, ProbedAt: r.now().UTC(), Status: StatusNoMethods}}
	}
	results := make([]Result, 0, len(methods))
	for _, m := range methods {
		res := r.prober.Probe(ctx, mintURL, m)
		res.MintURL = mintURL
		results = append(results, res)
	}
	return results
}
