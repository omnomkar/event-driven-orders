// Package health provides liveness and readiness endpoints backed by a
// registry of named dependency checks.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// CheckFunc reports whether one dependency is usable. It must respect ctx,
// which carries the check's timeout.
type CheckFunc func(ctx context.Context) error

type check struct {
	name    string
	timeout time.Duration
	fn      CheckFunc
	// optional checks never fail readiness; see AddOptional.
	optional bool
}

// Registry holds the readiness checks for a service. Add every check before
// the registry starts serving requests.
type Registry struct {
	checks []check
}

// NewRegistry returns an empty Registry. With no checks, it always reports ready.
func NewRegistry() *Registry {
	return &Registry{}
}

// Add registers a check that fails if it does not finish within timeout.
func (r *Registry) Add(name string, timeout time.Duration, fn CheckFunc) {
	r.checks = append(r.checks, check{name: name, timeout: timeout, fn: fn})
}

// AddOptional registers a check for a dependency the service can run
// without, such as a cache it falls back from. When it fails, the check and
// the report say "degraded", but the service still reports ready.
func (r *Registry) AddOptional(name string, timeout time.Duration, fn CheckFunc) {
	r.checks = append(r.checks, check{name: name, timeout: timeout, fn: fn, optional: true})
}

// Status values used in reports.
const (
	StatusOK = "ok"
	// StatusDegraded means only optional checks failed: the service is
	// ready but running without something it would normally use.
	StatusDegraded = "degraded"
	StatusFail     = "fail"
)

// CheckResult is the outcome of one check.
type CheckResult struct {
	Status     string `json:"status"`
	Error      string `json:"error,omitempty"`
	DurationMS int64  `json:"duration_ms"`
}

// Report is the JSON body served by /readyz.
type Report struct {
	Status string                 `json:"status"`
	Checks map[string]CheckResult `json:"checks"`
}

// Ready reports whether every required check passed. Optional checks may
// have failed (StatusDegraded).
func (r Report) Ready() bool {
	return r.Status == StatusOK || r.Status == StatusDegraded
}

// Run executes every check in parallel, each under its own timeout, and
// returns once all of them have finished.
func (r *Registry) Run(ctx context.Context) Report {
	results := make([]CheckResult, len(r.checks))
	var wg sync.WaitGroup
	for i, c := range r.checks {
		wg.Go(func() {
			results[i] = runCheck(ctx, c)
		})
	}
	wg.Wait()

	report := Report{Status: StatusOK, Checks: make(map[string]CheckResult, len(r.checks))}
	for i, c := range r.checks {
		res := results[i]
		switch {
		case res.Status == StatusOK:
		case c.optional:
			res.Status = StatusDegraded
			if report.Status == StatusOK {
				report.Status = StatusDegraded
			}
		default:
			report.Status = StatusFail
		}
		report.Checks[c.name] = res
	}
	return report
}

func runCheck(ctx context.Context, c check) CheckResult {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	start := time.Now()
	err := c.fn(ctx)
	res := CheckResult{Status: StatusOK, DurationMS: time.Since(start).Milliseconds()}
	if err != nil {
		res.Status = StatusFail
		res.Error = err.Error()
	}
	return res
}

// LiveHandler serves /healthz. It returns 200 whenever the process can serve
// HTTP at all, and deliberately checks no dependencies, so a database outage
// never causes an orchestrator to restart healthy processes.
func LiveHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": StatusOK})
	})
}

// ReadyHandler serves /readyz: 200 when every required check passes, 503
// otherwise, with a JSON body listing each check.
func (r *Registry) ReadyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		report := r.Run(req.Context())
		code := http.StatusOK
		if !report.Ready() {
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, report)
	})
}

// AdminMux returns a ServeMux with /healthz, /readyz, and /metrics. /metrics
// serves the default Prometheus registry, which includes Go runtime metrics.
func AdminMux(r *Registry) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", LiveHandler())
	mux.Handle("GET /readyz", r.ReadyHandler())
	mux.Handle("GET /metrics", promhttp.Handler())
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
