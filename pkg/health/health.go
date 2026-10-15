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

// Status values used in reports.
const (
	StatusOK   = "ok"
	StatusFail = "fail"
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

// Ready reports whether every check passed.
func (r Report) Ready() bool {
	return r.Status == StatusOK
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
		report.Checks[c.name] = results[i]
		if results[i].Status != StatusOK {
			report.Status = StatusFail
		}
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

// ReadyHandler serves /readyz: 200 when every check passes, 503 otherwise,
// with a JSON body listing each check.
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
