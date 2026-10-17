package health

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func pass(context.Context) error { return nil }

func fail(context.Context) error { return errors.New("connection refused") }

// hang blocks until its context ends, standing in for a dependency that never answers.
func hang(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRegistryRun(t *testing.T) {
	type entry struct {
		name     string
		fn       CheckFunc
		optional bool
	}
	tests := []struct {
		name       string
		checks     []entry
		wantStatus string
		wantChecks map[string]string
	}{
		{
			name:       "no checks is ready",
			wantStatus: StatusOK,
			wantChecks: map[string]string{},
		},
		{
			name:       "all pass",
			checks:     []entry{{"postgres", pass, false}, {"kafka", pass, false}},
			wantStatus: StatusOK,
			wantChecks: map[string]string{"postgres": StatusOK, "kafka": StatusOK},
		},
		{
			name:       "one fails",
			checks:     []entry{{"postgres", pass, false}, {"kafka", fail, false}},
			wantStatus: StatusFail,
			wantChecks: map[string]string{"postgres": StatusOK, "kafka": StatusFail},
		},
		{
			name:       "timeout fails",
			checks:     []entry{{"redis", hang, false}},
			wantStatus: StatusFail,
			wantChecks: map[string]string{"redis": StatusFail},
		},
		{
			name:       "optional failure degrades",
			checks:     []entry{{"postgres", pass, false}, {"redis", fail, true}},
			wantStatus: StatusDegraded,
			wantChecks: map[string]string{"postgres": StatusOK, "redis": StatusDegraded},
		},
		{
			name:       "optional pass is ok",
			checks:     []entry{{"postgres", pass, false}, {"redis", pass, true}},
			wantStatus: StatusOK,
			wantChecks: map[string]string{"postgres": StatusOK, "redis": StatusOK},
		},
		{
			name:       "required failure wins over degraded",
			checks:     []entry{{"redis", fail, true}, {"postgres", fail, false}},
			wantStatus: StatusFail,
			wantChecks: map[string]string{"postgres": StatusFail, "redis": StatusDegraded},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			for _, c := range tt.checks {
				if c.optional {
					r.AddOptional(c.name, 50*time.Millisecond, c.fn)
				} else {
					r.Add(c.name, 50*time.Millisecond, c.fn)
				}
			}
			got := r.Run(context.Background())
			if got.Status != tt.wantStatus {
				t.Errorf("Status = %q, want %q", got.Status, tt.wantStatus)
			}
			if len(got.Checks) != len(tt.wantChecks) {
				t.Fatalf("got %d checks, want %d", len(got.Checks), len(tt.wantChecks))
			}
			for name, want := range tt.wantChecks {
				res := got.Checks[name]
				if res.Status != want {
					t.Errorf("check %q status = %q, want %q", name, res.Status, want)
				}
				if (res.Error != "") != (want != StatusOK) {
					t.Errorf("check %q error = %q, want error only on failure", name, res.Error)
				}
			}
		})
	}
}

// TestRegistryRunsChecksInParallel makes sure slow checks overlap instead of
// adding up, so /readyz stays fast with several dependencies.
func TestRegistryRunsChecksInParallel(t *testing.T) {
	r := NewRegistry()
	for _, name := range []string{"a", "b", "c", "d"} {
		r.Add(name, 100*time.Millisecond, hang)
	}
	start := time.Now()
	r.Run(context.Background())
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("Run took %v, want checks to run in parallel (about 100ms)", elapsed)
	}
}

func TestAdminMux(t *testing.T) {
	tests := []struct {
		name     string
		check    CheckFunc
		path     string
		wantCode int
		wantBody string
	}{
		{"healthz ignores failing deps", fail, "/healthz", http.StatusOK, `"status":"ok"`},
		{"readyz passes", pass, "/readyz", http.StatusOK, `"postgres":{"status":"ok"`},
		{"readyz fails", fail, "/readyz", http.StatusServiceUnavailable, `"error":"connection refused"`},
		{"metrics", pass, "/metrics", http.StatusOK, "go_goroutines"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			r.Add("postgres", time.Second, tt.check)
			rec := httptest.NewRecorder()
			AdminMux(r).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.path, nil))

			if rec.Code != tt.wantCode {
				t.Errorf("code = %d, want %d", rec.Code, tt.wantCode)
			}
			if body := rec.Body.String(); !strings.Contains(body, tt.wantBody) {
				t.Errorf("body = %s, want it to contain %s", body, tt.wantBody)
			}
		})
	}
}

func TestReadyzBodyIsValidJSON(t *testing.T) {
	r := NewRegistry()
	r.Add("kafka", time.Second, fail)
	rec := httptest.NewRecorder()
	r.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	var report Report
	if err := json.Unmarshal(rec.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if report.Ready() || report.Checks["kafka"].Status != StatusFail {
		t.Errorf("report = %+v, want kafka failing", report)
	}
}

// A degraded service is still ready: /readyz answers 200 and names the
// optional check that failed.
func TestReadyzDegradedIsReady(t *testing.T) {
	r := NewRegistry()
	r.Add("postgres", time.Second, pass)
	r.AddOptional("redis", time.Second, fail)
	rec := httptest.NewRecorder()
	r.ReadyHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", rec.Code)
	}
	var report Report
	if err := json.Unmarshal(rec.Body.Bytes(), &report); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if !report.Ready() || report.Status != StatusDegraded || report.Checks["redis"].Status != StatusDegraded {
		t.Errorf("report = %+v, want ready, degraded, with redis degraded", report)
	}
}

func TestProbe(t *testing.T) {
	tests := []struct {
		name    string
		code    int
		wantErr bool
	}{
		{"ready", http.StatusOK, false},
		{"not ready", http.StatusServiceUnavailable, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path != "/readyz" {
					t.Errorf("path = %s, want /readyz", req.URL.Path)
				}
				w.WriteHeader(tt.code)
			}))
			defer srv.Close()

			// Probe takes a listen address like ":8081"; pass only the port to
			// exercise the localhost substitution.
			_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
			err := Probe(context.Background(), ":"+port)
			if (err != nil) != tt.wantErr {
				t.Errorf("Probe() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
