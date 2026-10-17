package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// observations gathers reg and returns, for the named histogram, the number
// of observations per label set, keyed like "method=GET,route=/x,status=200"
// (labels sorted by name).
func observations(t *testing.T, reg *prometheus.Registry, name string) map[string]uint64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	out := map[string]uint64{}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			var parts []string
			for _, l := range m.GetLabel() {
				parts = append(parts, l.GetName()+"="+l.GetValue())
			}
			sort.Strings(parts)
			out[strings.Join(parts, ",")] = m.GetHistogram().GetSampleCount()
		}
	}
	return out
}

func TestHTTPMiddlewareLabelsByRoutePatternAndStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("id") == "missing" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte("ok")) // no WriteHeader: implicit 200
	})
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	reg := prometheus.NewRegistry()
	h := NewHTTP(reg).Middleware(mux)

	for _, req := range []struct{ method, path string }{
		{http.MethodGet, "/orders/0192f5d1-0000-7000-8000-000000000001"},
		{http.MethodGet, "/orders/0192f5d1-0000-7000-8000-000000000002"},
		{http.MethodGet, "/orders/missing"},
		{http.MethodPost, "/orders"},
		{http.MethodGet, "/no/such/route"},
	} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(req.method, req.path, nil))
	}

	got := observations(t, reg, "http_server_request_duration_seconds")
	want := map[string]uint64{
		// Two different order IDs share one series: the pattern, not the path.
		"method=GET,route=/orders/{id},status=200": 2,
		"method=GET,route=/orders/{id},status=404": 1,
		"method=POST,route=/orders,status=202":     1,
		"method=GET,route=unmatched,status=404":    1,
	}
	if len(got) != len(want) {
		t.Errorf("got series %v, want %v", got, want)
	}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("series %s has %d observations, want %d (all: %v)", k, got[k], n, got)
		}
	}
}

func TestStatusWriterKeepsFirstStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec, status: http.StatusOK}
	sw.WriteHeader(http.StatusServiceUnavailable)
	sw.WriteHeader(http.StatusOK) // ignored by net/http too, with a warning
	if sw.status != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", sw.status)
	}
}

func TestGRPCInterceptorLabelsByMethodAndCode(t *testing.T) {
	reg := prometheus.NewRegistry()
	intercept := NewGRPC(reg).UnaryInterceptor()
	info := &grpc.UnaryServerInfo{FullMethod: "/eventorders.order.v1.OrderService/GetOrder"}

	ok := func(context.Context, any) (any, error) { return "resp", nil }
	notFound := func(context.Context, any) (any, error) { return nil, status.Error(codes.NotFound, "order not found") }

	if resp, err := intercept(context.Background(), nil, info, ok); resp != "resp" || err != nil {
		t.Fatalf("interceptor changed the result: %v, %v", resp, err)
	}
	if _, err := intercept(context.Background(), nil, info, notFound); status.Code(err) != codes.NotFound {
		t.Fatalf("interceptor changed the error: %v", err)
	}

	got := observations(t, reg, "grpc_server_handling_seconds")
	for _, code := range []string{"OK", "NotFound"} {
		key := "code=" + code + ",method=" + info.FullMethod
		if got[key] != 1 {
			t.Errorf("series %s has %d observations, want 1 (all: %v)", key, got[key], got)
		}
	}
}
