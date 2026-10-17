// Package metrics records request latency for the HTTP gateway and the gRPC
// services as Prometheus histograms.
package metrics

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// LatencyBuckets are the histogram bucket upper bounds, in seconds, shared by
// every latency metric: fine-grained from 1ms to 100ms, where most requests
// on one host land, then coarser up to 10s.
var LatencyBuckets = []float64{
	0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
}

// UnmatchedRoute is the route label for requests that matched no pattern
// (404s and 405s from the ServeMux).
const UnmatchedRoute = "unmatched"

// HTTP records the latency of every request a handler serves.
type HTTP struct {
	duration *prometheus.HistogramVec
}

// NewHTTP creates the HTTP latency histogram and registers it with reg.
// Services pass prometheus.DefaultRegisterer; tests pass a fresh registry.
func NewHTTP(reg prometheus.Registerer) *HTTP {
	m := &HTTP{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_server_request_duration_seconds",
			Help:    "Time to serve an HTTP request, by route pattern, method, and status code.",
			Buckets: LatencyBuckets,
		}, []string{"route", "method", "status"}),
	}
	reg.MustRegister(m.duration)
	return m
}

// Middleware wraps next, which must be (or end in) an http.ServeMux.
//
// The route label is the ServeMux pattern that matched, such as
// "/orders/{id}", never the raw path. Raw paths contain order IDs, and every
// distinct label value is a separate time series in Prometheus, so labelling
// by path would grow without bound.
func (m *HTTP) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		// ServeMux sets r.Pattern on this same request while routing, so it
		// is available once the handler returns.
		m.duration.WithLabelValues(route(r.Pattern), r.Method, strconv.Itoa(sw.status)).
			Observe(time.Since(start).Seconds())
	})
}

// route turns a ServeMux pattern ("GET /orders/{id}") into a route label
// ("/orders/{id}"). The method is its own label.
func route(pattern string) string {
	if pattern == "" {
		return UnmatchedRoute
	}
	if _, path, ok := strings.Cut(pattern, " "); ok {
		return path
	}
	return pattern
}

// statusWriter remembers the status code a handler writes. It embeds the
// real ResponseWriter, so every other method (Header, Write) passes straight
// through. A handler that never calls WriteHeader gets 200.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// GRPC records the latency of every unary RPC a server handles.
type GRPC struct {
	duration *prometheus.HistogramVec
}

// NewGRPC creates the gRPC latency histogram and registers it with reg.
func NewGRPC(reg prometheus.Registerer) *GRPC {
	m := &GRPC{
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "grpc_server_handling_seconds",
			Help:    "Time to handle a unary gRPC call, by full method name and status code.",
			Buckets: LatencyBuckets,
		}, []string{"method", "code"}),
	}
	reg.MustRegister(m.duration)
	return m
}

// UnaryInterceptor returns a server interceptor that times each call. Pass it
// to grpc.NewServer with grpc.ChainUnaryInterceptor.
func (m *GRPC) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		// status.Code returns codes.OK for a nil error.
		m.duration.WithLabelValues(info.FullMethod, status.Code(err).String()).
			Observe(time.Since(start).Seconds())
		return resp, err
	}
}
