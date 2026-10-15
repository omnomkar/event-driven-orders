// Package server runs HTTP and gRPC servers until a context is cancelled, then
// shuts them down within a bounded time.
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"google.golang.org/grpc"
)

// NewHTTP returns an http.Server with a header read timeout, which protects
// against clients that open connections and send headers very slowly.
func NewHTTP(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// RunHTTP serves srv until ctx is cancelled, then stops accepting connections
// and waits up to timeout for in-flight requests. Requests still running after
// that are cut off so a stuck handler cannot block shutdown.
func RunHTTP(ctx context.Context, srv *http.Server, timeout time.Duration) error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server %s: %w", srv.Addr, err)
	case <-ctx.Done():
	}

	// ctx is already cancelled, so the shutdown deadline needs a fresh context.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		_ = srv.Close()
		return fmt.Errorf("http server %s: shutdown: %w", srv.Addr, err)
	}
	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server %s: %w", srv.Addr, err)
	}
	return nil
}

// RunGRPC listens on addr and serves srv until ctx is cancelled, then lets
// in-flight RPCs finish for up to timeout before closing everything.
func RunGRPC(ctx context.Context, srv *grpc.Server, addr string, timeout time.Duration) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc server %s: listen: %w", addr, err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(lis) }()

	select {
	case err := <-errCh:
		return fmt.Errorf("grpc server %s: %w", addr, err)
	case <-ctx.Done():
	}

	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
		return nil
	case <-time.After(timeout):
		srv.Stop()
		return fmt.Errorf("grpc server %s: graceful stop exceeded %s, forced stop", addr, timeout)
	}
}
