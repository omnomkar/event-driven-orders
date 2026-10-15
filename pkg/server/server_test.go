package server

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

// freeAddr returns a localhost address with a port that is free right now.
func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

// TestRunHTTPBoundsShutdown starts a request that never finishes, cancels the
// context, and checks RunHTTP returns after the timeout instead of hanging.
func TestRunHTTPBoundsShutdown(t *testing.T) {
	addr := freeAddr(t)
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	srv := NewHTTP(addr, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
	}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunHTTP(ctx, srv, 100*time.Millisecond) }()

	// Retry until the listener is up, then leave a request hanging.
	go func() {
		for {
			resp, err := http.Get("http://" + addr + "/")
			if err == nil {
				_ = resp.Body.Close()
				return
			}
			select {
			case <-started:
				return
			case <-time.After(10 * time.Millisecond):
			}
		}
	}()
	<-started
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Error("RunHTTP() = nil, want a shutdown timeout error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunHTTP did not return; shutdown is not bounded")
	}
}

func TestRunHTTPCleanShutdown(t *testing.T) {
	srv := NewHTTP(freeAddr(t), http.NotFoundHandler())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunHTTP(ctx, srv, time.Second) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Errorf("RunHTTP() = %v, want nil", err)
	}
}
