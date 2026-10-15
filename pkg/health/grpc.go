package health

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	grpchealth "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// GRPCCheck returns a check that calls the standard grpc.health.v1 service on
// conn and passes only when the server reports SERVING.
func GRPCCheck(conn grpc.ClientConnInterface) CheckFunc {
	client := healthpb.NewHealthClient(conn)
	return func(ctx context.Context) error {
		resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
		if err != nil {
			return fmt.Errorf("grpc health check: %w", err)
		}
		if s := resp.GetStatus(); s != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("grpc health status %s", s)
		}
		return nil
	}
}

// SyncGRPCHealth keeps a gRPC health server in step with the readiness
// registry, so callers that use grpc.health.v1 see the same answer as /readyz.
// It reports NOT_SERVING until the first passing run, and again after ctx is
// cancelled, which tells clients to stop sending new requests during shutdown.
func SyncGRPCHealth(ctx context.Context, r *Registry, hs *grpchealth.Server, interval time.Duration) {
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		status := healthpb.HealthCheckResponse_NOT_SERVING
		if r.Run(ctx).Ready() {
			status = healthpb.HealthCheckResponse_SERVING
		}
		hs.SetServingStatus("", status)

		select {
		case <-ctx.Done():
			hs.Shutdown()
			return
		case <-ticker.C:
		}
	}
}
