package grpctest

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func RegisterHealth(srv *grpc.Server, serviceName string) {
	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	hs.SetServingStatus(serviceName, healthpb.HealthCheckResponse_SERVING)
}
