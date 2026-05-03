package grpcservices

import (
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/logserver"
	"vectis/internal/queue"
	"vectis/internal/registry"
	"vectis/internal/testutil/grpctest"

	"google.golang.org/grpc"
)

func StartQueueServer(t *testing.T, logger interfaces.Logger) (*grpctest.Server, interfaces.QueueClient, interfaces.QueueService) {
	t.Helper()

	server := grpctest.StartServer(t, func(srv *grpc.Server) {
		_ = queue.RegisterQueueService(srv, logger, queue.QueueOptions{}, nil)
	})

	client := interfaces.NewGRPCQueueClient(server.Conn)
	service := interfaces.NewQueueService(api.NewQueueServiceClient(server.Conn))
	return server, client, service
}

func StartLogServer(t *testing.T, logger interfaces.Logger, store logserver.RunLogStore) (*grpctest.Server, interfaces.LogClient) {
	t.Helper()

	server := grpctest.StartServer(t, func(srv *grpc.Server) {
		api.RegisterLogServiceServer(srv, logserver.NewServerWithStore(logger, store))
	})

	return server, interfaces.NewGRPCLogClient(server.Conn)
}

func StartRegistryServer(t *testing.T, logger interfaces.Logger) (*grpctest.Server, api.RegistryServiceClient) {
	t.Helper()

	server := grpctest.StartServer(t, func(srv *grpc.Server) {
		RegisterRegistryService(srv, logger)
	})

	return server, api.NewRegistryServiceClient(server.Conn)
}

func RegisterRegistryService(srv *grpc.Server, logger interfaces.Logger) {
	api.RegisterRegistryServiceServer(srv, registry.NewRegistryService(logger))
}
