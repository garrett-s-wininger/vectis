package server

import (
	"context"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
)

type server struct {
	api.UnimplementedQueueServiceServer
}

func NewQueueService() api.QueueServiceServer {
	return &server{}
}

func (s *server) Enqueue(ctx context.Context, req *api.Empty) (*api.Empty, error) {
	_ = req
	return &api.Empty{}, nil
}

func RegisterQueueService(s grpc.ServiceRegistrar) {
	api.RegisterQueueServiceServer(s, NewQueueService())
}
