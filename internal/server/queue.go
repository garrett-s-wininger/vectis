package server

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
)

type queueServer struct {
	api.UnimplementedQueueServiceServer
}

func NewQueueService() api.QueueServiceServer {
	return &queueServer{}
}

func (s *queueServer) Enqueue(ctx context.Context, req *api.Empty) (*api.Empty, error) {
	_ = req
	fmt.Println("Received enqueue request")
	return &api.Empty{}, nil
}

func RegisterQueueService(s grpc.ServiceRegistrar) {
	api.RegisterQueueServiceServer(s, NewQueueService())
}
