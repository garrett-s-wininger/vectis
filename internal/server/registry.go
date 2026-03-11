package server

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
)

type registry struct {
	apiServerAddress    string
	queueServiceAddress string
}

type registryServer struct {
	api.UnimplementedRegistryServiceServer
}

func NewRegistryService() api.RegistryServiceServer {
	return &registryServer{}
}

func (s *registryServer) Register(ctx context.Context, req *api.Registration) (*api.Empty, error) {
	_ = req
	fmt.Println("Received register request")
	return &api.Empty{}, nil
}
