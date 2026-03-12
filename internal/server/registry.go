package server

import (
	"context"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/log"
)

type registry struct {
	mu                  sync.RWMutex
	queueServiceAddress string
}

type registryServer struct {
	api.UnimplementedRegistryServiceServer
	reg *registry
	log *log.Logger
}

func NewRegistryService(logger *log.Logger) api.RegistryServiceServer {
	return &registryServer{reg: &registry{}, log: logger}
}

func (s *registryServer) Register(ctx context.Context, req *api.Registration) (*api.Empty, error) {
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()

	if req.Component != nil && *req.Component == api.Component_COMPONENT_QUEUE && req.Address != nil {
		s.reg.queueServiceAddress = *req.Address
		s.log.Info("Registered queue at: %s", *req.Address)
	}

	return &api.Empty{}, nil
}

func (s *registryServer) GetAddress(ctx context.Context, req *api.AddressRequest) (*api.AddressResponse, error) {
	s.reg.mu.RLock()
	defer s.reg.mu.RUnlock()

	var address string
	if req.Component != nil && *req.Component == api.Component_COMPONENT_QUEUE {
		address = s.reg.queueServiceAddress
	}

	return &api.AddressResponse{Address: &address}, nil
}
