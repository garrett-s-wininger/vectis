package registry

import (
	"context"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
)

type reg struct {
	mu                  sync.RWMutex
	queueServiceAddress string
	logServiceAddress   string
}

type registryServer struct {
	api.UnimplementedRegistryServiceServer
	reg *reg
	log interfaces.Logger
}

func NewRegistryService(logger interfaces.Logger) api.RegistryServiceServer {
	return &registryServer{reg: &reg{}, log: logger}
}

func (s *registryServer) Register(ctx context.Context, req *api.Registration) (*api.Empty, error) {
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()

	if req.Component == nil || req.Address == nil {
		return &api.Empty{}, nil
	}

	switch *req.Component {
	case api.Component_COMPONENT_QUEUE:
		s.reg.queueServiceAddress = *req.Address
		s.log.Info("Registered queue at: %s", *req.Address)
	case api.Component_COMPONENT_LOG:
		s.reg.logServiceAddress = *req.Address
		s.log.Info("Registered log at: %s", *req.Address)
	}

	return &api.Empty{}, nil
}

func (s *registryServer) GetAddress(ctx context.Context, req *api.AddressRequest) (*api.AddressResponse, error) {
	s.reg.mu.RLock()
	defer s.reg.mu.RUnlock()

	var address string
	if req.Component != nil {
		switch *req.Component {
		case api.Component_COMPONENT_QUEUE:
			address = s.reg.queueServiceAddress
		case api.Component_COMPONENT_LOG:
			address = s.reg.logServiceAddress
		}
	}

	return &api.AddressResponse{Address: &address}, nil
}
