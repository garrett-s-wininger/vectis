package registry

import (
	"context"
	"fmt"
	"sort"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
)

type registrationEntry struct {
	component  api.Component
	address    string
	instanceID string
}

type reg struct {
	mu            sync.RWMutex
	registrations map[string]registrationEntry // key: "component:instance_id"
}

type registryServer struct {
	api.UnimplementedRegistryServiceServer
	reg *reg
	log interfaces.Logger
}

func NewRegistryService(logger interfaces.Logger) api.RegistryServiceServer {
	return &registryServer{reg: &reg{registrations: make(map[string]registrationEntry)}, log: logger}
}

func makeRegKey(component api.Component, instanceID string) string {
	return component.String() + ":" + instanceID
}

func (s *registryServer) Register(ctx context.Context, req *api.Registration) (*api.Empty, error) {
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()

	if req.Component == nil || req.Address == nil {
		return nil, fmt.Errorf("component and address are required")
	}

	comp := *req.Component
	instanceID := req.GetInstanceId()
	key := makeRegKey(comp, instanceID)

	s.reg.registrations[key] = registrationEntry{
		component:  comp,
		address:    *req.Address,
		instanceID: instanceID,
	}

	switch comp {
	case api.Component_COMPONENT_QUEUE:
		s.log.Info("Registered queue at: %s", *req.Address)
	case api.Component_COMPONENT_LOG:
		s.log.Info("Registered log at: %s", *req.Address)
	case api.Component_COMPONENT_WORKER:
		s.log.Info("Registered worker %s at: %s", instanceID, *req.Address)
	}

	return &api.Empty{}, nil
}

func (s *registryServer) GetAddress(ctx context.Context, req *api.AddressRequest) (*api.AddressResponse, error) {
	s.reg.mu.RLock()
	defer s.reg.mu.RUnlock()

	if req.Component == nil {
		return nil, fmt.Errorf("component is required")
	}

	comp := *req.Component
	instanceID := req.GetInstanceId()
	var address string

	if instanceID != "" {
		// Exact lookup by instance ID
		key := makeRegKey(comp, instanceID)
		if entry, ok := s.reg.registrations[key]; ok {
			address = entry.address
		}
	} else {
		// Return first match for this component type (sorted by instance ID for determinism).
		var matches []string
		for key, entry := range s.reg.registrations {
			if entry.component == comp {
				matches = append(matches, key)
			}
		}
		sort.Strings(matches)
		if len(matches) > 0 {
			address = s.reg.registrations[matches[0]].address
		}
	}

	return &api.AddressResponse{Address: &address}, nil
}
