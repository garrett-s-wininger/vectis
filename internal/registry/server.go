package registry

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
)

type ServiceOptions struct {
	NodeID              string
	AdvertiseAddress    string
	PeerAddresses       []string
	GossipInterval      time.Duration
	AntiEntropyInterval time.Duration
	LeaseTTL            time.Duration
	TombstoneTTL        time.Duration
	PeerDialTimeout     time.Duration
}

const (
	defaultRegistryLeaseTTL            = 2 * time.Minute
	defaultRegistryTombstoneTTL        = 5 * time.Minute
	defaultRegistryGossipInterval      = 2 * time.Second
	defaultRegistryAntiEntropyInterval = 30 * time.Second
	defaultRegistryPeerDialTimeout     = 3 * time.Second
)

type registryServer struct {
	api.UnimplementedRegistryServiceServer
	reg         *reg
	log         interfaces.Logger
	opts        ServiceOptions
	peerMu      sync.Mutex
	peerClients map[string]*Registry
}

func NewRegistryService(logger interfaces.Logger) api.RegistryServiceServer {
	return NewRegistryServiceWithOptions(logger, ServiceOptions{})
}

func NewRegistryServiceWithOptions(logger interfaces.Logger, opts ServiceOptions) *registryServer {
	opts = sanitizeServiceOptions(opts)
	return &registryServer{
		reg:         newReg(opts.NodeID, opts.LeaseTTL, opts.TombstoneTTL),
		log:         logger,
		opts:        opts,
		peerClients: make(map[string]*Registry),
	}
}

func sanitizeServiceOptions(opts ServiceOptions) ServiceOptions {
	if opts.NodeID == "" {
		opts.NodeID = opts.AdvertiseAddress
	}

	if opts.NodeID == "" {
		opts.NodeID = "registry"
	}

	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = defaultRegistryLeaseTTL
	}

	if opts.TombstoneTTL <= 0 {
		opts.TombstoneTTL = defaultRegistryTombstoneTTL
	}

	if opts.GossipInterval <= 0 {
		opts.GossipInterval = defaultRegistryGossipInterval
	}

	if opts.AntiEntropyInterval <= 0 {
		opts.AntiEntropyInterval = defaultRegistryAntiEntropyInterval
	}

	if opts.PeerDialTimeout <= 0 {
		opts.PeerDialTimeout = defaultRegistryPeerDialTimeout
	}

	opts.PeerAddresses = cleanPeerAddresses(opts.PeerAddresses, opts.AdvertiseAddress)
	return opts
}

func (s *registryServer) Register(ctx context.Context, req *api.Registration) (*api.Empty, error) {
	if req.Component == nil || req.Address == nil {
		return nil, fmt.Errorf("component and address are required")
	}

	comp := *req.Component
	instanceID := req.GetInstanceId()

	_, change := s.reg.registerWithChange(comp, instanceID, *req.Address, req.GetMetadata(), time.Now())
	s.logRegistrationChange(change, comp, instanceID, *req.Address)

	return &api.Empty{}, nil
}

func (s *registryServer) logRegistrationChange(change registrationChange, comp api.Component, instanceID, address string) {
	log := s.log.Info
	if change == registrationChangeRenewed {
		log = s.log.Debug
	}

	action := "New"
	switch change {
	case registrationChangeUpdated:
		action = "Updated"
	case registrationChangeRenewed:
		action = "Renewed"
	}

	switch comp {
	case api.Component_COMPONENT_QUEUE:
		log("%s queue registration at: %s", action, address)
	case api.Component_COMPONENT_LOG:
		log("%s log registration at: %s", action, address)
	case api.Component_COMPONENT_ARTIFACT:
		log("%s artifact registration %s at: %s", action, instanceID, address)
	case api.Component_COMPONENT_WORKER:
		log("%s worker registration %s at: %s", action, instanceID, address)
	case api.Component_COMPONENT_ORCHESTRATOR:
		log("%s orchestrator registration %s at: %s", action, instanceID, address)
	}
}

func (s *registryServer) GetAddress(ctx context.Context, req *api.AddressRequest) (*api.AddressResponse, error) {
	if req.Component == nil {
		return nil, fmt.Errorf("component is required")
	}

	comp := *req.Component
	instanceID := req.GetInstanceId()
	var address string

	if instanceID != "" {
		if entry, ok := s.reg.get(comp, instanceID, time.Now()); ok {
			address = entry.address
		}
	} else {
		matches := s.reg.listByComponent(comp, time.Now())
		sort.Strings(matches)

		if len(matches) > 0 {
			if entry, ok := s.reg.getByKey(matches[0], time.Now()); ok {
				address = entry.address
			}
		}
	}

	return &api.AddressResponse{Address: &address}, nil
}

func (s *registryServer) ListRegistrations(ctx context.Context, req *api.ListRegistrationsRequest) (*api.ListRegistrationsResponse, error) {
	if req == nil {
		return &api.ListRegistrationsResponse{}, nil
	}

	entries := s.reg.listEntries(req.GetComponent(), req.GetMetadata(), time.Now())
	resp := &api.ListRegistrationsResponse{
		Entries: make([]*api.RegistryEntry, 0, len(entries)),
	}

	for _, entry := range entries {
		resp.Entries = append(resp.Entries, registryEntryToProto(entry))
	}

	return resp, nil
}

func (s *registryServer) Gossip(ctx context.Context, req *api.GossipRequest) (*api.GossipResponse, error) {
	if req == nil {
		return &api.GossipResponse{}, nil
	}

	s.reg.mergeProtoEntries(req.GetEntries(), time.Now())
	return &api.GossipResponse{}, nil
}

func (s *registryServer) GetSnapshot(ctx context.Context, req *api.RegistrySnapshotRequest) (*api.RegistrySnapshotResponse, error) {
	if req == nil {
		return &api.RegistrySnapshotResponse{Entries: s.reg.snapshotProtoEntries(time.Now())}, nil
	}

	return &api.RegistrySnapshotResponse{
		Entries: s.reg.entriesNewerThanDigests(req.GetDigests(), time.Now()),
	}, nil
}
