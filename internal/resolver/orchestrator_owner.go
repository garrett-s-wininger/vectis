package resolver

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/interfaces"
	"vectis/internal/registry"

	"google.golang.org/grpc"
)

const (
	orchestratorOwnerPinnedPrefix   = "orchestrator:pinned:"
	orchestratorOwnerRegistryPrefix = "orchestrator:registry:"
	orchestratorOwnerInstancePrefix = "orchestrator:instance:"
	orchestratorOwnerAddressPrefix  = "orchestrator:address:"
)

type OrchestratorOwnerKind string

const (
	OrchestratorOwnerUnknown  OrchestratorOwnerKind = ""
	OrchestratorOwnerPinned   OrchestratorOwnerKind = "pinned"
	OrchestratorOwnerRegistry OrchestratorOwnerKind = "registry"
	OrchestratorOwnerInstance OrchestratorOwnerKind = "instance"
	OrchestratorOwnerAddress  OrchestratorOwnerKind = "address"
)

type OrchestratorOwner struct {
	Kind  OrchestratorOwnerKind
	Value string
}

func OrchestratorPinnedOwnerID(address string) string {
	return orchestratorOwnerPinnedPrefix + strings.TrimSpace(address)
}

func OrchestratorRegistryOwnerID(cellID string) string {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		cellID = registry.DefaultCellID
	}

	return orchestratorOwnerRegistryPrefix + cellID
}

func OrchestratorInstanceOwnerID(instanceID string) string {
	return orchestratorOwnerInstancePrefix + strings.TrimSpace(instanceID)
}

func OrchestratorAddressOwnerID(address string) string {
	return orchestratorOwnerAddressPrefix + strings.TrimSpace(address)
}

func ParseOrchestratorOwnerID(ownerID string) (OrchestratorOwner, bool) {
	ownerID = strings.TrimSpace(ownerID)
	for _, candidate := range []struct {
		prefix string
		kind   OrchestratorOwnerKind
	}{
		{orchestratorOwnerPinnedPrefix, OrchestratorOwnerPinned},
		{orchestratorOwnerRegistryPrefix, OrchestratorOwnerRegistry},
		{orchestratorOwnerInstancePrefix, OrchestratorOwnerInstance},
		{orchestratorOwnerAddressPrefix, OrchestratorOwnerAddress},
	} {
		if value, ok := strings.CutPrefix(ownerID, candidate.prefix); ok {
			value = strings.TrimSpace(value)
			if value == "" {
				return OrchestratorOwner{}, false
			}

			return OrchestratorOwner{Kind: candidate.kind, Value: value}, true
		}
	}

	return OrchestratorOwner{}, false
}

type OrchestratorDial struct {
	Conn       *grpc.ClientConn
	OwnerID    string
	Address    string
	InstanceID string
}

func DialOrchestratorWithOwner(ctx context.Context, logger interfaces.Logger, pinnedOrchestratorAddr, registryDialAddr, cellID, selectionKey string, retryMetrics backoff.RetryMetrics) (OrchestratorDial, func(), error) {
	pinnedOrchestratorAddr = strings.TrimSpace(pinnedOrchestratorAddr)
	if pinnedOrchestratorAddr != "" {
		logger.Info("Using pinned orchestrator address: %s", pinnedOrchestratorAddr)
		conn, cleanup, err := NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_ORCHESTRATOR, pinnedOrchestratorAddr, logger, nil, retryMetrics)
		if err != nil {
			return OrchestratorDial{}, nil, err
		}

		return OrchestratorDial{
			Conn:    conn,
			OwnerID: OrchestratorPinnedOwnerID(pinnedOrchestratorAddr),
			Address: pinnedOrchestratorAddr,
		}, cleanup, nil
	}

	regClient, err := NewRegistryClient(ctx, registryDialAddr, logger, interfaces.SystemClock{}, retryMetrics)
	if err != nil {
		return OrchestratorDial{}, nil, err
	}
	defer regClient.Close()

	entry, err := selectOrchestratorRegistryEntry(ctx, regClient, cellID, selectionKey)
	if err != nil {
		return OrchestratorDial{}, nil, err
	}

	address := normalizeRegistryDialAddr(entry.GetAddress())
	if address == "" {
		return OrchestratorDial{}, nil, fmt.Errorf("%s address not available", api.Component_COMPONENT_ORCHESTRATOR.String())
	}

	conn, cleanup, err := NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_ORCHESTRATOR, address, logger, nil, retryMetrics)
	if err != nil {
		return OrchestratorDial{}, nil, err
	}

	instanceID := strings.TrimSpace(entry.GetInstanceId())
	ownerID := OrchestratorAddressOwnerID(address)
	if instanceID != "" {
		ownerID = OrchestratorInstanceOwnerID(instanceID)
	}

	return OrchestratorDial{
		Conn:       conn,
		OwnerID:    ownerID,
		Address:    address,
		InstanceID: instanceID,
	}, cleanup, nil
}

type orchestratorRegistry interface {
	InstanceAddress(ctx context.Context, component api.Component, instanceID string) (string, error)
	ListRegistrations(ctx context.Context, component api.Component, metadata map[string]string) ([]*api.RegistryEntry, error)
	Close() error
}

func selectOrchestratorRegistryEntry(ctx context.Context, reg orchestratorRegistry, cellID, selectionKey string) (*api.RegistryEntry, error) {
	entries, err := reg.ListRegistrations(ctx, api.Component_COMPONENT_ORCHESTRATOR, registry.DefaultServiceMetadataForCell(cellID))
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		entries, err = reg.ListRegistrations(ctx, api.Component_COMPONENT_ORCHESTRATOR, nil)
		if err != nil {
			return nil, err
		}
	}

	entry := selectLiveOrchestratorRegistration(entries, selectionKey)
	if entry == nil {
		return nil, fmt.Errorf("%s address not available", api.Component_COMPONENT_ORCHESTRATOR.String())
	}

	return entry, nil
}

func selectLiveOrchestratorRegistration(entries []*api.RegistryEntry, selectionKey string) *api.RegistryEntry {
	filtered := make([]*api.RegistryEntry, 0, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.GetAddress() == "" {
			continue
		}

		filtered = append(filtered, entry)
	}

	sort.SliceStable(filtered, func(i, j int) bool {
		left := filtered[i].GetInstanceId() + "\x00" + filtered[i].GetAddress()
		right := filtered[j].GetInstanceId() + "\x00" + filtered[j].GetAddress()
		return left < right
	})

	if len(filtered) == 0 {
		return nil
	}

	selectionKey = strings.TrimSpace(selectionKey)
	if selectionKey != "" {
		selected := filtered[0]
		selectedScore := orchestratorSelectionScore(selectionKey, selected)
		for _, entry := range filtered[1:] {
			score := orchestratorSelectionScore(selectionKey, entry)
			if score > selectedScore {
				selected = entry
				selectedScore = score
			}
		}

		return selected
	}

	return filtered[0]
}

func orchestratorSelectionScore(selectionKey string, entry *api.RegistryEntry) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(selectionKey))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(entry.GetInstanceId()))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(entry.GetAddress()))
	return h.Sum64()
}

type OrchestratorOwnerClientResolver struct {
	logger          interfaces.Logger
	registryAddress string
	retryMetrics    backoff.RetryMetrics

	mu       sync.Mutex
	registry orchestratorRegistry
	clients  map[string]orchestratorOwnerClient
}

type orchestratorOwnerClient struct {
	client api.OrchestratorServiceClient
	close  func()
}

func NewOrchestratorOwnerClientResolver(logger interfaces.Logger, registryAddress string, retryMetrics backoff.RetryMetrics) *OrchestratorOwnerClientResolver {
	return &OrchestratorOwnerClientResolver{
		logger:          logger,
		registryAddress: strings.TrimSpace(registryAddress),
		retryMetrics:    retryMetrics,
		clients:         map[string]orchestratorOwnerClient{},
	}
}

func (r *OrchestratorOwnerClientResolver) Client(ctx context.Context, ownerID, cellID, selectionKey string) (api.OrchestratorServiceClient, bool, error) {
	if r == nil {
		return nil, false, nil
	}

	owner, ok := ParseOrchestratorOwnerID(ownerID)
	if !ok {
		return nil, false, nil
	}

	var address string
	var err error
	switch owner.Kind {
	case OrchestratorOwnerPinned, OrchestratorOwnerAddress:
		address = normalizeRegistryDialAddr(owner.Value)
	case OrchestratorOwnerInstance:
		reg, err := r.registryClient(ctx)
		if err != nil {
			return nil, false, err
		}

		address, err = reg.InstanceAddress(ctx, api.Component_COMPONENT_ORCHESTRATOR, owner.Value)
		if err != nil {
			return nil, false, err
		}

		address = normalizeRegistryDialAddr(address)
	case OrchestratorOwnerRegistry:
		reg, err := r.registryClient(ctx)
		if err != nil {
			return nil, false, err
		}

		entry, err := selectOrchestratorRegistryEntry(ctx, reg, owner.Value, selectionKey)
		if err != nil {
			return nil, false, err
		}

		address = normalizeRegistryDialAddr(entry.GetAddress())
	default:
		return nil, false, nil
	}

	client, err := r.clientForAddress(ctx, address)
	if err != nil {
		return nil, false, err
	}

	return client, true, nil
}

func (r *OrchestratorOwnerClientResolver) registryClient(ctx context.Context) (orchestratorRegistry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.registry != nil {
		return r.registry, nil
	}

	reg, err := NewRegistryClient(ctx, r.registryAddress, r.logger, interfaces.SystemClock{}, r.retryMetrics)
	if err != nil {
		return nil, err
	}

	r.registry = reg
	return reg, nil
}

func (r *OrchestratorOwnerClientResolver) clientForAddress(ctx context.Context, address string) (api.OrchestratorServiceClient, error) {
	address = strings.TrimSpace(address)
	if address == "" {
		return nil, fmt.Errorf("%s address not available", api.Component_COMPONENT_ORCHESTRATOR.String())
	}

	r.mu.Lock()
	if cached, ok := r.clients[address]; ok {
		r.mu.Unlock()
		return cached.client, nil
	}
	r.mu.Unlock()

	conn, cleanup, err := NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_ORCHESTRATOR, address, r.logger, nil, r.retryMetrics)
	if err != nil {
		return nil, err
	}

	client := api.NewOrchestratorServiceClient(conn)
	r.mu.Lock()
	defer r.mu.Unlock()
	if cached, ok := r.clients[address]; ok {
		cleanup()
		return cached.client, nil
	}

	r.clients[address] = orchestratorOwnerClient{client: client, close: cleanup}
	return client, nil
}

func (r *OrchestratorOwnerClientResolver) Close() error {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var err error
	for address, cached := range r.clients {
		if cached.close != nil {
			cached.close()
		}

		delete(r.clients, address)
	}

	if r.registry != nil {
		err = r.registry.Close()
		r.registry = nil
	}

	return err
}
