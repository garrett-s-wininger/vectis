package registry

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/interfaces"
	"vectis/internal/networking"
)

type Registry struct {
	mu        sync.Mutex
	client    *networking.Client[api.RegistryServiceClient]
	addresses []string
	active    int
	logger    interfaces.Logger
	maxTries  int
	baseDelay time.Duration
	clock     interfaces.Clock
	metrics   backoff.RetryMetrics
}

func New(ctx context.Context, addr string, logger interfaces.Logger, clock interfaces.Clock, retryMetrics backoff.RetryMetrics) (*Registry, error) {
	addresses := splitRegistryAddresses(addr)
	if len(addresses) == 0 {
		return nil, fmt.Errorf("registry address is required")
	}

	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	r := &Registry{
		addresses: addresses,
		logger:    logger,
		maxTries:  5,
		baseDelay: 500 * time.Millisecond,
		clock:     clock,
		metrics:   retryMetrics,
	}

	if _, err := r.connectLocked(ctx); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Registry) Register(ctx context.Context, component api.Component, address string) error {
	return r.RegisterWithMetadata(ctx, component, address, nil)
}

func (r *Registry) RegisterWithMetadata(ctx context.Context, component api.Component, address string, metadata map[string]string) error {
	return r.RegisterInstanceWithMetadata(ctx, component, "", address, metadata)
}

func (r *Registry) RegisterInstance(ctx context.Context, component api.Component, instanceID, address string) error {
	return r.RegisterInstanceWithMetadata(ctx, component, instanceID, address, nil)
}

func (r *Registry) RegisterInstanceWithMetadata(ctx context.Context, component api.Component, instanceID, address string, metadata map[string]string) error {
	metadata = cloneMetadata(metadata)
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  r.maxTries,
		BaseDelay: r.baseDelay,
		Clock:     r.clock,
		Metrics:   r.metrics,
		Component: "registry",
	})

	return retryer.Do(ctx, func() error {
		return r.RegisterInstanceOnceWithMetadata(ctx, component, instanceID, address, metadata)
	}, func(attempt int, nextDelay time.Duration, err error) {
		r.logger.Debug("Failed to register with registry (attempt %d/%d): %v. Retrying in %v...", attempt, r.maxTries, err, nextDelay)
	})
}

func (r *Registry) RegisterOnce(ctx context.Context, component api.Component, address string) error {
	return r.RegisterOnceWithMetadata(ctx, component, address, nil)
}

func (r *Registry) RegisterOnceWithMetadata(ctx context.Context, component api.Component, address string, metadata map[string]string) error {
	return r.RegisterInstanceOnceWithMetadata(ctx, component, "", address, metadata)
}

func (r *Registry) RegisterInstanceOnce(ctx context.Context, component api.Component, instanceID, address string) error {
	return r.RegisterInstanceOnceWithMetadata(ctx, component, instanceID, address, nil)
}

func (r *Registry) RegisterInstanceOnceWithMetadata(ctx context.Context, component api.Component, instanceID, address string, metadata map[string]string) error {
	comp := component
	addr := address
	req := &api.Registration{
		Component: &comp,
		Address:   &addr,
		Metadata:  cloneMetadata(metadata),
	}

	if instanceID != "" {
		req.InstanceId = &instanceID
	}

	return r.withClient(ctx, func(client api.RegistryServiceClient) error {
		_, err := client.Register(ctx, req)
		return err
	})
}

func (r *Registry) ListRegistrations(ctx context.Context, component api.Component, metadata map[string]string) ([]*api.RegistryEntry, error) {
	req := &api.ListRegistrationsRequest{
		Component: component.Enum(),
		Metadata:  cloneMetadata(metadata),
	}

	var resp *api.ListRegistrationsResponse
	err := r.withClient(ctx, func(client api.RegistryServiceClient) error {
		var e error
		resp, e = client.ListRegistrations(ctx, req)
		return e
	})

	if err != nil {
		return nil, err
	}

	return resp.GetEntries(), nil
}

func (r *Registry) Address(ctx context.Context, component api.Component) (string, error) {
	return r.InstanceAddress(ctx, component, "")
}

func (r *Registry) InstanceAddress(ctx context.Context, component api.Component, instanceID string) (string, error) {
	address, err := r.getAddressOnce(ctx, component, instanceID)
	if err != nil {
		return "", err
	}

	if address == "" {
		if instanceID != "" {
			return "", fmt.Errorf("%s instance %s address not available", component.String(), instanceID)
		}

		return "", fmt.Errorf("%s address not available", component.String())
	}

	return address, nil
}

func (r *Registry) getAddressOnce(ctx context.Context, component api.Component, instanceID string) (string, error) {
	req := &api.AddressRequest{
		Component: component.Enum(),
	}

	if instanceID != "" {
		req.InstanceId = &instanceID
	}

	var resp *api.AddressResponse
	err := r.withClient(ctx, func(client api.RegistryServiceClient) error {
		var e error
		resp, e = client.GetAddress(ctx, req)
		return e
	})

	if err != nil {
		return "", err
	}

	return resp.GetAddress(), nil
}

func (r *Registry) GossipOnce(ctx context.Context, req *api.GossipRequest) (*api.GossipResponse, error) {
	var resp *api.GossipResponse
	err := r.withClient(ctx, func(client api.RegistryServiceClient) error {
		var e error
		resp, e = client.Gossip(ctx, req)
		return e
	})

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (r *Registry) SnapshotOnce(ctx context.Context, req *api.RegistrySnapshotRequest) (*api.RegistrySnapshotResponse, error) {
	var resp *api.RegistrySnapshotResponse
	err := r.withClient(ctx, func(client api.RegistryServiceClient) error {
		var e error
		resp, e = client.GetSnapshot(ctx, req)
		return e
	})

	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client == nil {
		return nil
	}

	err := r.client.Close()
	r.client = nil
	return err
}

func (r *Registry) withClient(ctx context.Context, call func(api.RegistryServiceClient) error) error {
	var lastErr error
	for range r.addresses {
		client, err := r.currentClient(ctx)
		if err != nil {
			lastErr = err
			r.advance()
			continue
		}

		if err := call(client.Client()); err != nil {
			lastErr = err
			r.dropActive()
			continue
		}

		return nil
	}

	if lastErr != nil {
		return lastErr
	}

	return fmt.Errorf("registry client has no configured addresses")
}

func (r *Registry) currentClient(ctx context.Context) (*networking.Client[api.RegistryServiceClient], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client != nil {
		return r.client, nil
	}

	return r.connectLocked(ctx)
}

func (r *Registry) connectLocked(ctx context.Context) (*networking.Client[api.RegistryServiceClient], error) {
	if len(r.addresses) == 0 {
		return nil, fmt.Errorf("registry address is required")
	}

	var lastErr error
	for range r.addresses {
		addr := r.addresses[r.active]
		client, err := networking.NewClient(ctx, addr, api.NewRegistryServiceClient, r.logger, r.clock, r.metrics)
		if err == nil {
			r.client = client
			return client, nil
		}

		lastErr = err
		r.active = (r.active + 1) % len(r.addresses)
	}

	return nil, lastErr
}

func (r *Registry) dropActive() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.client != nil {
		_ = r.client.Close()
		r.client = nil
	}

	r.active = (r.active + 1) % len(r.addresses)
}

func (r *Registry) advance() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.addresses) == 0 {
		return
	}

	r.active = (r.active + 1) % len(r.addresses)
}

func splitRegistryAddresses(addr string) []string {
	parts := strings.FieldsFunc(addr, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\t' || r == ' '
	})

	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if _, ok := seen[part]; ok {
			continue
		}

		seen[part] = struct{}{}
		out = append(out, part)
	}

	return out
}
