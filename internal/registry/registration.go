package registry

import (
	"context"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/interfaces"
)

type RegistrationOptions struct {
	RegistryAddress string
	Component       api.Component
	InstanceID      string
	PublishAddress  string
	Metadata        map[string]string
	RefreshInterval time.Duration
	Logger          interfaces.Logger
	Clock           interfaces.Clock
	Metrics         backoff.RetryMetrics
}

// RegisterWithHeartbeat registers a service with the sponsor-preferred
// registry target and keeps the registration fresh with static metadata.
func RegisterWithHeartbeat(ctx context.Context, opts RegistrationOptions) (func(), error) {
	return RegisterWithDynamicMetadataHeartbeat(ctx, opts, nil)
}

// RegisterWithDynamicMetadataHeartbeat registers a service with the configured
// registry addresses, preferring the rendezvous-hashed sponsor and failing over
// to other targets on errors. It refreshes metadata from metadataProvider on
// each heartbeat.
func RegisterWithDynamicMetadataHeartbeat(ctx context.Context, opts RegistrationOptions, metadataProvider func() map[string]string) (func(), error) {
	clock := opts.Clock
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	registryAddress := sponsorOrderedRegistryAddress(opts.RegistryAddress, opts.Component, opts.InstanceID, opts.PublishAddress)
	if metadataProvider == nil {
		metadata := cloneMetadata(opts.Metadata)
		metadataProvider = func() map[string]string {
			return cloneMetadata(metadata)
		}
	}

	registryClient, err := New(ctx, registryAddress, opts.Logger, clock, opts.Metrics)
	if err != nil {
		return nil, err
	}

	if err := registryClient.RegisterInstanceWithMetadata(ctx, opts.Component, opts.InstanceID, opts.PublishAddress, metadataProvider()); err != nil {
		_ = registryClient.Close()
		return nil, err
	}

	stopHeartbeat := startDynamicRegistrationHeartbeat(ctx, registryClient, opts.Component, opts.InstanceID, opts.PublishAddress, metadataProvider, opts.RefreshInterval, opts.Logger)
	return func() {
		stopHeartbeat()
		_ = registryClient.Close()
	}, nil
}

func startDynamicRegistrationHeartbeat(ctx context.Context, r *Registry, comp api.Component, instanceID, address string, metadataProvider func() map[string]string, interval time.Duration, logger interfaces.Logger) func() {
	if r == nil || interval <= 0 || logger == nil {
		return func() {}
	}

	loopCtx, cancel := context.WithCancel(ctx)
	go dynamicRegistrationHeartbeatLoop(loopCtx, r, comp, instanceID, address, metadataProvider, interval, logger)

	return cancel
}

func dynamicRegistrationHeartbeatLoop(ctx context.Context, r *Registry, comp api.Component, instanceID, address string, metadataProvider func() map[string]string, interval time.Duration, logger interfaces.Logger) {
	for {
		wait := intervalWithJitter(interval)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		hbCtx, cancel := context.WithTimeout(ctx, registrationHeartbeatRPCTimeout)
		err := r.RegisterInstanceOnceWithMetadata(hbCtx, comp, instanceID, address, metadataProvider())
		cancel()

		if err != nil {
			logger.Debug("Registry registration heartbeat failed for %s: %v", comp.String(), err)
		}
	}
}

func sponsorOrderedRegistryAddress(addresses string, component api.Component, instanceID, publishAddress string) string {
	parsed := splitRegistryAddresses(addresses)
	if len(parsed) <= 1 {
		return addresses
	}

	key := component.String() + "\x00" + instanceID + "\x00" + publishAddress
	sort.SliceStable(parsed, func(i, j int) bool {
		return rendezvousScore(key, parsed[i]) > rendezvousScore(key, parsed[j])
	})

	return strings.Join(parsed, ",")
}

func rendezvousScore(key, node string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(node))
	return h.Sum64()
}
