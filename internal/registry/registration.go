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

func RegisterWithHeartbeat(ctx context.Context, opts RegistrationOptions) (func(), error) {
	clock := opts.Clock
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	registryAddress := sponsorOrderedRegistryAddress(opts.RegistryAddress, opts.Component, opts.InstanceID, opts.PublishAddress)
	registryClient, err := New(ctx, registryAddress, opts.Logger, clock, opts.Metrics)
	if err != nil {
		return nil, err
	}

	metadata := cloneMetadata(opts.Metadata)
	if err := registryClient.RegisterInstanceWithMetadata(ctx, opts.Component, opts.InstanceID, opts.PublishAddress, metadata); err != nil {
		_ = registryClient.Close()
		return nil, err
	}

	stopHeartbeat := StartInstanceRegistrationHeartbeatWithMetadata(ctx, registryClient, opts.Component, opts.InstanceID, opts.PublishAddress, metadata, opts.RefreshInterval, opts.Logger)
	return func() {
		stopHeartbeat()
		_ = registryClient.Close()
	}, nil
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
