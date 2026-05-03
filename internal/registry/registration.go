package registry

import (
	"context"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
)

type RegistrationOptions struct {
	RegistryAddress string
	Component       api.Component
	InstanceID      string
	PublishAddress  string
	RefreshInterval time.Duration
	Logger          interfaces.Logger
	Clock           interfaces.Clock
}

func RegisterWithHeartbeat(ctx context.Context, opts RegistrationOptions) (func(), error) {
	clock := opts.Clock
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	registryClient, err := New(ctx, opts.RegistryAddress, opts.Logger, clock)
	if err != nil {
		return nil, err
	}

	if err := registryClient.RegisterInstance(ctx, opts.Component, opts.InstanceID, opts.PublishAddress); err != nil {
		_ = registryClient.Close()
		return nil, err
	}

	stopHeartbeat := StartRegistrationHeartbeat(ctx, registryClient, opts.Component, opts.PublishAddress, opts.RefreshInterval, opts.Logger)
	return func() {
		stopHeartbeat()
		_ = registryClient.Close()
	}, nil
}
