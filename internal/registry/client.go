package registry

import (
	"context"
	"fmt"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/interfaces"
	"vectis/internal/networking"
)

type Registry struct {
	*networking.Client[api.RegistryServiceClient]
}

func New(ctx context.Context, logger interfaces.Logger) (*Registry, error) {
	c, err := networking.NewClient(ctx, networking.RegistryPort, api.NewRegistryServiceClient, logger)
	if err != nil {
		return nil, err
	}

	return &Registry{Client: c}, nil
}

func (r *Registry) Register(ctx context.Context, component api.Component, address string) error {
	return backoff.RetryWithBackoff(r.MaxTries, r.BaseDelay, func() error {
		return r.registerOnce(ctx, component, address)
	}, func(attempt int, nextDelay time.Duration, err error) {
		r.Logger.Warn("Failed to register with registry (attempt %d/%d): %v. Retrying in %v...", attempt, r.MaxTries, err, nextDelay)
	})
}

func (r *Registry) registerOnce(ctx context.Context, component api.Component, address string) error {
	comp := component
	addr := address
	_, err := r.Client.Client().Register(ctx, &api.Registration{
		Component: &comp,
		Address:   &addr,
	})

	return err
}

func (r *Registry) Address(ctx context.Context, component api.Component) (string, error) {
	var address string
	err := backoff.RetryWithBackoff(r.MaxTries, r.BaseDelay, func() error {
		var err error
		address, err = r.getAddressOnce(ctx, component)
		if err != nil {
			return err
		}
		if address == "" {
			return fmt.Errorf("%s address not available", component.String())
		}
		return nil
	}, func(attempt int, delay time.Duration, err error) {
		r.Logger.Warn("Failed to get %s address (attempt %d): %v. Retrying in %v...", component.String(), attempt, err, delay)
	})

	if err != nil {
		return "", err
	}

	return address, nil
}

func (r *Registry) getAddressOnce(ctx context.Context, component api.Component) (string, error) {
	resp, err := r.Client.Client().GetAddress(ctx, &api.AddressRequest{
		Component: component.Enum(),
	})

	if err != nil {
		return "", err
	}

	return resp.GetAddress(), nil
}
