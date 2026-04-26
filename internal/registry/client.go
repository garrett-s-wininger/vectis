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

func New(ctx context.Context, addr string, logger interfaces.Logger, clock interfaces.Clock) (*Registry, error) {
	c, err := networking.NewClient(ctx, addr, api.NewRegistryServiceClient, logger, clock)
	if err != nil {
		return nil, err
	}

	return &Registry{Client: c}, nil
}

func (r *Registry) Register(ctx context.Context, component api.Component, address string) error {
	return r.RegisterInstance(ctx, component, "", address)
}

func (r *Registry) RegisterInstance(ctx context.Context, component api.Component, instanceID, address string) error {
	retryer := backoff.NewRetryer(backoff.RetryConfig{
		MaxTries:  r.MaxTries,
		BaseDelay: r.BaseDelay,
		Clock:     r.Clock,
	})

	return retryer.Do(ctx, func() error {
		return r.RegisterInstanceOnce(ctx, component, instanceID, address)
	}, func(attempt int, nextDelay time.Duration, err error) {
		r.Logger.Debug("Failed to register with registry (attempt %d/%d): %v. Retrying in %v...", attempt, r.MaxTries, err, nextDelay)
	})
}

func (r *Registry) RegisterOnce(ctx context.Context, component api.Component, address string) error {
	return r.RegisterInstanceOnce(ctx, component, "", address)
}

func (r *Registry) RegisterInstanceOnce(ctx context.Context, component api.Component, instanceID, address string) error {
	comp := component
	addr := address
	req := &api.Registration{
		Component: &comp,
		Address:   &addr,
	}

	if instanceID != "" {
		req.InstanceId = &instanceID
	}

	_, err := r.Client.Client().Register(ctx, req)
	return err
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

	resp, err := r.Client.Client().GetAddress(ctx, req)

	if err != nil {
		return "", err
	}

	return resp.GetAddress(), nil
}
