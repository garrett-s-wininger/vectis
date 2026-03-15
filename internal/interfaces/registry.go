package interfaces

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
)

type RegistryClient interface {
	Register(ctx context.Context, component api.Component, address string) error
	Address(ctx context.Context, component api.Component) (string, error)
	Close() error
}

type GRPCRegistryClient struct {
	conn   *grpc.ClientConn
	client api.RegistryServiceClient
}

func NewGRPCRegistryClient(conn *grpc.ClientConn) *GRPCRegistryClient {
	return &GRPCRegistryClient{
		conn:   conn,
		client: api.NewRegistryServiceClient(conn),
	}
}

func (c *GRPCRegistryClient) Register(ctx context.Context, component api.Component, address string) error {
	_, err := c.client.Register(ctx, &api.Registration{
		Component: &component,
		Address:   &address,
	})

	if err != nil {
		return fmt.Errorf("failed to register component %s: %w", component.String(), err)
	}

	return nil
}

func (c *GRPCRegistryClient) Address(ctx context.Context, component api.Component) (string, error) {
	resp, err := c.client.GetAddress(ctx, &api.AddressRequest{
		Component: component.Enum(),
	})

	if err != nil {
		return "", fmt.Errorf("failed to get address for component %s: %w", component.String(), err)
	}

	return resp.GetAddress(), nil
}

func (c *GRPCRegistryClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
