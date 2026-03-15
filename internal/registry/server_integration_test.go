package registry_test

import (
	"context"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/registry"
	"vectis/internal/testutil/grpctest"

	"google.golang.org/grpc"
)

func setupRegistryServer(t *testing.T) api.RegistryServiceClient {
	t.Helper()

	logger := mocks.NewMockLogger()
	registryService := registry.NewRegistryService(logger)

	_, _, conn := grpctest.SetupGRPCServer(t, func(s *grpc.Server) {
		api.RegisterRegistryServiceServer(s, registryService)
	})

	return api.NewRegistryServiceClient(conn)
}

func TestIntegrationRegistry_RegisterGetAddressRoundTrip(t *testing.T) {
	client := setupRegistryServer(t)
	ctx := context.Background()

	queueAddr := "localhost:50051"
	component := api.Component_COMPONENT_QUEUE
	_, err := client.Register(ctx, &api.Registration{
		Component: &component,
		Address:   &queueAddr,
	})

	if err != nil {
		t.Fatalf("register queue failed: %v", err)
	}

	resp, err := client.GetAddress(ctx, &api.AddressRequest{
		Component: component.Enum(),
	})

	if err != nil {
		t.Fatalf("get address failed: %v", err)
	}

	if resp.GetAddress() != queueAddr {
		t.Errorf("expected address %q, got %q", queueAddr, resp.GetAddress())
	}

	logAddr := "localhost:50052"
	logComponent := api.Component_COMPONENT_LOG
	_, err = client.Register(ctx, &api.Registration{
		Component: &logComponent,
		Address:   &logAddr,
	})

	if err != nil {
		t.Fatalf("register log failed: %v", err)
	}

	resp, err = client.GetAddress(ctx, &api.AddressRequest{
		Component: logComponent.Enum(),
	})

	if err != nil {
		t.Fatalf("get address failed: %v", err)
	}

	if resp.GetAddress() != logAddr {
		t.Errorf("expected address %q, got %q", logAddr, resp.GetAddress())
	}
}

func TestIntegrationRegistry_AddressNotFound(t *testing.T) {
	client := setupRegistryServer(t)
	ctx := context.Background()

	component := api.Component_COMPONENT_QUEUE
	resp, err := client.GetAddress(ctx, &api.AddressRequest{
		Component: component.Enum(),
	})

	if err != nil {
		t.Fatalf("get address for unregistered component failed: %v", err)
	}

	if resp.GetAddress() != "" {
		t.Errorf("expected empty address for unregistered component, got %q", resp.GetAddress())
	}

	addr := "localhost:50051"
	_, err = client.Register(ctx, &api.Registration{
		Component: &component,
		Address:   &addr,
	})

	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	resp, err = client.GetAddress(ctx, &api.AddressRequest{
		Component: component.Enum(),
	})

	if err != nil {
		t.Fatalf("get address after register failed: %v", err)
	}

	if resp.GetAddress() != addr {
		t.Errorf("expected address %q, got %q", addr, resp.GetAddress())
	}
}

func TestIntegrationRegistry_RegisterOverwrite(t *testing.T) {
	client := setupRegistryServer(t)
	ctx := context.Background()
	component := api.Component_COMPONENT_QUEUE

	addr1 := "localhost:50051"
	_, err := client.Register(ctx, &api.Registration{
		Component: &component,
		Address:   &addr1,
	})

	if err != nil {
		t.Fatalf("first register failed: %v", err)
	}

	resp, err := client.GetAddress(ctx, &api.AddressRequest{
		Component: component.Enum(),
	})

	if err != nil {
		t.Fatalf("get address failed: %v", err)
	}

	if resp.GetAddress() != addr1 {
		t.Errorf("expected address %q, got %q", addr1, resp.GetAddress())
	}

	addr2 := "localhost:50052"
	_, err = client.Register(ctx, &api.Registration{
		Component: &component,
		Address:   &addr2,
	})

	if err != nil {
		t.Fatalf("second register failed: %v", err)
	}

	resp, err = client.GetAddress(ctx, &api.AddressRequest{
		Component: component.Enum(),
	})

	if err != nil {
		t.Fatalf("get address after overwrite failed: %v", err)
	}

	if resp.GetAddress() != addr2 {
		t.Errorf("expected overwritten address %q, got %q", addr2, resp.GetAddress())
	}
}

func TestIntegrationRegistry_MultipleComponentsIndependent(t *testing.T) {
	client := setupRegistryServer(t)
	ctx := context.Background()

	queueAddr := "localhost:50051"
	logAddr := "localhost:50052"
	queueComponent := api.Component_COMPONENT_QUEUE
	logComponent := api.Component_COMPONENT_LOG

	_, err := client.Register(ctx, &api.Registration{
		Component: &queueComponent,
		Address:   &queueAddr,
	})

	if err != nil {
		t.Fatalf("register queue failed: %v", err)
	}

	_, err = client.Register(ctx, &api.Registration{
		Component: &logComponent,
		Address:   &logAddr,
	})

	if err != nil {
		t.Fatalf("register log failed: %v", err)
	}

	resp, err := client.GetAddress(ctx, &api.AddressRequest{
		Component: queueComponent.Enum(),
	})

	if err != nil {
		t.Fatalf("get queue address failed: %v", err)
	}

	if resp.GetAddress() != queueAddr {
		t.Errorf("expected queue address %q, got %q", queueAddr, resp.GetAddress())
	}

	resp, err = client.GetAddress(ctx, &api.AddressRequest{
		Component: logComponent.Enum(),
	})

	if err != nil {
		t.Fatalf("get log address failed: %v", err)
	}

	if resp.GetAddress() != logAddr {
		t.Errorf("expected log address %q, got %q", logAddr, resp.GetAddress())
	}

	newQueueAddr := "localhost:50053"
	_, err = client.Register(ctx, &api.Registration{
		Component: &queueComponent,
		Address:   &newQueueAddr,
	})

	if err != nil {
		t.Fatalf("overwrite queue failed: %v", err)
	}

	resp, err = client.GetAddress(ctx, &api.AddressRequest{
		Component: queueComponent.Enum(),
	})

	if err != nil {
		t.Fatalf("get queue address after overwrite failed: %v", err)
	}

	if resp.GetAddress() != newQueueAddr {
		t.Errorf("expected new queue address %q, got %q", newQueueAddr, resp.GetAddress())
	}

	resp, err = client.GetAddress(ctx, &api.AddressRequest{
		Component: logComponent.Enum(),
	})

	if err != nil {
		t.Fatalf("get log address after queue overwrite failed: %v", err)
	}

	if resp.GetAddress() != logAddr {
		t.Errorf("expected log address %q to be unchanged, got %q", logAddr, resp.GetAddress())
	}
}
