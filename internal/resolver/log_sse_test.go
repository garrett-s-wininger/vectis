package resolver

import (
	"context"
	"fmt"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/registry"
	"vectis/internal/testutil/grpctest"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func TestResolveLogSSEAddress_pinned(t *testing.T) {
	viper.Set("api.log.address", "192.168.1.1:8080")
	t.Cleanup(func() { viper.Set("api.log.address", "") })

	logger := mocks.NewMockLogger()
	addr, err := ResolveLogSSEAddress(context.Background(), logger, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if addr != "192.168.1.1:8080" {
		t.Fatalf("expected 192.168.1.1:8080, got %s", addr)
	}
}

func TestResolveLogSSEAddress_noPinnedNoRegistry(t *testing.T) {
	viper.Set("api.log.address", "")

	logger := mocks.NewMockLogger()
	_, err := ResolveLogSSEAddress(context.Background(), logger, "")
	if err == nil {
		t.Fatal("expected error when no pinned address and no registry")
	}
}

func TestResolveLogSSEAddress_viaRegistry(t *testing.T) {
	t.Setenv("VECTIS_API_LOG_SSE_ADDRESS", "")

	logger := mocks.NewMockLogger()

	_, listener, _ := grpctest.SetupGRPCServer(t, func(srv *grpc.Server) {
		api.RegisterRegistryServiceServer(srv, registry.NewRegistryService(mocks.NewMockLogger()))
	})

	regAddr := listener.Addr().String()

	// Register a log component
	regClient, err := registry.New(context.Background(), regAddr, mocks.NewMockLogger(), mocks.NewMockClock())
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer regClient.Close()

	if err := regClient.Register(context.Background(), api.Component_COMPONENT_LOG, "10.0.0.1:50051"); err != nil {
		t.Fatalf("failed to register: %v", err)
	}

	addr, err := ResolveLogSSEAddress(context.Background(), logger, regAddr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := fmt.Sprintf("10.0.0.1:%d", config.LogSSEPort())
	if addr != expected {
		t.Fatalf("expected %s, got %s", expected, addr)
	}
}

func TestResolveLogSSEAddressWithTimeout(t *testing.T) {
	viper.Set("api.log.address", "1.2.3.4:9090")
	t.Cleanup(func() { viper.Set("api.log.address", "") })

	logger := mocks.NewMockLogger()
	addr, err := ResolveLogSSEAddressWithTimeout(logger, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if addr != "1.2.3.4:9090" {
		t.Fatalf("expected 1.2.3.4:9090, got %s", addr)
	}
}
