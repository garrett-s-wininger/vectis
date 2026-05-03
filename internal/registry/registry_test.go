package registry

import (
	"context"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/grpctest"

	"google.golang.org/grpc"
)

func setupTestRegistry(t *testing.T) (string, *grpc.Server) {
	t.Helper()
	srv, listener, _ := grpctest.SetupGRPCServer(t, func(srv *grpc.Server) {
		api.RegisterRegistryServiceServer(srv, NewRegistryService(mocks.NewMockLogger()))
	})

	return listener.Addr().String(), srv
}

func TestRegistry_RegisterAndAddress(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	if err := reg.Register(context.Background(), api.Component_COMPONENT_QUEUE, ":50051"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	got, err := reg.Address(context.Background(), api.Component_COMPONENT_QUEUE)
	if err != nil {
		t.Fatalf("address lookup failed: %v", err)
	}

	if got != ":50051" {
		t.Fatalf("expected :50051, got %s", got)
	}
}

func TestRegistry_InstanceAddress(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	if err := reg.RegisterInstance(context.Background(), api.Component_COMPONENT_WORKER, "worker-1", "10.0.0.1:50051"); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	got, err := reg.InstanceAddress(context.Background(), api.Component_COMPONENT_WORKER, "worker-1")
	if err != nil {
		t.Fatalf("instance address lookup failed: %v", err)
	}

	if got != "10.0.0.1:50051" {
		t.Fatalf("expected 10.0.0.1:50051, got %s", got)
	}
}

func TestRegistry_InstanceAddress_notFound(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	_, err = reg.InstanceAddress(context.Background(), api.Component_COMPONENT_WORKER, "missing")
	if err == nil {
		t.Fatal("expected error for missing instance")
	}
}

func TestRegistry_Address_notFound(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	_, err = reg.Address(context.Background(), api.Component_COMPONENT_QUEUE)
	if err == nil {
		t.Fatal("expected error for missing component")
	}
}

func TestRegistry_RegisterInstanceOnce(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	if err := reg.RegisterInstanceOnce(context.Background(), api.Component_COMPONENT_LOG, "log-1", "127.0.0.1:50051"); err != nil {
		t.Fatalf("register once failed: %v", err)
	}
}

func TestStartRegistrationHeartbeat(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	stop := StartRegistrationHeartbeat(context.Background(), reg, api.Component_COMPONENT_QUEUE, ":50051", 50*time.Millisecond, logger)
	defer stop()

	// Wait for at least one heartbeat
	time.Sleep(150 * time.Millisecond)

	got, err := reg.Address(context.Background(), api.Component_COMPONENT_QUEUE)
	if err != nil {
		t.Fatalf("address lookup failed: %v", err)
	}

	if got != ":50051" {
		t.Fatalf("expected :50051 after heartbeat, got %s", got)
	}
}

func TestStartInstanceRegistrationHeartbeat(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	stop := StartInstanceRegistrationHeartbeat(context.Background(), reg, api.Component_COMPONENT_WORKER, "worker-1", "10.0.0.1:50051", 50*time.Millisecond, logger)
	defer stop()

	// Wait for at least one heartbeat.
	time.Sleep(150 * time.Millisecond)

	got, err := reg.InstanceAddress(context.Background(), api.Component_COMPONENT_WORKER, "worker-1")
	if err != nil {
		t.Fatalf("instance address lookup failed: %v", err)
	}

	if got != "10.0.0.1:50051" {
		t.Fatalf("expected 10.0.0.1:50051 after heartbeat, got %s", got)
	}
}

func TestStartRegistrationHeartbeat_nilRegistry(t *testing.T) {
	logger := mocks.NewMockLogger()
	stop := StartRegistrationHeartbeat(context.Background(), nil, api.Component_COMPONENT_QUEUE, ":50051", 50*time.Millisecond, logger)
	stop()
}

func TestStartRegistrationHeartbeat_zeroInterval(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	stop := StartRegistrationHeartbeat(context.Background(), reg, api.Component_COMPONENT_QUEUE, ":50051", 0, logger)
	stop()
}

func TestIntervalWithJitter(t *testing.T) {
	base := 100 * time.Millisecond
	for range 20 {
		got := intervalWithJitter(base)
		if got < base {
			t.Fatalf("jittered interval %v < base %v", got, base)
		}

		if got > base+base/4 {
			t.Fatalf("jittered interval %v > max %v", got, base+base/4)
		}
	}
}

func TestIntervalWithJitter_zero(t *testing.T) {
	if intervalWithJitter(0) != 0 {
		t.Fatal("expected 0 for zero interval")
	}
}

func TestIntervalWithJitter_small(t *testing.T) {
	base := 2 * time.Nanosecond
	got := intervalWithJitter(base)
	if got != base {
		t.Fatalf("expected %v for small interval, got %v", base, got)
	}
}
