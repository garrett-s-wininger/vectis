package registry

import (
	"context"
	"sync/atomic"
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
	reg, err := New(context.Background(), addr, logger, clock, nil)
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
	reg, err := New(context.Background(), addr, logger, clock, nil)
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
	reg, err := New(context.Background(), addr, logger, clock, nil)
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
	reg, err := New(context.Background(), addr, logger, clock, nil)
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
	reg, err := New(context.Background(), addr, logger, clock, nil)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	if err := reg.RegisterInstanceOnce(context.Background(), api.Component_COMPONENT_LOG, "log-1", "127.0.0.1:50051"); err != nil {
		t.Fatalf("register once failed: %v", err)
	}
}

func TestRegistryService_RegisterLogsNewUpdatedAndRenewed(t *testing.T) {
	logger := mocks.NewMockLogger()
	svc := NewRegistryServiceWithOptions(logger, ServiceOptions{})
	ctx := context.Background()
	component := api.Component_COMPONENT_QUEUE
	addr := "queue-1:50051"

	if _, err := svc.Register(ctx, &api.Registration{Component: &component, Address: &addr}); err != nil {
		t.Fatalf("register new failed: %v", err)
	}

	if got := logger.GetInfoCalls(); len(got) != 1 || got[0] != "New queue registration at: queue-1:50051" {
		t.Fatalf("new registration info logs = %+v", got)
	}

	if _, err := svc.Register(ctx, &api.Registration{Component: &component, Address: &addr}); err != nil {
		t.Fatalf("renew registration failed: %v", err)
	}

	if got := logger.GetDebugCalls(); len(got) != 1 || got[0] != "Renewed queue registration at: queue-1:50051" {
		t.Fatalf("renew registration debug logs = %+v", got)
	}

	updatedAddr := "queue-2:50051"
	if _, err := svc.Register(ctx, &api.Registration{Component: &component, Address: &updatedAddr}); err != nil {
		t.Fatalf("update registration failed: %v", err)
	}

	if got := logger.GetInfoCalls(); len(got) != 2 || got[1] != "Updated queue registration at: queue-2:50051" {
		t.Fatalf("updated registration info logs = %+v", got)
	}
}

func TestRegistry_ListRegistrationsFiltersByMetadata(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock, nil)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	if err := reg.RegisterInstanceWithMetadata(context.Background(), api.Component_COMPONENT_QUEUE, "ingress", "queue-ingress:50051", QueueIngressMetadata()); err != nil {
		t.Fatalf("register ingress queue failed: %v", err)
	}

	if err := reg.RegisterInstanceWithMetadata(context.Background(), api.Component_COMPONENT_QUEUE, "pool-linux", "queue-linux:50051", map[string]string{
		MetadataCellID:    DefaultCellID,
		MetadataQueueRole: QueueRolePool,
		"pool":            "linux",
		"trait.os":        "linux",
	}); err != nil {
		t.Fatalf("register pool queue failed: %v", err)
	}

	got, err := reg.ListRegistrations(context.Background(), api.Component_COMPONENT_QUEUE, map[string]string{MetadataQueueRole: QueueRolePool, "trait.os": "linux"})
	if err != nil {
		t.Fatalf("list registrations failed: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected one filtered registration, got %d", len(got))
	}

	entry := got[0]
	if entry.GetInstanceId() != "pool-linux" || entry.GetAddress() != "queue-linux:50051" {
		t.Fatalf("unexpected filtered entry: %+v", entry)
	}

	if entry.GetMetadata()[MetadataCellID] != DefaultCellID || entry.GetMetadata()[MetadataQueueRole] != QueueRolePool {
		t.Fatalf("expected metadata on filtered entry, got %+v", entry.GetMetadata())
	}
}

func TestServiceMetadataForCell(t *testing.T) {
	service := DefaultServiceMetadataForCell("iad-a")
	if service[MetadataCellID] != "iad-a" {
		t.Fatalf("service metadata cell: got %+v", service)
	}

	queue := QueueIngressMetadataForCell("dfw-b")
	if queue[MetadataCellID] != "dfw-b" || queue[MetadataQueueRole] != QueueRoleIngress {
		t.Fatalf("queue metadata: got %+v", queue)
	}

	worker := WorkerExecutionMetadataForCell("sjc-c", "lima", "vm", []string{"host", "vm", "host", " "})
	if worker[MetadataCellID] != "sjc-c" ||
		worker[MetadataWorkerExecutionBackend] != "lima" ||
		worker[MetadataWorkerDefaultIsolation] != "vm" ||
		worker[MetadataWorkerSupportedIsolation] != "host,vm" {
		t.Fatalf("worker execution metadata: got %+v", worker)
	}

	if got := DefaultServiceMetadataForCell(" ")[MetadataCellID]; got != DefaultCellID {
		t.Fatalf("blank cell should fall back to %q, got %q", DefaultCellID, got)
	}
}

func TestStartRegistrationHeartbeatWithMetadata(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock, nil)
	if err != nil {
		t.Fatalf("failed to create registry client: %v", err)
	}
	defer reg.Close()

	stop := StartRegistrationHeartbeatWithMetadata(context.Background(), reg, api.Component_COMPONENT_QUEUE, ":50051", QueueIngressMetadata(), 50*time.Millisecond, logger)
	defer stop()

	time.Sleep(150 * time.Millisecond)

	got, err := reg.ListRegistrations(context.Background(), api.Component_COMPONENT_QUEUE, map[string]string{MetadataQueueRole: QueueRoleIngress})
	if err != nil {
		t.Fatalf("list registrations failed: %v", err)
	}

	if len(got) != 1 || got[0].GetAddress() != ":50051" {
		t.Fatalf("expected metadata heartbeat registration, got %+v", got)
	}
}

func TestStartRegistrationHeartbeat(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	reg, err := New(context.Background(), addr, logger, clock, nil)
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
	reg, err := New(context.Background(), addr, logger, clock, nil)
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

func TestRegisterWithHeartbeatUsesSponsorPrimaryOnly(t *testing.T) {
	addr1, _ := setupTestRegistry(t)
	addr2, _ := setupTestRegistry(t)

	component := api.Component_COMPONENT_QUEUE
	instanceID := "queue-1"
	publishAddress := "queue-1:8081"
	metadata := QueueIngressMetadata()
	ordered := splitRegistryAddresses(sponsorOrderedRegistryAddress(addr1+","+addr2, component, instanceID, publishAddress))
	if len(ordered) != 2 {
		t.Fatalf("expected two ordered registry addresses, got %+v", ordered)
	}

	logger := mocks.NewMockLogger()
	stop, err := RegisterWithHeartbeat(context.Background(), RegistrationOptions{
		RegistryAddress: addr1 + "," + addr2,
		Component:       component,
		InstanceID:      instanceID,
		PublishAddress:  publishAddress,
		Metadata:        metadata,
		RefreshInterval: time.Hour,
		Logger:          logger,
		Clock:           mocks.NewMockClock(),
	})

	if err != nil {
		t.Fatalf("register with heartbeat: %v", err)
	}
	defer stop()

	waitForRegistryRegistration(t, ordered[0], component, metadata, instanceID, publishAddress)
	assertRegistryRegistrationAbsent(t, ordered[1], component, metadata, instanceID, publishAddress)
}

func TestRegisterWithHeartbeatSucceedsWhenARegistryTargetFails(t *testing.T) {
	liveAddr, _ := setupTestRegistry(t)

	logger := mocks.NewMockLogger()
	stop, err := RegisterWithHeartbeat(context.Background(), RegistrationOptions{
		RegistryAddress: "127.0.0.1:1," + liveAddr,
		Component:       api.Component_COMPONENT_LOG,
		InstanceID:      "log-1",
		PublishAddress:  "log-1:8083",
		Metadata:        DefaultServiceMetadata(),
		RefreshInterval: time.Hour,
		Logger:          logger,
		Clock:           mocks.NewMockClock(),
	})
	if err != nil {
		t.Fatalf("register with heartbeat: %v", err)
	}
	defer stop()

	waitForRegistryRegistration(t, liveAddr, api.Component_COMPONENT_LOG, DefaultServiceMetadata(), "log-1", "log-1:8083")
}

func TestRegisterWithDynamicMetadataHeartbeatRefreshesMetadata(t *testing.T) {
	addr, _ := setupTestRegistry(t)

	var state atomic.Value
	state.Store(LogWriteStateWritable)

	metadata := func() map[string]string {
		m := DefaultServiceMetadata()
		m[MetadataLogWriteState] = state.Load().(string)
		return m
	}

	logger := mocks.NewMockLogger()
	stop, err := RegisterWithDynamicMetadataHeartbeat(context.Background(), RegistrationOptions{
		RegistryAddress: addr,
		Component:       api.Component_COMPONENT_LOG,
		InstanceID:      "log-1",
		PublishAddress:  "log-1:8083",
		RefreshInterval: 20 * time.Millisecond,
		Logger:          logger,
		Clock:           mocks.NewMockClock(),
	}, metadata)
	if err != nil {
		t.Fatalf("register with heartbeat: %v", err)
	}
	defer stop()

	waitForRegistryRegistration(t, addr, api.Component_COMPONENT_LOG, map[string]string{MetadataLogWriteState: LogWriteStateWritable}, "log-1", "log-1:8083")

	state.Store(LogWriteStateReadOnly)
	waitForRegistryRegistration(t, addr, api.Component_COMPONENT_LOG, map[string]string{MetadataLogWriteState: LogWriteStateReadOnly}, "log-1", "log-1:8083")
}

func assertRegistryRegistrationAbsent(t *testing.T, registryAddr string, component api.Component, metadata map[string]string, wantInstanceID, wantAddress string) {
	t.Helper()

	client := newRegistryTestClient(t, registryAddr)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	got, err := client.ListRegistrations(ctx, component, metadata)
	cancel()
	if err != nil {
		t.Fatalf("list registrations from %s: %v", registryAddr, err)
	}

	for _, entry := range got {
		if entry.GetInstanceId() == wantInstanceID && entry.GetAddress() == wantAddress {
			t.Fatalf("registry %s unexpectedly listed %s/%q at %q", registryAddr, component.String(), wantInstanceID, wantAddress)
		}
	}
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
