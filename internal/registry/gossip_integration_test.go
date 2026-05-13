package registry

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/grpc"
)

type testRegistryNode struct {
	id     string
	addr   string
	svc    *registryServer
	server *grpc.Server
	cancel context.CancelFunc
}

func startTestRegistryCluster(t *testing.T, optsForNode func(i int, addr string, peers []string) ServiceOptions) []*testRegistryNode {
	t.Helper()

	listeners := make([]net.Listener, 3)
	addrs := make([]string, 3)
	for i := range listeners {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen registry node %d: %v", i, err)
		}

		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	nodes := make([]*testRegistryNode, 3)
	for i, ln := range listeners {
		peers := make([]string, 0, len(addrs)-1)
		for j, addr := range addrs {
			if i != j {
				peers = append(peers, addr)
			}
		}

		opts := optsForNode(i, addrs[i], peers)
		if opts.NodeID == "" {
			opts.NodeID = fmt.Sprintf("node-%d", i)
		}

		opts.AdvertiseAddress = addrs[i]
		opts.PeerAddresses = peers

		svc := NewRegistryServiceWithOptions(mocks.NopLogger{}, opts)
		grpcServer := grpc.NewServer()
		api.RegisterRegistryServiceServer(grpcServer, svc)

		ctx, cancel := context.WithCancel(context.Background())
		nodes[i] = &testRegistryNode{
			id:     opts.NodeID,
			addr:   addrs[i],
			svc:    svc,
			server: grpcServer,
			cancel: cancel,
		}

		go func() {
			if err := grpcServer.Serve(ln); err != nil && err != grpc.ErrServerStopped {
				t.Logf("registry test server failed: %v", err)
			}
		}()

		svc.StartCluster(ctx)
	}

	t.Cleanup(func() {
		for _, node := range nodes {
			node.cancel()
			node.server.Stop()
		}
	})

	return nodes
}

func TestRegistryCluster_GossipConvergesOverGRPC(t *testing.T) {
	nodes := startTestRegistryCluster(t, func(i int, addr string, peers []string) ServiceOptions {
		return ServiceOptions{
			GossipInterval:      20 * time.Millisecond,
			AntiEntropyInterval: time.Hour,
			LeaseTTL:            time.Minute,
			TombstoneTTL:        5 * time.Minute,
			PeerDialTimeout:     250 * time.Millisecond,
		}
	})

	client := newRegistryTestClient(t, nodes[0].addr)
	defer client.Close()

	if err := client.Register(context.Background(), api.Component_COMPONENT_QUEUE, "queue.gossip:8081"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for _, node := range nodes {
		waitForRegistryAddress(t, node.addr, api.Component_COMPONENT_QUEUE, "", "queue.gossip:8081")
	}
}

func TestRegistryCluster_GossipConvergesMetadataOverGRPC(t *testing.T) {
	nodes := startTestRegistryCluster(t, func(i int, addr string, peers []string) ServiceOptions {
		return ServiceOptions{
			GossipInterval:      20 * time.Millisecond,
			AntiEntropyInterval: time.Hour,
			LeaseTTL:            time.Minute,
			TombstoneTTL:        5 * time.Minute,
			PeerDialTimeout:     250 * time.Millisecond,
		}
	})

	client := newRegistryTestClient(t, nodes[0].addr)
	defer client.Close()

	metadata := map[string]string{
		MetadataCellID:    DefaultCellID,
		MetadataQueueRole: QueueRolePool,
		"pool":            "linux",
		"trait.os":        "linux",
	}

	if err := client.RegisterInstanceWithMetadata(context.Background(), api.Component_COMPONENT_QUEUE, "pool-linux", "queue.pool:8081", metadata); err != nil {
		t.Fatalf("RegisterInstanceWithMetadata: %v", err)
	}

	for _, node := range nodes {
		waitForRegistryRegistration(t, node.addr, api.Component_COMPONENT_QUEUE, map[string]string{MetadataQueueRole: QueueRolePool, "trait.os": "linux"}, "pool-linux", "queue.pool:8081")
	}
}

func TestRegistryCluster_AntiEntropyRepairsMissedGossipOverGRPC(t *testing.T) {
	nodes := startTestRegistryCluster(t, func(i int, addr string, peers []string) ServiceOptions {
		return ServiceOptions{
			GossipInterval:      time.Hour,
			AntiEntropyInterval: 20 * time.Millisecond,
			LeaseTTL:            time.Minute,
			TombstoneTTL:        5 * time.Minute,
			PeerDialTimeout:     250 * time.Millisecond,
		}
	})

	client := newRegistryTestClient(t, nodes[0].addr)
	defer client.Close()

	if err := client.Register(context.Background(), api.Component_COMPONENT_LOG, "log.snapshot:8083"); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for _, node := range nodes {
		waitForRegistryAddress(t, node.addr, api.Component_COMPONENT_LOG, "", "log.snapshot:8083")
	}
}

func TestRegistryClient_FailsOverAfterActiveRegistryDies(t *testing.T) {
	nodes := startTestRegistryCluster(t, func(i int, addr string, peers []string) ServiceOptions {
		return ServiceOptions{
			GossipInterval:      time.Hour,
			AntiEntropyInterval: time.Hour,
			LeaseTTL:            time.Minute,
			TombstoneTTL:        5 * time.Minute,
			PeerDialTimeout:     250 * time.Millisecond,
		}
	})

	client := newRegistryTestClient(t, nodes[0].addr+","+nodes[1].addr)
	defer client.Close()

	nodes[0].cancel()
	nodes[0].server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Register(ctx, api.Component_COMPONENT_QUEUE, "queue.failover:8081"); err != nil {
		t.Fatalf("Register after active registry death: %v", err)
	}

	waitForRegistryAddress(t, nodes[1].addr, api.Component_COMPONENT_QUEUE, "", "queue.failover:8081")
}

func newRegistryTestClient(t *testing.T, addr string) *Registry {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := New(ctx, addr, mocks.NopLogger{}, mocks.NewMockClock(), nil)
	if err != nil {
		t.Fatalf("new registry client %s: %v", addr, err)
	}

	return client
}

func waitForRegistryAddress(t *testing.T, registryAddr string, component api.Component, instanceID, want string) {
	t.Helper()

	client := newRegistryTestClient(t, registryAddr)
	defer client.Close()

	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	var got string
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		if instanceID == "" {
			got, lastErr = client.Address(ctx, component)
		} else {
			got, lastErr = client.InstanceAddress(ctx, component, instanceID)
		}

		cancel()

		if lastErr == nil && got == want {
			return
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("registry %s never resolved %s/%q to %q; last got %q err %v", registryAddr, component.String(), instanceID, want, got, lastErr)
}

func waitForRegistryRegistration(t *testing.T, registryAddr string, component api.Component, metadata map[string]string, wantInstanceID, wantAddress string) {
	t.Helper()

	client := newRegistryTestClient(t, registryAddr)
	defer client.Close()

	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	var got []*api.RegistryEntry
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		got, lastErr = client.ListRegistrations(ctx, component, metadata)
		cancel()

		if lastErr == nil {
			for _, entry := range got {
				if entry.GetInstanceId() == wantInstanceID && entry.GetAddress() == wantAddress {
					return
				}
			}
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("registry %s never listed %s/%q at %q; last got %+v err %v", registryAddr, component.String(), wantInstanceID, wantAddress, got, lastErr)
}
