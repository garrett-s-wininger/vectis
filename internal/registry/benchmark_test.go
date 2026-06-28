package registry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const benchmarkRegistryBufSize = 16 * 1024 * 1024

func BenchmarkRegistryStore_RenewHeartbeat(b *testing.B) {
	for _, entries := range benchmarkRegistryFleetSizes() {
		b.Run(benchmarkRegistryFleetName(entries), func(b *testing.B) {
			reg := newBenchmarkRegistryStore(entries)
			now := time.Now()
			metadata := benchmarkRegistryWorkerMetadata(0)

			b.ReportAllocs()
			b.ReportMetric(float64(entries), "registrations")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reg.registerWithChange(
					api.Component_COMPONENT_WORKER,
					"worker-000000",
					"10.0.0.1:50051",
					metadata,
					now.Add(time.Duration(i)*time.Nanosecond),
				)
			}
		})
	}
}

func BenchmarkRegistryStore_GetInstance(b *testing.B) {
	for _, entries := range benchmarkRegistryFleetSizes() {
		b.Run(benchmarkRegistryFleetName(entries), func(b *testing.B) {
			reg := newBenchmarkRegistryStore(entries)
			now := time.Now()
			instanceID := fmt.Sprintf("worker-%06d", max(entries/2, 0))

			b.ReportAllocs()
			b.ReportMetric(float64(entries), "registrations")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := reg.get(api.Component_COMPONENT_WORKER, instanceID, now); !ok {
					b.Fatalf("missing registry instance %s", instanceID)
				}
			}
		})
	}
}

func BenchmarkRegistryStore_ListByComponent(b *testing.B) {
	for _, entries := range benchmarkRegistryFleetSizes() {
		b.Run(benchmarkRegistryFleetName(entries), func(b *testing.B) {
			reg := newBenchmarkRegistryStore(entries)
			now := time.Now()

			b.ReportAllocs()
			b.ReportMetric(float64(entries), "registrations")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got := reg.listEntries(api.Component_COMPONENT_WORKER, nil, now)
				if len(got) != entries {
					b.Fatalf("listed %d entries, want %d", len(got), entries)
				}
			}
		})
	}
}

func BenchmarkRegistryStore_ListByMetadata(b *testing.B) {
	for _, entries := range benchmarkRegistryFleetSizes() {
		b.Run(benchmarkRegistryFleetName(entries), func(b *testing.B) {
			reg := newBenchmarkRegistryStore(entries)
			now := time.Now()
			filter := map[string]string{MetadataWorkerExecutionBackend: "host"}
			want := (entries + 1) / 2

			b.ReportAllocs()
			b.ReportMetric(float64(entries), "registrations")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got := reg.listEntries(api.Component_COMPONENT_WORKER, filter, now)
				if len(got) != want {
					b.Fatalf("listed %d entries, want %d", len(got), want)
				}
			}
		})
	}
}

func BenchmarkRegistryStore_Snapshot(b *testing.B) {
	for _, entries := range benchmarkRegistryFleetSizes() {
		b.Run(benchmarkRegistryFleetName(entries), func(b *testing.B) {
			reg := newBenchmarkRegistryStore(entries)
			now := time.Now()

			b.ReportAllocs()
			b.ReportMetric(float64(entries), "registrations")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got := reg.snapshotProtoEntries(now)
				if len(got) != entries {
					b.Fatalf("snapshot has %d entries, want %d", len(got), entries)
				}
			}
		})
	}
}

func BenchmarkRegistryStore_EntriesNewerThanKnownDigests(b *testing.B) {
	for _, entries := range benchmarkRegistryFleetSizes() {
		b.Run(benchmarkRegistryFleetName(entries), func(b *testing.B) {
			reg := newBenchmarkRegistryStore(entries)
			now := time.Now()
			digests := reg.digestProtoEntries(now)

			b.ReportAllocs()
			b.ReportMetric(float64(entries), "registrations")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got := reg.entriesNewerThanDigests(digests, now)
				if len(got) != 0 {
					b.Fatalf("snapshot delta has %d entries, want 0", len(got))
				}
			}
		})
	}
}

func BenchmarkRegistryGRPC_RenewHeartbeat(b *testing.B) {
	for _, entries := range []int{100, 1000, 5000} {
		b.Run(benchmarkRegistryFleetName(entries), func(b *testing.B) {
			svc := newBenchmarkRegistryService(entries)
			client := newBenchmarkRegistryGRPCClient(b, svc)
			ctx := context.Background()
			component := api.Component_COMPONENT_WORKER
			instanceID := "worker-000000"
			address := "10.0.0.1:50051"
			metadata := benchmarkRegistryWorkerMetadata(0)

			b.ReportAllocs()
			b.ReportMetric(float64(entries), "registrations")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := client.Register(ctx, &api.Registration{
					Component:  &component,
					InstanceId: &instanceID,
					Address:    &address,
					Metadata:   metadata,
				}); err != nil {
					b.Fatalf("renew registry heartbeat: %v", err)
				}
			}
		})
	}
}

func BenchmarkRegistryGRPC_ListRegistrations(b *testing.B) {
	for _, entries := range []int{100, 1000, 5000} {
		b.Run(benchmarkRegistryFleetName(entries), func(b *testing.B) {
			svc := newBenchmarkRegistryService(entries)
			client := newBenchmarkRegistryGRPCClient(b, svc)
			ctx := context.Background()
			component := api.Component_COMPONENT_WORKER
			req := &api.ListRegistrationsRequest{Component: &component}

			b.ReportAllocs()
			b.ReportMetric(float64(entries), "registrations")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := client.ListRegistrations(ctx, req)
				if err != nil {
					b.Fatalf("list registry registrations: %v", err)
				}
				if len(resp.GetEntries()) != entries {
					b.Fatalf("listed %d entries, want %d", len(resp.GetEntries()), entries)
				}
			}
		})
	}
}

func benchmarkRegistryFleetSizes() []int {
	return []int{100, 1000, 5000}
}

func benchmarkRegistryFleetName(entries int) string {
	return fmt.Sprintf("registrations_%05d", entries)
}

func newBenchmarkRegistryService(entries int) *registryServer {
	svc := NewRegistryServiceWithOptions(mocks.NopLogger{}, ServiceOptions{
		NodeID:       "bench-registry",
		LeaseTTL:     time.Hour,
		TombstoneTTL: time.Hour,
	})
	seedBenchmarkRegistryStore(svc.reg, entries)
	return svc
}

func newBenchmarkRegistryStore(entries int) *reg {
	reg := newReg("bench-registry", time.Hour, time.Hour)
	seedBenchmarkRegistryStore(reg, entries)
	return reg
}

func seedBenchmarkRegistryStore(reg *reg, entries int) {
	now := time.Now()
	for i := 0; i < entries; i++ {
		reg.register(
			api.Component_COMPONENT_WORKER,
			fmt.Sprintf("worker-%06d", i),
			fmt.Sprintf("10.%d.%d.%d:50051", (i/65536)%256, (i/256)%256, i%256),
			benchmarkRegistryWorkerMetadata(i),
			now,
		)
	}
}

func benchmarkRegistryWorkerMetadata(i int) map[string]string {
	backend := "host"
	if i%2 == 1 {
		backend = "container"
	}

	return map[string]string{
		MetadataCellID:                     DefaultCellID,
		MetadataWorkerExecutionBackend:     backend,
		MetadataWorkerDefaultIsolation:     backend,
		MetadataWorkerSupportedIsolation:   "host,container",
		"trait.os":                         "linux",
		"trait.arch":                       "arm64",
		"benchmark.registry.worker.parity": fmt.Sprintf("%d", i%2),
	}
}

func newBenchmarkRegistryGRPCClient(b *testing.B, svc api.RegistryServiceServer) api.RegistryServiceClient {
	b.Helper()

	lis := bufconn.Listen(benchmarkRegistryBufSize)
	srv := grpc.NewServer()
	api.RegisterRegistryServiceServer(srv, svc)

	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			b.Logf("registry benchmark gRPC server error: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough:///registry-bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	if err != nil {
		srv.Stop()
		_ = lis.Close()
		b.Fatalf("new registry benchmark client: %v", err)
	}

	b.Cleanup(func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
	})

	return api.NewRegistryServiceClient(conn)
}
