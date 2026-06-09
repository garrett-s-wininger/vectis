package artifact

import (
	"bytes"
	"context"
	"errors"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/registry"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/grpctest"

	"google.golang.org/grpc"
)

func TestNewPublisherForRun_PinnedAddressPublishesWithPinnedShard(t *testing.T) {
	ctx := context.Background()
	artifactServer, store := startArtifactRouteServer(t)
	repos := dal.NewSQLRepositories(dbtest.NewTestDB(t))
	runID := createPublisherTestRun(t, ctx, repos, "job-artifact-route-pinned")

	publisher, err := NewPublisherForRun(ctx, runID, RoutedPublisherOptions{
		Manifests:     repos.Artifacts(),
		Logger:        mocks.NewMockLogger(),
		PinnedAddress: artifactServer.Addr(),
		PinnedShardID: "artifact-local",
	})
	if err != nil {
		t.Fatalf("new routed publisher: %v", err)
	}
	defer publisher.Close()

	content := []byte("hello pinned artifact")
	got, err := publisher.Publish(ctx, PublishRequest{
		RunID:  runID,
		Name:   "pinned-report",
		Reader: bytes.NewReader(content),
	})

	if err != nil {
		t.Fatalf("publish artifact: %v", err)
	}

	if publisher.Endpoint.ID != "artifact-local" || publisher.Endpoint.Address != artifactServer.Addr() {
		t.Fatalf("unexpected endpoint: %+v", publisher.Endpoint)
	}

	if got.Manifest.ArtifactShardID != "artifact-local" {
		t.Fatalf("artifact shard = %q, want artifact-local", got.Manifest.ArtifactShardID)
	}

	if _, _, err := store.Open(ctx, got.Blob.Key); err != nil {
		t.Fatalf("expected blob in pinned store: %v", err)
	}
}

func TestNewPublisherForRun_UsesWritableRegistryArtifactShard(t *testing.T) {
	ctx := context.Background()
	logger := mocks.NewMockLogger()
	registryServer := startArtifactRouteRegistry(t, logger)
	readonlyServer, readonlyStore := startArtifactRouteServer(t)
	writableServer, writableStore := startArtifactRouteServer(t)
	repos := dal.NewSQLRepositoriesWithCellID(dbtest.NewTestDB(t), "iad-a")
	runID := createPublisherTestRun(t, ctx, repos, "job-artifact-route-registry")

	reg := newArtifactRouteRegistryClient(t, registryServer.Addr(), logger)
	registerArtifactRouteEndpoint(t, reg, "artifact-readonly", readonlyServer.Addr(), "iad-a", false)
	registerArtifactRouteEndpoint(t, reg, "artifact-writable", writableServer.Addr(), "iad-a", true)

	publisher, err := NewPublisherForRun(ctx, runID, RoutedPublisherOptions{
		Manifests:       repos.Artifacts(),
		Logger:          logger,
		RegistryAddress: registryServer.Addr(),
		CellID:          "iad-a",
	})
	if err != nil {
		t.Fatalf("new routed publisher: %v", err)
	}
	defer publisher.Close()

	content := []byte("hello writable artifact")
	got, err := publisher.Publish(ctx, PublishRequest{
		RunID:  runID,
		Name:   "coverage",
		Reader: bytes.NewReader(content),
	})

	if err != nil {
		t.Fatalf("publish artifact: %v", err)
	}

	if publisher.Endpoint.ID != "artifact-writable" {
		t.Fatalf("selected endpoint = %+v, want artifact-writable", publisher.Endpoint)
	}

	if got.Manifest.ArtifactShardID != "artifact-writable" {
		t.Fatalf("artifact shard = %q, want artifact-writable", got.Manifest.ArtifactShardID)
	}

	if _, _, err := writableStore.Open(ctx, got.Blob.Key); err != nil {
		t.Fatalf("expected blob in writable store: %v", err)
	}

	if _, _, err := readonlyStore.Open(ctx, got.Blob.Key); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("expected no blob in read-only store, got %v", err)
	}
}

func TestResolveArtifactPublishEndpoint_FallsBackWhenCellHasNoArtifacts(t *testing.T) {
	ctx := context.Background()
	logger := mocks.NewMockLogger()
	registryServer := startArtifactRouteRegistry(t, logger)
	reg := newArtifactRouteRegistryClient(t, registryServer.Addr(), logger)
	registerArtifactRouteEndpoint(t, reg, "artifact-remote", "artifact-remote:8086", "pdx-b", true)

	got, err := resolveArtifactPublishEndpoint(ctx, "run-cell-fallback", RoutedPublisherOptions{
		Logger:          logger,
		RegistryAddress: registryServer.Addr(),
		CellID:          "iad-a",
	})

	if err != nil {
		t.Fatalf("resolve artifact endpoint: %v", err)
	}

	if got.ID != "artifact-remote" {
		t.Fatalf("fallback endpoint = %+v, want artifact-remote", got)
	}
}

func TestResolveArtifactPublishEndpoint_FallsBackWhenCellArtifactsAreReadOnly(t *testing.T) {
	ctx := context.Background()
	logger := mocks.NewMockLogger()
	registryServer := startArtifactRouteRegistry(t, logger)
	reg := newArtifactRouteRegistryClient(t, registryServer.Addr(), logger)
	registerArtifactRouteEndpoint(t, reg, "artifact-local-readonly", "artifact-local:8086", "iad-a", false)
	registerArtifactRouteEndpoint(t, reg, "artifact-remote-writable", "artifact-remote:8086", "pdx-b", true)

	got, err := resolveArtifactPublishEndpoint(ctx, "run-cell-readonly", RoutedPublisherOptions{
		Logger:          logger,
		RegistryAddress: registryServer.Addr(),
		CellID:          "iad-a",
	})

	if err != nil {
		t.Fatalf("resolve artifact endpoint: %v", err)
	}

	if got.ID != "artifact-remote-writable" {
		t.Fatalf("fallback endpoint = %+v, want artifact-remote-writable", got)
	}
}

func TestResolveArtifactPublishEndpoint_NoWritableArtifact(t *testing.T) {
	ctx := context.Background()
	logger := mocks.NewMockLogger()
	registryServer := startArtifactRouteRegistry(t, logger)
	reg := newArtifactRouteRegistryClient(t, registryServer.Addr(), logger)
	registerArtifactRouteEndpoint(t, reg, "artifact-readonly", "artifact-readonly:8086", "iad-a", false)

	_, err := resolveArtifactPublishEndpoint(ctx, "run-no-writable", RoutedPublisherOptions{
		Logger:          logger,
		RegistryAddress: registryServer.Addr(),
		CellID:          "iad-a",
	})

	if err == nil {
		t.Fatal("expected no writable artifact endpoint error")
	}
}

func TestResolveArtifactPublishEndpoint_RequiresRunID(t *testing.T) {
	_, err := resolveArtifactPublishEndpoint(context.Background(), " ", RoutedPublisherOptions{
		PinnedAddress: "127.0.0.1:8086",
	})

	if err == nil {
		t.Fatal("expected missing run id to fail")
	}
}

func startArtifactRouteServer(t *testing.T) (*grpctest.Server, *LocalStore) {
	t.Helper()

	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	server := grpctest.StartServer(t, func(srv *grpc.Server) {
		api.RegisterArtifactServiceServer(srv, NewServer(store))
	})

	return server, store
}

func startArtifactRouteRegistry(t *testing.T, logger *mocks.MockLogger) *grpctest.Server {
	t.Helper()

	return grpctest.StartServer(t, func(srv *grpc.Server) {
		api.RegisterRegistryServiceServer(srv, registry.NewRegistryService(logger))
	})
}

func newArtifactRouteRegistryClient(t *testing.T, address string, logger *mocks.MockLogger) *registry.Registry {
	t.Helper()

	reg, err := registry.New(context.Background(), address, logger, mocks.NewMockClock(), nil)
	if err != nil {
		t.Fatalf("new registry client: %v", err)
	}
	t.Cleanup(func() { _ = reg.Close() })

	return reg
}

func registerArtifactRouteEndpoint(t *testing.T, reg *registry.Registry, instanceID, address, cellID string, writable bool) {
	t.Helper()

	metadata := registry.DefaultServiceMetadataForCell(cellID)
	if writable {
		metadata[registry.MetadataArtifactWriteState] = registry.ArtifactWriteStateWritable
	} else {
		metadata[registry.MetadataArtifactWriteState] = registry.ArtifactWriteStateReadOnly
	}

	if err := reg.RegisterInstanceWithMetadata(context.Background(), api.Component_COMPONENT_ARTIFACT, instanceID, address, metadata); err != nil {
		t.Fatalf("register artifact endpoint %s: %v", instanceID, err)
	}
}
