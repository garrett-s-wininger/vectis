package artifact

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/registry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type RunOptions struct {
	InstanceID string
}

type newBlobWritableReporter interface {
	NewBlobWritable() bool
}

func DefaultInstanceID(bindAddr string) string {
	hostname, err := os.Hostname()
	if err == nil {
		hostname = strings.TrimSpace(hostname)
	}

	if hostname == "" {
		hostname = "artifact"
	}

	_, port, err := net.SplitHostPort(bindAddr)
	if err != nil || port == "" {
		return hostname
	}

	return hostname + "-" + port
}

func (s *Server) RunGRPC(ctx context.Context, bindAddr string) error {
	lis, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return err
	}

	srvOpts, err := config.GRPCServerOptionsForRole(config.ServiceIdentityRoleArtifact)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(srvOpts...)
	api.RegisterArtifactServiceServer(grpcServer, s)

	hs := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, hs)
	hs.SetServingStatus("artifact", healthpb.HealthCheckResponse_SERVING)

	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	return grpcServer.Serve(lis)
}

func Run(ctx context.Context, logger interfaces.Logger, store Store) error {
	return RunWithOptions(ctx, logger, store, RunOptions{})
}

func RunWithOptions(ctx context.Context, logger interfaces.Logger, store Store, opts RunOptions) error {
	if err := validateStore(store); err != nil {
		return err
	}

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonArtifact); err != nil {
		return err
	}
	config.StartGRPCTLSReloadLoop(ctx)

	server := NewServer(store)
	bindGRPC := config.ArtifactGRPCListenAddr()

	if config.ArtifactRegisterWithRegistry() {
		instanceID := opts.InstanceID
		if instanceID == "" {
			instanceID = DefaultInstanceID(bindGRPC)
		}

		publishAddr := config.ArtifactGRPCRegistryPublishAddress(bindGRPC)
		stopRegistration, err := registerArtifactWithHeartbeat(ctx, config.ArtifactRegistrationRegistryAddress(), instanceID, publishAddr, store, logger)
		if err != nil {
			return err
		}

		defer stopRegistration()
		logger.Info("Registered artifact service %s with registry at %s", instanceID, publishAddr)
	} else {
		logger.Info("Skipping registry registration (artifact.grpc.register_with_registry is false)")
	}

	logger.Info("gRPC artifact server listening on %s", bindGRPC)
	return server.RunGRPC(ctx, bindGRPC)
}

func registerArtifactWithHeartbeat(ctx context.Context, registryAddress, instanceID, publishAddress string, store Store, logger interfaces.Logger) (func(), error) {
	interval := config.RegistryRegistrationRefresh()
	if interval <= 0 {
		interval = 45 * time.Second
	}

	return registry.RegisterWithDynamicMetadataHeartbeat(ctx, registry.RegistrationOptions{
		RegistryAddress: registryAddress,
		Component:       api.Component_COMPONENT_ARTIFACT,
		InstanceID:      instanceID,
		PublishAddress:  publishAddress,
		RefreshInterval: interval,
		Logger:          logger,
	}, func() map[string]string {
		return artifactServiceMetadata(store)
	})
}

func artifactServiceMetadata(store Store) map[string]string {
	metadata := registry.DefaultServiceMetadataForCell(config.CellID())
	metadata[registry.MetadataArtifactWriteState] = registry.ArtifactWriteStateWritable

	if reporter, ok := store.(newBlobWritableReporter); ok && !reporter.NewBlobWritable() {
		metadata[registry.MetadataArtifactWriteState] = registry.ArtifactWriteStateReadOnly
	}

	return metadata
}

func validateStore(store Store) error {
	if store == nil {
		return fmt.Errorf("artifact store is required")
	}
	return nil
}
