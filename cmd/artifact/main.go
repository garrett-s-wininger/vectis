package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	s3artifact "vectis/extensions/artifacts/s3"
	"vectis/internal/artifact"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/utils"
)

func runArtifact(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	logger := interfaces.NewAsyncLogger("artifact")
	defer func() { _ = logger.Close() }()

	cli.SetLogLevel(logger)
	logger.Info("Starting artifact service...")

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("%v", err)
	}
	config.StartMetricsTLSReloadLoop(ctx)

	instanceID := viper.GetString("instance_id")
	if instanceID == "" {
		instanceID = artifact.DefaultInstanceID(config.ArtifactGRPCListenAddr())
	}

	store, closeStore, err := newArtifactStorage(instanceID, logger)
	if err != nil {
		logger.Fatal("Failed to initialize artifact storage: %v", err)
	}
	defer closeStore()

	logger.Info("Artifact instance ID: %s", instanceID)

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(ctx, "vectis-artifact")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}
	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	if err := observability.RegisterArtifactStorageMetrics(store); err != nil {
		logger.Fatal("Failed to register artifact storage metrics: %v", err)
	}

	metricsAddr := config.ArtifactMetricsListenAddr()
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Artifact", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	if err := artifact.RunWithOptions(ctx, logger, store, artifact.RunOptions{InstanceID: instanceID}); err != nil {
		logger.Fatal("Artifact service failed: %v", err)
	}
}

type artifactStorage interface {
	artifact.Store
	StorageStats(context.Context) (artifact.StorageStats, error)
}

func newArtifactStorage(instanceID string, logger interfaces.Logger) (artifactStorage, func(), error) {
	backend := strings.ToLower(strings.TrimSpace(config.ArtifactStorageBackend()))
	if backend == "" {
		backend = "local"
	}

	switch backend {
	case "local":
		storageDir := viper.GetString("storage_dir")
		if storageDir == "" {
			storageDir = filepath.Join(utils.DataHome(), "vectis", "artifact", instanceID)
		}

		readOnlyMinFreeBytes := config.ArtifactStorageReadOnlyMinFreeBytes()
		store, err := artifact.NewLocalStoreWithOptions(storageDir, artifact.LocalStoreOptions{
			NewBlobMinFreeBytes: readOnlyMinFreeBytes,
		})
		if err != nil {
			return nil, nil, err
		}

		logger.Info("Using local artifact storage directory: %s", storageDir)
		logger.Info("Artifact storage new-blob read-only threshold: %d free bytes", readOnlyMinFreeBytes)
		return store, func() {
			if err := store.Close(); err != nil {
				logger.Warn("Failed to close artifact storage: %v", err)
			}
		}, nil

	case "s3":
		cfg := s3artifact.ConfigFromViper(viper.GetViper())
		store, err := cfg.NewStore()
		if err != nil {
			return nil, nil, err
		}

		logger.Info("Using S3-compatible artifact storage endpoint=%s bucket=%s prefix=%s path_style=%t", cfg.Endpoint, cfg.Bucket, cfg.Prefix, cfg.PathStyle)
		return store, func() {}, nil

	default:
		return nil, nil, fmt.Errorf("artifact.storage_backend %q is invalid; must be local or s3", backend)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-artifact",
	Short: "Vectis artifact service",
	Run:   runArtifact,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	viper.SetDefault("storage_backend", config.ArtifactStorageBackend())
	viper.SetDefault("storage_dir", "")
	viper.SetDefault("instance_id", "")
	viper.SetDefault("grpc_port", config.ArtifactGRPCPort())
	viper.SetDefault("metrics_host", config.ArtifactMetricsHost())
	viper.SetDefault("metrics_port", config.ArtifactMetricsPort())
	viper.SetDefault("storage_read_only_min_free_bytes", config.ArtifactStorageReadOnlyMinFreeBytes())

	rootCmd.PersistentFlags().String("storage-backend", config.ArtifactStorageBackend(), "Artifact storage backend: local or s3")
	rootCmd.PersistentFlags().String("storage-dir", "", "Directory for durable artifact blobs (default: $XDG_DATA_HOME/vectis/artifact/<instance-id>)")
	rootCmd.PersistentFlags().String("instance-id", "", "Stable artifact shard identifier used for registry routing (default: hostname-port)")
	rootCmd.PersistentFlags().Int("grpc-port", config.ArtifactGRPCPort(), "gRPC port for artifact uploads and reads")
	rootCmd.PersistentFlags().String("metrics-host", config.ArtifactMetricsHost(), "Host/IP for the Prometheus /metrics HTTP server to bind")
	rootCmd.PersistentFlags().Int("metrics-port", config.ArtifactMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().Uint64("storage-read-only-min-free-bytes", config.ArtifactStorageReadOnlyMinFreeBytes(), "Minimum free bytes required before accepting new artifact blobs (0 disables)")
	s3artifact.AddConfigFlags(rootCmd.PersistentFlags())

	_ = viper.BindPFlag("storage_backend", rootCmd.PersistentFlags().Lookup("storage-backend"))
	_ = viper.BindPFlag("storage_dir", rootCmd.PersistentFlags().Lookup("storage-dir"))
	_ = viper.BindPFlag("instance_id", rootCmd.PersistentFlags().Lookup("instance-id"))
	_ = viper.BindPFlag("grpc_port", rootCmd.PersistentFlags().Lookup("grpc-port"))
	_ = viper.BindPFlag("metrics_host", rootCmd.PersistentFlags().Lookup("metrics-host"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("storage_read_only_min_free_bytes", rootCmd.PersistentFlags().Lookup("storage-read-only-min-free-bytes"))
	mustBindArtifactProviderConfig(s3artifact.BindConfig(viper.GetViper(), rootCmd.PersistentFlags()))
	_ = viper.BindEnv("storage_backend", "VECTIS_ARTIFACT_STORAGE_BACKEND")
	_ = viper.BindEnv("artifact.storage_backend", "VECTIS_ARTIFACT_STORAGE_BACKEND")
	_ = viper.BindEnv("artifact.grpc.advertise_address", "VECTIS_ARTIFACT_GRPC_ADVERTISE_ADDRESS")
	_ = viper.BindEnv("artifact.grpc.register_with_registry", "VECTIS_ARTIFACT_GRPC_REGISTER_WITH_REGISTRY")

	viper.SetEnvPrefix("VECTIS_ARTIFACT")
	viper.AutomaticEnv()
}

func mustBindArtifactProviderConfig(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
