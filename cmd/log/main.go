package main

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/logserver"
	"vectis/internal/observability"
	"vectis/internal/platform"
)

func runLog(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

	logger := interfaces.NewAsyncLogger("log-aggregator")
	defer func() { _ = logger.Close() }()

	cli.SetLogLevel(logger)
	logger.Info("Starting log service...")

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("%v", err)
	}
	config.StartMetricsTLSReloadLoop(ctx)

	instanceID := viper.GetString("instance_id")
	if instanceID == "" {
		instanceID = logserver.DefaultInstanceID(config.LogGRPCListenAddr())
	}

	storageDir := viper.GetString("storage_dir")
	if storageDir == "" {
		storageDir = filepath.Join(platform.DataHome(), "vectis", "log", instanceID)
	}

	readOnlyMinFreeBytes := viper.GetUint64("storage_read_only_min_free_bytes")
	store, err := logserver.NewLocalRunLogStoreWithOptions(storageDir, logserver.LocalRunLogStoreOptions{
		NewRunMinFreeBytes: readOnlyMinFreeBytes,
	})

	if err != nil {
		logger.Fatal("Failed to initialize log storage: %v", err)
	}

	defer func() {
		if err := store.Close(); err != nil {
			logger.Warn("Failed to close log storage: %v", err)
		}
	}()

	logger.Info("Log instance ID: %s", instanceID)
	logger.Info("Using durable log storage directory: %s", storageDir)
	logger.Info("Log storage new-run read-only threshold: %d free bytes", readOnlyMinFreeBytes)

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(ctx, "vectis-log")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}

	logMetrics, err := observability.NewLogMetrics()
	if err != nil {
		logger.Fatal("Failed to register log metrics: %v", err)
	}

	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	metricsAddr := config.LogMetricsListenAddr()
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Log", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	if err := logserver.RunWithOptions(ctx, logger, store, logMetrics, logserver.RunOptions{InstanceID: instanceID}); err != nil {
		logger.Fatal("Log service failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-log",
	Short: "Vectis log aggregation service",
	Run:   runLog,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	viper.SetDefault("storage_dir", "")
	viper.SetDefault("instance_id", "")
	viper.SetDefault("grpc_port", config.LogGRPCPort())
	viper.SetDefault("metrics_host", config.LogMetricsHost())
	viper.SetDefault("metrics_port", config.LogMetricsPort())
	viper.SetDefault("max_run_buffers", config.LogMaxRunBuffers())
	viper.SetDefault("storage_read_only_min_free_bytes", config.LogStorageReadOnlyMinFreeBytes())

	rootCmd.PersistentFlags().String("storage-dir", "", "Directory for durable run log files (default: $XDG_DATA_HOME/vectis/log/<instance-id>)")
	rootCmd.PersistentFlags().String("instance-id", "", "Stable log shard identifier used for registry routing (default: hostname-port)")
	rootCmd.PersistentFlags().Int("grpc-port", config.LogGRPCPort(), "gRPC port for log ingest and reads")
	rootCmd.PersistentFlags().String("metrics-host", config.LogMetricsHost(), "Host/IP for the Prometheus /metrics HTTP server to bind")
	rootCmd.PersistentFlags().Int("metrics-port", config.LogMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().Int("max-run-buffers", config.LogMaxRunBuffers(), "Maximum in-memory run log buffers before terminal buffers are evicted")
	rootCmd.PersistentFlags().Uint64("storage-read-only-min-free-bytes", config.LogStorageReadOnlyMinFreeBytes(), "Minimum free bytes required before accepting logs for a new run (0 disables)")
	_ = viper.BindPFlag("storage_dir", rootCmd.PersistentFlags().Lookup("storage-dir"))
	_ = viper.BindPFlag("instance_id", rootCmd.PersistentFlags().Lookup("instance-id"))
	_ = viper.BindPFlag("grpc_port", rootCmd.PersistentFlags().Lookup("grpc-port"))
	_ = viper.BindPFlag("metrics_host", rootCmd.PersistentFlags().Lookup("metrics-host"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("max_run_buffers", rootCmd.PersistentFlags().Lookup("max-run-buffers"))
	_ = viper.BindPFlag("storage_read_only_min_free_bytes", rootCmd.PersistentFlags().Lookup("storage-read-only-min-free-bytes"))
	_ = viper.BindEnv("log.grpc.advertise_address", "VECTIS_LOG_GRPC_ADVERTISE_ADDRESS")

	viper.SetEnvPrefix("VECTIS_LOG")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
