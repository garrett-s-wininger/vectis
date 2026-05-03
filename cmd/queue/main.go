package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	api "vectis/api/gen/go"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/queue"
	"vectis/internal/registry"
	"vectis/internal/utils"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func runVectisQueue(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("queue")
	cli.SetLogLevel(logger)
	logger.Info("Starting queue server...")

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonQueue); err != nil {
		logger.Fatal("%v", err)
	}

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("%v", err)
	}

	config.StartGRPCTLSReloadLoop(cmd.Context())
	config.StartMetricsTLSReloadLoop(cmd.Context())

	shutdownTracer, err := observability.InitTracer(cmd.Context(), "vectis-queue")
	if err != nil {
		logger.Fatal("Failed to initialize tracer: %v", err)
	}
	defer cli.DeferShutdown(logger, "Tracer", shutdownTracer)()

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(cmd.Context(), "vectis-queue")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}

	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	port := config.QueueEffectiveListenPort()
	addr := fmt.Sprintf(":%d", port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	srvOpts, err := config.GRPCServerOptions()
	if err != nil {
		logger.Fatal("grpc tls: %v", err)
	}

	grpcServer := grpc.NewServer(srvOpts...)
	persistenceDir := viper.GetString("persistence_dir")
	snapshotEvery := viper.GetInt("persistence_snapshot_every")

	if persistenceDir == "" {
		logger.Info("Queue persistence disabled")
	} else {
		logger.Info("Using queue persistence directory: %s (snapshot every %d mutations)", persistenceDir, snapshotEvery)
	}

	queueMetrics, err := observability.NewQueueMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize queue metrics: %v", err)
	}

	qSvc := queue.RegisterQueueService(grpcServer, logger, queue.QueueOptions{
		PersistenceDir: persistenceDir,
		SnapshotEvery:  snapshotEvery,
	}, queueMetrics)

	if err := observability.RegisterQueueGauges(func() (int64, int64, int64) {
		return queue.MetricsSnapshot(qSvc)
	}); err != nil {
		logger.Fatal("Failed to register queue metrics: %v", err)
	}

	metricsPort := config.QueueMetricsEffectiveListenPort()
	metricsAddr := fmt.Sprintf(":%d", metricsPort)
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Queue", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	logger.Info("Queue server listening on %s", addr)

	if config.QueueRegisterWithRegistry() {
		publishAddr := config.QueueRegistryPublishAddress(addr)
		stopRegistration, err := registry.RegisterWithHeartbeat(cmd.Context(), registry.RegistrationOptions{
			RegistryAddress: config.QueueRegistrationRegistryAddress(),
			Component:       api.Component_COMPONENT_QUEUE,
			PublishAddress:  publishAddr,
			RefreshInterval: config.RegistryRegistrationRefresh(),
			Logger:          logger,
		})

		if err != nil {
			logger.Fatal("Failed to register with registry: %v", err)
		}

		defer stopRegistration()
		logger.Info("Registered with registry service at %s", publishAddr)
	} else {
		logger.Info("Skipping registry registration (queue.register_with_registry is false)")
	}

	if err := cli.ServeGRPC(cmd.Context(), grpcServer, ln, "Queue", logger); err != nil {
		logger.Error("gRPC server failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-queue",
	Short: "Vectis Queue Service",
	Long:  `The Vectis Queue Service is responsible for receiving and processing jobs from the Vectis API.`,
	Run:   runVectisQueue,
}

func init() {
	defaultPersistenceDir := filepath.Join(utils.DataHome(), "vectis", "queue")

	viper.SetDefault("port", config.QueuePort())
	viper.SetDefault("metrics_port", config.QueueMetricsPort())
	viper.SetDefault("persistence_dir", defaultPersistenceDir)
	viper.SetDefault("persistence_snapshot_every", 128)

	rootCmd.PersistentFlags().Int("port", config.QueuePort(), "Port for the queue")
	rootCmd.PersistentFlags().Int("metrics-port", config.QueueMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().String("persistence-dir", defaultPersistenceDir, "Directory for queue WAL/snapshot persistence")
	rootCmd.PersistentFlags().Int("persistence-snapshot-every", 128, "Persisted queue snapshot interval in queue mutations")

	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("persistence_dir", rootCmd.PersistentFlags().Lookup("persistence-dir"))
	_ = viper.BindPFlag("persistence_snapshot_every", rootCmd.PersistentFlags().Lookup("persistence-snapshot-every"))

	viper.SetEnvPrefix("VECTIS_QUEUE")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
