package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

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
	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracer(shutCtx); err != nil {
			logger.Warn("Tracer shutdown: %v", err)
		}
	}()

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(cmd.Context(), "vectis-queue")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}

	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := shutdownMetrics(shutCtx); err != nil {
			logger.Warn("Metrics shutdown: %v", err)
		}
	}()

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
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", metricsHandler)
	metricsSrv := &http.Server{
		Addr:    metricsAddr,
		Handler: metricsMux,
	}

	metricsLn, err := net.Listen("tcp", metricsAddr)
	if err != nil {
		logger.Fatal("Failed to listen for metrics: %v", err)
	}

	metricsLn, err = config.MetricsHTTPSListener(metricsLn)
	if err != nil {
		logger.Fatal("metrics tls: %v", err)
	}

	go func() {
		if err := metricsSrv.Serve(metricsLn); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server: %v", err)
		}
	}()

	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsSrv.Shutdown(shutCtx); err != nil {
			logger.Warn("Metrics HTTP shutdown: %v", err)
		}
	}()

	logger.Info("Queue server listening on %s", addr)
	if !config.MetricsTLSInsecure() {
		logger.Info("Queue metrics listening on %s (HTTPS /metrics)", metricsAddr)
	} else {
		logger.Info("Queue metrics listening on %s (/metrics)", metricsAddr)
	}

	if config.QueueRegisterWithRegistry() {
		regAddr := config.QueueRegistrationRegistryAddress()

		registryClient, err := registry.New(cmd.Context(), regAddr, logger, interfaces.SystemClock{})
		if err != nil {
			logger.Fatal("Failed to connect to registry: %v", err)
		}
		defer registryClient.Close()

		publishAddr := config.QueueRegistryPublishAddress(addr)
		if err := registryClient.Register(cmd.Context(), api.Component_COMPONENT_QUEUE, publishAddr); err != nil {
			logger.Fatal("Failed to register with registry: %v", err)
		}

		stopHeartbeat := registry.StartRegistrationHeartbeat(
			cmd.Context(), registryClient, api.Component_COMPONENT_QUEUE, publishAddr,
			config.RegistryRegistrationRefresh(), logger,
		)
		defer stopHeartbeat()

		logger.Info("Registered with registry service at %s", publishAddr)
	} else {
		logger.Info("Skipping registry registration (queue.register_with_registry is false)")
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(ln)
	}()

	select {
	case <-cmd.Context().Done():
		logger.Info("Shutting down gRPC server...")
		grpcServer.GracefulStop()
		logger.Info("gRPC server stopped")
	case err := <-serveErr:
		if err != nil {
			logger.Error("gRPC server failed: %v", err)
		}
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
