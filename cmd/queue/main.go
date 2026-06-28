package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

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

const (
	defaultQueuePool          = "default"
	queuePersistenceDirEnvVar = "VECTIS_QUEUE_PERSISTENCE_DIR"
)

func runVectisQueue(cmd *cobra.Command, args []string) {
	logger := interfaces.NewAsyncLogger("queue")
	defer func() { _ = logger.Close() }()

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
	publishAddr := config.QueueRegistryPublishAddress(addr)
	pool := normalizeQueuePoolName(viper.GetString("pool"))
	instanceID := viper.GetString("instance_id")

	if instanceID == "" {
		instanceID = defaultQueueInstanceID(port)
	}

	persistenceDir := queuePersistenceDir(cmd, pool, instanceID)

	var listenConfig net.ListenConfig
	ln, err := listenConfig.Listen(cmd.Context(), "tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	srvOpts, err := config.GRPCServerOptionsForRole(config.ServiceIdentityRoleQueue)
	if err != nil {
		logger.Fatal("grpc tls: %v", err)
	}

	grpcServer := grpc.NewServer(srvOpts...)
	snapshotEvery := viper.GetInt("persistence_snapshot_every")

	logger.Info("Queue instance %s in pool %s", instanceID, pool)
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
		InstanceID:     instanceID,
	}, queueMetrics)
	if closer, ok := qSvc.(interface{ Close() error }); ok {
		defer cli.DeferShutdown(logger, "Queue persistence", func(context.Context) error {
			return closer.Close()
		})()
	}

	if err := observability.RegisterQueueGauges(func() (int64, int64, int64) {
		return queue.MetricsSnapshot(qSvc)
	}); err != nil {
		logger.Fatal("Failed to register queue metrics: %v", err)
	}

	metricsAddr := config.QueueMetricsListenAddr()
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Queue", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	logger.Info("Queue server listening on %s", addr)

	if config.QueueRegisterWithRegistry() {
		stopRegistration, err := registry.RegisterWithHeartbeat(cmd.Context(), registry.RegistrationOptions{
			RegistryAddress: config.QueueRegistrationRegistryAddress(),
			Component:       api.Component_COMPONENT_QUEUE,
			InstanceID:      instanceID,
			PublishAddress:  publishAddr,
			Metadata:        registry.QueueIngressMetadataForCell(config.CellID()),
			RefreshInterval: config.RegistryRegistrationRefresh(),
			Logger:          logger,
		})

		if err != nil {
			logger.Fatal("Failed to register with registry: %v", err)
		}

		defer stopRegistration()
		logger.Info("Registered with registry service at %s as %s", publishAddr, instanceID)
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

func normalizeQueuePoolName(pool string) string {
	pool = strings.TrimSpace(pool)
	if pool == "" {
		return defaultQueuePool
	}

	return pool
}

func queuePersistenceDir(cmd *cobra.Command, pool, instanceID string) string {
	configured := viper.GetString("persistence_dir")
	if configured != "" || queuePersistenceDirExplicitlySet(cmd) || viper.InConfig("persistence_dir") {
		return configured
	}

	return defaultQueuePersistenceDir(pool, instanceID)
}

func queuePersistenceDirExplicitlySet(cmd *cobra.Command) bool {
	if cmd != nil && cmd.Flags().Changed("persistence-dir") {
		return true
	}

	_, ok := os.LookupEnv(queuePersistenceDirEnvVar)
	return ok
}

func defaultQueueInstanceID(port int) string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}

	return defaultQueueInstanceIDForHost(hostname, port)
}

func defaultQueueInstanceIDForHost(hostname string, port int) string {
	if strings.TrimSpace(hostname) == "" {
		hostname = "localhost"
	}

	host := sanitizeQueuePathComponent(hostname)
	if host == "" {
		host = "localhost"
	}

	if port <= 0 {
		return host
	}

	return fmt.Sprintf("%s-%d", host, port)
}

func defaultQueuePersistenceDir(pool, instanceID string) string {
	return filepath.Join(utils.DataHome(), "vectis", "queue", sanitizeQueuePathComponent(pool), sanitizeQueuePathComponent(instanceID))
}

func sanitizeQueuePathComponent(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))

	var b strings.Builder
	lastDash := false
	for _, r := range value {
		valid := (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') ||
			r == '-' ||
			r == '_' ||
			r == '.'

		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}

		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}

	cleaned := strings.Trim(b.String(), "-.")
	if cleaned == "" || cleaned == "." || cleaned == ".." {
		return "queue"
	}

	return cleaned
}

func init() {
	cli.ConfigureVersion(rootCmd)

	viper.SetDefault("port", config.QueuePort())
	viper.SetDefault("metrics_host", config.QueueMetricsHost())
	viper.SetDefault("metrics_port", config.QueueMetricsPort())
	viper.SetDefault("pool", defaultQueuePool)
	viper.SetDefault("instance_id", "")
	viper.SetDefault("persistence_snapshot_every", 128)

	rootCmd.PersistentFlags().Int("port", config.QueuePort(), "Port for the queue")
	rootCmd.PersistentFlags().String("metrics-host", config.QueueMetricsHost(), "Host/IP for the Prometheus /metrics HTTP server to bind")
	rootCmd.PersistentFlags().Int("metrics-port", config.QueueMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().String("pool", defaultQueuePool, "Queue pool name used for default local persistence grouping")
	rootCmd.PersistentFlags().String("instance-id", "", "Stable queue instance ID for registry and delivery routing")
	rootCmd.PersistentFlags().String("persistence-dir", "", "Directory for queue WAL/snapshot persistence (default: $XDG_DATA_HOME/vectis/queue/<pool>/<instance-id>; empty disables)")
	rootCmd.PersistentFlags().Int("persistence-snapshot-every", 128, "Persisted queue snapshot interval in queue mutations")

	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("metrics_host", rootCmd.PersistentFlags().Lookup("metrics-host"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("pool", rootCmd.PersistentFlags().Lookup("pool"))
	_ = viper.BindPFlag("instance_id", rootCmd.PersistentFlags().Lookup("instance-id"))
	_ = viper.BindPFlag("persistence_dir", rootCmd.PersistentFlags().Lookup("persistence-dir"))
	_ = viper.BindPFlag("persistence_snapshot_every", rootCmd.PersistentFlags().Lookup("persistence-snapshot-every"))
	_ = viper.BindEnv("queue.register_with_registry", "VECTIS_QUEUE_REGISTER_WITH_REGISTRY")
	_ = viper.BindEnv("queue.advertise_address", "VECTIS_QUEUE_ADVERTISE_ADDRESS")

	viper.SetEnvPrefix("VECTIS_QUEUE")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
