package main

import (
	"fmt"
	"net"
	"os"

	api "vectis/api/gen/go"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/orchestrator"
	"vectis/internal/registry"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func runVectisOrchestrator(cmd *cobra.Command, args []string) {
	logger := interfaces.NewAsyncLogger("orchestrator")
	defer logger.Close()

	cli.SetLogLevel(logger)
	logger.Info("Starting orchestrator server...")

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonOrchestrator); err != nil {
		logger.Fatal("%v", err)
	}

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("%v", err)
	}

	config.StartGRPCTLSReloadLoop(cmd.Context())
	config.StartMetricsTLSReloadLoop(cmd.Context())

	shutdownTracer, err := observability.InitTracer(cmd.Context(), "vectis-orchestrator")
	if err != nil {
		logger.Fatal("Failed to initialize tracer: %v", err)
	}
	defer cli.DeferShutdown(logger, "Tracer", shutdownTracer)()

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(cmd.Context(), "vectis-orchestrator")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}
	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	port := config.OrchestratorEffectiveListenPort()
	addr := fmt.Sprintf(":%d", port)
	publishAddr := config.OrchestratorRegistryPublishAddress(addr)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	srvOpts, err := config.GRPCServerOptionsForRole(config.ServiceIdentityRoleOrchestrator)
	if err != nil {
		logger.Fatal("grpc tls: %v", err)
	}

	service := orchestrator.New(config.OrchestratorShards())
	defer service.Close()

	grpcServer := grpc.NewServer(srvOpts...)
	orchestrator.RegisterOrchestratorService(grpcServer, service, logger)

	metricsPort := config.OrchestratorMetricsEffectiveListenPort()
	metricsAddr := fmt.Sprintf(":%d", metricsPort)
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Orchestrator", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	if config.OrchestratorRegisterWithRegistry() {
		stopRegistration, err := registry.RegisterWithHeartbeat(cmd.Context(), registry.RegistrationOptions{
			RegistryAddress: config.OrchestratorRegistrationRegistryAddress(),
			Component:       api.Component_COMPONENT_ORCHESTRATOR,
			InstanceID:      defaultOrchestratorInstanceID(port),
			PublishAddress:  publishAddr,
			Metadata:        map[string]string{"cell": config.CellID()},
			RefreshInterval: config.RegistryRegistrationRefresh(),
			Logger:          logger,
		})

		if err != nil {
			logger.Fatal("Failed to register with registry: %v", err)
		}

		defer stopRegistration()
		logger.Info("Registered with registry service at %s", publishAddr)
	} else {
		logger.Info("Skipping registry registration (orchestrator.register_with_registry is false)")
	}

	logger.Info("Orchestrator server listening on %s", addr)
	if err := cli.ServeGRPC(cmd.Context(), grpcServer, ln, "Orchestrator", logger); err != nil {
		logger.Error("gRPC server failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-orchestrator",
	Short: "Vectis Orchestrator Service",
	Long:  `The Vectis Orchestrator Service owns hot run state and lease-fenced task execution choreography.`,
	Run:   runVectisOrchestrator,
}

func defaultOrchestratorInstanceID(port int) string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "localhost"
	}

	return fmt.Sprintf("%s-%d", hostname, port)
}

func init() {
	cli.ConfigureVersion(rootCmd)
	viper.SetDefault("port", config.OrchestratorPort())
	viper.SetDefault("metrics_port", config.OrchestratorMetricsPort())
	viper.SetDefault("shards", config.OrchestratorShards())

	rootCmd.PersistentFlags().Int("port", config.OrchestratorPort(), "Port for the orchestrator gRPC service")
	rootCmd.PersistentFlags().Int("metrics-port", config.OrchestratorMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().Int("shards", config.OrchestratorShards(), "Number of run ownership shards; 0 uses GOMAXPROCS")

	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("shards", rootCmd.PersistentFlags().Lookup("shards"))
	_ = viper.BindEnv("orchestrator.register_with_registry", "VECTIS_ORCHESTRATOR_REGISTER_WITH_REGISTRY")
	_ = viper.BindEnv("orchestrator.advertise_address", "VECTIS_ORCHESTRATOR_ADVERTISE_ADDRESS")
	_ = viper.BindEnv("orchestrator.registry.address", "VECTIS_ORCHESTRATOR_REGISTRY_ADDRESS")

	viper.SetEnvPrefix("VECTIS_ORCHESTRATOR")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
