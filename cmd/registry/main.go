package main

import (
	"fmt"
	"net"
	"os"

	api "vectis/api/gen/go"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/registry"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func runVectisRegistry(cmd *cobra.Command, args []string) {
	logger := interfaces.NewAsyncLogger("registry")
	defer logger.Close()

	cli.SetLogLevel(logger)
	logger.Info("Starting registry server...")

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonRegistry); err != nil {
		logger.Fatal("%v", err)
	}
	config.StartGRPCTLSReloadLoop(cmd.Context())

	port := config.RegistryEffectiveListenPort()
	addr := fmt.Sprintf(":%d", port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	registrySvc := registry.NewRegistryService(logger)
	srvOpts, err := config.GRPCServerOptions()
	if err != nil {
		logger.Fatal("grpc tls: %v", err)
	}

	grpcServer := grpc.NewServer(srvOpts...)
	api.RegisterRegistryServiceServer(grpcServer, registrySvc)

	hs := health.NewServer()
	healthgrpc.RegisterHealthServer(grpcServer, hs)
	hs.SetServingStatus("registry", healthpb.HealthCheckResponse_SERVING)

	logger.Info("Registry server listening on %s", addr)

	if err := cli.ServeGRPC(cmd.Context(), grpcServer, ln, "Registry", logger); err != nil {
		logger.Error("gRPC server failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-registry",
	Short: "Vectis Registry Service",
	Long:  `The Vectis Registry Service is responsible for discovering and registering build system components.`,
	Run:   runVectisRegistry,
}

func init() {
	rootCmd.PersistentFlags().Int("port", config.RegistryPort(), "Port for the registry")
	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	viper.SetEnvPrefix("VECTIS_REGISTRY")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
