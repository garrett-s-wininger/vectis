package main

import (
	"fmt"
	"net"
	"os"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/registry"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func runVectisRegistry(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("registry")
	logger.Info("Starting registry server...")

	port := config.RegistryEffectiveListenPort()
	addr := fmt.Sprintf(":%d", port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	registrySvc := registry.NewRegistryService(logger)
	grpcServer := grpc.NewServer()
	api.RegisterRegistryServiceServer(grpcServer, registrySvc)

	logger.Info("Registry server listening on %s", addr)

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
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
