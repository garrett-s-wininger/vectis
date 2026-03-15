package main

import (
	"fmt"
	"net"
	"os"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/registry"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func runVectisRegistry(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("registry")
	logger.Info("Starting registry server...")

	port := viper.GetInt("port")
	if port <= 0 {
		port = 8082
	}
	addr := fmt.Sprintf(":%d", port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	registrySvc := registry.NewRegistryService(logger)
	grpcServer := grpc.NewServer()
	api.RegisterRegistryServiceServer(grpcServer, registrySvc)

	logger.Info("Registry server listening on %s", addr)
	if err := grpcServer.Serve(ln); err != nil {
		logger.Fatal("gRPC server failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-registry",
	Short: "Vectis Registry Service",
	Long:  `The Vectis Registry Service is responsible for discovering and registering build system components.`,
	Run:   runVectisRegistry,
}

func init() {
	viper.SetDefault("port", 8082)
	rootCmd.PersistentFlags().Int("port", 8082, "Port for the registry")
	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	viper.SetEnvPrefix("VECTIS_REGISTRY")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
