package main

import (
	"net"
	"os"

	api "vectis/api/gen/go"
	"vectis/internal/log"
	"vectis/internal/server"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func runVectisRegistry(cmd *cobra.Command, args []string) {
	logger := log.New("registry")
	logger.Info("Starting registry server...")

	ln, err := net.Listen("tcp", server.RegistryPort)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	registry := server.NewRegistryService(logger)
	grpcServer := grpc.NewServer()
	api.RegisterRegistryServiceServer(grpcServer, registry)

	logger.Info("Registry server listening on %s", server.RegistryPort)
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

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
