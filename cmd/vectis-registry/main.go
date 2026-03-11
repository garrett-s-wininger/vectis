package main

import (
	"fmt"
	"net"
	"os"

	api "vectis/api/gen/go"
	"vectis/internal/server"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func runVectisRegistry(cmd *cobra.Command, args []string) {
	fmt.Println("Starting registry server...")

	ln, err := net.Listen("tcp", server.RegistryPort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
		os.Exit(1)
	}

	registry := server.NewRegistryService()
	grpcServer := grpc.NewServer()
	api.RegisterRegistryServiceServer(grpcServer, registry)

	fmt.Printf("Registry server listening on %s\n", server.RegistryPort)
	if err := grpcServer.Serve(ln); err != nil {
		fmt.Fprintf(os.Stderr, "gRPC server failed: %v\n", err)
		os.Exit(1)
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
