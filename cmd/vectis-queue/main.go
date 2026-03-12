package main

import (
	"net"
	"os"

	api "vectis/api/gen/go"
	"vectis/internal/log"
	"vectis/internal/registry"
	"vectis/internal/server"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func runVectisQueue(cmd *cobra.Command, args []string) {
	logger := log.New("queue")
	logger.Info("Starting queue server...")

	ln, err := net.Listen("tcp", server.QueuePort)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	// TODO(garrett): Move to after queue is running.
	registryClient, err := registry.New(cmd.Context(), logger)
	if err != nil {
		logger.Fatal("Failed to connect to registry: %v", err)
	}

	defer registryClient.Close()

	if err := registryClient.Register(cmd.Context(), api.Component_COMPONENT_QUEUE, server.QueuePort); err != nil {
		logger.Fatal("Failed to register with registry: %v", err)
	}

	logger.Info("Registered with registry service")
	grpcServer := grpc.NewServer()
	server.RegisterQueueService(grpcServer, logger)

	logger.Info("Queue server listening on %s", server.QueuePort)
	if err := grpcServer.Serve(ln); err != nil {
		logger.Fatal("gRPC server failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-queue",
	Short: "Vectis Queue Service",
	Long:  `The Vectis Queue Service is responsible for receiving and processing jobs from the Vectis API.`,
	Run:   runVectisQueue,
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
