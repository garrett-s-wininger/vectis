package main

import (
	"fmt"
	"net"
	"os"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/queue"
	"vectis/internal/registry"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func runVectisQueue(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("queue")
	logger.Info("Starting queue server...")

	port := config.QueueEffectiveListenPort()
	addr := fmt.Sprintf(":%d", port)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	// TODO(garrett): Move to after queue is running.
	if config.QueueRegisterWithRegistry() {
		regAddr := config.QueueRegistrationRegistryAddress()

		registryClient, err := registry.New(cmd.Context(), regAddr, logger, interfaces.SystemClock{})
		if err != nil {
			logger.Fatal("Failed to connect to registry: %v", err)
		}
		defer registryClient.Close()

		if err := registryClient.Register(cmd.Context(), api.Component_COMPONENT_QUEUE, addr); err != nil {
			logger.Fatal("Failed to register with registry: %v", err)
		}

		logger.Info("Registered with registry service")
	} else {
		logger.Info("Skipping registry registration (queue.register_with_registry is false)")
	}

	grpcServer := grpc.NewServer()
	queue.RegisterQueueService(grpcServer, logger)

	logger.Info("Queue server listening on %s", addr)
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

func init() {
	rootCmd.PersistentFlags().Int("port", config.QueuePort(), "Port for the queue")
	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	viper.SetEnvPrefix("VECTIS_QUEUE")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
