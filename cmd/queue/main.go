package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/database"
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

	grpcServer := grpc.NewServer()
	persistenceDir := viper.GetString("persistence_dir")
	snapshotEvery := viper.GetInt("persistence_snapshot_every")

	if persistenceDir == "" {
		logger.Info("Queue persistence disabled")
	} else {
		logger.Info("Using queue persistence directory: %s (snapshot every %d mutations)", persistenceDir, snapshotEvery)
	}

	queueService, err := queue.NewQueueServiceWithOptions(logger, queue.QueueOptions{
		PersistenceDir: persistenceDir,
		SnapshotEvery:  snapshotEvery,
	})

	if err != nil {
		logger.Fatal("Failed to initialize queue persistence: %v", err)
	}

	api.RegisterQueueServiceServer(grpcServer, queueService)

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
	defaultPersistenceDir := filepath.Join(database.DataHome(), "vectis", "queue")

	viper.SetDefault("port", config.QueuePort())
	viper.SetDefault("persistence_dir", defaultPersistenceDir)
	viper.SetDefault("persistence_snapshot_every", 128)

	rootCmd.PersistentFlags().Int("port", config.QueuePort(), "Port for the queue")
	rootCmd.PersistentFlags().String("persistence-dir", defaultPersistenceDir, "Directory for queue WAL/snapshot persistence")
	rootCmd.PersistentFlags().Int("persistence-snapshot-every", 128, "Persisted queue snapshot interval in queue mutations")

	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("persistence_dir", rootCmd.PersistentFlags().Lookup("persistence-dir"))
	_ = viper.BindPFlag("persistence_snapshot_every", rootCmd.PersistentFlags().Lookup("persistence-snapshot-every"))

	viper.SetEnvPrefix("VECTIS_QUEUE")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
