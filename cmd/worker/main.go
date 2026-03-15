package main

import (
	"context"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	api "vectis/api/gen/go"
	"vectis/internal/job"
	"vectis/internal/log"
	"vectis/internal/registry"
)

func runWorker(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	logger := log.New("worker")

	logger.Info("Connecting to registry...")
	registryClient, err := registry.New(ctx, logger)
	if err != nil {
		logger.Fatal("Failed to connect to registry: %v", err)
	}
	defer registryClient.Close()

	logger.Info("Getting queue service address from registry...")
	queueAddr, err := registryClient.Address(ctx, api.Component_COMPONENT_QUEUE)
	if err != nil {
		logger.Fatal("Failed to get queue address: %v", err)
	}

	logger.Info("Connecting to queue at %s...", queueAddr)
	queueConn, err := grpc.NewClient(queueAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("Failed to connect to queue: %v", err)
	}
	defer queueConn.Close()
	queueClient := api.NewQueueServiceClient(queueConn)

	logger.Info("Getting log service address from registry...")
	logAddr, err := registryClient.Address(ctx, api.Component_COMPONENT_LOG)
	if err != nil {
		logger.Fatal("Failed to get log service address: %v", err)
	}

	logger.Info("Connecting to log service at %s...", logAddr)
	logConn, err := grpc.NewClient(logAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("Failed to connect to log service: %v", err)
	}
	defer logConn.Close()

	logClient := api.NewLogServiceClient(logConn)
	executor := job.NewExecutor()

	for {
		logger.Debug("Initiating long poll from queue...")
		pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		job, err := queueClient.Dequeue(pollCtx, &api.Empty{})
		cancel()

		if err != nil {
			st, ok := status.FromError(err)
			switch {
			case ok && st.Code() == codes.DeadlineExceeded:
				logger.Debug("Long poll timed out. Retrying...")
				continue
			default:
				logger.Fatal("Failed to dequeue job: %v", err)
			}
		}

		jobID := job.GetId()
		logger.Info("Executing job: %s", jobID)

		if err := executor.ExecuteJob(ctx, job, logClient, logger); err != nil {
			logger.Error("Job %s failed: %v", jobID, err)
			// TODO(garrett): Report job failure.
			continue
		}

		logger.Info("Job completed successfully: %s", jobID)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-worker",
	Short: "Vectis Worker",
	Long:  `The Vectis Worker executes jobs from the queue using the action system.`,
	Run:   runWorker,
}

func init() {
	viper.SetEnvPrefix("VECTIS_WORKER")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
