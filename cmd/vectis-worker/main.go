package main

import (
	"context"
	"os"
	"os/exec"

	api "vectis/api/gen/go"
	"vectis/internal/log"
	"vectis/internal/registry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()
	logger := log.New("worker")

	logger.Info("Connecting to registry...")
	registryClient, err := registry.New(ctx, logger)
	if err != nil {
		logger.Fatal("Failed to connect to registry: %v", err)
	}

	defer registryClient.Close()

	// TODO(garrett): Retry to refresh queue address if it's not available.
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

	logger.Info("Dequeuing job...")
	job, err := queueClient.Dequeue(ctx, &api.Empty{})
	if err != nil {
		logger.Fatal("Failed to dequeue job: %v", err)
	}

	logger.Info("Executing job: %s", *job.Id)
	for i, step := range job.Steps {
		logger.Info("Running step %d: %s", i+1, *step.Command)
		cmd := exec.Command("sh", "-c", *step.Command)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			logger.Fatal("Step %d failed: %v", i+1, err)
		}
	}

	logger.Info("Job completed successfully")
}
