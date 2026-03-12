package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/log"
	"vectis/internal/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var maxTries = 5
var baseDelay = 500 * time.Millisecond

func main() {
	ctx := context.Background()
	logger := log.New("worker")

	logger.Info("Connecting to registry...")
	registryConn, err := grpc.NewClient(server.RegistryPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("Failed to connect to registry: %v", err)
	}

	defer registryConn.Close()

	registryClient := api.NewRegistryServiceClient(registryConn)

	logger.Info("Getting queue service address from registry...")
	var queueAddr string
	err = backoff.RetryWithBackoff(maxTries, baseDelay, func() error {
		resp, err := registryClient.GetAddress(ctx, &api.AddressRequest{
			Component: api.Component_COMPONENT_QUEUE.Enum(),
		})

		if err != nil {
			return err
		}

		queueAddr = resp.GetAddress()
		if queueAddr == "" {
			return fmt.Errorf("queue address not available")
		}

		return nil
	}, func(attempt int, delay time.Duration, err error) {
		logger.Warn("Failed to get queue address (attempt %d): %v. Retrying in %v...", attempt, err, delay)
	})

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
