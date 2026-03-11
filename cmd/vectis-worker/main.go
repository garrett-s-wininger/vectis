package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var maxTries = 5
var baseDelay = 500 * time.Millisecond

func main() {
	ctx := context.Background()

	fmt.Println("Connecting to registry...")
	registryConn, err := grpc.NewClient(server.RegistryPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to registry: %v\n", err)
		os.Exit(1)
	}

	defer registryConn.Close()

	registryClient := api.NewRegistryServiceClient(registryConn)

	fmt.Println("Getting queue service address from registry...")
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
		fmt.Fprintf(os.Stderr, "Failed to get queue address (attempt %d): %v. Retrying in %v...\n", attempt, err, delay)
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get queue address: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Connecting to queue at %s...\n", queueAddr)
	queueConn, err := grpc.NewClient(queueAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to queue: %v\n", err)
		os.Exit(1)
	}

	defer queueConn.Close()
	queueClient := api.NewQueueServiceClient(queueConn)

	fmt.Println("Dequeuing job...")
	job, err := queueClient.Dequeue(ctx, &api.Empty{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to dequeue job: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Executing job: %s\n", *job.Id)
	for i, step := range job.Steps {
		fmt.Printf("Running step %d: %s\n", i+1, *step.Command)
		cmd := exec.Command("sh", "-c", *step.Command)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "Step %d failed: %v\n", i+1, err)
			os.Exit(1)
		}
	}

	fmt.Println("Job completed successfully")
}
