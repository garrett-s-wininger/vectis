package main

import (
	"fmt"
	"net"
	"os"
	"time"
	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/server"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func runVectisQueue(cmd *cobra.Command, args []string) {
	fmt.Println("Starting queue server...")

	ln, err := net.Listen("tcp", server.QueuePort)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
		os.Exit(1)
	}

	const maxTries = 5
	const baseDelay = 500 * time.Millisecond

	// TODO(garrett): Move to after queue is running.
	var conn *grpc.ClientConn
	lastErr := backoff.RetryWithBackoff(maxTries, baseDelay, func() error {
		var e error
		conn, e = grpc.NewClient(":8082", grpc.WithTransportCredentials(insecure.NewCredentials()))
		return e
	}, func(attempt int, nextDelay time.Duration, err error) {
		fmt.Fprintf(os.Stderr, "Failed to connect to registry service (attempt %d/%d): %v. Retrying in %v...\n", attempt, maxTries, err, nextDelay)
	})

	if lastErr != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to registry service after %d attempts: %v\n", maxTries, lastErr)
		os.Exit(1)
	}

	defer conn.Close()

	registryClient := api.NewRegistryServiceClient(conn)
	_, regErr := registryClient.Register(cmd.Context(), &api.Registration{
		Component: api.Component_COMPONENT_QUEUE.Enum(),
		Address:   &server.QueuePort,
	})

	if regErr != nil {
		regErr = backoff.RetryWithBackoff(maxTries, baseDelay, func() error {
			_, err := registryClient.Register(cmd.Context(), &api.Registration{
				Component: api.Component_COMPONENT_QUEUE.Enum(),
				Address:   &server.QueuePort,
			})
			return err
		}, func(attempt int, nextDelay time.Duration, err error) {
			fmt.Fprintf(os.Stderr, "Failed to register with registry (attempt %d/%d): %v. Retrying in %v...\n", attempt, maxTries, err, nextDelay)
		})
		if regErr != nil {
			fmt.Fprintf(os.Stderr, "Failed to register with registry after %d attempts: %v\n", maxTries, regErr)
			os.Exit(1)
		}
	}

	fmt.Println("Registered with registry service")
	grpcServer := grpc.NewServer()
	server.RegisterQueueService(grpcServer)

	fmt.Printf("Queue server listening on %s\n", server.QueuePort)
	if err := grpcServer.Serve(ln); err != nil {
		fmt.Fprintf(os.Stderr, "gRPC server failed: %v\n", err)
		os.Exit(1)
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
