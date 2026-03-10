package main

import (
	"fmt"
	"net"
	"os"
	"vectis/internal/server"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const listenAddr = ":8081"

func runVectisQueue(cmd *cobra.Command, args []string) {
	fmt.Println("Starting queue server...")

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	server.RegisterQueueService(grpcServer)

	fmt.Printf("Queue server listening on %s\n", listenAddr)
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
