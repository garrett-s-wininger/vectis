package main

import (
	"fmt"
	"net"
	"os"

	"vectis/internal/server"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func runVectis(cmd *cobra.Command, args []string) {
	fmt.Println("Starting queue server...")

	ln, err := net.Listen("tcp", ":50051")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to listen: %v\n", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	server.RegisterQueueService(grpcServer)

	fmt.Println("Queue server listening on :50051")
	if err := grpcServer.Serve(ln); err != nil {
		fmt.Fprintf(os.Stderr, "gRPC server failed: %v\n", err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis",
	Short: "A self-hosted, modern build system",
	Long: `Vectis is a modern, self-hosted build system.

It's designed to be easy to use and deploy for a wide-range of build types
and execution environments.`,
	Run: runVectis,
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
