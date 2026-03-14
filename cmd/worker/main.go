package main

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	api "vectis/api/gen/go"
	"vectis/internal/log"
	"vectis/internal/registry"
)

func runWorker(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	logger := log.New("worker")
	tee := viper.GetBool("tee")

	logger.Info("Connecting to registry...")
	registryClient, err := registry.New(ctx, logger)
	if err != nil {
		logger.Fatal("Failed to connect to registry: %v", err)
	}
	defer registryClient.Close()

	// Get queue address
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

	// Get log service address
	logger.Info("Getting log service address from registry...")
	logAddr, err := registryClient.Address(ctx, api.Component_COMPONENT_LOG)
	if err != nil {
		logger.Fatal("Failed to get log address: %v", err)
	}

	logger.Info("Connecting to log service at %s...", logAddr)
	logConn, err := grpc.NewClient(logAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("Failed to connect to log service: %v", err)
	}
	defer logConn.Close()
	logClient := api.NewLogServiceClient(logConn)

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

		jobID := *job.Id
		logger.Info("Executing job: %s", jobID)

		// Create log stream
		logStream, err := logClient.StreamLogs(ctx)
		if err != nil {
			logger.Error("Failed to create log stream: %v", err)
			continue
		}
		logger.Info("Connected to log service for job %s", jobID)

		var sequence int64

		for i, step := range job.Steps {
			logger.Info("Running step %d: %s", i+1, *step.Command)

			cmd := exec.Command("sh", "-c", *step.Command)

			// Create pipes for stdout and stderr
			stdoutPipe, err := cmd.StdoutPipe()
			if err != nil {
				logger.Fatal("Failed to create stdout pipe: %v", err)
			}

			stderrPipe, err := cmd.StderrPipe()
			if err != nil {
				logger.Fatal("Failed to create stderr pipe: %v", err)
			}

			// Start the command
			if err := cmd.Start(); err != nil {
				logger.Fatal("Failed to start step %d: %v", i+1, err)
			}

			// Stream both stdout and stderr
			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				streamOutput(stdoutPipe, jobID, api.Stream_STREAM_STDOUT, &sequence, logStream, tee)
			}()

			go func() {
				defer wg.Done()
				streamOutput(stderrPipe, jobID, api.Stream_STREAM_STDERR, &sequence, logStream, tee)
			}()

			// Wait for both streams to complete
			wg.Wait()

			// Wait for command to complete
			if err := cmd.Wait(); err != nil {
				logger.Fatal("Step %d failed: %v", i+1, err)
			}
		}

		logStream.CloseSend()
		logger.Info("Job completed successfully")
	}
}

func streamOutput(reader io.Reader, jobID string, streamType api.Stream, sequence *int64, logStream api.LogService_StreamLogsClient, tee bool) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		seq := atomic.AddInt64(sequence, 1)

		// Send to log service
		chunk := &api.LogChunk{
			JobId:    &jobID,
			Data:     []byte(line),
			Sequence: &seq,
			Stream:   &streamType,
		}

		if err := logStream.Send(chunk); err != nil {
			// Log locally if we can't send to log service
			if tee {
				printLine(streamType, line)
			}
			continue
		}

		// Tee to stdout/stderr if enabled
		if tee {
			printLine(streamType, line)
		}
	}
}

func printLine(streamType api.Stream, line string) {
	switch streamType {
	case api.Stream_STREAM_STDOUT:
		os.Stdout.WriteString(line + "\n")
	case api.Stream_STREAM_STDERR:
		os.Stderr.WriteString(line + "\n")
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-worker",
	Short: "Vectis Worker",
	Long:  `The Vectis Worker executes jobs from the queue and streams logs.`,
	Run:   runWorker,
}

func init() {
	viper.SetDefault("tee", false)
	rootCmd.PersistentFlags().Bool("tee", false, "Tee output to stdout/stderr in addition to streaming to log service")
	_ = viper.BindPFlag("tee", rootCmd.PersistentFlags().Lookup("tee"))
	viper.SetEnvPrefix("VECTIS_WORKER")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
