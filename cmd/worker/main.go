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
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/job"
	"vectis/internal/registry"
	"vectis/internal/runstore"

	_ "github.com/mattn/go-sqlite3"
)

const maxFailureReasonLen = 4096

func runWorker(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	logger := interfaces.NewLogger("worker")

	dbPath := database.GetDBPath()
	logger.Info("Using database: %s", dbPath)
	db, err := database.InitDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()
	statusStore := runstore.NewStore(db)

	logger.Info("Connecting to registry...")
	registryClient, err := registry.New(ctx, logger, interfaces.SystemClock{})
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
	queueClient := interfaces.NewGRPCQueueClient(queueConn)

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

	logClient := interfaces.NewGRPCLogClient(logConn)
	executor := job.NewExecutor()

	for {
		logger.Debug("Initiating long poll from queue...")
		pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		job, err := queueClient.Dequeue(pollCtx)
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
		runID := job.GetRunId()
		logger.Info("Executing job: %s", jobID)

		if runID != "" {
			if err := statusStore.MarkRunRunning(ctx, runID); err != nil {
				logger.Error("Failed to mark run %s running: %v", runID, err)
			}
		}

		if err := executor.ExecuteJob(ctx, job, logClient, logger); err != nil {
			logger.Error("Job %s failed: %v", jobID, err)
			if runID != "" {
				reason := err.Error()
				if len(reason) > maxFailureReasonLen {
					reason = reason[:maxFailureReasonLen] + "..."
				}

				if updateErr := statusStore.MarkRunFailed(ctx, runID, reason); updateErr != nil {
					logger.Error("Failed to mark run %s failed: %v", runID, updateErr)
				}
			}
			continue
		}

		if runID != "" {
			if err := statusStore.MarkRunSucceeded(ctx, runID); err != nil {
				logger.Error("Failed to mark run %s succeeded: %v", runID, err)
			}
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
