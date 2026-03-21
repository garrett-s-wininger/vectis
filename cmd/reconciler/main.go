package main

import (
	"context"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	api "vectis/api/gen/go"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/reconciler"
	"vectis/internal/registry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	_ "github.com/mattn/go-sqlite3"
)

func runReconciler(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	logger := interfaces.NewLogger("reconciler")

	dbPath := database.GetDBPath()
	logger.Info("Using database: %s", dbPath)
	db, err := database.OpenDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to open database: %v", err)
	}
	defer db.Close()

	registryClient, err := registry.New(ctx, logger, interfaces.SystemClock{})
	if err != nil {
		logger.Fatal("Failed to connect to registry: %v", err)
	}
	defer registryClient.Close()

	queueAddr, err := registryClient.Address(ctx, api.Component_COMPONENT_QUEUE)
	if err != nil {
		logger.Fatal("Failed to get queue address: %v", err)
	}

	conn, err := grpc.NewClient(queueAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logger.Fatal("Failed to connect to queue: %v", err)
	}
	defer conn.Close()

	queueClient := interfaces.NewQueueService(api.NewQueueServiceClient(conn))
	logger.Info("Connected to queue at %s", queueAddr)

	svc := reconciler.NewService(logger, db, queueClient, interfaces.SystemClock{})

	interval := viper.GetDuration("interval")
	if interval <= 0 {
		interval = 30 * time.Second
	}
	logger.Info("Reconciler polling every %v", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if err := svc.Process(ctx); err != nil {
		logger.Error("Initial reconcile failed: %v", err)
	}

	for {
		select {
		case <-cmd.Context().Done():
			logger.Info("Reconciler shutting down")
			return
		case <-ticker.C:
			if err := svc.Process(cmd.Context()); err != nil {
				logger.Error("Reconcile failed: %v", err)
			}
		}
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-reconciler",
	Short: "Re-enqueue queued job runs that were never dispatched or need a queue retry",
	Run:   runReconciler,
}

func init() {
	viper.SetDefault("interval", 30*time.Second)
	rootCmd.PersistentFlags().Duration("interval", 30*time.Second, "How often to scan for queued runs")
	_ = viper.BindPFlag("interval", rootCmd.PersistentFlags().Lookup("interval"))
	viper.SetEnvPrefix("VECTIS_RECONCILER")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
