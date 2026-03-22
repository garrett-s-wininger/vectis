package main

import (
	"context"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/reconciler"
	"vectis/internal/resolver"

	"google.golang.org/grpc"

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

	var conn *grpc.ClientConn
	queueCleanup := func() {}
	defer func() { queueCleanup() }()

	if pinned := config.ReconcilerQueueAddress(); pinned != "" {
		logger.Info("Using pinned queue address: %s", pinned)

		var err error
		conn, queueCleanup, err = resolver.NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_QUEUE, pinned, logger)
		if err != nil {
			logger.Fatal("Failed to connect to queue: %v", err)
		}
	} else {
		regAddr := config.ReconcilerRegistryDialAddress()

		regClient, err := resolver.NewRegistryClient(ctx, regAddr, logger, interfaces.SystemClock{})
		if err != nil {
			logger.Fatal("Failed to create registry client: %v", err)
		}

		var dialCleanup func()
		conn, dialCleanup, err = resolver.NewQueueClientWithRegistry(ctx, logger, regClient)
		if err != nil {
			regClient.Close()
			logger.Fatal("Failed to connect to queue: %v", err)
		}

		queueCleanup = func() {
			dialCleanup()
			regClient.Close()
		}

		logger.Info("Connected to queue via registry resolution")
	}

	queueClient := interfaces.NewQueueService(api.NewQueueServiceClient(conn))
	svc := reconciler.NewService(logger, db, queueClient, interfaces.SystemClock{})

	interval := config.ReconcilerInterval()
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
	rootCmd.PersistentFlags().Duration("interval", config.ReconcilerInterval(), "How often to scan for queued runs")
	_ = viper.BindPFlag("interval", rootCmd.PersistentFlags().Lookup("interval"))
	viper.SetEnvPrefix("VECTIS_RECONCILER")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
