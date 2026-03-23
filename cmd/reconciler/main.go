package main

import (
	"context"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/queueclient"
	"vectis/internal/reconciler"
	"vectis/internal/resolver"

	"google.golang.org/grpc"

	_ "github.com/mattn/go-sqlite3"
)

func runReconciler(cmd *cobra.Command, args []string) {
	rootCtx := cmd.Context()
	logger := interfaces.NewLogger("reconciler")

	dbPath := database.GetDBPath()
	logger.Info("Using database: %s", dbPath)
	db, err := database.OpenDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to open database: %v", err)
	}
	defer db.Close()

	pin := config.ReconcilerQueueAddress()
	mq, err := queueclient.NewManagingQueueService(rootCtx, logger, func(ctx context.Context) (*grpc.ClientConn, func(), error) {
		return resolver.DialQueue(ctx, logger, pin, config.ReconcilerRegistryDialAddress())
	})

	if err != nil {
		logger.Fatal("Failed to connect to queue: %v", err)
	}
	defer func() { _ = mq.Close() }()

	if pin == "" {
		logger.Info("Connected to queue via registry resolution")
	}

	svc := reconciler.NewService(logger, db, mq, interfaces.SystemClock{})

	interval := config.ReconcilerInterval()
	logger.Info("Reconciler polling every %v", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if err := svc.Process(rootCtx); err != nil {
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
