package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"vectis/internal/cli"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/logserver"
	"vectis/internal/utils"

	"github.com/spf13/viper"

	_ "vectis/internal/dbdrivers"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		cancel()
	}()

	viper.SetEnvPrefix("VECTIS_LOG")
	viper.AutomaticEnv()
	viper.SetDefault("storage_dir", filepath.Join(utils.DataHome(), "vectis", "jobs"))

	logger := interfaces.NewLogger("log-aggregator")
	cli.SetLogLevel(logger)
	logger.Info("Starting log service...")

	storageDir := viper.GetString("storage_dir")
	if storageDir == "" {
		logger.Fatal("log storage_dir must not be empty")
	}

	store, err := logserver.NewLocalRunLogStore(storageDir)
	if err != nil {
		logger.Fatal("Failed to initialize log storage: %v", err)
	}
	logger.Info("Using durable log storage directory: %s", storageDir)

	dbPath := database.GetDBPath()
	db, err := database.OpenDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to open database for run-status lookup: %v", err)
	}
	defer db.Close()

	if err := database.WaitForMigrations(db); err != nil {
		logger.Fatal("database wait for migrations failed: %v", err)
	}

	runStatus := logserver.NewDALRunStatusProvider(dal.NewSQLRepositories(db).Runs())

	if err := logserver.Run(ctx, logger, store, runStatus); err != nil {
		logger.Fatal("Log service failed: %v", err)
	}
}
