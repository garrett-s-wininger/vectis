package main

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/cron"
	"vectis/internal/database"
	"vectis/internal/log"

	_ "github.com/mattn/go-sqlite3"
)

func runVectisCron(cmd *cobra.Command, args []string) {
	logger := log.New("cron")
	logger.Info("Starting cron service...")

	dbPath := database.GetDBPath()
	logger.Info("Using database: %s", dbPath)

	db, err := database.InitDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	service := cron.NewCronService(logger, db)

	if err := service.ConnectToQueue(cmd.Context()); err != nil {
		logger.Fatal("Failed to connect to queue: %v", err)
	}

	if err := service.Run(cmd.Context()); err != nil {
		logger.Fatal("Cron service failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-cron",
	Short: "Vectis Cron Service",
	Long:  `The Vectis Cron Service polls for scheduled jobs and triggers them at the appropriate time.`,
	Run:   runVectisCron,
}

func init() {
	viper.SetEnvPrefix("VECTIS_CRON")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
