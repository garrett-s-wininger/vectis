package main

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/cli"
	"vectis/internal/cron"
	"vectis/internal/database"
	"vectis/internal/interfaces"

	_ "vectis/internal/dbdrivers"
)

func runVectisCron(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("cron")
	cli.SetLogLevel(logger)
	logger.Info("Starting cron service...")

	dbPath := database.GetDBPath()
	logger.Info("Using database: %s", dbPath)

	db, err := database.OpenDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := database.WaitForMigrations(db); err != nil {
		logger.Fatal("database wait for migrations failed: %v", err)
	}

	service := cron.NewCronService(logger, db)
	defer service.CloseQueueDial()

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
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
