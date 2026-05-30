package main

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/cron"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"

	_ "vectis/internal/dbdrivers"
)

func runVectisCron(cmd *cobra.Command, args []string) {
	logger := interfaces.NewAsyncLogger("cron")
	defer logger.Close()

	cli.SetLogLevel(logger)
	logger.Info("Starting cron service...")

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonClientOnly); err != nil {
		logger.Fatal("%v", err)
	}
	config.StartGRPCTLSReloadLoop(cmd.Context())

	db, _, err := database.OpenReadyDBForRole(logger, database.RoleGlobal)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	service := cron.NewCronService(logger, db)
	defer service.CloseQueueDial()

	retryMetrics, err := observability.NewRetryMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize retry metrics: %v", err)
	}
	service.SetRetryMetrics(retryMetrics)

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
	cli.ConfigureVersion(rootCmd)
	_ = viper.BindEnv("cell_ingress_endpoints", "VECTIS_CRON_CELL_INGRESS_ENDPOINTS", "VECTIS_CELL_INGRESS_ENDPOINTS")
	viper.SetEnvPrefix("VECTIS_CRON")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
