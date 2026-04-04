package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/api"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"

	_ "vectis/internal/dbdrivers"
)

func runVectisAPI(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("api")
	cli.SetLogLevel(logger)
	logger.Info("Starting API server...")

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

	metricsHandler, shutdownMetrics, err := observability.InitAPIMetrics(cmd.Context())
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}

	if err := observability.RegisterSQLDBPoolMetrics(db); err != nil {
		logger.Fatal("Failed to register DB pool metrics: %v", err)
	}

	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownMetrics(shutCtx); err != nil {
			logger.Warn("Metrics shutdown: %v", err)
		}
	}()

	server := api.NewAPIServer(logger, db)
	server.MetricsHandler = metricsHandler

	if err := server.ConnectToQueue(cmd.Context()); err != nil {
		logger.Fatal("Failed to connect to services: %v", err)
	}

	port := config.APIEffectiveListenPort()
	addr := fmt.Sprintf(":%d", port)
	if err := server.Run(cmd.Context(), addr); err != nil {
		logger.Fatal("Server failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-api-server",
	Short: "Vectis API Server",
	Long:  `The Vectis API Server provides REST endpoints for triggering stored jobs.`,
	Run:   runVectisAPI,
}

func init() {
	rootCmd.PersistentFlags().Int("port", config.APIPort(), "Port for the API server")
	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	viper.SetEnvPrefix("VECTIS_API_SERVER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
