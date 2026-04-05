package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/logserver"
	"vectis/internal/observability"
	"vectis/internal/utils"

	_ "vectis/internal/dbdrivers"
)

func runLog(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()

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

	if err := database.WaitForMigrations(db, logger); err != nil {
		logger.Fatal("database wait for migrations failed: %v", err)
	}

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(ctx, "vectis-log")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}

	if err := observability.RegisterSQLDBPoolMetrics(db); err != nil {
		logger.Fatal("Failed to register DB pool metrics: %v", err)
	}

	logMetrics, err := observability.NewLogMetrics()
	if err != nil {
		logger.Fatal("Failed to register log metrics: %v", err)
	}

	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := shutdownMetrics(shutCtx); err != nil {
			logger.Warn("Metrics shutdown: %v", err)
		}
	}()

	metricsPort := config.LogMetricsEffectiveListenPort()
	metricsAddr := fmt.Sprintf(":%d", metricsPort)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", metricsHandler)
	metricsSrv := &http.Server{
		Addr:    metricsAddr,
		Handler: metricsMux,
	}

	metricsLn, err := net.Listen("tcp", metricsAddr)
	if err != nil {
		logger.Fatal("Failed to listen for metrics: %v", err)
	}

	go func() {
		if err := metricsSrv.Serve(metricsLn); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server: %v", err)
		}
	}()

	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsSrv.Shutdown(shutCtx); err != nil {
			logger.Warn("Metrics HTTP shutdown: %v", err)
		}
	}()

	logger.Info("Log metrics listening on %s (/metrics)", metricsAddr)

	runStatus := logserver.NewDALRunStatusProvider(dal.NewSQLRepositories(db).Runs())

	if err := logserver.Run(ctx, logger, store, runStatus, logMetrics); err != nil {
		logger.Fatal("Log service failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-log",
	Short: "Vectis log aggregation service",
	Run:   runLog,
}

func init() {
	defaultStorage := filepath.Join(utils.DataHome(), "vectis", "jobs")
	viper.SetDefault("storage_dir", defaultStorage)
	viper.SetDefault("metrics_port", config.LogMetricsPort())

	rootCmd.PersistentFlags().String("storage-dir", defaultStorage, "Directory for durable run log files")
	rootCmd.PersistentFlags().Int("metrics-port", config.LogMetricsPort(), "HTTP port for Prometheus /metrics")
	_ = viper.BindPFlag("storage_dir", rootCmd.PersistentFlags().Lookup("storage-dir"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))

	viper.SetEnvPrefix("VECTIS_LOG")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
