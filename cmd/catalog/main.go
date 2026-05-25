package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/catalog"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"

	_ "vectis/internal/dbdrivers"
)

func runCatalog(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	logger := interfaces.NewAsyncLogger("catalog")
	defer logger.Close()

	cli.SetLogLevel(logger)
	logger.Info("Starting catalog service...")

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("%v", err)
	}

	config.StartMetricsTLSReloadLoop(ctx)

	shutdownTracer, err := observability.InitTracer(ctx, "vectis-catalog")
	if err != nil {
		logger.Fatal("Failed to initialize tracer: %v", err)
	}
	defer cli.DeferShutdown(logger, "Tracer", shutdownTracer)()

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(ctx, "vectis-catalog")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}
	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	db, _, err := database.OpenReadyDB(logger)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	repos := dal.NewSQLRepositories(db)
	svc := catalog.NewService(logger, repos.CatalogEvents(), repos.Runs())

	metricsAddr := fmt.Sprintf(":%d", viper.GetInt("metrics_port"))
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Catalog", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	if err := svc.Run(ctx, config.CatalogInterval(), config.CatalogBatchSize()); err != nil {
		logger.Fatal("Catalog service failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-catalog",
	Short: "Apply cell catalog events into the global run catalog",
	Run:   runCatalog,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	rootCmd.PersistentFlags().Duration("interval", config.CatalogInterval(), "How often to drain pending cell catalog events")
	rootCmd.PersistentFlags().Int("batch-size", config.CatalogBatchSize(), "Maximum cell catalog events to process per batch")
	rootCmd.PersistentFlags().Int("metrics-port", config.CatalogMetricsPort(), "HTTP port for Prometheus /metrics")
	_ = viper.BindPFlag("interval", rootCmd.PersistentFlags().Lookup("interval"))
	_ = viper.BindPFlag("batch_size", rootCmd.PersistentFlags().Lookup("batch-size"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	viper.SetDefault("interval", config.CatalogInterval())
	viper.SetDefault("batch_size", config.CatalogBatchSize())
	viper.SetDefault("metrics_port", config.CatalogMetricsPort())
	viper.SetEnvPrefix("VECTIS_CATALOG")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
