package main

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/catalog"
	"vectis/internal/cell"
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

	db, globalDBPath, err := database.OpenReadyDBForRole(logger, database.RoleGlobal)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	repos := dal.NewSQLRepositories(db)
	svc := catalog.NewService(logger, repos.CatalogEvents(), repos.Runs(), repos.Artifacts())
	svc.SetBackfill(catalog.NewBackfillProcessor(
		config.CellID(),
		repos.CatalogStatusBackfill(),
		cell.NewCatalogEventPublisher(config.CellID(), repos.CatalogEvents()),
	))

	fanInSources, closeFanIn, err := openCatalogFanInSources(logger, globalDBPath)
	if err != nil {
		logger.Fatal("Failed to initialize catalog fan-in: %v", err)
	}
	defer closeFanIn()

	catalogMetrics, err := observability.NewCatalogMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize catalog metrics: %v", err)
	}
	svc.SetMetrics(catalogMetrics)

	if len(fanInSources) > 0 {
		fanIn := catalog.NewFanInProcessor(repos.CatalogEvents(), fanInSources)
		fanIn.SetMetrics(catalogMetrics)
		svc.SetFanIn(fanIn)
		logger.Info("Catalog fan-in enabled for %d cell database(s)", len(fanInSources))
	}

	metricsAddr := config.CatalogMetricsListenAddr()
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Catalog", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	if err := svc.Run(ctx, config.CatalogInterval(), config.CatalogBatchSize()); err != nil {
		logger.Fatal("Catalog service failed: %v", err)
	}
}

func openCatalogFanInSources(logger interfaces.Logger, globalDBPath string) ([]catalog.FanInSource, func(), error) {
	cellDBs, err := config.CatalogCellDatabaseDSNs()
	if err != nil {
		return nil, func() {}, err
	}

	if len(cellDBs) == 0 {
		return nil, func() {}, nil
	}

	cellIDs := make([]string, 0, len(cellDBs))
	for cellID := range cellDBs {
		cellIDs = append(cellIDs, cellID)
	}

	sort.Strings(cellIDs)

	var dbs []*sql.DB
	closeAll := func() {
		for _, db := range dbs {
			_ = db.Close()
		}
	}

	sources := make([]catalog.FanInSource, 0, len(cellIDs))
	for _, cellID := range cellIDs {
		dsn := strings.TrimSpace(cellDBs[cellID])
		if dsn == "" || dsn == globalDBPath {
			continue
		}

		db, err := database.OpenDB(dsn)
		if err != nil {
			closeAll()
			return nil, func() {}, fmt.Errorf("open cell database %q for cell %q: %w", dsn, cellID, err)
		}

		dbs = append(dbs, db)
		if err := database.WaitForMigrations(db, logger); err != nil {
			closeAll()
			return nil, func() {}, fmt.Errorf("wait for cell database %q schema: %w", cellID, err)
		}

		repos := dal.NewSQLRepositoriesWithCellID(db, cellID)
		events := repos.CatalogEvents()
		sources = append(sources, catalog.FanInSource{
			CellID: cellID,
			Events: events,
			Backfill: catalog.NewBackfillProcessor(
				cellID,
				repos.CatalogStatusBackfill(),
				cell.NewCatalogEventPublisher(cellID, events),
			),
		})
	}

	return sources, closeAll, nil
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
	rootCmd.PersistentFlags().String("metrics-host", config.CatalogMetricsHost(), "Host/IP for the Prometheus /metrics HTTP server to bind")
	rootCmd.PersistentFlags().Int("metrics-port", config.CatalogMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().StringSlice("cell-database-dsn", config.CatalogCellDatabaseSpecs(), "Cell catalog source in cell_id=dsn form; may be repeated")
	_ = viper.BindPFlag("interval", rootCmd.PersistentFlags().Lookup("interval"))
	_ = viper.BindPFlag("batch_size", rootCmd.PersistentFlags().Lookup("batch-size"))
	_ = viper.BindPFlag("metrics_host", rootCmd.PersistentFlags().Lookup("metrics-host"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("cell_database_dsns", rootCmd.PersistentFlags().Lookup("cell-database-dsn"))
	_ = viper.BindEnv("cell_database_dsns", "VECTIS_CATALOG_CELL_DATABASE_DSNS")
	viper.SetDefault("interval", config.CatalogInterval())
	viper.SetDefault("batch_size", config.CatalogBatchSize())
	viper.SetDefault("metrics_host", config.CatalogMetricsHost())
	viper.SetDefault("metrics_port", config.CatalogMetricsPort())
	viper.SetEnvPrefix("VECTIS_CATALOG")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
