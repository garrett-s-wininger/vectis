package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/queueclient"
	"vectis/internal/reconciler"
	"vectis/internal/resolver"

	"google.golang.org/grpc"

	_ "vectis/internal/dbdrivers"
)

func runReconciler(cmd *cobra.Command, args []string) {
	rootCtx := cmd.Context()
	logger := interfaces.NewLogger("reconciler")
	cli.SetLogLevel(logger)

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonClientOnly); err != nil {
		logger.Fatal("%v", err)
	}

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("%v", err)
	}

	config.StartGRPCTLSReloadLoop(rootCtx)
	config.StartMetricsTLSReloadLoop(rootCtx)

	shutdownTracer, err := observability.InitTracer(rootCtx, "vectis-reconciler")
	if err != nil {
		logger.Fatal("Failed to initialize tracer: %v", err)
	}

	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracer(shutCtx); err != nil {
			logger.Warn("Tracer shutdown: %v", err)
		}
	}()

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(rootCtx, "vectis-reconciler")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}

	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownMetrics(shutCtx); err != nil {
			logger.Warn("Metrics shutdown: %v", err)
		}
	}()

	dbPath := database.GetDBPath()
	logger.Info("Using database: %s", dbPath)
	db, err := database.OpenDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := database.WaitForMigrations(db, logger); err != nil {
		logger.Fatal("database wait for migrations failed: %v", err)
	}

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
	reconcilerMetrics, err := observability.NewReconcilerMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize reconciler metrics: %v", err)
	}
	svc.SetMetrics(reconcilerMetrics)

	metricsPort := viper.GetInt("metrics_port")
	metricsAddr := fmt.Sprintf(":%d", metricsPort)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", metricsHandler)
	metricsSrv := &http.Server{Handler: metricsMux}

	metricsLn, err := net.Listen("tcp", metricsAddr)
	if err != nil {
		logger.Fatal("Failed to listen for metrics: %v", err)
	}

	metricsLn, err = config.MetricsHTTPSListener(metricsLn)
	if err != nil {
		logger.Fatal("metrics tls: %v", err)
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

	interval := config.ReconcilerInterval()
	logger.Info("Reconciler polling every %v", interval)

	if !config.MetricsTLSInsecure() {
		logger.Info("Reconciler metrics listening on %s (HTTPS /metrics)", metricsAddr)
	} else {
		logger.Info("Reconciler metrics listening on %s (/metrics)", metricsAddr)
	}

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
	rootCmd.PersistentFlags().Int("metrics-port", 9084, "HTTP port for Prometheus /metrics")
	_ = viper.BindPFlag("interval", rootCmd.PersistentFlags().Lookup("interval"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	viper.SetDefault("metrics_port", 9084)
	viper.SetEnvPrefix("VECTIS_RECONCILER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
