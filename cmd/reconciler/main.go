package main

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/queueclient"
	"vectis/internal/reconciler"

	_ "vectis/internal/dbdrivers"
)

func runReconciler(cmd *cobra.Command, args []string) {
	rootCtx := cmd.Context()
	logger := interfaces.NewAsyncLogger("reconciler")
	defer logger.Close()

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
	defer cli.DeferShutdown(logger, "Tracer", shutdownTracer)()

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(rootCtx, "vectis-reconciler")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}

	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	db, _, err := database.OpenReadyDBForRole(logger, database.RoleGlobal)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	retryMetrics, err := observability.NewRetryMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize retry metrics: %v", err)
	}

	pin := config.ReconcilerQueueAddress()
	mq, err := queueclient.NewManagingQueuePoolService(rootCtx, logger, queueclient.QueuePoolOptions{
		PinnedAddress:   pin,
		RegistryAddress: config.ReconcilerRegistryDialAddress(),
		RetryMetrics:    retryMetrics,
	})

	if err != nil {
		logger.Fatal("Failed to connect to queue: %v", err)
	}
	defer func() { _ = mq.Close() }()

	if pin == "" {
		logger.Info("Connected to queue via registry resolution")
	}

	svc := reconciler.NewService(logger, db, mq, interfaces.SystemClock{})
	leaseOwner := uuid.NewString()
	svc.SetLeaseOwner(leaseOwner)
	svc.SetLeaseTTL(config.ReconcilerLeaseTTL())
	logger.Info("Reconciler instance ID: %s", leaseOwner)

	reconcilerMetrics, err := observability.NewReconcilerMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize reconciler metrics: %v", err)
	}
	svc.SetMetrics(reconcilerMetrics)

	dispatchMetrics, err := observability.NewDispatchMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize dispatch metrics: %v", err)
	}
	svc.SetDispatchMetrics(dispatchMetrics)

	taskDispatchMetrics, err := observability.NewTaskDispatchMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize task dispatch metrics: %v", err)
	}
	svc.SetTaskDispatchMetrics(taskDispatchMetrics)

	metricsPort := viper.GetInt("metrics_port")
	metricsAddr := fmt.Sprintf(":%d", metricsPort)
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Reconciler", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	interval := config.ReconcilerInterval()
	logger.Info("Reconciler polling every %v with service lease ttl %v", interval, config.ReconcilerLeaseTTL())

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
	Short: "Repair queued run and task continuation dispatch",
	Run:   runReconciler,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	rootCmd.PersistentFlags().Duration("interval", config.ReconcilerInterval(), "How often to scan for queued runs")
	rootCmd.PersistentFlags().Duration("lease-ttl", config.ReconcilerLeaseTTL(), "How long a reconciler instance owns the active service lease")
	rootCmd.PersistentFlags().Int("metrics-port", config.ReconcilerMetricsPort(), "HTTP port for Prometheus /metrics")
	_ = viper.BindPFlag("interval", rootCmd.PersistentFlags().Lookup("interval"))
	_ = viper.BindPFlag("lease_ttl", rootCmd.PersistentFlags().Lookup("lease-ttl"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindEnv("cell_ingress_endpoints", "VECTIS_RECONCILER_CELL_INGRESS_ENDPOINTS", "VECTIS_CELL_INGRESS_ENDPOINTS")
	viper.SetDefault("metrics_port", config.ReconcilerMetricsPort())
	viper.SetEnvPrefix("VECTIS_RECONCILER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
