package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"vectis/internal/cellingress"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/queueclient"
	"vectis/internal/resolver"
)

func runCellIngress(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	logger := interfaces.NewAsyncLogger("cell-ingress")
	defer logger.Close()

	cli.SetLogLevel(logger)
	logger.Info("Starting cell ingress for cell %s...", config.CellID())

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonClientOnly); err != nil {
		logger.Fatal("%v", err)
	}

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("%v", err)
	}

	config.StartGRPCTLSReloadLoop(ctx)
	config.StartMetricsTLSReloadLoop(ctx)

	shutdownTracer, err := observability.InitTracer(ctx, "vectis-cell-ingress")
	if err != nil {
		logger.Fatal("Failed to initialize tracer: %v", err)
	}
	defer cli.DeferShutdown(logger, "Tracer", shutdownTracer)()

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(ctx, "vectis-cell-ingress")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}
	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	retryMetrics, err := observability.NewRetryMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize retry metrics: %v", err)
	}

	pinnedQueueAddress := config.CellIngressQueueAddress()
	queue, err := queueclient.NewManagingQueueService(ctx, logger, func(ctx context.Context) (*grpc.ClientConn, func(), error) {
		return resolver.DialQueue(ctx, logger, pinnedQueueAddress, config.CellIngressRegistryDialAddress(), retryMetrics)
	})
	if err != nil {
		logger.Fatal("Failed to connect to queue: %v", err)
	}
	defer func() { _ = queue.Close() }()

	if pinnedQueueAddress == "" {
		logger.Info("Connected to queue via registry resolution")
	}

	metricsAddr := fmt.Sprintf(":%d", config.CellIngressMetricsEffectiveListenPort())
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Cell ingress", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	addr := config.CellIngressListenAddr()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Listen: %v", err)
	}

	handler := cellingress.NewQueueServer(config.CellID(), queue, logger).Handler()
	httpSrv := cellingress.HTTPServer(addr, handler)
	logger.Info("Cell ingress listening on http://%s", addr)

	if err := cli.ServeHTTP(ctx, httpSrv, func() error { return httpSrv.Serve(ln) }, 10*time.Second, "Cell ingress HTTP", logger); err != nil {
		logger.Fatal("Cell ingress HTTP server failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-cell-ingress",
	Short: "Private HTTP ingress for cell-local execution submissions",
	Run:   runCellIngress,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	rootCmd.PersistentFlags().String("host", config.CellIngressHost(), "Host/IP for the cell ingress HTTP server to bind")
	rootCmd.PersistentFlags().Int("port", config.CellIngressPort(), "HTTP port for the cell ingress server")
	rootCmd.PersistentFlags().Int("metrics-port", config.CellIngressMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().String("queue-address", config.CellIngressQueueAddress(), "Pinned local queue gRPC address; empty uses registry discovery")
	rootCmd.PersistentFlags().String("registry-address", config.CellIngressRegistryAddress(), "Registry gRPC address for queue discovery")
	_ = viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("cell_ingress.queue.address", rootCmd.PersistentFlags().Lookup("queue-address"))
	_ = viper.BindPFlag("cell_ingress.registry.address", rootCmd.PersistentFlags().Lookup("registry-address"))
	_ = viper.BindEnv("host", "VECTIS_CELL_INGRESS_HOST")
	_ = viper.BindEnv("port", "VECTIS_CELL_INGRESS_PORT")
	_ = viper.BindEnv("metrics_port", "VECTIS_CELL_INGRESS_METRICS_PORT")
	_ = viper.BindEnv("cell_ingress.queue.address", "VECTIS_CELL_INGRESS_QUEUE_ADDRESS")
	_ = viper.BindEnv("cell_ingress.registry.address", "VECTIS_CELL_INGRESS_REGISTRY_ADDRESS")
	viper.SetDefault("host", config.CellIngressHost())
	viper.SetDefault("port", config.CellIngressPort())
	viper.SetDefault("metrics_port", config.CellIngressMetricsPort())
	viper.SetEnvPrefix("VECTIS_CELL_INGRESS")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
