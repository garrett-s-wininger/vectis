package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	apigen "vectis/api/gen/go"
	"vectis/internal/api"
	"vectis/internal/api/audit"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/registry"

	_ "vectis/internal/dbdrivers"
)

func buildAccessLogger(format string) (*slog.Logger, func() error) {
	if strings.EqualFold(format, "json") {
		handler := observability.NewAsyncSlogHandler(slog.NewJSONHandler(interfaces.NewLogOutput("api-access"), &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}), 0)

		return slog.New(handler), handler.Close
	}

	return nil, nil
}

func runVectisAPI(cmd *cobra.Command, args []string) {
	logger := interfaces.NewAsyncLogger("api")
	defer logger.Close()

	cli.SetLogLevel(logger)
	logger.Info("Starting API server...")

	exitCode := 0
	defer func() {
		if exitCode != 0 {
			_ = logger.Close()
			os.Exit(exitCode)
		}
	}()

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonClientOnly); err != nil {
		logger.Error("%v", err)
		exitCode = 1
		return
	}

	config.StartGRPCTLSReloadLoop(cmd.Context())

	db, _, err := database.OpenReadyDB(logger)
	if err != nil {
		logger.Error("Failed to initialize database: %v", err)
		exitCode = 1
		return
	}
	defer db.Close()

	authCtx, authCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer authCancel()

	if err := config.ValidateAPIAuthConfig(authCtx, dal.NewSQLRepositories(db).Auth()); err != nil {
		logger.Error("%v", err)
		exitCode = 1
		return
	}

	if err := config.ValidateAPIClientIPConfig(); err != nil {
		logger.Error("%v", err)
		exitCode = 1
		return
	}

	shutdownTracer, err := observability.InitTracer(cmd.Context(), "vectis-api")
	if err != nil {
		logger.Error("Failed to initialize tracer: %v", err)
		exitCode = 1
		return
	}
	defer cli.DeferShutdown(logger, "Tracer", shutdownTracer)()

	metricsHandler, shutdownMetrics, err := observability.InitAPIMetrics(cmd.Context())
	if err != nil {
		logger.Error("Failed to initialize metrics: %v", err)
		exitCode = 1
		return
	}

	if err := observability.RegisterSQLDBPoolMetrics(db); err != nil {
		logger.Error("Failed to register DB pool metrics: %v", err)
		exitCode = 1
		return
	}

	if err := observability.RegisterRetentionStorageMetrics(db); err != nil {
		logger.Error("Failed to register retention storage metrics: %v", err)
		exitCode = 1
		return
	}

	auditMetrics, err := observability.NewAuditMetrics()
	if err != nil {
		logger.Error("Failed to register audit metrics: %v", err)
		exitCode = 1
		return
	}

	retryMetrics, err := observability.NewRetryMetrics()
	if err != nil {
		logger.Error("Failed to initialize retry metrics: %v", err)
		exitCode = 1
		return
	}

	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	server := api.NewAPIServer(logger, db)
	server.MetricsHandler = metricsHandler
	accessLogger, closeAccessLogger := buildAccessLogger(config.APILogFormat())
	if closeAccessLogger != nil {
		defer func() { _ = closeAccessLogger() }()
	}
	server.AccessLogger = accessLogger

	// Wire up async auditor for production audit logging.
	auditOverrides, err := audit.ParseDurabilityOverrides(config.APIAuditDurabilityOverrides())
	if err != nil {
		logger.Error("Invalid audit config: %v", err)
		exitCode = 1
		return
	}

	auditPolicy := audit.Policy{
		Enabled:   config.APIAuditEnabled(),
		Overrides: auditOverrides,
	}

	auditor := audit.NewAsyncAuditorWithMetrics(&audit.DALRepository{Auth: dal.NewSQLRepositories(db).Auth()}, slog.Default(), auditMetrics)
	defer auditor.Stop()
	server.SetAuditor(auditor)
	server.SetAuditPolicy(auditPolicy)

	server.SetRetryMetrics(retryMetrics)

	// Wire up worker address resolution via registry for cancel endpoint.
	if regAddr := config.APIRegistryDialAddress(); regAddr != "" {
		regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
		registryClient, err := registry.New(regCtx, regAddr, logger, interfaces.SystemClock{}, retryMetrics)
		regCancel()
		if err != nil {
			logger.Warn("Failed to create registry client for worker resolution: %v", err)
		} else {
			server.ResolveWorkerAddress = func(ctx context.Context, workerID string) (string, error) {
				return registryClient.InstanceAddress(ctx, apigen.Component_COMPONENT_WORKER, workerID)
			}
			defer registryClient.Close()
		}
	}

	port := config.APIEffectiveListenPort()
	addr := net.JoinHostPort(config.APIHost(), fmt.Sprintf("%d", port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("Listen: %v", err)
		exitCode = 1
		return
	}

	logger.Info("Establishing queue client connection...")
	if err := server.ConnectToQueue(cmd.Context()); err != nil {
		logger.Error("Failed to connect to services: %v", err)
		exitCode = 1
		return
	}
	logger.Info("Queue client ready")

	logger.Info("Establishing log client connection...")
	if err := server.ConnectToLog(cmd.Context()); err != nil {
		logger.Error("Failed to connect to log service: %v", err)
		exitCode = 1
		return
	}
	logger.Info("Log client ready")

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(cmd.Context(), ln)
	}()

	if err := <-serveErr; err != nil {
		logger.Error("Server failed: %v", err)
		exitCode = 1
		return
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-api-server",
	Short: "Vectis API Server",
	Long:  `The Vectis API Server provides REST endpoints for triggering stored jobs.`,
	Run:   runVectisAPI,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	rootCmd.PersistentFlags().String("host", config.APIHost(), "Host/IP for the API server to bind")
	rootCmd.PersistentFlags().Int("port", config.APIPort(), "Port for the API server")
	_ = viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	viper.SetEnvPrefix("VECTIS_API_SERVER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
