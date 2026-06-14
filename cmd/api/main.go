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
	"vectis/internal/action/actionconfig"
	"vectis/internal/api"
	"vectis/internal/api/audit"
	"vectis/internal/api/ratelimit"
	"vectis/internal/cache"
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

func warnIfProcessLocalAPICache(logger interfaces.Logger, backend string, authEnabled bool) {
	if logger == nil || backend != config.APICacheBackendMemory || !authEnabled {
		return
	}

	logger.Warn("API auth is enabled with api.cache.backend=memory; login sessions and rate-limit buckets are process-local and will not be shared across API replicas")
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

	if err := config.ValidateAPIHTTPS(); err != nil {
		logger.Error("%v", err)
		exitCode = 1
		return
	}

	if err := config.ValidateCellIngressHTTPClientMTLSConfig(config.APICellIngressEndpointSpecs()); err != nil {
		logger.Error("Cell ingress HTTP mTLS config: %v", err)
		exitCode = 1
		return
	}

	if err := config.ValidateAPIHSTSConfig(); err != nil {
		logger.Error("%v", err)
		exitCode = 1
		return
	}

	config.StartGRPCTLSReloadLoop(cmd.Context())

	db, _, err := database.OpenReadyDBForRole(logger, database.RoleGlobal)
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

	if err := config.ValidateAPIHostConfig(); err != nil {
		logger.Error("%v", err)
		exitCode = 1
		return
	}

	if err := config.ValidateAPICORSConfig(); err != nil {
		logger.Error("%v", err)
		exitCode = 1
		return
	}

	if err := config.ValidateAPICacheConfig(); err != nil {
		logger.Error("%v", err)
		exitCode = 1
		return
	}

	if err := config.ValidateAPISessionConfig(); err != nil {
		logger.Error("%v", err)
		exitCode = 1
		return
	}

	metricsHandler, shutdownMetrics, err := observability.InitAPIMetrics(cmd.Context())
	if err != nil {
		logger.Error("Failed to initialize metrics: %v", err)
		exitCode = 1
		return
	}
	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

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

	dispatchMetrics, err := observability.NewDispatchMetrics()
	if err != nil {
		logger.Error("Failed to initialize dispatch metrics: %v", err)
		exitCode = 1
		return
	}

	logRoutingMetrics, err := observability.NewLogRoutingMetrics()
	if err != nil {
		logger.Error("Failed to initialize log routing metrics: %v", err)
		exitCode = 1
		return
	}

	apiDispatchMetrics, err := observability.NewAPIDispatchMetrics()
	if err != nil {
		logger.Error("Failed to initialize API dispatch metrics: %v", err)
		exitCode = 1
		return
	}

	apiSecurityMetrics, err := observability.NewAPISecurityMetrics()
	if err != nil {
		logger.Error("Failed to initialize API security metrics: %v", err)
		exitCode = 1
		return
	}

	sourceSyncMetrics, err := observability.NewSourceSyncMetrics()
	if err != nil {
		logger.Error("Failed to initialize source sync metrics: %v", err)
		exitCode = 1
		return
	}

	sourceCtx, sourceCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer sourceCancel()
	sourceRepos := dal.NewSQLRepositories(db)
	sourceCredentialResolver, err := newConfiguredSourceRepositoryCredentialResolver(logger)
	if err != nil {
		logger.Error("Failed to configure source repository credentials: %v", err)
		exitCode = 1
		return
	}

	sourceSyncStatus := configuredSourceRepositorySyncCheckoutStatusWithCredentialResolver(sourceCredentialResolver)
	if err := reconcileConfiguredSourceRepositories(sourceCtx, sourceRepos, logger); err != nil {
		logger.Error("Failed to reconcile configured source repositories: %v", err)
		exitCode = 1
		return
	}

	if err := reconcileConfiguredSourceSchedules(sourceCtx, sourceRepos, logger); err != nil {
		logger.Error("Failed to reconcile configured source schedules: %v", err)
		exitCode = 1
		return
	}

	sourceSyncCtx := sourceCtx
	sourceSyncCancel := func() {}
	if config.SourceSyncConfiguredRepositoriesOnStartup() {
		if timeout := config.SourceSyncRunningTimeout(); timeout > 0 {
			sourceSyncCtx, sourceSyncCancel = context.WithTimeout(context.Background(), timeout)
		} else {
			sourceSyncCtx, sourceSyncCancel = context.WithCancel(context.Background())
		}
	}
	defer sourceSyncCancel()

	if err := syncConfiguredSourceRepositoriesWithStatus(sourceSyncCtx, sourceRepos, logger, sourceSyncStatus, sourceSyncMetrics); err != nil {
		logger.Error("Failed to sync configured source repositories: %v", err)
		exitCode = 1
		return
	}

	sourcePeriodicSyncCtx, sourcePeriodicSyncCancel := context.WithCancel(cmd.Context())
	defer sourcePeriodicSyncCancel()
	startConfiguredSourceRepositoryPeriodicSyncWithStatus(sourcePeriodicSyncCtx, sourceRepos, logger, sourceSyncStatus, sourceSyncMetrics)

	shutdownTracer, err := observability.InitTracer(cmd.Context(), "vectis-api")
	if err != nil {
		logger.Error("Failed to initialize tracer: %v", err)
		exitCode = 1
		return
	}
	defer cli.DeferShutdown(logger, "Tracer", shutdownTracer)()

	actionDescriptorResolver, err := actionconfig.DescriptorResolver()
	if err != nil {
		logger.Error("Invalid action registry config: %v", err)
		exitCode = 1
		return
	}

	actionResolver, err := actionconfig.Resolver()
	if err != nil {
		logger.Error("Invalid action registry config: %v", err)
		exitCode = 1
		return
	}

	server := api.NewAPIServer(logger, db)
	server.SetActionDescriptorResolver(actionDescriptorResolver)
	server.SetActionResolver(actionResolver)
	server.MetricsHandler = metricsHandler
	var cacheService cache.Service
	switch config.APICacheBackend() {
	case config.APICacheBackendDatabase:
		cacheService = cache.NewSQLService(db, database.EffectiveDBDriver())
	case config.APICacheBackendMemory:
		cacheService = cache.NewMemoryService()
	}

	warnIfProcessLocalAPICache(logger, config.APICacheBackend(), config.APIAuthEnabled())
	server.SetCacheService(cacheService)
	server.SetRateLimiter(ratelimit.NewCacheRateLimiter(cacheService))

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
	server.SetDispatchMetrics(dispatchMetrics)
	server.SetLogRoutingMetrics(logRoutingMetrics)
	server.SetAPIDispatchMetrics(apiDispatchMetrics)
	server.SetAPISecurityMetrics(apiSecurityMetrics)
	server.SetSourceSyncMetrics(sourceSyncMetrics)

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

	apiTLSReloader, err := config.NewAPIHTTPSReloader()
	if err != nil {
		_ = ln.Close()
		logger.Error("API TLS: %v", err)
		exitCode = 1
		return
	}

	ln, err = config.APIHTTPSListener(ln, apiTLSReloader)
	if err != nil {
		logger.Error("API TLS: %v", err)
		exitCode = 1
		return
	}
	config.StartAPIHTTPSReloadLoop(cmd.Context(), apiTLSReloader)

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
	rootCmd.PersistentFlags().StringSlice("cell-ingress-endpoint", config.APICellIngressEndpointSpecs(), "Cell ingress route in cell_id=url form; may be repeated")
	rootCmd.PersistentFlags().String("tls-cert-file", config.APIHTTPSCertFile(), "Certificate file for browser-facing HTTPS")
	rootCmd.PersistentFlags().String("tls-key-file", config.APIHTTPSKeyFile(), "Private key file for browser-facing HTTPS")
	rootCmd.PersistentFlags().Duration("tls-reload-interval", config.APIHTTPSReloadInterval(), "How often to poll API HTTPS cert/key files for reload; 0 disables polling")
	rootCmd.PersistentFlags().String("source-credentials-encryptedfs-root", config.SecretsEncryptedFSRoot(), "Encryptedfs root for source repository Git credentials")
	rootCmd.PersistentFlags().String("source-credentials-encryptedfs-key-file", config.SecretsEncryptedFSKeyFile(), "Encryptedfs key file for source repository Git credentials")

	_ = viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("cell_ingress_endpoints", rootCmd.PersistentFlags().Lookup("cell-ingress-endpoint"))
	_ = viper.BindPFlag("api.tls.cert_file", rootCmd.PersistentFlags().Lookup("tls-cert-file"))
	_ = viper.BindPFlag("api.tls.key_file", rootCmd.PersistentFlags().Lookup("tls-key-file"))
	_ = viper.BindPFlag("api.tls.reload_interval", rootCmd.PersistentFlags().Lookup("tls-reload-interval"))
	_ = viper.BindPFlag("secrets.encryptedfs.root", rootCmd.PersistentFlags().Lookup("source-credentials-encryptedfs-root"))
	_ = viper.BindPFlag("secrets.encryptedfs.key_file", rootCmd.PersistentFlags().Lookup("source-credentials-encryptedfs-key-file"))
	_ = viper.BindEnv("cell_ingress_endpoints", "VECTIS_API_SERVER_CELL_INGRESS_ENDPOINTS", "VECTIS_CELL_INGRESS_ENDPOINTS")
	_ = viper.BindEnv("secrets.encryptedfs.root", "VECTIS_API_SERVER_SOURCE_CREDENTIALS_ENCRYPTEDFS_ROOT", "VECTIS_SOURCE_CREDENTIALS_ENCRYPTEDFS_ROOT")
	_ = viper.BindEnv("secrets.encryptedfs.key_file", "VECTIS_API_SERVER_SOURCE_CREDENTIALS_ENCRYPTEDFS_KEY_FILE", "VECTIS_SOURCE_CREDENTIALS_ENCRYPTEDFS_KEY_FILE")

	viper.SetEnvPrefix("VECTIS_API_SERVER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
