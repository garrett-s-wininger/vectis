package main

import (
	"context"
	"errors"
	"net"
	"os"

	api "vectis/api/gen/go"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	_ "vectis/internal/dbdrivers"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/secrets"
	"vectis/internal/workloadidentity"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func runVectisSecrets(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	logger := interfaces.NewAsyncLogger("secrets")
	defer logger.Close()

	cli.SetLogLevel(logger)
	logger.Info("Starting secrets service for cell %s...", config.CellID())

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonSecrets); err != nil {
		logger.Fatal("%v", err)
	}

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("%v", err)
	}

	config.StartGRPCTLSReloadLoop(ctx)
	config.StartMetricsTLSReloadLoop(ctx)

	shutdownTracer, err := observability.InitTracer(ctx, "vectis-secrets")
	if err != nil {
		logger.Fatal("Failed to initialize tracer: %v", err)
	}
	defer cli.DeferShutdown(logger, "Tracer", shutdownTracer)()

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(ctx, "vectis-secrets")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}
	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	db, _, err := database.OpenReadyDBForRole(logger, database.RoleCell)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	repos := dal.NewSQLRepositoriesWithCellID(db, config.CellID())
	runs := repos.Runs()
	claimValidator, ok := runs.(secrets.ExecutionClaimValidator)
	if !ok {
		logger.Fatal("Runs repository cannot validate active execution claims")
	}

	activeExecutions, ok := runs.(activeExecutionDispatchStore)
	if !ok {
		logger.Fatal("Runs repository cannot resolve active execution workload identities")
	}

	addr := config.SecretsListenAddr()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	srvOpts, err := config.GRPCServerOptionsForRole(config.ServiceIdentityRoleSecrets)
	if err != nil {
		logger.Fatal("grpc tls: %v", err)
	}

	provider := secrets.Provider(secrets.UnconfiguredProvider{})
	if root := config.SecretsEncryptedFSRoot(); root != "" {
		if !config.WorkerExecutionIdentityEnabled() {
			logger.Fatal("encryptedfs secret provider requires worker.execution_identity.enabled=true so workload callers can be authorized")
		}

		if err := config.ValidateWorkerExecutionIdentityConfig(); err != nil {
			logger.Fatal("Invalid worker execution identity config for secret authorization: %v", err)
		}

		keyFile := config.SecretsEncryptedFSKeyFile()
		if keyFile == "" {
			logger.Fatal("encryptedfs secret provider requires --encryptedfs-key-file or VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE")
		}

		fsProvider, err := secrets.NewEncryptedFSProvider(root, secrets.WithEncryptedFSKeyFile(keyFile))
		if err != nil {
			logger.Fatal("Failed to configure encryptedfs secret provider: %v", err)
		}

		provider = fsProvider
		logger.Info("Using encryptedfs secret provider rooted at %s with key file %s", root, keyFile)
	} else {
		logger.Info("Secret provider is not configured; set --encryptedfs-root or VECTIS_SECRETS_ENCRYPTEDFS_ROOT to enable encryptedfs resolution")
	}

	grpcServer := grpc.NewServer(srvOpts...)
	authorizer := secrets.NewClaimAuthorizer(
		claimValidator,
		secrets.WithExpectedWorkloadResolver(executionWorkloadResolver{store: activeExecutions}),
	)

	api.RegisterSecretsServiceServer(grpcServer, secrets.NewServer(provider, authorizer))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, hs)
	hs.SetServingStatus("secrets", healthpb.HealthCheckResponse_SERVING)

	metricsAddr := config.SecretsMetricsListenAddr()
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Secrets", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	logger.Info("Secrets service listening on %s", addr)

	if err := cli.ServeGRPC(ctx, grpcServer, ln, "Secrets", logger); err != nil {
		logger.Error("gRPC server failed: %v", err)
	}
}

type activeExecutionDispatchStore interface {
	GetActiveExecutionDispatch(ctx context.Context, runID, executionID string) (dal.ExecutionDispatchRecord, error)
}

type executionWorkloadResolver struct {
	store activeExecutionDispatchStore
}

func (r executionWorkloadResolver) ExpectedWorkloadSPIFFEID(ctx context.Context, runID, executionID string) (string, error) {
	if r.store == nil {
		return "", errors.New("active execution store is not configured")
	}

	if !config.WorkerExecutionIdentityEnabled() {
		return "", errors.New("worker execution identity is disabled")
	}

	dispatch, err := r.store.GetActiveExecutionDispatch(ctx, runID, executionID)
	if err != nil {
		return "", err
	}

	identity, err := workloadidentity.NewIdentity(
		config.WorkerExecutionIdentityTrustDomain(),
		config.WorkerExecutionIdentityPathTemplate(),
		workloadidentity.Execution{
			CellID:            dispatch.CellID,
			NamespacePath:     dispatch.NamespacePath,
			JobID:             dispatch.JobID,
			RunID:             dispatch.RunID,
			RunIndex:          dispatch.RunIndex,
			SegmentID:         dispatch.SegmentID,
			ExecutionID:       dispatch.ExecutionID,
			Attempt:           dispatch.Attempt,
			DefinitionVersion: dispatch.DefinitionVersion,
			DefinitionHash:    dispatch.DefinitionHash,
		},
	)

	if err != nil {
		return "", err
	}

	return identity.SPIFFEID, nil
}

var rootCmd = &cobra.Command{
	Use:   "vectis-secrets",
	Short: "Vectis secrets service",
	Run:   runVectisSecrets,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	viper.SetDefault("port", config.SecretsPort())
	viper.SetDefault("metrics_host", config.SecretsMetricsHost())
	viper.SetDefault("metrics_port", config.SecretsMetricsPort())
	viper.SetDefault("encryptedfs_root", "")
	viper.SetDefault("encryptedfs_key_file", "")

	rootCmd.PersistentFlags().Int("port", config.SecretsPort(), "Port for the secrets gRPC service")
	rootCmd.PersistentFlags().String("metrics-host", config.SecretsMetricsHost(), "Host/IP for the Prometheus /metrics HTTP server to bind")
	rootCmd.PersistentFlags().Int("metrics-port", config.SecretsMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().String("encryptedfs-root", "", "Root directory for encryptedfs secret files")
	rootCmd.PersistentFlags().String("encryptedfs-key-file", "", "32-byte, hex, or base64 key file for encryptedfs secret envelopes")

	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("metrics_host", rootCmd.PersistentFlags().Lookup("metrics-host"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("encryptedfs_root", rootCmd.PersistentFlags().Lookup("encryptedfs-root"))
	_ = viper.BindPFlag("encryptedfs_key_file", rootCmd.PersistentFlags().Lookup("encryptedfs-key-file"))
	_ = viper.BindEnv("secrets.encryptedfs.root", "VECTIS_SECRETS_ENCRYPTEDFS_ROOT")
	_ = viper.BindEnv("secrets.encryptedfs.key_file", "VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE")

	viper.SetEnvPrefix("VECTIS_SECRETS")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
