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
	defer func() { _ = logger.Close() }()

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

	secretMetrics, err := observability.NewSecretsMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize secrets metrics: %v", err)
	}

	db, _, err := database.OpenReadyDBForRole(logger, database.RoleCell)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer func() { _ = db.Close() }()

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
	var listenConfig net.ListenConfig
	ln, err := listenConfig.Listen(ctx, "tcp", addr)
	if err != nil {
		logger.Fatal("Failed to listen: %v", err)
	}

	srvOpts, err := config.GRPCServerOptionsForRole(config.ServiceIdentityRoleSecrets)
	if err != nil {
		logger.Fatal("grpc tls: %v", err)
	}

	accessPolicy, err := secrets.NewAccessPolicy(config.SecretsPolicyAllowRules())
	if err != nil {
		logger.Fatal("Invalid secret access policy: %v", err)
	}

	providerSet := secrets.NewProviderSet()
	providerConfigured := false
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

		if err := providerSet.Register(secrets.EncryptedFSScheme, fsProvider); err != nil {
			logger.Fatal("Failed to register encryptedfs secret provider: %v", err)
		}

		providerConfigured = true
		logger.Info("Using encryptedfs secret provider rooted at %s with key file %s", root, keyFile)
	} else {
		logger.Info("Secret provider is not configured; set --encryptedfs-root or VECTIS_SECRETS_ENCRYPTEDFS_ROOT to enable encryptedfs resolution")
	}

	provider := secrets.Provider(secrets.UnconfiguredProvider{})
	if providerConfigured {
		provider = providerSet
	}

	grpcServer := grpc.NewServer(srvOpts...)
	authorizer := secrets.NewClaimAuthorizer(
		claimValidator,
		secrets.WithExecutionScopeResolver(executionScopeResolver{store: activeExecutions}),
		secrets.WithAccessPolicy(accessPolicy),
	)

	api.RegisterSecretsServiceServer(grpcServer, secrets.NewServer(
		provider,
		authorizer,
		secrets.WithLogger(logger),
		secrets.WithMetrics(secretMetrics),
	))

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

type executionScopeResolver struct {
	store activeExecutionDispatchStore
}

func (r executionScopeResolver) ResolveExecutionScope(ctx context.Context, runID, executionID string) (secrets.ExecutionScope, error) {
	if r.store == nil {
		return secrets.ExecutionScope{}, errors.New("active execution store is not configured")
	}

	if !config.WorkerExecutionIdentityEnabled() {
		return secrets.ExecutionScope{}, errors.New("worker execution identity is disabled")
	}

	dispatch, err := r.store.GetActiveExecutionDispatch(ctx, runID, executionID)
	if err != nil {
		return secrets.ExecutionScope{}, err
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
		return secrets.ExecutionScope{}, err
	}

	return secrets.ExecutionScope{
		SPIFFEID:          identity.SPIFFEID,
		TrustDomain:       identity.TrustDomain,
		NamespacePath:     identity.NamespacePath,
		CellID:            identity.CellID,
		JobID:             identity.JobID,
		RunID:             identity.RunID,
		RunIndex:          dispatch.RunIndex,
		TaskID:            dispatch.TaskID,
		TaskKey:           dispatch.TaskKey,
		SegmentID:         dispatch.SegmentID,
		ExecutionID:       identity.ExecutionID,
		Attempt:           dispatch.Attempt,
		DefinitionVersion: dispatch.DefinitionVersion,
		DefinitionHash:    dispatch.DefinitionHash,
	}, nil
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
	viper.SetDefault("policy_allow", config.SecretsPolicyAllowRules())

	rootCmd.PersistentFlags().Int("port", config.SecretsPort(), "Port for the secrets gRPC service")
	rootCmd.PersistentFlags().String("metrics-host", config.SecretsMetricsHost(), "Host/IP for the Prometheus /metrics HTTP server to bind")
	rootCmd.PersistentFlags().Int("metrics-port", config.SecretsMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().String("encryptedfs-root", "", "Root directory for encryptedfs secret files")
	rootCmd.PersistentFlags().String("encryptedfs-key-file", "", "32-byte, hex, or base64 key file for encryptedfs secret envelopes")
	rootCmd.PersistentFlags().StringSlice("allow-secret", config.SecretsPolicyAllowRules(), "Secret access allow rule in namespace=...;job=...;task=...;ref=... form; may be repeated")

	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	_ = viper.BindPFlag("metrics_host", rootCmd.PersistentFlags().Lookup("metrics-host"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("encryptedfs_root", rootCmd.PersistentFlags().Lookup("encryptedfs-root"))
	_ = viper.BindPFlag("encryptedfs_key_file", rootCmd.PersistentFlags().Lookup("encryptedfs-key-file"))
	_ = viper.BindPFlag("policy_allow", rootCmd.PersistentFlags().Lookup("allow-secret"))
	_ = viper.BindEnv("secrets.encryptedfs.root", "VECTIS_SECRETS_ENCRYPTEDFS_ROOT")
	_ = viper.BindEnv("secrets.encryptedfs.key_file", "VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE")
	_ = viper.BindEnv("policy_allow", "VECTIS_SECRETS_POLICY_ALLOW")
	_ = viper.BindEnv("secrets.policy.allow", "VECTIS_SECRETS_POLICY_ALLOW")

	viper.SetEnvPrefix("VECTIS_SECRETS")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
