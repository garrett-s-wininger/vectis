package main

import (
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
	claimValidator, ok := repos.Runs().(secrets.ExecutionClaimValidator)
	if !ok {
		logger.Fatal("Runs repository cannot validate active execution claims")
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
	api.RegisterSecretsServiceServer(grpcServer, secrets.NewServer(provider, secrets.NewClaimAuthorizer(claimValidator)))

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
