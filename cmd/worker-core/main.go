package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/action/actionconfig"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/platform"
	"vectis/internal/registry"
	"vectis/internal/workercore"
	workersdk "vectis/sdk/workercore"
)

func runWorkerCore(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	logger := interfaces.NewAsyncLogger("worker-core")
	defer func() { _ = logger.Close() }()

	cli.SetLogLevel(logger)

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("Invalid metrics TLS config: %v", err)
	}
	config.StartMetricsTLSReloadLoop(ctx)

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(ctx, "vectis-worker-core")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}
	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, config.WorkerCoreMetricsListenAddr(), "Worker core", logger)
	if err != nil {
		logger.Fatal("Failed to start metrics server: %v", err)
	}
	defer metricsSrv.Shutdown()

	socketPath := strings.TrimSpace(viper.GetString("socket"))
	if socketPath == "" {
		socketPath = workercore.DefaultCoreSocketPath()
	}

	socketPath, err = workercore.SocketPathFromEndpoint(socketPath)
	if err != nil {
		logger.Fatal("Invalid worker core socket: %v", err)
	}

	executorConfig, err := workerCoreExecutorConfig()
	if err != nil {
		logger.Fatal("Invalid worker core source cache config: %v", err)
	}

	executor, backend, err := workercore.NewJobExecutor(executorConfig)
	if err != nil {
		logger.Fatal("Invalid worker core execution backend: %v", err)
	}

	actionResolver, err := actionconfig.DescriptorResolver()
	if err != nil {
		logger.Fatal("Invalid action registry config: %v", err)
	}

	backend, defaultIsolation, supportedIsolation := workercore.ExecutionCapabilitiesForBackend(backend)
	service := workercore.NewService(workercore.NewExecutorCore(
		executor,
		workercore.WithExecutorCheckoutCacheRoot(executorConfig.CheckoutCacheRoot),
	), workercore.ServiceOptions{
		Logger:         logger,
		ActionResolver: actionResolver,
		Description: workercore.CoreDescription{
			ProtocolVersion:    workercore.ProtocolVersion,
			Capabilities:       workerCoreCapabilities(executorConfig.CheckoutCacheRoot),
			SupportedIsolation: supportedIsolation,
			Metadata: map[string]string{
				registry.MetadataWorkerExecutionBackend: backend,
				registry.MetadataWorkerDefaultIsolation: defaultIsolation,
			},
		},
	})

	grpcServer, listener, err := workercore.NewUnixCoreServerContext(ctx, socketPath, service)
	if err != nil {
		logger.Fatal("Failed to create worker core server: %v", err)
	}

	go func() {
		<-ctx.Done()
		interfaces.TerminateActiveProcesses()
		grpcServer.GracefulStop()
		_ = os.Remove(socketPath)
	}()

	logger.Info("Worker core listening on %s", socketPath)
	logger.Info("Worker core execution backend: %s", backend)
	if strings.TrimSpace(executorConfig.CheckoutCacheRoot) != "" && len(executorConfig.CheckoutCacheRemoteURLs) > 0 {
		logger.Info("Worker core checkout cache enabled: root=%s persistent_remotes=%d", executorConfig.CheckoutCacheRoot, len(executorConfig.CheckoutCacheRemoteURLs))
	}

	if err := grpcServer.Serve(listener); err != nil && ctx.Err() == nil {
		logger.Fatal("Worker core server failed: %v", err)
	}
}

func workerCoreCapabilities(checkoutCacheRoot string) []workercore.CoreCapability {
	capabilities := []workercore.CoreCapability{
		{Name: workersdk.CapabilityExecute, Version: "v1"},
		{Name: workersdk.CapabilityCancelTask, Version: "v1"},
		{Name: workersdk.CapabilityShellLogCallback, Version: "v1"},
		{Name: workersdk.CapabilityShellArtifactPush, Version: "v1"},
	}

	if strings.TrimSpace(checkoutCacheRoot) != "" {
		capabilities = append(capabilities, workercore.CoreCapability{Name: workersdk.CapabilityCheckoutCacheWarm, Version: "v1"})
	}

	return capabilities
}

func workerCoreExecutorConfig() (workercore.ExecutorConfig, error) {
	persistentRemotes, err := workerCorePersistentCheckoutCacheRemoteURLs()
	if err != nil {
		return workercore.ExecutorConfig{}, err
	}

	checkoutCacheRoot := strings.TrimSpace(viper.GetString("checkout_cache_root"))
	if checkoutCacheRoot == "" {
		checkoutCacheRoot = config.WorkerExecutionCheckoutCacheRoot()
	}

	return workercore.ExecutorConfig{
		Backend:                 viper.GetString("execution_backend"),
		WorkspaceRoot:           viper.GetString("workspace_root"),
		CheckoutCacheRoot:       checkoutCacheRoot,
		CheckoutCacheRemoteURLs: persistentRemotes,
		Lima: platform.VirtualMachineConfig{
			Provider:           platform.VirtualMachineProviderLima,
			Instance:           viper.GetString("lima_instance"),
			ProviderPath:       viper.GetString("lima_path"),
			GuestWorkspaceRoot: viper.GetString("lima_guest_workspace_root"),
			Start:              viper.GetBool("lima_start"),
			PreserveEnv:        viper.GetBool("lima_preserve_env"),
		},
	}, nil
}

func workerCorePersistentCheckoutCacheRemoteURLs() ([]string, error) {
	decls, err := config.SourceRepositoryDeclarations()
	if err != nil {
		return nil, err
	}

	seen := make(map[string]struct{}, len(decls))
	out := make([]string, 0, len(decls))
	for _, decl := range decls {
		if strings.TrimSpace(decl.WorkerCacheMode) != dal.SourceWorkerCacheModePersistent {
			continue
		}

		for _, remoteURL := range append([]string{decl.CanonicalURL}, decl.FallbackRemoteURLs...) {
			remoteURL = strings.TrimSpace(remoteURL)
			if remoteURL == "" {
				continue
			}

			if _, ok := seen[remoteURL]; ok {
				continue
			}

			seen[remoteURL] = struct{}{}
			out = append(out, remoteURL)
		}
	}

	return out, nil
}

var rootCmd = &cobra.Command{
	Use:   "vectis-worker-core",
	Short: "Vectis Worker Core",
	Long:  `The Vectis Worker Core executes claimed worker tasks behind the worker shell/core UDS boundary.`,
	Run:   runWorkerCore,
}

func init() {
	cli.ConfigureVersion(rootCmd)

	rootCmd.PersistentFlags().String("socket", workercore.DefaultCoreSocketPath(), "Unix socket served by the worker core")
	rootCmd.PersistentFlags().String("metrics-host", config.WorkerCoreMetricsHost(), "Host/IP for the Prometheus /metrics HTTP server to bind")
	rootCmd.PersistentFlags().Int("metrics-port", config.WorkerCoreMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().String("execution-backend", workercore.ExecutionBackendHost, "Command execution backend: host or lima")
	rootCmd.PersistentFlags().String("workspace-root", "", "Parent directory for automatically-created run workspaces")
	rootCmd.PersistentFlags().String("checkout-cache-root", "", "Persistent worker checkout cache root for source repositories with worker_cache_mode=persistent")
	rootCmd.PersistentFlags().String("lima-path", "", "Path to limactl when --execution-backend=lima")
	rootCmd.PersistentFlags().String("lima-instance", "", "Lima instance name when --execution-backend=lima")
	rootCmd.PersistentFlags().String("lima-guest-workspace-root", "", "Guest-side parent directory for Lima workspaces")
	rootCmd.PersistentFlags().Bool("lima-start", false, "Start the Lima instance before each command when --execution-backend=lima")
	rootCmd.PersistentFlags().Bool("lima-preserve-env", false, "Preserve host environment variables in Lima shell commands")

	_ = viper.BindPFlag("socket", rootCmd.PersistentFlags().Lookup("socket"))
	_ = viper.BindPFlag("metrics_host", rootCmd.PersistentFlags().Lookup("metrics-host"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("execution_backend", rootCmd.PersistentFlags().Lookup("execution-backend"))
	_ = viper.BindPFlag("workspace_root", rootCmd.PersistentFlags().Lookup("workspace-root"))
	_ = viper.BindPFlag("checkout_cache_root", rootCmd.PersistentFlags().Lookup("checkout-cache-root"))
	_ = viper.BindPFlag("lima_path", rootCmd.PersistentFlags().Lookup("lima-path"))
	_ = viper.BindPFlag("lima_instance", rootCmd.PersistentFlags().Lookup("lima-instance"))
	_ = viper.BindPFlag("lima_guest_workspace_root", rootCmd.PersistentFlags().Lookup("lima-guest-workspace-root"))
	_ = viper.BindPFlag("lima_start", rootCmd.PersistentFlags().Lookup("lima-start"))
	_ = viper.BindPFlag("lima_preserve_env", rootCmd.PersistentFlags().Lookup("lima-preserve-env"))

	viper.SetEnvPrefix("VECTIS_WORKER_CORE")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
