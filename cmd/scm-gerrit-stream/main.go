package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	scmgerrit "vectis/extensions/scm/gerrit"
	"vectis/internal/action/actionconfig"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/queueclient"
	"vectis/internal/scmstream"
	"vectis/internal/scmtrigger"
	"vectis/sdk/scm"

	_ "vectis/internal/dbdrivers"
)

type gerritStreamRouter interface {
	HandleEvent(context.Context, scmstream.EventTarget, scm.Event) (scmstream.RouteResult, error)
}

func runSCMGerritStream(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	logger := interfaces.NewAsyncLogger("scm-gerrit-stream")
	defer logger.Close()

	cli.SetLogLevel(logger)
	logger.Info("Starting Gerrit SCM stream bridge...")

	baseURL := strings.TrimSpace(viper.GetString("url"))
	if baseURL == "" {
		logger.Fatal("Gerrit stream bridge requires --url")
	}

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonClientOnly); err != nil {
		logger.Fatal("%v", err)
	}

	if err := config.ValidateCellIngressHTTPClientMTLSConfig(config.CellIngressEndpointSpecs()); err != nil {
		logger.Fatal("Cell ingress HTTP mTLS config: %v", err)
	}

	config.StartGRPCTLSReloadLoop(ctx)
	db, _, err := database.OpenReadyDBForRole(logger, database.RoleGlobal)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	repos := dal.NewSQLRepositoriesWithCellID(db, config.CellID())
	actionResolver, err := actionconfig.DescriptorResolver()
	if err != nil {
		logger.Fatal("Invalid action registry config: %v", err)
	}

	retryMetrics, err := observability.NewRetryMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize retry metrics: %v", err)
	}

	pin := config.SCMGerritStreamQueueAddress()
	mq, err := queueclient.NewManagingQueuePoolService(ctx, logger, queueclient.QueuePoolOptions{
		PinnedAddress:   pin,
		RegistryAddress: config.SCMGerritStreamRegistryDialAddress(),
		RetryMetrics:    retryMetrics,
	})

	if err != nil {
		logger.Fatal("Failed to connect to queue: %v", err)
	}
	defer func() { _ = mq.Close() }()

	if pin == "" {
		logger.Info("Connected to queue via registry resolution")
	}

	processor := scmtrigger.Processor{
		Events:         repos.SCMPollTriggers(),
		Jobs:           repos.Jobs(),
		Runs:           repos.Runs(),
		Dispatch:       repos.DispatchEvents(),
		Invocations:    repos.TriggerInvocations(),
		QueueClient:    mq,
		Logger:         logger,
		Clock:          interfaces.SystemClock{},
		ActionResolver: actionResolver,
		Source:         dal.DispatchSourceSCMGerritStream,
		SourceInstance: streamInstanceID(viper.GetString("instance_id")),
	}

	router := scmstream.Router{
		Specs:     repos.SCMPollTriggers(),
		Processor: processor,
		Logger:    logger,
		Matcher: func(spec dal.SCMPollTriggerSpec, event scm.Event) bool {
			return scmgerrit.StreamEventMatchesQuery(event, spec.Query)
		},
		Limit: viper.GetInt("spec_limit"),
	}

	streamOpts := scmgerrit.StreamOptions{
		Provider: "gerrit",
		BaseURL:  baseURL,
	}

	err = consumeConfiguredStream(ctx, streamOpts, func(ctx context.Context, event scm.Event) error {
		result, err := routeGerritStreamEvent(ctx, baseURL, router, event)
		if err != nil {
			return err
		}

		if result.Handled > 0 {
			logger.Info("Gerrit stream event %s routed to %d trigger(s)", event.Key, result.Handled)
		}

		return nil
	}, logger)

	if err != nil {
		logger.Fatal("Gerrit stream bridge failed: %v", err)
	}
}

func routeGerritStreamEvent(ctx context.Context, baseURL string, router gerritStreamRouter, event scm.Event) (scmstream.RouteResult, error) {
	if router == nil {
		return scmstream.RouteResult{}, fmt.Errorf("gerrit stream router is required")
	}

	info, err := scmgerrit.StreamEventInfoFromEvent(event)
	if err != nil {
		return scmstream.RouteResult{}, err
	}

	return router.HandleEvent(ctx, scmstream.EventTarget{
		Provider: "gerrit",
		BaseURL:  baseURL,
		Project:  info.Project,
		Branch:   info.Branch,
	}, event)
}

func streamInstanceID(id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return "scm-gerrit-stream"
	}

	return id
}

var rootCmd = &cobra.Command{
	Use:   "vectis-scm-gerrit-stream",
	Short: "Vectis Gerrit SCM Stream Bridge",
	Long:  `The Vectis Gerrit SCM Stream Bridge consumes Gerrit stream-events JSON and dispatches deduplicated SCM trigger events.`,
	Run:   runSCMGerritStream,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	_ = viper.BindEnv("cell_ingress_endpoints", "VECTIS_SCM_GERRIT_STREAM_CELL_INGRESS_ENDPOINTS", "VECTIS_CELL_INGRESS_ENDPOINTS")
	_ = viper.BindEnv("scm_gerrit_stream.queue.address", "VECTIS_SCM_GERRIT_STREAM_QUEUE_ADDRESS")
	_ = viper.BindEnv("scm_gerrit_stream.registry.address", "VECTIS_SCM_GERRIT_STREAM_REGISTRY_ADDRESS")
	rootCmd.PersistentFlags().String("url", "", "Gerrit base URL used to normalize stream events and match trigger specs")
	rootCmd.PersistentFlags().String("transport", "auto", "Stream transport: auto, input, or ssh")
	rootCmd.PersistentFlags().String("input", "-", "Path to newline-delimited Gerrit stream JSON, or '-' for stdin")
	rootCmd.PersistentFlags().String("ssh-host", "", "Gerrit SSH host for managed stream mode")
	rootCmd.PersistentFlags().Int("ssh-port", defaultGerritSSHPort, "Gerrit SSH port for managed stream mode")
	rootCmd.PersistentFlags().String("ssh-user", "", "Gerrit SSH username; defaults to the current user when omitted")
	rootCmd.PersistentFlags().String("ssh-key-file", "", "SSH private key file for managed stream mode")
	rootCmd.PersistentFlags().Bool("ssh-use-agent", true, "Use SSH_AUTH_SOCK agent identities for managed stream mode")
	rootCmd.PersistentFlags().String("ssh-known-hosts-file", "", "Known hosts file for Gerrit SSH host-key verification; defaults to ~/.ssh/known_hosts")
	rootCmd.PersistentFlags().Bool("ssh-insecure-ignore-host-key", false, "Disable SSH host-key verification; intended only for local development")
	rootCmd.PersistentFlags().String("ssh-command", defaultGerritSSHCommand, "Remote Gerrit SSH stream command")
	rootCmd.PersistentFlags().Duration("ssh-connect-timeout", 10*time.Second, "Timeout for establishing the Gerrit SSH connection")
	rootCmd.PersistentFlags().Duration("ssh-reconnect-base-delay", time.Second, "Initial delay before reconnecting a dropped Gerrit SSH stream")
	rootCmd.PersistentFlags().Duration("ssh-reconnect-max-delay", 30*time.Second, "Maximum delay before reconnecting a dropped Gerrit SSH stream")
	rootCmd.PersistentFlags().Int("ssh-reconnect-max-attempts", 0, "Maximum managed SSH reconnect attempts; 0 retries forever until shutdown")
	rootCmd.PersistentFlags().String("instance-id", "", "Stable Gerrit stream bridge instance identifier recorded on trigger invocations and dispatch events")
	rootCmd.PersistentFlags().Int("spec-limit", scmstream.DefaultSpecLimit, "Maximum enabled Gerrit trigger specs to evaluate per stream event")
	rootCmd.PersistentFlags().String("queue-address", config.SCMGerritStreamQueueAddress(), "Pinned queue gRPC address")
	rootCmd.PersistentFlags().String("registry-address", config.SCMGerritStreamRegistryAddress(), "Registry gRPC address for queue discovery")
	_ = viper.BindPFlag("url", rootCmd.PersistentFlags().Lookup("url"))
	_ = viper.BindPFlag("transport", rootCmd.PersistentFlags().Lookup("transport"))
	_ = viper.BindPFlag("input", rootCmd.PersistentFlags().Lookup("input"))
	_ = viper.BindPFlag("ssh_host", rootCmd.PersistentFlags().Lookup("ssh-host"))
	_ = viper.BindPFlag("ssh_port", rootCmd.PersistentFlags().Lookup("ssh-port"))
	_ = viper.BindPFlag("ssh_user", rootCmd.PersistentFlags().Lookup("ssh-user"))
	_ = viper.BindPFlag("ssh_key_file", rootCmd.PersistentFlags().Lookup("ssh-key-file"))
	_ = viper.BindPFlag("ssh_use_agent", rootCmd.PersistentFlags().Lookup("ssh-use-agent"))
	_ = viper.BindPFlag("ssh_known_hosts_file", rootCmd.PersistentFlags().Lookup("ssh-known-hosts-file"))
	_ = viper.BindPFlag("ssh_insecure_ignore_host_key", rootCmd.PersistentFlags().Lookup("ssh-insecure-ignore-host-key"))
	_ = viper.BindPFlag("ssh_command", rootCmd.PersistentFlags().Lookup("ssh-command"))
	_ = viper.BindPFlag("ssh_connect_timeout", rootCmd.PersistentFlags().Lookup("ssh-connect-timeout"))
	_ = viper.BindPFlag("ssh_reconnect_base_delay", rootCmd.PersistentFlags().Lookup("ssh-reconnect-base-delay"))
	_ = viper.BindPFlag("ssh_reconnect_max_delay", rootCmd.PersistentFlags().Lookup("ssh-reconnect-max-delay"))
	_ = viper.BindPFlag("ssh_reconnect_max_attempts", rootCmd.PersistentFlags().Lookup("ssh-reconnect-max-attempts"))
	_ = viper.BindPFlag("instance_id", rootCmd.PersistentFlags().Lookup("instance-id"))
	_ = viper.BindPFlag("spec_limit", rootCmd.PersistentFlags().Lookup("spec-limit"))
	_ = viper.BindPFlag("scm_gerrit_stream.queue.address", rootCmd.PersistentFlags().Lookup("queue-address"))
	_ = viper.BindPFlag("scm_gerrit_stream.registry.address", rootCmd.PersistentFlags().Lookup("registry-address"))
	viper.SetDefault("transport", "auto")
	viper.SetDefault("input", "-")
	viper.SetDefault("ssh_port", defaultGerritSSHPort)
	viper.SetDefault("ssh_use_agent", true)
	viper.SetDefault("ssh_command", defaultGerritSSHCommand)
	viper.SetDefault("ssh_connect_timeout", 10*time.Second)
	viper.SetDefault("ssh_reconnect_base_delay", time.Second)
	viper.SetDefault("ssh_reconnect_max_delay", 30*time.Second)
	viper.SetDefault("instance_id", "")
	viper.SetDefault("spec_limit", scmstream.DefaultSpecLimit)
	viper.SetEnvPrefix("VECTIS_SCM_GERRIT_STREAM")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
