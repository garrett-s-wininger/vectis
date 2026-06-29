package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

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

	reader, closeInput, err := openStreamInput(viper.GetString("input"))
	if err != nil {
		logger.Fatal("Failed to open Gerrit stream input: %v", err)
	}
	defer func() { _ = closeInput() }()

	logger.Info("Gerrit stream bridge instance ID: %s; reading %s", processor.InstanceID(), streamInputLabel(viper.GetString("input")))
	err = scmgerrit.ConsumeStream(ctx, reader, scmgerrit.StreamOptions{
		Provider: "gerrit",
		BaseURL:  baseURL,
	}, func(ctx context.Context, event scm.Event) error {
		result, err := routeGerritStreamEvent(ctx, baseURL, router, event)
		if err != nil {
			return err
		}

		if result.Handled > 0 {
			logger.Info("Gerrit stream event %s routed to %d trigger(s)", event.Key, result.Handled)
		}

		return nil
	})

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

func openStreamInput(path string) (io.Reader, func() error, error) {
	path = strings.TrimSpace(path)
	if path == "" || path == "-" {
		return os.Stdin, func() error { return nil }, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	return f, f.Close, nil
}

func streamInputLabel(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || path == "-" {
		return "stdin"
	}

	return path
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
	rootCmd.PersistentFlags().String("input", "-", "Path to newline-delimited Gerrit stream JSON, or '-' for stdin")
	rootCmd.PersistentFlags().String("instance-id", "", "Stable Gerrit stream bridge instance identifier recorded on trigger invocations and dispatch events")
	rootCmd.PersistentFlags().Int("spec-limit", scmstream.DefaultSpecLimit, "Maximum enabled Gerrit trigger specs to evaluate per stream event")
	rootCmd.PersistentFlags().String("queue-address", config.SCMGerritStreamQueueAddress(), "Pinned queue gRPC address")
	rootCmd.PersistentFlags().String("registry-address", config.SCMGerritStreamRegistryAddress(), "Registry gRPC address for queue discovery")
	_ = viper.BindPFlag("url", rootCmd.PersistentFlags().Lookup("url"))
	_ = viper.BindPFlag("input", rootCmd.PersistentFlags().Lookup("input"))
	_ = viper.BindPFlag("instance_id", rootCmd.PersistentFlags().Lookup("instance-id"))
	_ = viper.BindPFlag("spec_limit", rootCmd.PersistentFlags().Lookup("spec-limit"))
	_ = viper.BindPFlag("scm_gerrit_stream.queue.address", rootCmd.PersistentFlags().Lookup("queue-address"))
	_ = viper.BindPFlag("scm_gerrit_stream.registry.address", rootCmd.PersistentFlags().Lookup("registry-address"))
	viper.SetDefault("input", "-")
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
