package main

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	scmgerrit "vectis/extensions/scm/gerrit"
	scmgit "vectis/extensions/scm/git"
	"vectis/internal/action/actionconfig"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/scmpoller"

	_ "vectis/internal/dbdrivers"
)

func runSCMPoller(cmd *cobra.Command, args []string) {
	logger := interfaces.NewAsyncLogger("scm-poller")
	defer logger.Close()

	cli.SetLogLevel(logger)
	logger.Info("Starting SCM poller service...")

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonClientOnly); err != nil {
		logger.Fatal("%v", err)
	}

	if err := config.ValidateCellIngressHTTPClientMTLSConfig(config.CellIngressEndpointSpecs()); err != nil {
		logger.Fatal("Cell ingress HTTP mTLS config: %v", err)
	}

	config.StartGRPCTLSReloadLoop(cmd.Context())
	db, _, err := database.OpenReadyDBForRole(logger, database.RoleGlobal)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	service := scmpoller.NewService(logger, db)
	defer service.CloseQueueDial()

	service.RegisterProvider("git", scmgit.NewProvider())
	gerritProvider, err := scmgerrit.ConfigFromViper(viper.GetViper()).NewProvider()
	if err != nil {
		logger.Fatal("Invalid Gerrit SCM provider config: %v", err)
	}

	service.RegisterProvider("gerrit", gerritProvider)
	service.SetInstanceID(viper.GetString("instance_id"))
	service.SetClaimTTL(config.SCMPollerClaimTTL())
	actionResolver, err := actionconfig.DescriptorResolver()
	if err != nil {
		logger.Fatal("Invalid action registry config: %v", err)
	}

	service.SetActionDescriptorResolver(actionResolver)

	retryMetrics, err := observability.NewRetryMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize retry metrics: %v", err)
	}

	service.SetRetryMetrics(retryMetrics)

	if err := service.ConnectToQueue(cmd.Context()); err != nil {
		logger.Fatal("Failed to connect to queue: %v", err)
	}

	interval := config.SCMPollerInterval()
	logger.Info("SCM poller instance ID: %s; polling every %v; claim ttl %v", service.InstanceID(), interval, config.SCMPollerClaimTTL())

	if err := service.Run(cmd.Context(), interval); err != nil {
		logger.Fatal("SCM poller failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-scm-poller",
	Short: "Vectis SCM Poller",
	Long:  `The Vectis SCM Poller polls source-control providers such as generic Git and records deduplicated trigger events.`,
	Run:   runSCMPoller,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	_ = viper.BindEnv("cell_ingress_endpoints", "VECTIS_SCM_POLLER_CELL_INGRESS_ENDPOINTS", "VECTIS_CELL_INGRESS_ENDPOINTS")
	rootCmd.PersistentFlags().String("instance-id", "", "Stable SCM poller instance identifier used in trigger claim tokens")
	rootCmd.PersistentFlags().Duration("interval", config.SCMPollerInterval(), "How often to scan for due SCM poll triggers")
	rootCmd.PersistentFlags().Duration("claim-ttl", config.SCMPollerClaimTTL(), "How long an SCM poller instance owns a trigger claim")
	scmgerrit.AddConfigFlags(rootCmd.PersistentFlags())
	_ = viper.BindPFlag("instance_id", rootCmd.PersistentFlags().Lookup("instance-id"))
	_ = viper.BindPFlag("interval", rootCmd.PersistentFlags().Lookup("interval"))
	_ = viper.BindPFlag("claim_ttl", rootCmd.PersistentFlags().Lookup("claim-ttl"))
	if err := scmgerrit.BindConfig(viper.GetViper(), rootCmd.PersistentFlags()); err != nil {
		panic(err)
	}

	viper.SetDefault("interval", config.SCMPollerInterval())
	viper.SetDefault("claim_ttl", config.SCMPollerClaimTTL())
	viper.SetDefault("instance_id", "")
	viper.SetEnvPrefix("VECTIS_SCM_POLLER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
