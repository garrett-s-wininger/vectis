package main

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/scmpoller"

	_ "vectis/internal/dbdrivers"
)

func runSCMPoller(cmd *cobra.Command, args []string) {
	logger := interfaces.NewAsyncLogger("scm-poller")
	defer logger.Close()

	cli.SetLogLevel(logger)
	logger.Info("Starting SCM poller service...")

	if err := config.ValidateCellIngressHTTPClientMTLSConfig(config.CellIngressEndpointSpecs()); err != nil {
		logger.Fatal("Cell ingress HTTP mTLS config: %v", err)
	}

	db, _, err := database.OpenReadyDBForRole(logger, database.RoleGlobal)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	service := scmpoller.NewService(logger, db)
	service.SetInstanceID(viper.GetString("instance_id"))
	service.SetClaimTTL(config.SCMPollerClaimTTL())

	interval := config.SCMPollerInterval()
	logger.Info("SCM poller instance ID: %s; polling every %v; claim ttl %v", service.InstanceID(), interval, config.SCMPollerClaimTTL())

	if err := service.Run(cmd.Context(), interval); err != nil {
		logger.Fatal("SCM poller failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-scm-poller",
	Short: "Vectis SCM Poller",
	Long:  `The Vectis SCM Poller polls source-control providers and records deduplicated trigger events.`,
	Run:   runSCMPoller,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	_ = viper.BindEnv("cell_ingress_endpoints", "VECTIS_SCM_POLLER_CELL_INGRESS_ENDPOINTS", "VECTIS_CELL_INGRESS_ENDPOINTS")
	rootCmd.PersistentFlags().String("instance-id", "", "Stable SCM poller instance identifier used in trigger claim tokens")
	rootCmd.PersistentFlags().Duration("interval", config.SCMPollerInterval(), "How often to scan for due SCM poll triggers")
	rootCmd.PersistentFlags().Duration("claim-ttl", config.SCMPollerClaimTTL(), "How long an SCM poller instance owns a trigger claim")
	_ = viper.BindPFlag("instance_id", rootCmd.PersistentFlags().Lookup("instance-id"))
	_ = viper.BindPFlag("interval", rootCmd.PersistentFlags().Lookup("interval"))
	_ = viper.BindPFlag("claim_ttl", rootCmd.PersistentFlags().Lookup("claim-ttl"))
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
