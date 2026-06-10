package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"vectis/internal/cli"
	"vectis/internal/interfaces"
	"vectis/internal/localspiffe"
	"vectis/internal/utils"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const defaultTrustDomain = "vectis.internal"

func runVectisSPIFFE(cmd *cobra.Command, args []string) {
	logger := interfaces.NewAsyncLogger("spiffe")
	defer logger.Close()
	cli.SetLogLevel(logger)

	cfg, initOnly := spiffeConfig()
	authority, err := localspiffe.Start(cmd.Context(), cfg)
	if err != nil {
		logger.Fatal("start SPIFFE authority: %v", err)
	}
	defer authority.Stop()

	logger.Info("SPIFFE authority ready for trust domain %s (workload=%s registration=%s bundle=%s)", cfg.TrustDomain, cfg.WorkloadSocketPath, cfg.RegistrationSocketPath, cfg.BundleFile)
	if initOnly {
		return
	}

	<-cmd.Context().Done()
}

func spiffeConfig() (localspiffe.Config, bool) {
	trustDomain := strings.TrimSpace(viper.GetString("trust_domain"))
	if trustDomain == "" {
		trustDomain = defaultTrustDomain
	}

	dataDir := strings.TrimSpace(viper.GetString("data_dir"))
	if dataDir == "" {
		dataDir = filepath.Join(utils.DataHome(), "vectis", "spiffe")
	}

	runtimeDir := strings.TrimSpace(viper.GetString("runtime_dir"))
	if runtimeDir == "" {
		runtimeDir = filepath.Join(utils.RuntimeDir(), "spiffe")
	}

	workloadSocket := strings.TrimSpace(viper.GetString("workload_socket"))
	if workloadSocket == "" {
		workloadSocket = filepath.Join(runtimeDir, "workload.sock")
	}

	registrationSocket := strings.TrimSpace(viper.GetString("registration_socket"))
	if registrationSocket == "" {
		registrationSocket = filepath.Join(runtimeDir, "registration.sock")
	}

	bundleFile := strings.TrimSpace(viper.GetString("bundle_file"))
	if bundleFile == "" {
		bundleFile = filepath.Join(dataDir, "bundle.pem")
	}

	selectors := cleanCommaSeparated(viper.GetStringSlice("selector"))
	if len(selectors) == 0 {
		selectors = []string{fmt.Sprintf("unix:uid:%d", os.Getuid())}
	}

	return localspiffe.Config{
		TrustDomain:            trustDomain,
		DataDir:                dataDir,
		RuntimeDir:             runtimeDir,
		WorkloadSocketPath:     workloadSocket,
		RegistrationSocketPath: registrationSocket,
		BundleFile:             bundleFile,
		Selectors:              selectors,
		DefaultX509SVIDTTL:     viper.GetDuration("x509_svid_ttl"),
	}, viper.GetBool("init_only")
}

func cleanCommaSeparated(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			part = strings.TrimSpace(part)
			if part == "" || seen[part] {
				continue
			}

			seen[part] = true
			out = append(out, part)
		}
	}

	return out
}

var rootCmd = &cobra.Command{
	Use:   "vectis-spiffe",
	Short: "Run the Vectis SPIFFE authority",
	Run:   runVectisSPIFFE,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	viper.SetDefault("log_level", "info")
	viper.SetDefault("trust_domain", defaultTrustDomain)
	viper.SetDefault("x509_svid_ttl", 5*time.Minute)

	rootCmd.PersistentFlags().String("log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().String("trust-domain", defaultTrustDomain, "SPIFFE trust domain")
	rootCmd.PersistentFlags().String("data-dir", "", "Directory for SPIFFE CA material")
	rootCmd.PersistentFlags().String("runtime-dir", "", "Directory for SPIFFE Unix sockets")
	rootCmd.PersistentFlags().String("workload-socket", "", "Workload API Unix socket path")
	rootCmd.PersistentFlags().String("registration-socket", "", "Entry API Unix socket path")
	rootCmd.PersistentFlags().String("bundle-file", "", "PEM bundle output path")
	rootCmd.PersistentFlags().StringArray("selector", nil, "Trusted workload selector, such as unix:uid:0; may be repeated")
	rootCmd.PersistentFlags().Duration("x509-svid-ttl", 5*time.Minute, "Default X.509-SVID TTL")
	rootCmd.PersistentFlags().Bool("init-only", false, "Initialize CA and bundle material, then exit")

	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("trust_domain", rootCmd.PersistentFlags().Lookup("trust-domain"))
	_ = viper.BindPFlag("data_dir", rootCmd.PersistentFlags().Lookup("data-dir"))
	_ = viper.BindPFlag("runtime_dir", rootCmd.PersistentFlags().Lookup("runtime-dir"))
	_ = viper.BindPFlag("workload_socket", rootCmd.PersistentFlags().Lookup("workload-socket"))
	_ = viper.BindPFlag("registration_socket", rootCmd.PersistentFlags().Lookup("registration-socket"))
	_ = viper.BindPFlag("bundle_file", rootCmd.PersistentFlags().Lookup("bundle-file"))
	_ = viper.BindPFlag("selector", rootCmd.PersistentFlags().Lookup("selector"))
	_ = viper.BindPFlag("x509_svid_ttl", rootCmd.PersistentFlags().Lookup("x509-svid-ttl"))
	_ = viper.BindPFlag("init_only", rootCmd.PersistentFlags().Lookup("init-only"))

	viper.SetEnvPrefix("VECTIS_SPIFFE")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
