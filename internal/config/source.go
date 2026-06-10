package config

import (
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

const (
	envSourceCheckoutRoot                = "VECTIS_SOURCE_CHECKOUT_ROOT"
	envAPIServerSourceCheckoutRoot       = "VECTIS_API_SERVER_SOURCE_CHECKOUT_ROOT"
	envSourceSyncRunningTimeout          = "VECTIS_SOURCE_SYNC_RUNNING_TIMEOUT"
	envAPIServerSourceSyncRunningTimeout = "VECTIS_API_SERVER_SOURCE_SYNC_RUNNING_TIMEOUT"
	defaultSourceSyncRunningTimeout      = 15 * time.Minute
	sourceSyncRunningTimeoutConfigKey    = "source.sync_running_timeout"
)

// SourceCheckoutRoot returns the root directory for Vectis-managed source checkouts.
func SourceCheckoutRoot(dataHome string) string {
	root := strings.TrimSpace(os.Getenv(envSourceCheckoutRoot))
	if root == "" {
		root = strings.TrimSpace(os.Getenv(envAPIServerSourceCheckoutRoot))
	}

	if root == "" && viper.IsSet("source.checkout_root") {
		root = strings.TrimSpace(viper.GetString("source.checkout_root"))
	}

	if root == "" {
		root = strings.TrimSpace(MustDefaults().Source.CheckoutRoot)
	}

	return strings.NewReplacer(
		"{{data_home}}", dataHome,
	).Replace(root)
}

// SourceSyncRunningTimeout returns how long a running source sync reservation may live
// before another caller can reclaim it.
func SourceSyncRunningTimeout() time.Duration {
	for _, envName := range []string{envSourceSyncRunningTimeout, envAPIServerSourceSyncRunningTimeout} {
		if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				return d
			}
		}
	}

	if viper.IsSet(sourceSyncRunningTimeoutConfigKey) {
		if d := viper.GetDuration(sourceSyncRunningTimeoutConfigKey); d > 0 {
			return d
		}
	}

	if d := time.Duration(MustDefaults().Source.SyncRunningTimeout); d > 0 {
		return d
	}

	return defaultSourceSyncRunningTimeout
}
