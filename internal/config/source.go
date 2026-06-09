package config

import (
	"os"
	"strings"

	"github.com/spf13/viper"
)

const (
	envSourceCheckoutRoot          = "VECTIS_SOURCE_CHECKOUT_ROOT"
	envAPIServerSourceCheckoutRoot = "VECTIS_API_SERVER_SOURCE_CHECKOUT_ROOT"
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
