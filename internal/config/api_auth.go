package config

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/viper"
)

const (
	envAPIAuthEnabled        = "VECTIS_API_AUTH_ENABLED"
	envAPIAuthBootstrapToken = "VECTIS_API_AUTH_BOOTSTRAP_TOKEN"
	envAPIAuthzEngine        = "VECTIS_API_AUTHZ_ENGINE" // must be authenticated_full until more engines ship
	envCLIAPIToken           = "VECTIS_API_TOKEN"

	// MinBootstrapTokenLen is the minimum length (bytes after trim) for api.auth.bootstrap_token
	// when HTTP API auth is enabled and initial setup is not yet complete in the database.
	MinBootstrapTokenLen = 16
)

// APIAuthEnabled reports whether HTTP API authentication is required (Bearer tokens after initial bootstrap).
// When false, the API does not enforce credentials (development / trusted-network only).
//
// Resolution order: VECTIS_API_AUTH_ENABLED, api.auth.enabled (viper),
// then embedded defaults.
func APIAuthEnabled() bool {
	if v := strings.TrimSpace(os.Getenv(envAPIAuthEnabled)); v != "" {
		return parseTruthy(v)
	}

	if viper.IsSet("api.auth.enabled") {
		return viper.GetBool("api.auth.enabled")
	}

	return MustDefaults().API.Auth.Enabled
}

func parseTruthy(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// APIAuthBootstrapToken returns the shared secret used to complete initial setup (e.g. Kubernetes Secret).
// After setup completes in the database, this may be unset in new deployments that restore an existing DB.
func APIAuthBootstrapToken() string {
	if v := os.Getenv(envAPIAuthBootstrapToken); v != "" {
		return v
	}

	if viper.IsSet("api.auth.bootstrap_token") {
		return viper.GetString("api.auth.bootstrap_token")
	}

	return MustDefaults().API.Auth.BootstrapToken
}

// CLIAPIToken returns the Bearer token used by the CLI for authenticated HTTP API requests.
func CLIAPIToken() string {
	return strings.TrimSpace(os.Getenv(envCLIAPIToken))
}

func resolvedAuthzEngine() string {
	if v := strings.TrimSpace(os.Getenv(envAPIAuthzEngine)); v != "" {
		return strings.ToLower(v)
	}

	if viper.IsSet("api.authz.engine") {
		return strings.ToLower(strings.TrimSpace(viper.GetString("api.authz.engine")))
	}

	return strings.ToLower(strings.TrimSpace(MustDefaults().API.Authz.Engine))
}

// APIAuthzEngine returns the resolved authorization engine name.
func APIAuthzEngine() string {
	return resolvedAuthzEngine()
}

// validAuthzEngines are the supported values for api.authz.engine.
var validAuthzEngines = map[string]struct{}{
	"authenticated_full": {},
	"hierarchical_rbac":  {},
}

// validateAuthzEngine rejects unknown api.authz.engine values.
func validateAuthzEngine() error {
	e := resolvedAuthzEngine()
	if e == "" {
		return nil
	}

	if _, ok := validAuthzEngines[e]; !ok {
		return fmt.Errorf("api.authz.engine must be one of authenticated_full or hierarchical_rbac (got %q)", e)
	}

	return nil
}

// AuthSetupState is implemented by the auth repository to read bootstrap completion from the database.
type AuthSetupState interface {
	IsSetupComplete(ctx context.Context) (bool, error)
}

// ValidateAPIAuthConfig checks policy: when auth is enabled, a bootstrap token is required until
// initial setup is recorded in the database (data-driven milestone). It also validates api.authz.engine.
func ValidateAPIAuthConfig(ctx context.Context, state AuthSetupState) error {
	if err := validateAuthzEngine(); err != nil {
		return err
	}

	if !APIAuthEnabled() {
		return nil
	}

	tok := strings.TrimSpace(APIAuthBootstrapToken())
	if len(tok) >= MinBootstrapTokenLen {
		return nil
	}

	if state == nil {
		return fmt.Errorf("api.auth.enabled requires either a bootstrap token of at least %d characters (%s) or a database to verify initial setup is complete", MinBootstrapTokenLen, envAPIAuthBootstrapToken)
	}

	complete, err := state.IsSetupComplete(ctx)
	if err != nil {
		return fmt.Errorf("api.auth: read setup state: %w", err)
	}

	if complete {
		return nil
	}

	return fmt.Errorf("api.auth.enabled requires a bootstrap token of at least %d characters (%s or api.auth.bootstrap_token) until initial setup completes", MinBootstrapTokenLen, envAPIAuthBootstrapToken)
}
