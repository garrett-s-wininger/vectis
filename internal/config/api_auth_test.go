package config

import (
	"context"
	"testing"

	"github.com/spf13/viper"
)

func TestAPIAuthEnabled_envTrue(t *testing.T) {
	t.Setenv(envAPIAuthEnabled, "true")
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("api.auth.enabled", false)

	if !APIAuthEnabled() {
		t.Fatal("env should override viper")
	}
}

func TestAPIAuthEnabled_viperBool(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("api.auth.enabled", true)

	if !APIAuthEnabled() {
		t.Fatal("expected enabled from viper")
	}
}

func TestAPIAuthBootstrapToken_envOverridesViper(t *testing.T) {
	t.Setenv(envAPIAuthBootstrapToken, "sixteenchars----")
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("api.auth.bootstrap_token", "ignored-by-env")

	if got := APIAuthBootstrapToken(); got != "sixteenchars----" {
		t.Fatalf("got %q", got)
	}
}

func TestCLIAPIToken_trimmedFromEnv(t *testing.T) {
	t.Setenv(envCLIAPIToken, "  token-value  ")

	if got := CLIAPIToken(); got != "token-value" {
		t.Fatalf("got %q", got)
	}
}

func TestValidateAPIAuthConfig_authzEngine(t *testing.T) {
	ctx := context.Background()

	t.Run("authenticated_full_ok", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		t.Setenv(envAPIAuthzEngine, "authenticated_full")
		viper.Set("api.auth.enabled", false)

		if err := ValidateAPIAuthConfig(ctx, stubAuthState{}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("hierarchical_rbac_ok", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		t.Setenv(envAPIAuthzEngine, "hierarchical_rbac")
		viper.Set("api.auth.enabled", false)

		if err := ValidateAPIAuthConfig(ctx, stubAuthState{}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("rejects_unknown_engine", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		t.Setenv(envAPIAuthzEngine, "off")
		viper.Set("api.auth.enabled", false)

		if err := ValidateAPIAuthConfig(ctx, stubAuthState{}); err == nil {
			t.Fatal("expected error")
		}
	})
}

type stubAuthState struct {
	complete bool
	err      error
}

func (s stubAuthState) IsSetupComplete(context.Context) (bool, error) {
	return s.complete, s.err
}

func TestValidateAPIAuthConfig(t *testing.T) {
	ctx := context.Background()

	t.Run("auth_disabled", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		viper.Set("api.auth.enabled", false)

		if err := ValidateAPIAuthConfig(ctx, stubAuthState{complete: false}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("enabled_with_bootstrap", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		t.Setenv(envAPIAuthEnabled, "true")
		t.Setenv(envAPIAuthBootstrapToken, "sixteenchars----")

		if err := ValidateAPIAuthConfig(ctx, nil); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("enabled_no_bootstrap_incomplete", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		t.Setenv(envAPIAuthEnabled, "true")
		t.Setenv(envAPIAuthBootstrapToken, "")

		if err := ValidateAPIAuthConfig(ctx, stubAuthState{complete: false}); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("enabled_no_bootstrap_complete_in_db", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		t.Setenv(envAPIAuthEnabled, "true")
		t.Setenv(envAPIAuthBootstrapToken, "")

		if err := ValidateAPIAuthConfig(ctx, stubAuthState{complete: true}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("enabled_short_bootstrap_incomplete", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		t.Setenv(envAPIAuthEnabled, "true")
		t.Setenv(envAPIAuthBootstrapToken, "short")

		if err := ValidateAPIAuthConfig(ctx, stubAuthState{complete: false}); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("enabled_bootstrap_one_below_min", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		t.Setenv(envAPIAuthEnabled, "true")
		// 15 bytes — MinBootstrapTokenLen is 16
		t.Setenv(envAPIAuthBootstrapToken, "123456789012345")

		if err := ValidateAPIAuthConfig(ctx, stubAuthState{complete: false}); err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("state_read_error", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		t.Setenv(envAPIAuthEnabled, "true")
		t.Setenv(envAPIAuthBootstrapToken, "")

		err := ValidateAPIAuthConfig(ctx, stubAuthState{err: context.Canceled})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}
