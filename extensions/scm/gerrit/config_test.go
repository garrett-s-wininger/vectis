package gerrit

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func TestConfigBindAndLoadFromViper(t *testing.T) {
	v := viper.New()
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	AddConfigFlags(flags)
	if err := BindConfig(v, flags); err != nil {
		t.Fatalf("BindConfig: %v", err)
	}

	if err := flags.Parse([]string{
		"--gerrit-username", "ci-bot",
		"--gerrit-password", "secret",
	}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	cfg := ConfigFromViper(v)
	if cfg.Username != "ci-bot" || cfg.Password != "secret" || cfg.PasswordFile != "" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestConfigBindsEnv(t *testing.T) {
	t.Setenv(EnvUsername, "env-bot")
	t.Setenv(EnvPassword, "env-secret")
	t.Setenv(EnvPasswordFile, "/env/gerrit-password")

	v := viper.New()
	if err := BindConfig(v, nil); err != nil {
		t.Fatalf("BindConfig: %v", err)
	}

	cfg := ConfigFromViper(v)
	if cfg.Username != "env-bot" || cfg.Password != "env-secret" || cfg.PasswordFile != "/env/gerrit-password" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestConfigReadsPasswordFile(t *testing.T) {
	dir := t.TempDir()
	passwordFile := filepath.Join(dir, "gerrit-password")
	if err := os.WriteFile(passwordFile, []byte("from-file\n"), 0o600); err != nil {
		t.Fatalf("write password file: %v", err)
	}

	provider, err := Config{
		Username:     "ci-bot",
		Password:     "inline",
		PasswordFile: passwordFile,
	}.NewProvider()
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	if provider.username != "ci-bot" || provider.password != "from-file" {
		t.Fatalf("provider auth = %q/%q", provider.username, provider.password)
	}
}

func TestConfigRejectsPartialBasicAuth(t *testing.T) {
	for name, cfg := range map[string]Config{
		"username only": {Username: "ci-bot"},
		"password only": {Password: "secret"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := cfg.NewProvider(); err == nil {
				t.Fatal("expected partial basic auth error")
			}
		})
	}
}

func TestConfigProviderAllowsAnonymousAuth(t *testing.T) {
	provider, err := Config{}.NewProvider()
	if err != nil {
		t.Fatalf("NewProvider: %v", err)
	}

	if provider.username != "" || provider.password != "" {
		t.Fatalf("provider auth = %q/%q, want anonymous", provider.username, provider.password)
	}
}
