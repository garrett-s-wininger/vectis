package ldap

import (
	"os"
	"path/filepath"
	"testing"
	"time"

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
		"--ldap-provider-id", "corp-ldap",
		"--ldap-url", "ldap://127.0.0.1:389",
		"--ldap-bind-dn", "cn=svc,dc=example,dc=org",
		"--ldap-bind-password", "secret",
		"--ldap-base-dn", "ou=people,dc=example,dc=org",
		"--ldap-user-filter", "(mail={username})",
		"--ldap-username-attribute", "mail",
		"--ldap-display-name-attribute", "displayName",
		"--ldap-start-tls",
		"--ldap-timeout", "3s",
		"--ldap-auto-create-users",
	}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	cfg := ConfigFromViper(v)
	if !cfg.Enabled() {
		t.Fatal("config should be enabled")
	}

	if cfg.ProviderID != "corp-ldap" ||
		cfg.URL != "ldap://127.0.0.1:389" ||
		cfg.BindDN != "cn=svc,dc=example,dc=org" ||
		cfg.BindPassword != "secret" ||
		cfg.BaseDN != "ou=people,dc=example,dc=org" ||
		cfg.UserFilter != "(mail={username})" ||
		cfg.UsernameAttribute != "mail" ||
		cfg.DisplayNameAttribute != "displayName" ||
		!cfg.StartTLS ||
		cfg.Timeout != 3*time.Second ||
		!cfg.AutoCreateUsers {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestConfigReadsBindPasswordFile(t *testing.T) {
	dir := t.TempDir()
	passwordFile := filepath.Join(dir, "ldap-password")
	if err := os.WriteFile(passwordFile, []byte("from-file\n"), 0o600); err != nil {
		t.Fatalf("write password file: %v", err)
	}

	cfg := Config{
		URL:              "ldap://127.0.0.1:389",
		BindDN:           "cn=svc,dc=example,dc=org",
		BindPassword:     "inline",
		BindPasswordFile: passwordFile,
		BaseDN:           "dc=example,dc=org",
	}

	password, err := cfg.serviceBindPassword()
	if err != nil {
		t.Fatalf("serviceBindPassword: %v", err)
	}

	if password != "from-file" {
		t.Fatalf("password = %q", password)
	}
}

func TestConfigBindsEnv(t *testing.T) {
	t.Setenv(EnvProviderID, "env-ldap")
	t.Setenv(EnvURL, "ldap://ldap.env:389")
	t.Setenv(EnvBaseDN, "dc=env,dc=example")
	t.Setenv(EnvAutoCreateUsers, "true")

	v := viper.New()
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	AddConfigFlags(flags)
	if err := BindConfig(v, flags); err != nil {
		t.Fatalf("BindConfig: %v", err)
	}

	cfg := ConfigFromViper(v)
	if cfg.ProviderID != "env-ldap" || cfg.URL != "ldap://ldap.env:389" || cfg.BaseDN != "dc=env,dc=example" || !cfg.AutoCreateUsers {
		t.Fatalf("unexpected env config: %+v", cfg)
	}
}
