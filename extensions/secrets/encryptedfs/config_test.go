package encryptedfs

import (
	"testing"

	"github.com/spf13/viper"
)

func TestConfigFromViperUsesCanonicalProviderKeys(t *testing.T) {
	t.Parallel()

	v := viper.New()
	if err := BindConfig(v, nil); err != nil {
		t.Fatalf("BindConfig: %v", err)
	}

	v.Set(ConfigKeyRoot, "/var/lib/vectis/secrets")
	v.Set(ConfigKeyKeyFile, "/etc/vectis/encryptedfs.key")

	cfg := ConfigFromViper(v)
	if cfg.Root != "/var/lib/vectis/secrets" || cfg.KeyFile != "/etc/vectis/encryptedfs.key" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestConfigFromViperUsesCanonicalProviderEnv(t *testing.T) {
	t.Setenv(EnvRoot, "/env/root")
	t.Setenv(EnvKeyFile, "/env/key")

	v := viper.New()
	if err := BindConfig(v, nil); err != nil {
		t.Fatalf("BindConfig: %v", err)
	}

	cfg := ConfigFromViper(v)
	if cfg.Root != "/env/root" || cfg.KeyFile != "/env/key" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestConfigNewProviderRequiresKeyFileWhenEnabled(t *testing.T) {
	t.Parallel()

	_, err := Config{Root: "/var/lib/vectis/secrets"}.NewProvider()
	if err == nil {
		t.Fatal("NewProvider succeeded without key file")
	}
}
