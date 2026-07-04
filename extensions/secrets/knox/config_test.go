package knox

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

	v.Set(ConfigKeyURL, "https://knox.service")
	v.Set(ConfigKeyAuthTokenFile, "/etc/vectis/knox-token")
	v.Set(ConfigKeyAuthToken, "0m-token")
	v.Set(ConfigKeyInsecureSkipVerify, true)
	v.Set(ConfigKeyCAFile, "/etc/vectis/knox-ca.crt")
	v.Set(ConfigKeyClientCertFile, "/etc/vectis/knox-client.crt")
	v.Set(ConfigKeyClientKeyFile, "/etc/vectis/knox-client.key")

	cfg := ConfigFromViper(v)
	if cfg.URL != "https://knox.service" ||
		cfg.AuthTokenFile != "/etc/vectis/knox-token" ||
		cfg.AuthToken != "0m-token" ||
		!cfg.InsecureSkipVerify ||
		cfg.CAFile != "/etc/vectis/knox-ca.crt" ||
		cfg.ClientCertFile != "/etc/vectis/knox-client.crt" ||
		cfg.ClientKeyFile != "/etc/vectis/knox-client.key" {
		t.Fatalf("config = %+v", cfg)
	}
}

func TestConfigFromViperUsesCanonicalProviderEnv(t *testing.T) {
	t.Setenv(EnvURL, "https://env-knox.service")
	t.Setenv(EnvAuthTokenFile, "/env/knox-token")
	t.Setenv(EnvAuthToken, "0m-env")
	t.Setenv(EnvInsecureSkipVerify, "true")
	t.Setenv(EnvCAFile, "/env/knox-ca.crt")
	t.Setenv(EnvClientCertFile, "/env/knox-client.crt")
	t.Setenv(EnvClientKeyFile, "/env/knox-client.key")

	v := viper.New()
	if err := BindConfig(v, nil); err != nil {
		t.Fatalf("BindConfig: %v", err)
	}

	cfg := ConfigFromViper(v)
	if cfg.URL != "https://env-knox.service" ||
		cfg.AuthTokenFile != "/env/knox-token" ||
		cfg.AuthToken != "0m-env" ||
		!cfg.InsecureSkipVerify ||
		cfg.CAFile != "/env/knox-ca.crt" ||
		cfg.ClientCertFile != "/env/knox-client.crt" ||
		cfg.ClientKeyFile != "/env/knox-client.key" {
		t.Fatalf("config = %+v", cfg)
	}
}
