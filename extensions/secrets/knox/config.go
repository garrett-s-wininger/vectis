package knox

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	FlagURL                = "knox-url"
	FlagAuthTokenFile      = "knox-auth-token-file"
	FlagAuthToken          = "knox-auth-token"
	FlagInsecureSkipVerify = "knox-insecure-skip-verify"
	FlagCAFile             = "knox-ca-file"
	FlagClientCertFile     = "knox-client-cert-file"
	FlagClientKeyFile      = "knox-client-key-file"

	ConfigKeyURL                = "secrets.providers.knox.url"
	ConfigKeyAuthTokenFile      = "secrets.providers.knox.auth_token_file"
	ConfigKeyAuthToken          = "secrets.providers.knox.auth_token"
	ConfigKeyInsecureSkipVerify = "secrets.providers.knox.insecure_skip_verify"
	ConfigKeyCAFile             = "secrets.providers.knox.ca_file"
	ConfigKeyClientCertFile     = "secrets.providers.knox.client_cert_file"
	ConfigKeyClientKeyFile      = "secrets.providers.knox.client_key_file"

	EnvURL                = "VECTIS_SECRETS_PROVIDERS_KNOX_URL"
	EnvAuthTokenFile      = "VECTIS_SECRETS_PROVIDERS_KNOX_AUTH_TOKEN_FILE"
	EnvAuthToken          = "VECTIS_SECRETS_PROVIDERS_KNOX_AUTH_TOKEN"
	EnvInsecureSkipVerify = "VECTIS_SECRETS_PROVIDERS_KNOX_INSECURE_SKIP_VERIFY"
	EnvCAFile             = "VECTIS_SECRETS_PROVIDERS_KNOX_CA_FILE"
	EnvClientCertFile     = "VECTIS_SECRETS_PROVIDERS_KNOX_CLIENT_CERT_FILE"
	EnvClientKeyFile      = "VECTIS_SECRETS_PROVIDERS_KNOX_CLIENT_KEY_FILE"
)

type Config struct {
	URL                string
	AuthTokenFile      string
	AuthToken          string
	InsecureSkipVerify bool
	CAFile             string
	ClientCertFile     string
	ClientKeyFile      string
}

func AddConfigFlags(flags *pflag.FlagSet) {
	if flags == nil {
		return
	}

	flags.String(FlagURL, "", "Base URL for a Knox secret service")
	flags.String(FlagAuthTokenFile, "", "File containing the Knox Authorization header value")
	flags.String(FlagAuthToken, "", "Knox Authorization header value; prefer --knox-auth-token-file")
	flags.Bool(FlagInsecureSkipVerify, false, "Skip Knox server TLS certificate verification for local development")
	flags.String(FlagCAFile, "", "Knox server CA certificate file")
	flags.String(FlagClientCertFile, "", "Knox mTLS client certificate file")
	flags.String(FlagClientKeyFile, "", "Knox mTLS client private key file")
}

func BindConfig(v *viper.Viper, flags *pflag.FlagSet) error {
	if v == nil {
		return fmt.Errorf("secrets: knox config requires a viper instance")
	}

	for _, key := range []string{
		ConfigKeyURL,
		ConfigKeyAuthTokenFile,
		ConfigKeyAuthToken,
		ConfigKeyCAFile,
		ConfigKeyClientCertFile,
		ConfigKeyClientKeyFile,
	} {
		v.SetDefault(key, "")
	}

	if flags != nil {
		if flag := flags.Lookup(FlagURL); flag != nil {
			if err := v.BindPFlag(ConfigKeyURL, flag); err != nil {
				return err
			}
		}

		if flag := flags.Lookup(FlagAuthTokenFile); flag != nil {
			if err := v.BindPFlag(ConfigKeyAuthTokenFile, flag); err != nil {
				return err
			}
		}

		if flag := flags.Lookup(FlagAuthToken); flag != nil {
			if err := v.BindPFlag(ConfigKeyAuthToken, flag); err != nil {
				return err
			}
		}

		if flag := flags.Lookup(FlagInsecureSkipVerify); flag != nil {
			if err := v.BindPFlag(ConfigKeyInsecureSkipVerify, flag); err != nil {
				return err
			}
		}

		if flag := flags.Lookup(FlagCAFile); flag != nil {
			if err := v.BindPFlag(ConfigKeyCAFile, flag); err != nil {
				return err
			}
		}

		if flag := flags.Lookup(FlagClientCertFile); flag != nil {
			if err := v.BindPFlag(ConfigKeyClientCertFile, flag); err != nil {
				return err
			}
		}

		if flag := flags.Lookup(FlagClientKeyFile); flag != nil {
			if err := v.BindPFlag(ConfigKeyClientKeyFile, flag); err != nil {
				return err
			}
		}
	}

	if err := v.BindEnv(ConfigKeyURL, EnvURL); err != nil {
		return err
	}
	if err := v.BindEnv(ConfigKeyAuthTokenFile, EnvAuthTokenFile); err != nil {
		return err
	}
	if err := v.BindEnv(ConfigKeyAuthToken, EnvAuthToken); err != nil {
		return err
	}
	if err := v.BindEnv(ConfigKeyInsecureSkipVerify, EnvInsecureSkipVerify); err != nil {
		return err
	}
	if err := v.BindEnv(ConfigKeyCAFile, EnvCAFile); err != nil {
		return err
	}
	if err := v.BindEnv(ConfigKeyClientCertFile, EnvClientCertFile); err != nil {
		return err
	}
	if err := v.BindEnv(ConfigKeyClientKeyFile, EnvClientKeyFile); err != nil {
		return err
	}

	return nil
}

func ConfigFromViper(v *viper.Viper) Config {
	if v == nil {
		return Config{}
	}

	return Config{
		URL:                configString(v, ConfigKeyURL),
		AuthTokenFile:      configString(v, ConfigKeyAuthTokenFile),
		AuthToken:          configString(v, ConfigKeyAuthToken),
		InsecureSkipVerify: configBool(v, ConfigKeyInsecureSkipVerify),
		CAFile:             configString(v, ConfigKeyCAFile),
		ClientCertFile:     configString(v, ConfigKeyClientCertFile),
		ClientKeyFile:      configString(v, ConfigKeyClientKeyFile),
	}
}

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.URL) != ""
}

func (c Config) NewProvider() (*KnoxProvider, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("secrets: knox provider url is required")
	}

	return NewKnoxProvider(
		c.URL,
		WithKnoxAuthToken(c.AuthToken),
		WithKnoxAuthTokenFile(c.AuthTokenFile),
		WithKnoxInsecureSkipVerify(c.InsecureSkipVerify),
		WithKnoxCAFile(c.CAFile),
		WithKnoxClientCertificateFiles(c.ClientCertFile, c.ClientKeyFile),
	)
}

func configString(v *viper.Viper, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(v.GetString(key)); value != "" {
			return value
		}
	}

	return ""
}

func configBool(v *viper.Viper, keys ...string) bool {
	for _, key := range keys {
		if v.IsSet(key) {
			return v.GetBool(key)
		}
	}

	return false
}
