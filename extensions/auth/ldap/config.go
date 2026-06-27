package ldap

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	FlagProviderID           = "ldap-provider-id"
	FlagURL                  = "ldap-url"
	FlagBindDN               = "ldap-bind-dn"
	FlagBindPassword         = "ldap-bind-password"
	FlagBindPasswordFile     = "ldap-bind-password-file"
	FlagBaseDN               = "ldap-base-dn"
	FlagUserFilter           = "ldap-user-filter"
	FlagUsernameAttribute    = "ldap-username-attribute"
	FlagDisplayNameAttribute = "ldap-display-name-attribute"
	FlagStartTLS             = "ldap-start-tls"
	FlagTimeout              = "ldap-timeout"
	FlagAutoCreateUsers      = "ldap-auto-create-users"

	ConfigKeyProviderID           = "api.auth.ldap.provider_id"
	ConfigKeyURL                  = "api.auth.ldap.url"
	ConfigKeyBindDN               = "api.auth.ldap.bind_dn"
	ConfigKeyBindPassword         = "api.auth.ldap.bind_password"
	ConfigKeyBindPasswordFile     = "api.auth.ldap.bind_password_file"
	ConfigKeyBaseDN               = "api.auth.ldap.base_dn"
	ConfigKeyUserFilter           = "api.auth.ldap.user_filter"
	ConfigKeyUsernameAttribute    = "api.auth.ldap.username_attribute"
	ConfigKeyDisplayNameAttribute = "api.auth.ldap.display_name_attribute"
	ConfigKeyStartTLS             = "api.auth.ldap.start_tls"
	ConfigKeyTimeout              = "api.auth.ldap.timeout"
	ConfigKeyAutoCreateUsers      = "api.auth.ldap.auto_create_users"

	EnvProviderID           = "VECTIS_API_AUTH_LDAP_PROVIDER_ID"
	EnvURL                  = "VECTIS_API_AUTH_LDAP_URL"
	EnvBindDN               = "VECTIS_API_AUTH_LDAP_BIND_DN"
	EnvBindPassword         = "VECTIS_API_AUTH_LDAP_BIND_PASSWORD"
	EnvBindPasswordFile     = "VECTIS_API_AUTH_LDAP_BIND_PASSWORD_FILE"
	EnvBaseDN               = "VECTIS_API_AUTH_LDAP_BASE_DN"
	EnvUserFilter           = "VECTIS_API_AUTH_LDAP_USER_FILTER"
	EnvUsernameAttribute    = "VECTIS_API_AUTH_LDAP_USERNAME_ATTRIBUTE"
	EnvDisplayNameAttribute = "VECTIS_API_AUTH_LDAP_DISPLAY_NAME_ATTRIBUTE"
	EnvStartTLS             = "VECTIS_API_AUTH_LDAP_START_TLS"
	EnvTimeout              = "VECTIS_API_AUTH_LDAP_TIMEOUT"
	EnvAutoCreateUsers      = "VECTIS_API_AUTH_LDAP_AUTO_CREATE_USERS"
)

const (
	defaultUserFilter           = "(uid={username})"
	defaultUsernameAttribute    = "uid"
	defaultDisplayNameAttribute = "cn"
	defaultTimeout              = 5 * time.Second
)

type Config struct {
	ProviderID           string
	URL                  string
	BindDN               string
	BindPassword         string
	BindPasswordFile     string
	BaseDN               string
	UserFilter           string
	UsernameAttribute    string
	DisplayNameAttribute string
	StartTLS             bool
	Timeout              time.Duration
	AutoCreateUsers      bool
}

func AddConfigFlags(flags *pflag.FlagSet) {
	if flags == nil {
		return
	}

	flags.String(FlagProviderID, DefaultProviderID, "Stable auth provider instance ID for LDAP login")
	flags.String(FlagURL, "", "LDAP server URL for API login authentication")
	flags.String(FlagBindDN, "", "LDAP service-account bind DN used before user search")
	flags.String(FlagBindPassword, "", "LDAP service-account bind password; prefer --ldap-bind-password-file")
	flags.String(FlagBindPasswordFile, "", "File containing the LDAP service-account bind password")
	flags.String(FlagBaseDN, "", "LDAP base DN used for user search")
	flags.String(FlagUserFilter, defaultUserFilter, "LDAP user search filter; {username} is replaced with the escaped login username")
	flags.String(FlagUsernameAttribute, defaultUsernameAttribute, "LDAP attribute mapped to the Vectis local username")
	flags.String(FlagDisplayNameAttribute, defaultDisplayNameAttribute, "LDAP attribute used as the display name")
	flags.Bool(FlagStartTLS, false, "Upgrade ldap:// connections with StartTLS before binding")
	flags.Duration(FlagTimeout, defaultTimeout, "LDAP dial and authentication timeout")
	flags.Bool(FlagAutoCreateUsers, false, "Create a local Vectis user row on successful LDAP login when none exists")
}

func BindConfig(v *viper.Viper, flags *pflag.FlagSet) error {
	if v == nil {
		return fmt.Errorf("auth: ldap config requires a viper instance")
	}

	for _, key := range []string{
		ConfigKeyURL,
		ConfigKeyBindDN,
		ConfigKeyBindPassword,
		ConfigKeyBindPasswordFile,
		ConfigKeyBaseDN,
	} {
		v.SetDefault(key, "")
	}

	v.SetDefault(ConfigKeyProviderID, DefaultProviderID)
	v.SetDefault(ConfigKeyUserFilter, defaultUserFilter)
	v.SetDefault(ConfigKeyUsernameAttribute, defaultUsernameAttribute)
	v.SetDefault(ConfigKeyDisplayNameAttribute, defaultDisplayNameAttribute)
	v.SetDefault(ConfigKeyStartTLS, false)
	v.SetDefault(ConfigKeyTimeout, defaultTimeout)
	v.SetDefault(ConfigKeyAutoCreateUsers, false)

	bindFlag := func(key, flagName string) error {
		if flags == nil {
			return nil
		}

		if flag := flags.Lookup(flagName); flag != nil {
			return v.BindPFlag(key, flag)
		}

		return nil
	}

	for _, pair := range []struct {
		key  string
		flag string
	}{
		{ConfigKeyProviderID, FlagProviderID},
		{ConfigKeyURL, FlagURL},
		{ConfigKeyBindDN, FlagBindDN},
		{ConfigKeyBindPassword, FlagBindPassword},
		{ConfigKeyBindPasswordFile, FlagBindPasswordFile},
		{ConfigKeyBaseDN, FlagBaseDN},
		{ConfigKeyUserFilter, FlagUserFilter},
		{ConfigKeyUsernameAttribute, FlagUsernameAttribute},
		{ConfigKeyDisplayNameAttribute, FlagDisplayNameAttribute},
		{ConfigKeyStartTLS, FlagStartTLS},
		{ConfigKeyTimeout, FlagTimeout},
		{ConfigKeyAutoCreateUsers, FlagAutoCreateUsers},
	} {
		if err := bindFlag(pair.key, pair.flag); err != nil {
			return err
		}
	}

	for _, pair := range []struct {
		key string
		env string
	}{
		{ConfigKeyProviderID, EnvProviderID},
		{ConfigKeyURL, EnvURL},
		{ConfigKeyBindDN, EnvBindDN},
		{ConfigKeyBindPassword, EnvBindPassword},
		{ConfigKeyBindPasswordFile, EnvBindPasswordFile},
		{ConfigKeyBaseDN, EnvBaseDN},
		{ConfigKeyUserFilter, EnvUserFilter},
		{ConfigKeyUsernameAttribute, EnvUsernameAttribute},
		{ConfigKeyDisplayNameAttribute, EnvDisplayNameAttribute},
		{ConfigKeyStartTLS, EnvStartTLS},
		{ConfigKeyTimeout, EnvTimeout},
		{ConfigKeyAutoCreateUsers, EnvAutoCreateUsers},
	} {
		if err := v.BindEnv(pair.key, pair.env); err != nil {
			return err
		}
	}

	return nil
}

func ConfigFromViper(v *viper.Viper) Config {
	if v == nil {
		return Config{}
	}

	return Config{
		ProviderID:           configStringWithDefault(v, DefaultProviderID, ConfigKeyProviderID),
		URL:                  configString(v, ConfigKeyURL),
		BindDN:               configString(v, ConfigKeyBindDN),
		BindPassword:         configString(v, ConfigKeyBindPassword),
		BindPasswordFile:     configString(v, ConfigKeyBindPasswordFile),
		BaseDN:               configString(v, ConfigKeyBaseDN),
		UserFilter:           configStringWithDefault(v, defaultUserFilter, ConfigKeyUserFilter),
		UsernameAttribute:    configStringWithDefault(v, defaultUsernameAttribute, ConfigKeyUsernameAttribute),
		DisplayNameAttribute: configStringWithDefault(v, defaultDisplayNameAttribute, ConfigKeyDisplayNameAttribute),
		StartTLS:             configBool(v, ConfigKeyStartTLS),
		Timeout:              configDuration(v, defaultTimeout, ConfigKeyTimeout),
		AutoCreateUsers:      configBool(v, ConfigKeyAutoCreateUsers),
	}
}

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.URL) != "" || strings.TrimSpace(c.BaseDN) != ""
}

func (c Config) NewProvider() (*Provider, error) {
	bindPassword, err := c.serviceBindPassword()
	if err != nil {
		return nil, err
	}

	return NewProvider(ProviderOptions{
		ProviderID:           c.ProviderID,
		URL:                  c.URL,
		BindDN:               c.BindDN,
		BindPassword:         bindPassword,
		BaseDN:               c.BaseDN,
		UserFilter:           c.UserFilter,
		UsernameAttribute:    c.UsernameAttribute,
		DisplayNameAttribute: c.DisplayNameAttribute,
		StartTLS:             c.StartTLS,
		Timeout:              c.Timeout,
	})
}

func (c Config) serviceBindPassword() (string, error) {
	if strings.TrimSpace(c.BindPasswordFile) == "" {
		return c.BindPassword, nil
	}

	data, err := os.ReadFile(c.BindPasswordFile)
	if err != nil {
		return "", fmt.Errorf("read ldap bind password file: %w", err)
	}

	return strings.TrimSpace(string(data)), nil
}

func configString(v *viper.Viper, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(v.GetString(key)); value != "" {
			return value
		}
	}

	return ""
}

func configStringWithDefault(v *viper.Viper, fallback string, keys ...string) string {
	if value := configString(v, keys...); value != "" {
		return value
	}

	return fallback
}

func configBool(v *viper.Viper, keys ...string) bool {
	for _, key := range keys {
		if v.IsSet(key) {
			return v.GetBool(key)
		}
	}

	return false
}

func configDuration(v *viper.Viper, fallback time.Duration, keys ...string) time.Duration {
	for _, key := range keys {
		if v.IsSet(key) {
			if d := v.GetDuration(key); d > 0 {
				return d
			}
		}
	}

	return fallback
}
