package gerrit

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	FlagUsername     = "gerrit-username"
	FlagPassword     = "gerrit-password"
	FlagPasswordFile = "gerrit-password-file"

	ConfigKeyUsername     = "scm_poller.providers.gerrit.username"
	ConfigKeyPassword     = "scm_poller.providers.gerrit.password"
	ConfigKeyPasswordFile = "scm_poller.providers.gerrit.password_file"

	EnvUsername     = "VECTIS_SCM_POLLER_PROVIDERS_GERRIT_USERNAME"
	EnvPassword     = "VECTIS_SCM_POLLER_PROVIDERS_GERRIT_PASSWORD"
	EnvPasswordFile = "VECTIS_SCM_POLLER_PROVIDERS_GERRIT_PASSWORD_FILE"
)

type Config struct {
	Username     string
	Password     string
	PasswordFile string
}

func AddConfigFlags(flags *pflag.FlagSet) {
	if flags == nil {
		return
	}

	flags.String(FlagUsername, "", "Gerrit HTTP username for SCM polling")
	flags.String(FlagPassword, "", "Gerrit HTTP password for SCM polling; prefer --gerrit-password-file")
	flags.String(FlagPasswordFile, "", "File containing the Gerrit HTTP password for SCM polling")
}

func BindConfig(v *viper.Viper, flags *pflag.FlagSet) error {
	if v == nil {
		return fmt.Errorf("scm: gerrit config requires a viper instance")
	}

	for _, key := range []string{
		ConfigKeyUsername,
		ConfigKeyPassword,
		ConfigKeyPasswordFile,
	} {
		v.SetDefault(key, "")
	}

	if flags != nil {
		for _, pair := range []struct {
			key  string
			flag string
		}{
			{ConfigKeyUsername, FlagUsername},
			{ConfigKeyPassword, FlagPassword},
			{ConfigKeyPasswordFile, FlagPasswordFile},
		} {
			if flag := flags.Lookup(pair.flag); flag != nil {
				if err := v.BindPFlag(pair.key, flag); err != nil {
					return err
				}
			}
		}
	}

	for _, pair := range []struct {
		key string
		env string
	}{
		{ConfigKeyUsername, EnvUsername},
		{ConfigKeyPassword, EnvPassword},
		{ConfigKeyPasswordFile, EnvPasswordFile},
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
		Username:     configString(v, ConfigKeyUsername),
		Password:     configString(v, ConfigKeyPassword),
		PasswordFile: configString(v, ConfigKeyPasswordFile),
	}
}

func (c Config) NewProvider() (*Provider, error) {
	password, err := c.providerPassword()
	if err != nil {
		return nil, err
	}

	username := strings.TrimSpace(c.Username)
	if (username == "") != (password == "") {
		return nil, fmt.Errorf("gerrit username and password must be configured together")
	}

	return NewProvider(WithBasicAuth(username, password)), nil
}

func (c Config) providerPassword() (string, error) {
	passwordFile := strings.TrimSpace(c.PasswordFile)
	if passwordFile == "" {
		return strings.TrimSpace(c.Password), nil
	}

	data, err := os.ReadFile(passwordFile)
	if err != nil {
		return "", fmt.Errorf("read Gerrit password file: %w", err)
	}

	return strings.TrimSpace(string(data)), nil
}

func configString(v *viper.Viper, key string) string {
	return strings.TrimSpace(v.GetString(key))
}
