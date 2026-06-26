package encryptedfs

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	FlagRoot    = "encryptedfs-root"
	FlagKeyFile = "encryptedfs-key-file"

	ConfigKeyRoot    = "secrets.providers.encryptedfs.root"
	ConfigKeyKeyFile = "secrets.providers.encryptedfs.key_file"

	EnvRoot    = "VECTIS_SECRETS_PROVIDERS_ENCRYPTEDFS_ROOT"
	EnvKeyFile = "VECTIS_SECRETS_PROVIDERS_ENCRYPTEDFS_KEY_FILE"
)

type Config struct {
	Root    string
	KeyFile string
}

func AddConfigFlags(flags *pflag.FlagSet) {
	if flags == nil {
		return
	}

	flags.String(FlagRoot, "", "Root directory for encryptedfs secret files")
	flags.String(FlagKeyFile, "", "32-byte, hex, or base64 key file for encryptedfs secret envelopes")
}

func BindConfig(v *viper.Viper, flags *pflag.FlagSet) error {
	if v == nil {
		return fmt.Errorf("secrets: encryptedfs config requires a viper instance")
	}

	v.SetDefault(ConfigKeyRoot, "")
	v.SetDefault(ConfigKeyKeyFile, "")

	if flags != nil {
		if flag := flags.Lookup(FlagRoot); flag != nil {
			if err := v.BindPFlag(ConfigKeyRoot, flag); err != nil {
				return err
			}
		}

		if flag := flags.Lookup(FlagKeyFile); flag != nil {
			if err := v.BindPFlag(ConfigKeyKeyFile, flag); err != nil {
				return err
			}
		}
	}

	if err := v.BindEnv(ConfigKeyRoot, EnvRoot); err != nil {
		return err
	}
	if err := v.BindEnv(ConfigKeyKeyFile, EnvKeyFile); err != nil {
		return err
	}

	return nil
}

func ConfigFromViper(v *viper.Viper) Config {
	if v == nil {
		return Config{}
	}

	return Config{
		Root:    configString(v, ConfigKeyRoot),
		KeyFile: configString(v, ConfigKeyKeyFile),
	}
}

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.Root) != ""
}

func (c Config) NewProvider() (*EncryptedFSProvider, error) {
	if !c.Enabled() {
		return nil, fmt.Errorf("secrets: encryptedfs provider root is required")
	}

	if strings.TrimSpace(c.KeyFile) == "" {
		return nil, fmt.Errorf("secrets: encryptedfs provider requires %s, %s, or --%s", ConfigKeyKeyFile, EnvKeyFile, FlagKeyFile)
	}

	return NewEncryptedFSProvider(c.Root, WithEncryptedFSKeyFile(c.KeyFile))
}

func configString(v *viper.Viper, keys ...string) string {
	for _, key := range keys {
		if value := strings.TrimSpace(v.GetString(key)); value != "" {
			return value
		}
	}

	return ""
}
