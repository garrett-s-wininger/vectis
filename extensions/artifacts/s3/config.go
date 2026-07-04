package s3

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	FlagEndpoint            = "s3-endpoint"
	FlagRegion              = "s3-region"
	FlagBucket              = "s3-bucket"
	FlagPrefix              = "s3-prefix"
	FlagAccessKeyID         = "s3-access-key-id"
	FlagSecretAccessKey     = "s3-secret-access-key"
	FlagSecretAccessKeyFile = "s3-secret-access-key-file"
	FlagSessionToken        = "s3-session-token"
	FlagPathStyle           = "s3-path-style"
	FlagTempDir             = "s3-temp-dir"

	ConfigKeyEndpoint            = "artifact.storage.s3.endpoint"
	ConfigKeyRegion              = "artifact.storage.s3.region"
	ConfigKeyBucket              = "artifact.storage.s3.bucket"
	ConfigKeyPrefix              = "artifact.storage.s3.prefix"
	ConfigKeyAccessKeyID         = "artifact.storage.s3.access_key_id"
	ConfigKeySecretAccessKey     = "artifact.storage.s3.secret_access_key"
	ConfigKeySecretAccessKeyFile = "artifact.storage.s3.secret_access_key_file"
	ConfigKeySessionToken        = "artifact.storage.s3.session_token"
	ConfigKeyPathStyle           = "artifact.storage.s3.path_style"
	ConfigKeyTempDir             = "artifact.storage.s3.temp_dir"

	EnvEndpoint            = "VECTIS_ARTIFACT_STORAGE_S3_ENDPOINT"
	EnvRegion              = "VECTIS_ARTIFACT_STORAGE_S3_REGION"
	EnvBucket              = "VECTIS_ARTIFACT_STORAGE_S3_BUCKET"
	EnvPrefix              = "VECTIS_ARTIFACT_STORAGE_S3_PREFIX"
	EnvAccessKeyID         = "VECTIS_ARTIFACT_STORAGE_S3_ACCESS_KEY_ID"
	EnvSecretAccessKey     = "VECTIS_ARTIFACT_STORAGE_S3_SECRET_ACCESS_KEY"
	EnvSecretAccessKeyFile = "VECTIS_ARTIFACT_STORAGE_S3_SECRET_ACCESS_KEY_FILE"
	EnvSessionToken        = "VECTIS_ARTIFACT_STORAGE_S3_SESSION_TOKEN"
	EnvPathStyle           = "VECTIS_ARTIFACT_STORAGE_S3_PATH_STYLE"
	EnvTempDir             = "VECTIS_ARTIFACT_STORAGE_S3_TEMP_DIR"
)

type Config struct {
	Endpoint            string
	Region              string
	Bucket              string
	Prefix              string
	AccessKeyID         string
	SecretAccessKey     string
	SecretAccessKeyFile string
	SessionToken        string
	PathStyle           bool
	TempDir             string
}

func AddConfigFlags(flags *pflag.FlagSet) {
	if flags == nil {
		return
	}

	flags.String(FlagEndpoint, "", "S3-compatible artifact storage endpoint")
	flags.String(FlagRegion, defaultRegion, "S3 artifact storage signing region")
	flags.String(FlagBucket, "", "S3 artifact storage bucket")
	flags.String(FlagPrefix, "", "S3 artifact storage object key prefix")
	flags.String(FlagAccessKeyID, "", "S3 artifact storage access key id")
	flags.String(FlagSecretAccessKey, "", "S3 artifact storage secret access key; prefer --s3-secret-access-key-file")
	flags.String(FlagSecretAccessKeyFile, "", "File containing the S3 artifact storage secret access key")
	flags.String(FlagSessionToken, "", "S3 artifact storage session token")
	flags.Bool(FlagPathStyle, true, "Use path-style S3 URLs for local S3-compatible services")
	flags.String(FlagTempDir, "", "Directory for temporary files while hashing S3 artifact uploads (default: OS temp directory)")
}

func BindConfig(v *viper.Viper, flags *pflag.FlagSet) error {
	if v == nil {
		return fmt.Errorf("artifact: s3 config requires a viper instance")
	}

	for _, key := range []string{
		ConfigKeyEndpoint,
		ConfigKeyBucket,
		ConfigKeyPrefix,
		ConfigKeyAccessKeyID,
		ConfigKeySecretAccessKey,
		ConfigKeySecretAccessKeyFile,
		ConfigKeySessionToken,
		ConfigKeyTempDir,
	} {
		v.SetDefault(key, "")
	}

	v.SetDefault(ConfigKeyRegion, defaultRegion)
	v.SetDefault(ConfigKeyPathStyle, true)

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
		{ConfigKeyEndpoint, FlagEndpoint},
		{ConfigKeyRegion, FlagRegion},
		{ConfigKeyBucket, FlagBucket},
		{ConfigKeyPrefix, FlagPrefix},
		{ConfigKeyAccessKeyID, FlagAccessKeyID},
		{ConfigKeySecretAccessKey, FlagSecretAccessKey},
		{ConfigKeySecretAccessKeyFile, FlagSecretAccessKeyFile},
		{ConfigKeySessionToken, FlagSessionToken},
		{ConfigKeyPathStyle, FlagPathStyle},
		{ConfigKeyTempDir, FlagTempDir},
	} {
		if err := bindFlag(pair.key, pair.flag); err != nil {
			return err
		}
	}

	for _, pair := range []struct {
		key string
		env string
	}{
		{ConfigKeyEndpoint, EnvEndpoint},
		{ConfigKeyRegion, EnvRegion},
		{ConfigKeyBucket, EnvBucket},
		{ConfigKeyPrefix, EnvPrefix},
		{ConfigKeyAccessKeyID, EnvAccessKeyID},
		{ConfigKeySecretAccessKey, EnvSecretAccessKey},
		{ConfigKeySecretAccessKeyFile, EnvSecretAccessKeyFile},
		{ConfigKeySessionToken, EnvSessionToken},
		{ConfigKeyPathStyle, EnvPathStyle},
		{ConfigKeyTempDir, EnvTempDir},
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
		Endpoint:            configString(v, ConfigKeyEndpoint),
		Region:              configString(v, ConfigKeyRegion),
		Bucket:              configString(v, ConfigKeyBucket),
		Prefix:              configString(v, ConfigKeyPrefix),
		AccessKeyID:         configString(v, ConfigKeyAccessKeyID),
		SecretAccessKey:     configString(v, ConfigKeySecretAccessKey),
		SecretAccessKeyFile: configString(v, ConfigKeySecretAccessKeyFile),
		SessionToken:        configString(v, ConfigKeySessionToken),
		PathStyle:           configBool(v, ConfigKeyPathStyle),
		TempDir:             configString(v, ConfigKeyTempDir),
	}
}

func (c Config) Enabled() bool {
	return strings.TrimSpace(c.Endpoint) != "" || strings.TrimSpace(c.Bucket) != ""
}

func (c Config) NewStore() (*Store, error) {
	secret, err := c.secretAccessKey()
	if err != nil {
		return nil, err
	}

	return NewStore(StoreOptions{
		Endpoint:        c.Endpoint,
		Region:          c.Region,
		Bucket:          c.Bucket,
		Prefix:          c.Prefix,
		AccessKeyID:     c.AccessKeyID,
		SecretAccessKey: secret,
		SessionToken:    c.SessionToken,
		PathStyle:       c.PathStyle,
		TempDir:         c.TempDir,
	})
}

func (c Config) secretAccessKey() (string, error) {
	if strings.TrimSpace(c.SecretAccessKeyFile) == "" {
		return c.SecretAccessKey, nil
	}

	data, err := os.ReadFile(c.SecretAccessKeyFile)
	if err != nil {
		return "", fmt.Errorf("read artifact s3 secret access key file: %w", err)
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

func configBool(v *viper.Viper, keys ...string) bool {
	for _, key := range keys {
		if v.IsSet(key) {
			return v.GetBool(key)
		}
	}

	return false
}
