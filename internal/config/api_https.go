package config

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"vectis/internal/tlsconfig"

	"github.com/spf13/viper"
)

const (
	envAPIHTTPSCertFile       = "VECTIS_API_TLS_CERT_FILE"
	envAPIHTTPSKeyFile        = "VECTIS_API_TLS_KEY_FILE"
	envAPIHTTPSReloadInterval = "VECTIS_API_TLS_RELOAD_INTERVAL"
)

func init() {
	_ = viper.BindEnv("api.tls.cert_file", envAPIHTTPSCertFile, "VECTIS_API_SERVER_TLS_CERT_FILE")
	_ = viper.BindEnv("api.tls.key_file", envAPIHTTPSKeyFile, "VECTIS_API_SERVER_TLS_KEY_FILE")
	_ = viper.BindEnv("api.tls.reload_interval", envAPIHTTPSReloadInterval, "VECTIS_API_SERVER_TLS_RELOAD_INTERVAL")
}

func APIHTTPSCertFile() string {
	if v := strings.TrimSpace(os.Getenv(envAPIHTTPSCertFile)); v != "" {
		return v
	}

	if viper.IsSet("api.tls.cert_file") {
		return strings.TrimSpace(viper.GetString("api.tls.cert_file"))
	}

	return strings.TrimSpace(MustDefaults().API.TLS.CertFile)
}

func APIHTTPSKeyFile() string {
	if v := strings.TrimSpace(os.Getenv(envAPIHTTPSKeyFile)); v != "" {
		return v
	}

	if viper.IsSet("api.tls.key_file") {
		return strings.TrimSpace(viper.GetString("api.tls.key_file"))
	}

	return strings.TrimSpace(MustDefaults().API.TLS.KeyFile)
}

func APIHTTPSReloadInterval() time.Duration {
	if v := strings.TrimSpace(os.Getenv(envAPIHTTPSReloadInterval)); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil && d > 0 {
			return d
		}

		return 0
	}

	if viper.IsSet("api.tls.reload_interval") {
		if d := viper.GetDuration("api.tls.reload_interval"); d > 0 {
			return d
		}

		return 0
	}

	d := time.Duration(MustDefaults().API.TLS.ReloadInterval)
	if d < 0 {
		return 0
	}

	return d
}

func APIHTTPSEnabled() bool {
	return APIHTTPSCertFile() != "" || APIHTTPSKeyFile() != ""
}

func APIURLScheme() string {
	if APIHTTPSEnabled() {
		return "https"
	}

	return "http"
}

func apiHTTPSOptionsFromConfig() tlsconfig.Options {
	return tlsconfig.Options{
		ServerCert: APIHTTPSCertFile(),
		ServerKey:  APIHTTPSKeyFile(),
	}
}

func ValidateAPIHTTPS() error {
	if !APIHTTPSEnabled() {
		return nil
	}

	o := apiHTTPSOptionsFromConfig()
	if o.ServerCert == "" || o.ServerKey == "" {
		return errors.New("api.tls: cert_file and key_file are required together")
	}

	if _, err := tlsconfig.NewReloader(o); err != nil {
		return fmt.Errorf("api.tls: load PEM material: %w", err)
	}

	if v := strings.TrimSpace(os.Getenv(envAPIHTTPSReloadInterval)); v != "" {
		if _, err := time.ParseDuration(v); err != nil {
			return fmt.Errorf("api.tls.reload_interval must be a valid duration (got %q): %w", v, err)
		}
	}

	return nil
}

func NewAPIHTTPSReloader() (*tlsconfig.Reloader, error) {
	if !APIHTTPSEnabled() {
		return nil, nil
	}

	return tlsconfig.NewReloader(apiHTTPSOptionsFromConfig())
}

func APIHTTPSListener(ln net.Listener, reloader *tlsconfig.Reloader) (net.Listener, error) {
	if reloader == nil {
		return ln, nil
	}

	cfg, err := reloader.ServerTLS()
	if err != nil {
		_ = ln.Close()
		return nil, err
	}

	return tls.NewListener(ln, cfg), nil
}

func StartAPIHTTPSReloadLoop(ctx context.Context, reloader *tlsconfig.Reloader) {
	if reloader == nil {
		return
	}

	interval := APIHTTPSReloadInterval()
	if interval <= 0 {
		return
	}

	go func() {
		if err := reloader.RunReloadLoop(ctx, interval); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "api TLS reload loop error: %v\n", err)
		}
	}()
}
