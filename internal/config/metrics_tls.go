package config

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"vectis/internal/tlsconfig"

	"github.com/spf13/viper"
)

func init() {
	_ = viper.BindEnv("metrics_tls.insecure", "VECTIS_METRICS_TLS_INSECURE")
	_ = viper.BindEnv("metrics_tls.cert_file", "VECTIS_METRICS_TLS_CERT_FILE")
	_ = viper.BindEnv("metrics_tls.key_file", "VECTIS_METRICS_TLS_KEY_FILE")
	_ = viper.BindEnv("metrics_tls.reload_interval", "VECTIS_METRICS_TLS_RELOAD_INTERVAL")
}

func MetricsTLSInsecure() bool {
	if viper.IsSet("metrics_tls.insecure") {
		return viper.GetBool("metrics_tls.insecure")
	}

	return MustDefaults().MetricsTLS.Insecure
}

func MetricsTLSReloadInterval() time.Duration {
	if viper.IsSet("metrics_tls.reload_interval") {
		if d := viper.GetDuration("metrics_tls.reload_interval"); d > 0 {
			return d
		}

		return 0
	}

	d := time.Duration(MustDefaults().MetricsTLS.ReloadInterval)
	if d < 0 {
		return 0
	}

	return d
}

func metricsTLSDefaults() MetricsTLSDefaults {
	return MustDefaults().MetricsTLS
}

func metricsTLSOptionsFromViper() tlsconfig.Options {
	d := metricsTLSDefaults()
	return tlsconfig.Options{
		ServerCert: coalesceNonEmpty(viper.GetString("metrics_tls.cert_file"), d.CertFile),
		ServerKey:  coalesceNonEmpty(viper.GetString("metrics_tls.key_file"), d.KeyFile),
	}
}

func ValidateMetricsTLS() error {
	if MetricsTLSInsecure() {
		return nil
	}

	o := metricsTLSOptionsFromViper()
	if o.ServerCert == "" || o.ServerKey == "" {
		return errors.New("metrics_tls: cert_file and key_file are required when metrics_tls.insecure is false")
	}

	if _, err := tlsconfig.NewReloader(o); err != nil {
		return fmt.Errorf("metrics_tls: load PEM material: %w", err)
	}

	return nil
}

var (
	metricsTLSOnce sync.Once
	metricsTLSRel  *tlsconfig.Reloader
	metricsTLSErr  error
)

func metricsTLSReloader() (*tlsconfig.Reloader, error) {
	if MetricsTLSInsecure() {
		return nil, nil
	}

	metricsTLSOnce.Do(func() {
		metricsTLSRel, metricsTLSErr = tlsconfig.NewReloader(metricsTLSOptionsFromViper())
	})

	return metricsTLSRel, metricsTLSErr
}

func MetricsHTTPSListener(ln net.Listener) (net.Listener, error) {
	if MetricsTLSInsecure() {
		return ln, nil
	}

	r, err := metricsTLSReloader()
	if err != nil {
		return nil, err
	}

	if r == nil {
		return nil, errors.New("metrics_tls: reloader is nil")
	}

	cfg, err := r.ServerTLS()
	if err != nil {
		return nil, err
	}

	return tls.NewListener(ln, cfg), nil
}

func StartMetricsTLSReloadLoop(ctx context.Context) {
	if MetricsTLSInsecure() {
		return
	}

	interval := MetricsTLSReloadInterval()
	if interval <= 0 {
		return
	}

	r, err := metricsTLSReloader()
	if err != nil || r == nil {
		return
	}

	go func() { _ = r.RunReloadLoop(ctx, interval) }()
}
