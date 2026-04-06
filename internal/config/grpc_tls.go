package config

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"vectis/internal/tlsconfig"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	_ = viper.BindEnv("grpc_tls.insecure", "VECTIS_GRPC_TLS_INSECURE")
	_ = viper.BindEnv("grpc_tls.ca_file", "VECTIS_GRPC_TLS_CA_FILE")
	_ = viper.BindEnv("grpc_tls.cert_file", "VECTIS_GRPC_TLS_CERT_FILE")
	_ = viper.BindEnv("grpc_tls.key_file", "VECTIS_GRPC_TLS_KEY_FILE")
	_ = viper.BindEnv("grpc_tls.client_ca_file", "VECTIS_GRPC_TLS_CLIENT_CA_FILE")
	_ = viper.BindEnv("grpc_tls.client_cert_file", "VECTIS_GRPC_TLS_CLIENT_CERT_FILE")
	_ = viper.BindEnv("grpc_tls.client_key_file", "VECTIS_GRPC_TLS_CLIENT_KEY_FILE")
	_ = viper.BindEnv("grpc_tls.server_name", "VECTIS_GRPC_TLS_SERVER_NAME")
	_ = viper.BindEnv("grpc_tls.reload_interval", "VECTIS_GRPC_TLS_RELOAD_INTERVAL")
}

type GRPCTLSDaemonRole int

const (
	GRPCTLSDaemonRegistry   GRPCTLSDaemonRole = iota // gRPC server only (vectis-registry)
	GRPCTLSDaemonQueue                               // server + dials registry
	GRPCTLSDaemonLog                                 // server + dials registry
	GRPCTLSDaemonClientOnly                          // vectis-api, worker, cron, reconciler (dial-only)
)

func GRPCTLSInsecure() bool {
	if viper.IsSet("grpc_tls.insecure") {
		return viper.GetBool("grpc_tls.insecure")
	}
	return MustDefaults().GRPCTLS.Insecure
}

func GRPCTLSReloadInterval() time.Duration {
	if viper.IsSet("grpc_tls.reload_interval") {
		if d := viper.GetDuration("grpc_tls.reload_interval"); d > 0 {
			return d
		}
		return 0
	}

	d := time.Duration(MustDefaults().GRPCTLS.ReloadInterval)
	if d < 0 {
		return 0
	}

	return d
}

func grpctlsDefaults() GRPCTLSDefaults {
	return MustDefaults().GRPCTLS
}

func grpctlsOptionsFromViper() tlsconfig.Options {
	d := grpctlsDefaults()
	return tlsconfig.Options{
		ServerCert: coalesceNonEmpty(viper.GetString("grpc_tls.cert_file"), d.CertFile),
		ServerKey:  coalesceNonEmpty(viper.GetString("grpc_tls.key_file"), d.KeyFile),
		RootCA:     coalesceNonEmpty(viper.GetString("grpc_tls.ca_file"), d.CAFile),
		ClientCA:   coalesceNonEmpty(viper.GetString("grpc_tls.client_ca_file"), d.ClientCAFile),
		ClientCert: coalesceNonEmpty(viper.GetString("grpc_tls.client_cert_file"), d.ClientCertFile),
		ClientKey:  coalesceNonEmpty(viper.GetString("grpc_tls.client_key_file"), d.ClientKeyFile),
	}
}

func ValidateGRPCTLSForRole(role GRPCTLSDaemonRole) error {
	if GRPCTLSInsecure() {
		return nil
	}

	o := grpctlsOptionsFromViper()
	switch role {
	case GRPCTLSDaemonRegistry:
		if o.ServerCert == "" || o.ServerKey == "" {
			return errors.New("grpc_tls: cert_file and key_file are required for vectis-registry when grpc_tls.insecure is false")
		}
	case GRPCTLSDaemonQueue, GRPCTLSDaemonLog:
		if o.ServerCert == "" || o.ServerKey == "" {
			return errors.New("grpc_tls: cert_file and key_file are required when grpc_tls.insecure is false")
		}
		if o.RootCA == "" {
			return errors.New("grpc_tls: ca_file is required to dial the registry when grpc_tls.insecure is false")
		}
	case GRPCTLSDaemonClientOnly:
		if o.RootCA == "" {
			return errors.New("grpc_tls: ca_file is required when grpc_tls.insecure is false")
		}
	default:
		return fmt.Errorf("grpc_tls: unknown daemon role %d", role)
	}

	if _, err := tlsconfig.NewReloader(o); err != nil {
		return fmt.Errorf("grpc_tls: load PEM material: %w", err)
	}

	return nil
}

var (
	grpcTLSOnce sync.Once
	grpcTLSRel  *tlsconfig.Reloader
	grpcTLSErr  error
)

func grpcTLSReloader() (*tlsconfig.Reloader, error) {
	if GRPCTLSInsecure() {
		return nil, nil
	}

	grpcTLSOnce.Do(func() {
		grpcTLSRel, grpcTLSErr = tlsconfig.NewReloader(grpctlsOptionsFromViper())
	})

	return grpcTLSRel, grpcTLSErr
}

func GRPCServerOptions() ([]grpc.ServerOption, error) {
	if GRPCTLSInsecure() {
		return nil, nil
	}

	r, err := grpcTLSReloader()
	if err != nil {
		return nil, err
	}

	if r == nil {
		return nil, errors.New("grpc_tls: reloader is nil")
	}

	creds, err := r.ServerGRPC()
	if err != nil {
		return nil, err
	}

	return []grpc.ServerOption{grpc.Creds(creds)}, nil
}

func grpcTransportCredsForTarget(directHostPort string) (credentials.TransportCredentials, error) {
	if GRPCTLSInsecure() {
		return insecure.NewCredentials(), nil
	}

	r, err := grpcTLSReloader()
	if err != nil {
		return nil, err
	}

	if r == nil {
		return nil, errors.New("grpc_tls: reloader is nil")
	}

	sn := strings.TrimSpace(viper.GetString("grpc_tls.server_name"))
	if sn == "" {
		sn = strings.TrimSpace(grpctlsDefaults().ServerName)
	}

	if sn == "" && directHostPort != "" {
		sn = tlsServerNameFromHostPort(directHostPort)
	}

	return r.ClientGRPC(sn)
}

func GRPCClientDialOptions(directHostPort string) ([]grpc.DialOption, error) {
	tc, err := grpcTransportCredsForTarget(directHostPort)
	if err != nil {
		return nil, err
	}

	return []grpc.DialOption{grpc.WithTransportCredentials(tc)}, nil
}

func GRPCResolverDialOptions() ([]grpc.DialOption, error) {
	return GRPCClientDialOptions("")
}

func tlsServerNameFromHostPort(hostPort string) string {
	host, _, err := net.SplitHostPort(hostPort)
	if err != nil || host == "" {
		return "localhost"
	}

	return host
}

func TLSServerNameForDial(hostPort string) string {
	if sn := strings.TrimSpace(viper.GetString("grpc_tls.server_name")); sn != "" {
		return sn
	}

	if sn := strings.TrimSpace(grpctlsDefaults().ServerName); sn != "" {
		return sn
	}

	return tlsServerNameFromHostPort(hostPort)
}

func StartGRPCTLSReloadLoop(ctx context.Context) {
	if GRPCTLSInsecure() {
		return
	}

	interval := GRPCTLSReloadInterval()
	if interval <= 0 {
		return
	}

	r, err := grpcTLSReloader()
	if err != nil || r == nil {
		return
	}

	go func() { _ = r.RunReloadLoop(ctx, interval) }()
}
