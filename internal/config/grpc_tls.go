package config

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"vectis/internal/serviceidentity"
	"vectis/internal/tlsconfig"

	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
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
	GRPCTLSDaemonArtifact                            // server + dials registry
	GRPCTLSDaemonWorker                              // server + dials registry/queue/log
	GRPCTLSDaemonClientOnly                          // vectis-api, cron, reconciler, log-forwarder (dial-only)
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
	case GRPCTLSDaemonQueue, GRPCTLSDaemonLog, GRPCTLSDaemonArtifact, GRPCTLSDaemonWorker:
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

// GRPCServerOptions is a compatibility wrapper for tests and role-neutral
// servers. Internal service listeners should use GRPCServerOptionsForRole.
func GRPCServerOptions() ([]grpc.ServerOption, error) {
	return GRPCServerOptionsForRole(ServiceIdentityRoleNone)
}

func GRPCServerOptionsForRole(role ServiceIdentityRole) ([]grpc.ServerOption, error) {
	opts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	}

	allowedIdentities, err := grpcServiceIdentityAllowlist(role)
	if err != nil {
		return nil, err
	}

	if len(allowedIdentities) > 0 {
		if GRPCTLSInsecure() {
			return nil, fmt.Errorf("%s requires grpc_tls.insecure=false", serviceIdentityAllowlistLabel(role))
		}

		if grpctlsOptionsFromViper().ClientCA == "" {
			return nil, fmt.Errorf("%s requires grpc_tls.client_ca_file so peer certificates are verified", serviceIdentityAllowlistLabel(role))
		}

		opts = append(opts,
			grpc.ChainUnaryInterceptor(serviceIdentityUnaryInterceptor(role, allowedIdentities)),
			grpc.ChainStreamInterceptor(serviceIdentityStreamInterceptor(role, allowedIdentities)),
		)
	}

	if GRPCTLSInsecure() {
		return opts, nil
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

	opts = append(opts, grpc.Creds(creds))
	return opts, nil
}

func grpcServiceIdentityAllowlist(role ServiceIdentityRole) ([]string, error) {
	switch role {
	case ServiceIdentityRoleNone:
		return nil, nil
	case ServiceIdentityRoleRegistry, ServiceIdentityRoleQueue, ServiceIdentityRoleLog, ServiceIdentityRoleArtifact, ServiceIdentityRoleWorkerControl:
	default:
		return nil, fmt.Errorf("service_identity: unknown gRPC service identity role %d", role)
	}

	return validateServiceIdentityAllowlist(serviceIdentityAllowlistLabel(role), ServiceIdentityAllowedClientIdentities(role))
}

func serviceIdentityAllowlistLabel(role ServiceIdentityRole) string {
	switch role {
	case ServiceIdentityRoleRegistry:
		return "service_identity.registry_allowed_client_identities"
	case ServiceIdentityRoleQueue:
		return "service_identity.queue_allowed_client_identities"
	case ServiceIdentityRoleLog:
		return "service_identity.log_allowed_client_identities"
	case ServiceIdentityRoleArtifact:
		return "service_identity.artifact_allowed_client_identities"
	case ServiceIdentityRoleWorkerControl:
		return "service_identity.worker_control_allowed_client_identities"
	default:
		return "service_identity"
	}
}

func serviceIdentityUnaryInterceptor(role ServiceIdentityRole, allowedIdentities []string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authorizeGRPCPeerIdentity(ctx, role, allowedIdentities); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func serviceIdentityStreamInterceptor(role ServiceIdentityRole, allowedIdentities []string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorizeGRPCPeerIdentity(ss.Context(), role, allowedIdentities); err != nil {
			return err
		}

		return handler(srv, ss)
	}
}

func authorizeGRPCPeerIdentity(ctx context.Context, role ServiceIdentityRole, allowedIdentities []string) error {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "service identity: missing gRPC peer")
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return status.Error(codes.Unauthenticated, "service identity: verified mTLS peer certificate is required")
	}

	if _, err := serviceidentity.AuthorizePeerCertificate(tlsInfo.State.PeerCertificates, allowedIdentities); err != nil {
		switch {
		case errors.Is(err, serviceidentity.ErrNoPeerCertificate), errors.Is(err, serviceidentity.ErrNoPeerIdentity):
			return status.Error(codes.Unauthenticated, err.Error())
		case errors.Is(err, serviceidentity.ErrIdentityDenied):
			return status.Error(codes.PermissionDenied, err.Error())
		default:
			return status.Errorf(codes.PermissionDenied, "service identity: invalid %s policy: %v", role.String(), err)
		}
	}

	return nil
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

	return []grpc.DialOption{
		grpc.WithTransportCredentials(tc),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	}, nil
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
