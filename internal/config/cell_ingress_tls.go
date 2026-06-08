package config

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"vectis/internal/tlsconfig"
)

// ValidateCellIngressHTTPMTLSConfig verifies that exposed cell ingress HTTP uses
// the same mutual TLS trust root as internal gRPC.
func ValidateCellIngressHTTPMTLSConfig(localCellID, bindHost string) error {
	required, err := cellIngressMTLSRequired(localCellID, bindHost)
	if err != nil {
		return err
	}

	allowedIdentities, err := validateServiceIdentityAllowlist(
		"service_identity.cell_ingress_allowed_producer_identities",
		CellIngressAllowedProducerIdentities(),
	)

	if err != nil {
		return err
	}

	if GRPCTLSInsecure() {
		if len(allowedIdentities) > 0 {
			return errors.New("cell_ingress: grpc_tls.insecure must be false when service_identity.cell_ingress_allowed_producer_identities is configured")
		}

		if required {
			return errors.New("cell_ingress: grpc_tls.insecure must be false when cell ingress binds or advertises a non-loopback endpoint")
		}

		return nil
	}

	o := grpctlsOptionsFromViper()
	if o.ServerCert == "" || o.ServerKey == "" {
		return errors.New("cell_ingress: grpc_tls.cert_file and grpc_tls.key_file are required for HTTPS")
	}

	if o.ClientCA == "" {
		return errors.New("cell_ingress: grpc_tls.client_ca_file is required so cell ingress can verify producer client certificates")
	}

	if _, err := tlsconfig.NewReloader(o); err != nil {
		return fmt.Errorf("cell_ingress: load mTLS material: %w", err)
	}

	return nil
}

func CellIngressHTTPSListener(ln net.Listener) (net.Listener, string, error) {
	if GRPCTLSInsecure() {
		return ln, "http", nil
	}

	r, err := grpcTLSReloader()
	if err != nil {
		_ = ln.Close()
		return nil, "", err
	}

	if r == nil {
		_ = ln.Close()
		return nil, "", errors.New("cell_ingress: grpc TLS reloader is nil")
	}

	cfg, err := r.ServerTLS()
	if err != nil {
		_ = ln.Close()
		return nil, "", err
	}

	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		_ = ln.Close()
		return nil, "", errors.New("cell_ingress: producer mTLS requires grpc_tls.client_ca_file")
	}

	return tls.NewListener(ln, cfg), "https", nil
}

func CellIngressHTTPClientTLSConfig(endpoint string) (*tls.Config, error) {
	u, err := cellIngressEndpointURL(endpoint)
	if err != nil {
		return nil, err
	}

	host := u.Host
	switch u.Scheme {
	case "http":
		if !cellIngressHostIsLoopback(host) {
			return nil, fmt.Errorf("cell ingress endpoint %q must use https with mTLS when it is not loopback", endpoint)
		}

		if !GRPCTLSInsecure() {
			return nil, fmt.Errorf("cell ingress endpoint %q must use https when grpc_tls.insecure is false", endpoint)
		}

		return nil, nil
	case "https":
		if GRPCTLSInsecure() {
			return nil, fmt.Errorf("cell ingress endpoint %q uses https, but grpc_tls.insecure is true", endpoint)
		}
	default:
		return nil, fmt.Errorf("cell ingress endpoint must use http or https")
	}

	o := grpctlsOptionsFromViper()
	if o.RootCA == "" {
		return nil, errors.New("cell_ingress: grpc_tls.ca_file is required to verify cell ingress HTTPS")
	}

	if o.ClientCert == "" || o.ClientKey == "" {
		return nil, errors.New("cell_ingress: grpc_tls.client_cert_file and grpc_tls.client_key_file are required for producer mTLS")
	}

	r, err := grpcTLSReloader()
	if err != nil {
		return nil, err
	}

	if r == nil {
		return nil, errors.New("cell_ingress: grpc TLS reloader is nil")
	}

	cfg, err := r.ClientTLS(cellIngressTLSServerName(u.Host))
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func ValidateCellIngressHTTPClientMTLSConfig(endpointSpecs []string) error {
	endpoints, err := ParseCellIngressEndpoints(endpointSpecs)
	if err != nil {
		return err
	}

	for cellID, endpoint := range endpoints {
		if _, err := CellIngressHTTPClientTLSConfig(endpoint); err != nil {
			return fmt.Errorf("cell ingress endpoint %q: %w", cellID, err)
		}
	}

	return nil
}

func cellIngressMTLSRequired(localCellID, bindHost string) (bool, error) {
	if !cellIngressHostIsLoopback(bindHost) {
		return true, nil
	}

	host, err := localCellIngressEndpointHost(localCellID)
	if err != nil {
		return false, err
	}

	if host == "" {
		return false, nil
	}

	return !cellIngressHostIsLoopback(host), nil
}

func localCellIngressEndpointHost(localCellID string) (string, error) {
	endpoints, err := CellIngressEndpoints()
	if err != nil {
		return "", err
	}

	localCellID = strings.TrimSpace(localCellID)
	if localCellID == "" {
		localCellID = CellID()
	}

	endpoint := strings.TrimSpace(endpoints[localCellID])
	if endpoint == "" {
		return "", nil
	}

	host, err := cellIngressEndpointHost(endpoint)
	if err != nil {
		return "", fmt.Errorf("cell ingress mTLS: %w", err)
	}

	return host, nil
}

func cellIngressEndpointURL(endpoint string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return nil, fmt.Errorf("parse cell ingress endpoint: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("cell ingress endpoint must use http or https")
	}

	if strings.TrimSpace(u.Host) == "" {
		return nil, fmt.Errorf("cell ingress endpoint host is required")
	}

	return u, nil
}

func cellIngressTLSServerName(hostPort string) string {
	return TLSServerNameForDial(hostPort)
}

func cellIngressHostIsLoopback(raw string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(raw))
	if err != nil {
		host = strings.Trim(strings.TrimSpace(raw), "[]")
	}

	host = strings.ToLower(strings.TrimSuffix(host, "."))
	switch host {
	case "", "localhost":
		return true
	case "0.0.0.0", "::":
		return false
	}

	if strings.HasSuffix(host, ".localhost") {
		return true
	}

	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
