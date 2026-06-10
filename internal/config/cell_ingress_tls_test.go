package config

import (
	"strings"
	"testing"

	"github.com/spf13/viper"

	"vectis/internal/localpki"
)

func TestValidateCellIngressHTTPMTLSConfig(t *testing.T) {
	tests := []struct {
		name      string
		bindHost  string
		endpoints []string
		setupTLS  func(*testing.T)
		wantErr   string
	}{
		{
			name:     "loopback plaintext allowed with insecure grpc",
			bindHost: "localhost",
			setupTLS: func(t *testing.T) {
				t.Helper()
				viper.Set("grpc_tls.insecure", true)
			},
		},
		{
			name:     "identity allowlist requires mtls even on loopback",
			bindHost: "localhost",
			setupTLS: func(t *testing.T) {
				t.Helper()
				viper.Set("grpc_tls.insecure", true)
				viper.Set("service_identity.cell_ingress_allowed_producer_identities", []string{"spiffe://vectis.internal/service/api"})
			},
			wantErr: "cell_ingress_allowed_producer_identities",
		},
		{
			name:     "identity allowlist validates spiffe ids",
			bindHost: "localhost",
			setupTLS: func(t *testing.T) {
				t.Helper()
				viper.Set("grpc_tls.insecure", true)
				viper.Set("service_identity.cell_ingress_allowed_producer_identities", []string{"https://vectis.internal/service/api"})
			},
			wantErr: "spiffe://",
		},
		{
			name:     "non loopback bind requires mtls",
			bindHost: "0.0.0.0",
			setupTLS: func(t *testing.T) {
				t.Helper()
				viper.Set("grpc_tls.insecure", true)
			},
			wantErr: "grpc_tls.insecure must be false",
		},
		{
			name:      "non loopback endpoint requires mtls",
			bindHost:  "localhost",
			endpoints: []string{"local=https://ingress.example:8085"},
			setupTLS: func(t *testing.T) {
				t.Helper()
				viper.Set("grpc_tls.insecure", true)
			},
			wantErr: "grpc_tls.insecure must be false",
		},
		{
			name:     "mtls requires client ca",
			bindHost: "localhost",
			setupTLS: func(t *testing.T) {
				t.Helper()
				m := mustLocalPKI(t)
				m.ApplyParentViper(viper.Set)
				viper.Set("grpc_tls.client_ca_file", "")
			},
			wantErr: "client_ca_file",
		},
		{
			name:     "mtls material valid",
			bindHost: "0.0.0.0",
			setupTLS: func(t *testing.T) {
				t.Helper()
				mustLocalPKI(t).ApplyParentViper(viper.Set)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			if tt.setupTLS != nil {
				tt.setupTLS(t)
			}

			if len(tt.endpoints) > 0 {
				viper.Set("cell_ingress_endpoints", tt.endpoints)
			}

			err := ValidateCellIngressHTTPMTLSConfig("local", tt.bindHost)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateCellIngressHTTPMTLSConfig: %v", err)
				}

				return
			}

			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateCellIngressHTTPMTLSConfig error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestCellIngressHTTPClientTLSConfig(t *testing.T) {
	t.Run("loopback http allowed when insecure", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		viper.Set("grpc_tls.insecure", true)

		cfg, err := CellIngressHTTPClientTLSConfig("http://localhost:8085")
		if err != nil {
			t.Fatalf("CellIngressHTTPClientTLSConfig: %v", err)
		}

		if cfg != nil {
			t.Fatalf("TLS config = %+v, want nil for plaintext loopback", cfg)
		}
	})

	t.Run("non loopback http rejected", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		viper.Set("grpc_tls.insecure", true)

		if _, err := CellIngressHTTPClientTLSConfig("http://ingress.example:8085"); err == nil {
			t.Fatal("CellIngressHTTPClientTLSConfig succeeded for non-loopback HTTP")
		}
	})

	t.Run("https requires grpc tls", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		viper.Set("grpc_tls.insecure", true)

		if _, err := CellIngressHTTPClientTLSConfig("https://localhost:8085"); err == nil {
			t.Fatal("CellIngressHTTPClientTLSConfig succeeded with grpc_tls.insecure=true")
		}
	})

	t.Run("https requires client certificate", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		mustLocalPKI(t).ApplyParentViper(viper.Set)
		viper.Set("grpc_tls.client_cert_file", "")

		_, err := CellIngressHTTPClientTLSConfig("https://localhost:8085")
		if err == nil || !strings.Contains(err.Error(), "client_cert_file") {
			t.Fatalf("CellIngressHTTPClientTLSConfig error = %v, want client_cert_file", err)
		}
	})

	t.Run("https builds mtls client config", func(t *testing.T) {
		viper.Reset()
		t.Cleanup(viper.Reset)
		mustLocalPKI(t).ApplyParentViper(viper.Set)

		cfg, err := CellIngressHTTPClientTLSConfig("https://localhost:8085")
		if err != nil {
			t.Fatalf("CellIngressHTTPClientTLSConfig: %v", err)
		}

		if cfg == nil {
			t.Fatal("TLS config is nil")
			return
		}

		if cfg.GetClientCertificate == nil {
			t.Fatal("TLS config does not provide a client certificate")
		}
	})
}

func mustLocalPKI(t *testing.T) *localpki.Material {
	t.Helper()
	m, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatalf("localpki.Ensure: %v", err)
	}

	return m
}
