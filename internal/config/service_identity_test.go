package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/spf13/viper"

	"vectis/internal/localpki"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestServiceIdentityAllowedClientIdentitiesFromViper(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("service_identity.queue_allowed_client_identities", []string{
		"spiffe://vectis.local/service/api, spiffe://vectis.local/service/cron",
		"spiffe://vectis.local/service/api",
	})

	got := ServiceIdentityAllowedClientIdentities(ServiceIdentityRoleQueue)
	want := []string{
		"spiffe://vectis.local/service/api",
		"spiffe://vectis.local/service/cron",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ServiceIdentityAllowedClientIdentities = %v, want %v", got, want)
	}
}

func TestGRPCServerOptionsForRoleRejectsInvalidServiceIdentity(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("service_identity.queue_allowed_client_identities", []string{"https://vectis.local/service/api"})

	_, err := GRPCServerOptionsForRole(ServiceIdentityRoleQueue)
	if err == nil || !strings.Contains(err.Error(), "spiffe://") {
		t.Fatalf("GRPCServerOptionsForRole error = %v, want SPIFFE validation error", err)
	}
}

func TestGRPCServerOptionsForRoleRejectsServiceIdentityWithInsecureTLS(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("grpc_tls.insecure", true)
	viper.Set("service_identity.queue_allowed_client_identities", []string{"spiffe://vectis.local/service/api"})

	_, err := GRPCServerOptionsForRole(ServiceIdentityRoleQueue)
	if err == nil || !strings.Contains(err.Error(), "grpc_tls.insecure=false") {
		t.Fatalf("GRPCServerOptionsForRole error = %v, want grpc_tls.insecure=false", err)
	}
}

func TestGRPCServerOptionsForRoleRejectsServiceIdentityWithoutClientCA(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("grpc_tls.insecure", false)
	viper.Set("service_identity.queue_allowed_client_identities", []string{"spiffe://vectis.local/service/api"})

	_, err := GRPCServerOptionsForRole(ServiceIdentityRoleQueue)
	if err == nil || !strings.Contains(err.Error(), "grpc_tls.client_ca_file") {
		t.Fatalf("GRPCServerOptionsForRole error = %v, want grpc_tls.client_ca_file", err)
	}
}

func TestGRPCServerOptionsForRoleAllowsLocalServiceIdentity(t *testing.T) {
	resetGRPCTLSCache(t)
	viper.Reset()
	t.Cleanup(viper.Reset)

	m, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	m.ApplyParentViper(viper.Set)
	viper.Set("service_identity.queue_allowed_client_identities", []string{localpki.LocalServiceIdentity})

	opts, err := GRPCServerOptionsForRole(ServiceIdentityRoleQueue)
	if err != nil {
		t.Fatalf("GRPCServerOptionsForRole: %v", err)
	}

	if len(opts) == 0 {
		t.Fatal("GRPCServerOptionsForRole returned no options")
	}
}

func TestAuthorizeGRPCPeerIdentity(t *testing.T) {
	allowed := []string{"spiffe://vectis.local/service/api"}

	t.Run("allows matching identity", func(t *testing.T) {
		ctx := grpcPeerContextWithURI(t, "spiffe://vectis.local/service/api")
		if err := authorizeGRPCPeerIdentity(ctx, ServiceIdentityRoleQueue, allowed); err != nil {
			t.Fatalf("authorizeGRPCPeerIdentity: %v", err)
		}
	})

	t.Run("rejects missing peer", func(t *testing.T) {
		err := authorizeGRPCPeerIdentity(context.Background(), ServiceIdentityRoleQueue, allowed)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("authorizeGRPCPeerIdentity status = %v, want %v; err=%v", status.Code(err), codes.Unauthenticated, err)
		}
	})

	t.Run("rejects missing uri san", func(t *testing.T) {
		ctx := peer.NewContext(context.Background(), &peer.Peer{
			AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
				PeerCertificates: []*x509.Certificate{{}},
			}},
		})

		err := authorizeGRPCPeerIdentity(ctx, ServiceIdentityRoleQueue, allowed)
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("authorizeGRPCPeerIdentity status = %v, want %v; err=%v", status.Code(err), codes.Unauthenticated, err)
		}
	})

	t.Run("rejects wrong identity", func(t *testing.T) {
		ctx := grpcPeerContextWithURI(t, "spiffe://vectis.local/service/worker")
		err := authorizeGRPCPeerIdentity(ctx, ServiceIdentityRoleQueue, allowed)
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("authorizeGRPCPeerIdentity status = %v, want %v; err=%v", status.Code(err), codes.PermissionDenied, err)
		}
	})
}

func resetGRPCTLSCache(t *testing.T) {
	t.Helper()

	grpcTLSOnce = sync.Once{}
	grpcTLSRel = nil
	grpcTLSErr = nil
	t.Cleanup(func() {
		grpcTLSOnce = sync.Once{}
		grpcTLSRel = nil
		grpcTLSErr = nil
	})
}

func grpcPeerContextWithURI(t *testing.T, raw string) context.Context {
	t.Helper()

	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse URI %q: %v", raw, err)
	}

	return peer.NewContext(context.Background(), &peer.Peer{
		AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				{URIs: []*url.URL{u}},
			},
		}},
	})
}
