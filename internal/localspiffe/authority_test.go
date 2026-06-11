package localspiffe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/localpki"
	secretstore "vectis/internal/secrets"
	"vectis/internal/spire"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestAuthorityRegistersAndFetchesX509SVID(t *testing.T) {
	cfg := testConfig(t)
	authority, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(authority.Stop)

	selector := spire.Selector{Type: "unix", Value: "uid:" + strconv.Itoa(os.Getuid())}
	spiffeID := "spiffe://" + cfg.TrustDomain + "/cell/local/namespace/root/job/demo/run/1/execution/1"
	parentID := "spiffe://" + cfg.TrustDomain + "/vectis-spiffe/agent/local"
	key, err := spire.RegistrationKey(spiffeID, parentID, []spire.Selector{selector})
	if err != nil {
		t.Fatalf("RegistrationKey: %v", err)
	}

	registrar, cleanup, err := spire.DialSPIREServerRegistrar("unix://" + cfg.RegistrationSocketPath)
	if err != nil {
		t.Fatalf("DialSPIREServerRegistrar: %v", err)
	}
	t.Cleanup(cleanup)

	intent := spire.RegistrationIntent{
		Key:            key,
		SPIFFEID:       spiffeID,
		ParentSPIFFEID: parentID,
		Selectors:      []spire.Selector{selector},
		ExpiresAt:      time.Now().Add(time.Minute).UTC(),
	}

	result, err := registrar.EnsureRegistration(context.Background(), intent)
	if err != nil {
		t.Fatalf("EnsureRegistration create: %v", err)
	}

	if !result.Created || result.Handle.EntryID == "" {
		t.Fatalf("create result = %+v, want created handle", result)
	}

	result, err = registrar.EnsureRegistration(context.Background(), intent)
	if err != nil {
		t.Fatalf("EnsureRegistration existing/update: %v", err)
	}

	if result.Created {
		t.Fatalf("second EnsureRegistration Created = true, want update of managed existing entry")
	}

	source, err := spire.NewWorkloadAPISource("unix://" + cfg.WorkloadSocketPath)
	if err != nil {
		t.Fatalf("NewWorkloadAPISource: %v", err)
	}

	svid, err := spire.FetchX509SVID(context.Background(), source, spiffeID)
	if err != nil {
		t.Fatalf("FetchX509SVID: %v", err)
	}

	if svid.SPIFFEID != spiffeID {
		t.Fatalf("SVID SPIFFE ID = %q, want %q", svid.SPIFFEID, spiffeID)
	}

	if len(svid.Certificates) != 1 || svid.PrivateKey == nil {
		t.Fatalf("SVID material = certs:%d key:%v", len(svid.Certificates), svid.PrivateKey != nil)
	}

	bundlePEM, err := os.ReadFile(cfg.BundleFile)
	if err != nil {
		t.Fatalf("read bundle: %v", err)
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(bundlePEM) {
		t.Fatal("bundle did not contain a PEM certificate")
	}

	if _, err := svid.Certificates[0].Verify(x509.VerifyOptions{
		Roots:     roots,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		t.Fatalf("verify SVID certificate: %v", err)
	}

	if err := registrar.ReleaseRegistration(context.Background(), result.Handle); err != nil {
		t.Fatalf("ReleaseRegistration: %v", err)
	}

	if _, err := spire.FetchX509SVID(context.Background(), source, spiffeID); err == nil || !strings.Contains(err.Error(), "no SVIDs") {
		t.Fatalf("FetchX509SVID after release error = %v, want no SVIDs", err)
	}
}

func TestAuthoritySVIDResolvesEncryptedFSSecretOverGRPC(t *testing.T) {
	cfg := testConfig(t)
	authority, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(authority.Stop)

	spiffeID := "spiffe://" + cfg.TrustDomain + "/cell/local/namespace/root/job/demo/run/run-1/execution/execution-1"
	svid := fetchRegisteredTestSVID(t, cfg, spiffeID)

	root := t.TempDir()
	key := []byte("0123456789abcdef0123456789abcdef")
	if err := secretstore.WriteEncryptedFSSecretFile(root, "encryptedfs://team/token", []byte("secret-value"), key); err != nil {
		t.Fatalf("WriteEncryptedFSSecretFile: %v", err)
	}

	provider, err := secretstore.NewEncryptedFSProvider(root, secretstore.WithEncryptedFSKey(key))
	if err != nil {
		t.Fatalf("NewEncryptedFSProvider: %v", err)
	}

	policy, err := secretstore.NewAccessPolicy([]string{"namespace=root;job=demo;task=verify;ref=encryptedfs://team/*"})
	if err != nil {
		t.Fatalf("NewAccessPolicy: %v", err)
	}

	authorizer := secretstore.NewClaimAuthorizer(
		staticExecutionClaimValidator{},
		secretstore.WithExecutionScopeResolver(staticExecutionScopeResolver{scope: secretstore.ExecutionScope{
			SPIFFEID:      spiffeID,
			TrustDomain:   cfg.TrustDomain,
			NamespacePath: "root",
			CellID:        "local",
			JobID:         "demo",
			RunID:         "run-1",
			TaskKey:       "verify",
			ExecutionID:   "execution-1",
		}}),
		secretstore.WithAccessPolicy(policy),
	)
	server := secretstore.NewServer(provider, authorizer)

	tlsMaterial, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatalf("localpki.Ensure: %v", err)
	}

	listener := startTestSecretsService(t, tlsMaterial, cfg.BundleFile, server)
	conn := dialTestSecretsService(t, tlsMaterial.CAFile, listener, svid)
	defer conn.Close()

	resolver := secretstore.NewGRPCResolver(conn)
	bundle, err := resolver.Resolve(context.Background(), secretstore.ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: "claim-token",
		Secrets: []secretstore.Reference{{
			ID:  "token",
			Ref: "encryptedfs://team/token",
			Delivery: secretstore.Delivery{
				Type: secretstore.DeliveryTypeFile,
				Path: "secrets/token",
			},
		}},
	})

	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if len(bundle.Files) != 1 {
		t.Fatalf("files = %+v, want one", bundle.Files)
	}

	file := bundle.Files[0]
	if file.ID != "token" || file.Path != "secrets/token" || string(file.Data) != "secret-value" || file.Mode != secretstore.DefaultFileMode {
		t.Fatalf("file = %+v", file)
	}
}

func TestAuthoritySVIDRejectsUnauthorizedSecretResolveOverGRPC(t *testing.T) {
	cfg := testConfig(t)
	authority, err := Start(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(authority.Stop)

	spiffeID := "spiffe://" + cfg.TrustDomain + "/cell/local/namespace/root/job/demo/run/run-1/execution/execution-1"
	svid := fetchRegisteredTestSVID(t, cfg, spiffeID)
	wrongSVID := fetchRegisteredTestSVID(t, cfg, "spiffe://"+cfg.TrustDomain+"/cell/local/namespace/root/job/demo/run/run-1/execution/other")

	root := t.TempDir()
	key := []byte("0123456789abcdef0123456789abcdef")
	if err := secretstore.WriteEncryptedFSSecretFile(root, "encryptedfs://team/token", []byte("secret-value"), key); err != nil {
		t.Fatalf("WriteEncryptedFSSecretFile: %v", err)
	}

	provider, err := secretstore.NewEncryptedFSProvider(root, secretstore.WithEncryptedFSKey(key))
	if err != nil {
		t.Fatalf("NewEncryptedFSProvider: %v", err)
	}

	policy, err := secretstore.NewAccessPolicy([]string{"namespace=root;job=demo;task=verify;ref=encryptedfs://team/token"})
	if err != nil {
		t.Fatalf("NewAccessPolicy: %v", err)
	}

	authorizer := secretstore.NewClaimAuthorizer(
		staticExecutionClaimValidator{},
		secretstore.WithExecutionScopeResolver(staticExecutionScopeResolver{scope: secretstore.ExecutionScope{
			SPIFFEID:      spiffeID,
			TrustDomain:   cfg.TrustDomain,
			NamespacePath: "root",
			CellID:        "local",
			JobID:         "demo",
			RunID:         "run-1",
			TaskKey:       "verify",
			ExecutionID:   "execution-1",
		}}),
		secretstore.WithAccessPolicy(policy),
	)

	server := secretstore.NewServer(provider, authorizer)
	tlsMaterial, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatalf("localpki.Ensure: %v", err)
	}

	listener := startTestSecretsService(t, tlsMaterial, cfg.BundleFile, server)
	tests := []struct {
		name      string
		conn      func(*testing.T) *grpc.ClientConn
		req       secretstore.ResolveRequest
		wantCodes []codes.Code
	}{
		{
			name: "wrong claim token",
			conn: func(t *testing.T) *grpc.ClientConn {
				return dialTestSecretsService(t, tlsMaterial.CAFile, listener, svid)
			},
			req:       testResolveRequest("bad-claim", "encryptedfs://team/token"),
			wantCodes: []codes.Code{codes.PermissionDenied},
		},
		{
			name: "mismatched svid",
			conn: func(t *testing.T) *grpc.ClientConn {
				return dialTestSecretsService(t, tlsMaterial.CAFile, listener, wrongSVID)
			},
			req:       testResolveRequest("claim-token", "encryptedfs://team/token"),
			wantCodes: []codes.Code{codes.PermissionDenied},
		},
		{
			name: "policy denied",
			conn: func(t *testing.T) *grpc.ClientConn {
				return dialTestSecretsService(t, tlsMaterial.CAFile, listener, svid)
			},
			req:       testResolveRequest("claim-token", "encryptedfs://team/other"),
			wantCodes: []codes.Code{codes.PermissionDenied},
		},
		{
			name: "missing client certificate",
			conn: func(t *testing.T) *grpc.ClientConn {
				return dialTestSecretsServiceWithoutClientCert(t, tlsMaterial.CAFile, listener)
			},
			req:       testResolveRequest("claim-token", "encryptedfs://team/token"),
			wantCodes: []codes.Code{codes.Unavailable},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := tt.conn(t)
			defer conn.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_, err := secretstore.NewGRPCResolver(conn).Resolve(ctx, tt.req)
			if err == nil {
				t.Fatal("Resolve succeeded, want denial")
			}

			got := status.Code(err)
			for _, want := range tt.wantCodes {
				if got == want {
					return
				}
			}
			t.Fatalf("Resolve code = %v, want one of %v (err=%v)", got, tt.wantCodes, err)
		})
	}
}

func fetchRegisteredTestSVID(t *testing.T, cfg Config, spiffeID string) spire.X509SVID {
	t.Helper()

	selector := spire.Selector{Type: "unix", Value: "uid:" + strconv.Itoa(os.Getuid())}
	parentID := "spiffe://" + cfg.TrustDomain + "/vectis-spiffe/agent/local"
	key, err := spire.RegistrationKey(spiffeID, parentID, []spire.Selector{selector})
	if err != nil {
		t.Fatalf("RegistrationKey: %v", err)
	}

	registrar, cleanup, err := spire.DialSPIREServerRegistrar("unix://" + cfg.RegistrationSocketPath)
	if err != nil {
		t.Fatalf("DialSPIREServerRegistrar: %v", err)
	}
	t.Cleanup(cleanup)

	result, err := registrar.EnsureRegistration(context.Background(), spire.RegistrationIntent{
		Key:            key,
		SPIFFEID:       spiffeID,
		ParentSPIFFEID: parentID,
		Selectors:      []spire.Selector{selector},
		ExpiresAt:      time.Now().Add(time.Minute).UTC(),
	})
	if err != nil {
		t.Fatalf("EnsureRegistration: %v", err)
	}
	t.Cleanup(func() { _ = registrar.ReleaseRegistration(context.Background(), result.Handle) })

	source, err := spire.NewWorkloadAPISource("unix://" + cfg.WorkloadSocketPath)
	if err != nil {
		t.Fatalf("NewWorkloadAPISource: %v", err)
	}

	svid, err := spire.FetchX509SVID(context.Background(), source, spiffeID)
	if err != nil {
		t.Fatalf("FetchX509SVID: %v", err)
	}

	return svid
}

func startTestSecretsService(t *testing.T, tlsMaterial *localpki.Material, clientCAFile string, server api.SecretsServiceServer) *bufconn.Listener {
	t.Helper()

	serverCert, err := tls.LoadX509KeyPair(tlsMaterial.ServerCert, tlsMaterial.ServerKey)
	if err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}

	clientCAPEM, err := os.ReadFile(clientCAFile)
	if err != nil {
		t.Fatalf("read client CA: %v", err)
	}

	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		t.Fatal("client CA bundle did not contain a PEM certificate")
	}

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})))

	api.RegisterSecretsServiceServer(grpcServer, server)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("secrets", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("test secrets service stopped: %v", err)
		}
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	return listener
}

func dialTestSecretsService(t *testing.T, caFile string, listener *bufconn.Listener, svid spire.X509SVID) *grpc.ClientConn {
	t.Helper()

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("read server CA: %v", err)
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("server CA did not contain a PEM certificate")
	}

	clientCert := tls.Certificate{
		PrivateKey: svid.PrivateKey,
		Leaf:       svid.Certificates[0],
	}

	for _, cert := range svid.Certificates {
		clientCert.Certificate = append(clientCert.Certificate, cert.Raw)
	}

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			ServerName:   "localhost",
			RootCAs:      roots,
			Certificates: []tls.Certificate{clientCert},
		})),
	)

	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	return conn
}

func dialTestSecretsServiceWithoutClientCert(t *testing.T, caFile string, listener *bufconn.Listener) *grpc.ClientConn {
	t.Helper()

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		t.Fatalf("read server CA: %v", err)
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		t.Fatal("server CA did not contain a PEM certificate")
	}

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: "localhost",
			RootCAs:    roots,
		})),
	)

	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	return conn
}

func testResolveRequest(claimToken, ref string) secretstore.ResolveRequest {
	return secretstore.ResolveRequest{
		RunID:               "run-1",
		ExecutionID:         "execution-1",
		ExecutionClaimToken: claimToken,
		Secrets: []secretstore.Reference{{
			ID:  "token",
			Ref: ref,
			Delivery: secretstore.Delivery{
				Type: secretstore.DeliveryTypeFile,
				Path: "secrets/token",
			},
		}},
	}
}

type staticExecutionClaimValidator struct{}

func (staticExecutionClaimValidator) ValidateActiveExecutionClaim(_ context.Context, runID, executionID, claimToken string) error {
	if runID != "run-1" || executionID != "execution-1" || claimToken != "claim-token" {
		return os.ErrPermission
	}

	return nil
}

type staticExecutionScopeResolver struct {
	scope secretstore.ExecutionScope
}

func (r staticExecutionScopeResolver) ResolveExecutionScope(context.Context, string, string) (secretstore.ExecutionScope, error) {
	return r.scope, nil
}

func testConfig(t *testing.T) Config {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "vectis-spiffe-*")
	if err != nil {
		t.Fatalf("create short temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return Config{
		TrustDomain:            "vectis.internal",
		DataDir:                filepath.Join(dir, "data"),
		RuntimeDir:             filepath.Join(dir, "run"),
		WorkloadSocketPath:     filepath.Join(dir, "run", "workload.sock"),
		RegistrationSocketPath: filepath.Join(dir, "run", "registration.sock"),
		BundleFile:             filepath.Join(dir, "data", "bundle.pem"),
		Selectors:              []string{"unix:uid:" + strconv.Itoa(os.Getuid())},
	}
}
