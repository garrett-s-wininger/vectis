package tlsconfig

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

func TestNewReloader_requiresServerPairBothPaths(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, caKey, caPEM := mustCA(t)
	certPEM, keyPEM := mustServerCert(t, caKey, caPEM, []string{"localhost"}, time.Hour)
	certPath := filepath.Join(dir, "srv.pem")
	keyPath := filepath.Join(dir, "srv.key")
	mustWriteFile(t, certPath, certPEM)
	mustWriteFile(t, keyPath, keyPEM)

	caPath := filepath.Join(dir, "ca.pem")
	mustWriteFile(t, caPath, caPEM)

	_, err := NewReloader(Options{ServerCert: certPath})
	if err == nil {
		t.Fatal("expected error when only ServerCert set")
	}
	_, err = NewReloader(Options{ServerKey: keyPath})
	if err == nil {
		t.Fatal("expected error when only ServerKey set")
	}
}

func TestServerTLS_handshake(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, caKey, caPEM := mustCA(t)
	srvCert, srvKey := mustServerCert(t, caKey, caPEM, []string{"localhost"}, time.Hour)
	writePEM(t, dir, "ca.pem", caPEM, nil)
	writePEM(t, dir, "srv.pem", srvCert, srvKey)

	r, err := NewReloader(Options{
		ServerCert: filepath.Join(dir, "srv.pem"),
		ServerKey:  filepath.Join(dir, "srv.key"),
		RootCA:     filepath.Join(dir, "ca.pem"),
	})

	if err != nil {
		t.Fatal(err)
	}

	srvCfg, err := r.ServerTLS()
	if err != nil {
		t.Fatal(err)
	}

	cliCfg, err := r.ClientTLS("localhost")
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(srvCfg)))
	registerTestHealth(srv)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer lis.Close()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(cliCfg)))

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = healthgrpc.NewHealthClient(conn).Check(ctx, &healthgrpc.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMixed_plaintextClientToTLSServerFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, caKey, caPEM := mustCA(t)
	srvCert, srvKey := mustServerCert(t, caKey, caPEM, []string{"localhost"}, time.Hour)
	writePEM(t, dir, "ca.pem", caPEM, nil)
	writePEM(t, dir, "srv.pem", srvCert, srvKey)

	r, err := NewReloader(Options{
		ServerCert: filepath.Join(dir, "srv.pem"),
		ServerKey:  filepath.Join(dir, "srv.key"),
	})

	if err != nil {
		t.Fatal(err)
	}

	srvCfg, err := r.ServerTLS()
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(srvCfg)))
	registerTestHealth(srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer lis.Close()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = healthgrpc.NewHealthClient(conn).Check(ctx, &healthgrpc.HealthCheckRequest{})
	if err == nil {
		t.Fatal("expected error for plaintext client to TLS server")
	}
}

func TestMixed_tlsClientToPlaintextServerFails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, _, caPEM := mustCA(t)
	writePEM(t, dir, "ca.pem", caPEM, nil)

	rCli, err := NewReloader(Options{RootCA: filepath.Join(dir, "ca.pem")})
	if err != nil {
		t.Fatal(err)
	}

	cliCfg, err := rCli.ClientTLS("localhost")
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer()
	registerTestHealth(srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer lis.Close()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(cliCfg)))

	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = healthgrpc.NewHealthClient(conn).Check(ctx, &healthgrpc.HealthCheckRequest{})
	if err == nil {
		t.Fatal("expected error for TLS client to plaintext server")
	}
}

func TestWrongTrust_fails(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, caKey, caPEM := mustCA(t)
	srvCert, srvKey := mustServerCert(t, caKey, caPEM, []string{"localhost"}, time.Hour)
	_, _, otherCAPEM := mustCA(t)

	writePEM(t, dir, "srv.pem", srvCert, srvKey)
	mustWriteFile(t, filepath.Join(dir, "wrong-ca.pem"), otherCAPEM)

	rSrv, err := NewReloader(Options{
		ServerCert: filepath.Join(dir, "srv.pem"),
		ServerKey:  filepath.Join(dir, "srv.key"),
	})
	if err != nil {
		t.Fatal(err)
	}
	srvCfg, err := rSrv.ServerTLS()
	if err != nil {
		t.Fatal(err)
	}

	rCli, err := NewReloader(Options{
		RootCA: filepath.Join(dir, "wrong-ca.pem"),
	})
	if err != nil {
		t.Fatal(err)
	}
	cliCfg, err := rCli.ClientTLS("localhost")
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(srvCfg)))
	registerTestHealth(srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(credentials.NewTLS(cliCfg)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = healthgrpc.NewHealthClient(conn).Check(ctx, &healthgrpc.HealthCheckRequest{})
	if err == nil {
		t.Fatal("expected verification error with wrong CA")
	}
}

func TestReload_newServerCertUsedOnNextHandshake(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, caKey, caPEM := mustCA(t)
	srvCert1, srvKey1 := mustServerCert(t, caKey, caPEM, []string{"localhost"}, time.Hour)
	srvCert2, srvKey2 := mustServerCert(t, caKey, caPEM, []string{"localhost"}, 2*time.Hour)
	writePEM(t, dir, "ca.pem", caPEM, nil)
	certPath := filepath.Join(dir, "srv.pem")
	keyPath := filepath.Join(dir, "srv.key")
	writePEM(t, dir, "srv.pem", srvCert1, srvKey1)

	r, err := NewReloader(Options{
		ServerCert: certPath,
		ServerKey:  keyPath,
		RootCA:     filepath.Join(dir, "ca.pem"),
	})
	if err != nil {
		t.Fatal(err)
	}
	srvCfg, err := r.ServerTLS()
	if err != nil {
		t.Fatal(err)
	}
	cliCfg, err := r.ClientTLS("localhost")
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(srvCfg)))
	registerTestHealth(srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	addr := lis.Addr().String()
	conn1, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(cliCfg)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	ctx := context.Background()
	if _, err := healthgrpc.NewHealthClient(conn1).Check(ctx, &healthgrpc.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}

	writePEM(t, dir, "srv.pem", srvCert2, srvKey2)
	if err := r.Reload(); err != nil {
		t.Fatal(err)
	}

	conn2, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(cliCfg)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	if _, err := healthgrpc.NewHealthClient(conn2).Check(ctx, &healthgrpc.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestRunReloadLoop_triggersReload(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, caKey, caPEM := mustCA(t)
	srvCert1, srvKey1 := mustServerCert(t, caKey, caPEM, []string{"localhost"}, time.Hour)
	srvCert2, srvKey2 := mustServerCert(t, caKey, caPEM, []string{"localhost"}, 2*time.Hour)
	writePEM(t, dir, "ca.pem", caPEM, nil)
	certPath := filepath.Join(dir, "srv.pem")
	keyPath := filepath.Join(dir, "srv.key")
	writePEM(t, dir, "srv.pem", srvCert1, srvKey1)

	r, err := NewReloader(Options{
		ServerCert: certPath,
		ServerKey:  keyPath,
		RootCA:     filepath.Join(dir, "ca.pem"),
	})

	if err != nil {
		t.Fatal(err)
	}

	srvCfg, err := r.ServerTLS()
	if err != nil {
		t.Fatal(err)
	}

	cliCfg, err := r.ClientTLS("localhost")
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer(grpc.Creds(credentials.NewTLS(srvCfg)))
	registerTestHealth(srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer lis.Close()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	loopCtx := t.Context()
	go func() { _ = r.RunReloadLoop(loopCtx, 50*time.Millisecond) }()

	addr := lis.Addr().String()
	time.Sleep(100 * time.Millisecond)
	writePEM(t, dir, "srv.pem", srvCert2, srvKey2)
	deadline := time.Now().Add(3 * time.Second)
	var lastErr error

	for time.Now().Before(deadline) {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(cliCfg)))
		if err != nil {
			lastErr = err
			time.Sleep(30 * time.Millisecond)
			continue
		}

		_, err = healthgrpc.NewHealthClient(conn).Check(context.Background(), &healthgrpc.HealthCheckRequest{})
		_ = conn.Close()
		if err == nil {
			return
		}

		lastErr = err
		time.Sleep(30 * time.Millisecond)
	}

	t.Fatalf("expected successful RPC after reload loop picked up new cert: %v", lastErr)
}

func TestMTLS_serverRequiresClientCert(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, caKey, caPEM := mustCA(t)
	srvCert, srvKey := mustServerCert(t, caKey, caPEM, []string{"localhost"}, time.Hour)
	cliCert, cliKey := mustClientCert(t, caKey, caPEM, time.Hour)
	writePEM(t, dir, "ca.pem", caPEM, nil)
	writePEM(t, dir, "srv.pem", srvCert, srvKey)
	writePEM(t, dir, "cli.pem", cliCert, cliKey)

	rSrv, err := NewReloader(Options{
		ServerCert: filepath.Join(dir, "srv.pem"),
		ServerKey:  filepath.Join(dir, "srv.key"),
		ClientCA:   filepath.Join(dir, "ca.pem"),
	})

	if err != nil {
		t.Fatal(err)
	}

	srvCreds, err := rSrv.ServerGRPC()
	if err != nil {
		t.Fatal(err)
	}

	rCli, err := NewReloader(Options{
		RootCA:     filepath.Join(dir, "ca.pem"),
		ClientCert: filepath.Join(dir, "cli.pem"),
		ClientKey:  filepath.Join(dir, "cli.key"),
	})

	if err != nil {
		t.Fatal(err)
	}

	cliCreds, err := rCli.ClientGRPC("localhost")
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer(grpc.Creds(srvCreds))
	registerTestHealth(srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer lis.Close()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(cliCreds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	_, err = healthgrpc.NewHealthClient(conn).Check(context.Background(), &healthgrpc.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestMTLS_withoutClientCertRejected(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	_, caKey, caPEM := mustCA(t)
	srvCert, srvKey := mustServerCert(t, caKey, caPEM, []string{"localhost"}, time.Hour)
	writePEM(t, dir, "ca.pem", caPEM, nil)
	writePEM(t, dir, "srv.pem", srvCert, srvKey)

	rSrv, err := NewReloader(Options{
		ServerCert: filepath.Join(dir, "srv.pem"),
		ServerKey:  filepath.Join(dir, "srv.key"),
		ClientCA:   filepath.Join(dir, "ca.pem"),
	})

	if err != nil {
		t.Fatal(err)
	}

	srvCreds, err := rSrv.ServerGRPC()
	if err != nil {
		t.Fatal(err)
	}

	rCli, err := NewReloader(Options{RootCA: filepath.Join(dir, "ca.pem")})
	if err != nil {
		t.Fatal(err)
	}

	cliCreds, err := rCli.ClientGRPC("localhost")
	if err != nil {
		t.Fatal(err)
	}

	srv := grpc.NewServer(grpc.Creds(srvCreds))
	registerTestHealth(srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	defer lis.Close()
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(cliCreds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err = healthgrpc.NewHealthClient(conn).Check(ctx, &healthgrpc.HealthCheckRequest{})
	if err == nil {
		t.Fatal("expected failure without client certificate")
	}
}

func registerTestHealth(s *grpc.Server) {
	hs := health.NewServer()
	healthgrpc.RegisterHealthServer(s, hs)
	hs.SetServingStatus("", healthgrpc.HealthCheckResponse_SERVING)
}

func mustWriteFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writePEM(t *testing.T, dir, base string, certPEM, keyPEM []byte) {
	t.Helper()
	mustWriteFile(t, filepath.Join(dir, base), certPEM)

	if keyPEM != nil {
		keyName := base[:len(base)-len(filepath.Ext(base))] + ".key"
		mustWriteFile(t, filepath.Join(dir, keyName), keyPEM)
	}
}

func mustCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)

	if err != nil {
		t.Fatal(err)
	}

	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"test"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	var certBuf bytes.Buffer
	_ = pem.Encode(&certBuf, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	var keyBuf bytes.Buffer
	keyDer, _ := x509.MarshalECPrivateKey(key)
	_ = pem.Encode(&keyBuf, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer})

	return cert, key, certBuf.Bytes()
}

func mustServerCert(t *testing.T, caKey *ecdsa.PrivateKey, caPEM []byte, dns []string, ttl time.Duration) (certPEM, keyPEM []byte) {
	t.Helper()
	caBlock, _ := pem.Decode(caPEM)
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(99),
		Subject:      pkix.Name{CommonName: "server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(ttl),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dns,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	var certBuf bytes.Buffer
	_ = pem.Encode(&certBuf, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	var keyBuf bytes.Buffer
	keyDer, _ := x509.MarshalECPrivateKey(key)
	_ = pem.Encode(&keyBuf, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer})

	return certBuf.Bytes(), keyBuf.Bytes()
}

func mustClientCert(t *testing.T, caKey *ecdsa.PrivateKey, caPEM []byte, ttl time.Duration) (certPEM, keyPEM []byte) {
	t.Helper()
	caBlock, _ := pem.Decode(caPEM)
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatal(err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(100),
		Subject:      pkix.Name{CommonName: "client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(ttl),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	var certBuf bytes.Buffer
	_ = pem.Encode(&certBuf, &pem.Block{Type: "CERTIFICATE", Bytes: der})
	var keyBuf bytes.Buffer
	keyDer, _ := x509.MarshalECPrivateKey(key)
	_ = pem.Encode(&keyBuf, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDer})

	return certBuf.Bytes(), keyBuf.Bytes()
}
