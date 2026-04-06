package localpki

import (
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
)

func TestClassifyMaterial_renewWhenLeafNearExpiry(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caCertPath := filepath.Join(dir, caCertFile)
	caKeyPath := filepath.Join(dir, caKeyFile)
	srvCertPath := filepath.Join(dir, serverCertFile)
	srvKeyPath := filepath.Join(dir, serverKeyFile)

	if err := generateFullChain(caCertPath, caKeyPath, srvCertPath, srvKeyPath); err != nil {
		t.Fatal(err)
	}

	a, err := classifyMaterial(caCertPath, caKeyPath, srvCertPath, srvKeyPath)
	if err != nil {
		t.Fatal(err)
	}

	if a != materialOK {
		t.Fatalf("fresh chain: got action %v want materialOK", a)
	}

	caCert, caKey, err := loadCAKeyPair(caCertPath, caKeyPath)
	if err != nil {
		t.Fatal(err)
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	srvSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	srvTmpl := &x509.Certificate{
		SerialNumber: srvSerial,
		Subject:      pkix.Name{Organization: []string{"vectis-local"}, CommonName: "localhost"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(10 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	srvCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER})
	srvKeyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		t.Fatal(err)
	}

	srvKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: srvKeyDER})
	if err := os.WriteFile(srvCertPath, srvCertPEM, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(srvKeyPath, srvKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	a, err = classifyMaterial(caCertPath, caKeyPath, srvCertPath, srvKeyPath)
	if err != nil {
		t.Fatal(err)
	}

	if a != materialRenewServer {
		t.Fatalf("short-lived leaf: got %v want materialRenewServer", a)
	}
}

func TestEnsure_renewsNearExpiryLeaf(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caCertPath := filepath.Join(dir, caCertFile)
	caKeyPath := filepath.Join(dir, caKeyFile)
	srvCertPath := filepath.Join(dir, serverCertFile)
	srvKeyPath := filepath.Join(dir, serverKeyFile)

	if err := generateFullChain(caCertPath, caKeyPath, srvCertPath, srvKeyPath); err != nil {
		t.Fatal(err)
	}

	caCert, caKey, err := loadCAKeyPair(caCertPath, caKeyPath)
	if err != nil {
		t.Fatal(err)
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	srvSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	srvTmpl := &x509.Certificate{
		SerialNumber: srvSerial,
		Subject:      pkix.Name{Organization: []string{"vectis-local"}, CommonName: "localhost"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(10 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}

	srvCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER})
	srvKeyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		t.Fatal(err)
	}

	srvKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: srvKeyDER})
	if err := os.WriteFile(srvCertPath, srvCertPEM, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(srvKeyPath, srvKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := Ensure(dir); err != nil {
		t.Fatal(err)
	}

	a, err := classifyMaterial(caCertPath, caKeyPath, srvCertPath, srvKeyPath)
	if err != nil {
		t.Fatal(err)
	}

	if a != materialOK {
		t.Fatalf("after Ensure renew: got %v want materialOK", a)
	}
}

func TestClassifyMaterial_fullRegenWhenCABad(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	caCertPath := filepath.Join(dir, caCertFile)
	caKeyPath := filepath.Join(dir, caKeyFile)
	srvCertPath := filepath.Join(dir, serverCertFile)
	srvKeyPath := filepath.Join(dir, serverKeyFile)

	if err := generateFullChain(caCertPath, caKeyPath, srvCertPath, srvKeyPath); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(caCertPath, []byte("not pem"), 0o644); err != nil {
		t.Fatal(err)
	}

	a, err := classifyMaterial(caCertPath, caKeyPath, srvCertPath, srvKeyPath)
	if err != nil {
		t.Fatal(err)
	}

	if a != materialRegenerateFull {
		t.Fatalf("corrupt CA: got %v want materialRegenerateFull", a)
	}
}
