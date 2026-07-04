package platform

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type capturedTrustCommand struct {
	name string
	args []string
}

func captureTrustCommands(t *testing.T, exists map[string]bool) *[]capturedTrustCommand {
	t.Helper()

	oldExists := trustCommandExists
	oldRun := runTrustCommand
	var commands []capturedTrustCommand

	trustCommandExists = func(name string) bool {
		return exists[name]
	}

	runTrustCommand = func(ctx context.Context, name string, args ...string) ([]byte, error) {
		commands = append(commands, capturedTrustCommand{name: name, args: append([]string(nil), args...)})
		return nil, nil
	}

	t.Cleanup(func() {
		trustCommandExists = oldExists
		runTrustCommand = oldRun
	})

	return &commands
}

func writeTestCA(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "ca.pem")

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "vectis-test-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		t.Fatal(err)
	}

	return path
}

func TestInstallCertificateAuthority_requiresExistingCAFile(t *testing.T) {
	if err := InstallCertificateAuthority(context.Background(), filepath.Join(t.TempDir(), "missing.pem")); err == nil {
		t.Fatal("expected missing CA file to fail")
	}
}

func TestInstallCertificateAuthority_requiresCACertificate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, []byte("not pem"), 0o644); err != nil {
		t.Fatal(err)
	}
	commands := captureTrustCommands(t, map[string]bool{
		"certutil":               true,
		"install":                true,
		"security":               true,
		"update-ca-certificates": true,
		"update-ca-trust":        true,
	})

	if err := InstallCertificateAuthority(context.Background(), path); err == nil {
		t.Fatal("expected invalid CA file to fail")
	}
	assertCapturedTrustCommands(t, commands, nil)
}

func assertCapturedTrustCommands(t *testing.T, got *[]capturedTrustCommand, want []capturedTrustCommand) {
	t.Helper()
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("commands = %#v, want %#v", *got, want)
	}
}
