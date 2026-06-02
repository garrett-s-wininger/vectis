package platform

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

type trustCommandRunner func(context.Context, string, ...string) ([]byte, error)
type trustCommandExistsFunc func(string) bool

var (
	runTrustCommand    trustCommandRunner     = defaultRunTrustCommand
	trustCommandExists trustCommandExistsFunc = defaultTrustCommandExists
)

func defaultRunTrustCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

func defaultTrustCommandExists(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

// InstallCertificateAuthority installs caFile into the operating system trust
// store using the implementation compiled for the current GOOS.
func InstallCertificateAuthority(ctx context.Context, caFile string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if caFile == "" {
		return errors.New("platform: CA file is required")
	}

	absCAFile, err := filepath.Abs(caFile)
	if err != nil {
		return fmt.Errorf("platform: resolve CA file %q: %w", caFile, err)
	}
	if _, err := os.Stat(absCAFile); err != nil {
		return fmt.Errorf("platform: stat CA file %q: %w", absCAFile, err)
	}
	if err := validateCertificateAuthority(absCAFile, time.Now()); err != nil {
		return err
	}

	return installCertificateAuthority(ctx, absCAFile)
}

func runTrustStoreCommand(ctx context.Context, name string, args ...string) error {
	out, err := runTrustCommand(ctx, name, args...)
	if err != nil {
		return fmt.Errorf("platform: run %s: %w: %s", name, err, string(out))
	}

	return nil
}

// ServerCertificateTrusted reports whether certFile verifies for serverName
// against the operating system trust store.
func ServerCertificateTrusted(certFile, serverName string) (bool, error) {
	leaf, err := parseCertificateFile(certFile)
	if err != nil {
		return false, err
	}

	roots, err := x509.SystemCertPool()
	if err != nil {
		return false, fmt.Errorf("platform: load system cert pool: %w", err)
	}
	if roots == nil {
		roots = x509.NewCertPool()
	}

	_, err = leaf.Verify(x509.VerifyOptions{
		Roots:     roots,
		DNSName:   serverName,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if err == nil {
		return true, nil
	}

	var unknown x509.UnknownAuthorityError
	if errors.As(err, &unknown) {
		return false, nil
	}

	return false, nil
}

func parseCertificateFile(path string) (*x509.Certificate, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("platform: read certificate %q: %w", path, err)
	}

	block, _ := pem.Decode(b)
	if block == nil {
		return nil, fmt.Errorf("platform: no PEM certificate found in %q", path)
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("platform: parse certificate %q: %w", path, err)
	}

	return cert, nil
}

func validateCertificateAuthority(path string, now time.Time) error {
	cert, err := parseCertificateFile(path)
	if err != nil {
		return err
	}

	if !cert.BasicConstraintsValid || !cert.IsCA {
		return fmt.Errorf("platform: certificate %q is not a CA certificate", path)
	}
	if cert.KeyUsage&x509.KeyUsageCertSign == 0 {
		return fmt.Errorf("platform: CA certificate %q cannot sign certificates", path)
	}
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) {
		return fmt.Errorf("platform: CA certificate %q is not currently valid", path)
	}
	if err := cert.CheckSignatureFrom(cert); err != nil {
		return fmt.Errorf("platform: CA certificate %q is not self-signed: %w", path, err)
	}

	return nil
}
