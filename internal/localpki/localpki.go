package localpki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	caCertFile     = "ca.pem"
	caKeyFile      = "ca.key"
	serverCertFile = "server.pem"
	serverKeyFile  = "server.key"

	serverCertLifetime = 47 * 24 * time.Hour
	serverRenewBefore  = 14 * 24 * time.Hour
	caValidity         = 10 * 365 * 24 * time.Hour
	caRenewBefore      = 90 * 24 * time.Hour
)

type Material struct {
	CAFile     string
	ServerCert string
	ServerKey  string
}

func EnsureDir(dataHome string) string {
	return filepath.Join(dataHome, "vectis", "local-tls")
}

func Ensure(dir string) (*Material, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("localpki: mkdir %q: %w", dir, err)
	}

	caCertPath := filepath.Join(dir, caCertFile)
	caKeyPath := filepath.Join(dir, caKeyFile)
	srvCertPath := filepath.Join(dir, serverCertFile)
	srvKeyPath := filepath.Join(dir, serverKeyFile)

	action, err := classifyMaterial(caCertPath, caKeyPath, srvCertPath, srvKeyPath)
	if err != nil {
		return nil, err
	}

	switch action {
	case materialRegenerateFull:
		if err := generateFullChain(caCertPath, caKeyPath, srvCertPath, srvKeyPath); err != nil {
			return nil, err
		}
	case materialRenewServer:
		if err := renewServerCert(caCertPath, caKeyPath, srvCertPath, srvKeyPath); err != nil {
			return nil, err
		}
	case materialOK:
		// NOTE(garrett): NOP
	}

	return &Material{
		CAFile:     caCertPath,
		ServerCert: srvCertPath,
		ServerKey:  srvKeyPath,
	}, nil
}

type materialAction int

const (
	materialOK materialAction = iota
	materialRenewServer
	materialRegenerateFull
)

func classifyMaterial(caCertPath, caKeyPath, srvCertPath, srvKeyPath string) (materialAction, error) {
	for _, p := range []string{caCertPath, caKeyPath, srvCertPath, srvKeyPath} {
		if _, err := os.Stat(p); err != nil {
			if os.IsNotExist(err) {
				return materialRegenerateFull, nil
			}

			return materialOK, fmt.Errorf("localpki: stat %q: %w", p, err)
		}
	}

	caPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return materialRegenerateFull, nil
	}

	srvPEM, err := os.ReadFile(srvCertPath)
	if err != nil {
		return materialRegenerateFull, nil
	}

	caBlock, _ := pem.Decode(caPEM)
	if caBlock == nil {
		return materialRegenerateFull, nil
	}

	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return materialRegenerateFull, nil
	}

	srvBlock, _ := pem.Decode(srvPEM)
	if srvBlock == nil {
		return materialRegenerateFull, nil
	}

	srvCert, err := x509.ParseCertificate(srvBlock.Bytes)
	if err != nil {
		return materialRegenerateFull, nil
	}

	if time.Until(caCert.NotAfter) < caRenewBefore {
		return materialRegenerateFull, nil
	}

	if !hasLocalhostSANs(srvCert) {
		return materialRegenerateFull, nil
	}

	if time.Until(srvCert.NotAfter) < serverRenewBefore {
		return materialRenewServer, nil
	}

	if err := verifyChain(srvCert, caCert); err != nil {
		return materialRegenerateFull, nil
	}

	return materialOK, nil
}

func hasLocalhostSANs(c *x509.Certificate) bool {
	hasLocal := false
	has127 := false
	hasV6 := false

	for _, d := range c.DNSNames {
		if d == "localhost" {
			hasLocal = true
		}
	}

	for _, ip := range c.IPAddresses {
		if ip.Equal(net.IPv4(127, 0, 0, 1)) {
			has127 = true
		}

		if ip.Equal(net.IPv6loopback) {
			hasV6 = true
		}
	}

	return hasLocal && has127 && hasV6
}

func verifyChain(leaf, ca *x509.Certificate) error {
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	opts := x509.VerifyOptions{
		Roots:     pool,
		DNSName:   "localhost",
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	_, err := leaf.Verify(opts)
	return err
}

func generateFullChain(caCertPath, caKeyPath, srvCertPath, srvKeyPath string) error {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("localpki: generate CA key: %w", err)
	}

	caSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("localpki: serial: %w", err)
	}

	now := time.Now()
	caTmpl := &x509.Certificate{
		SerialNumber:          caSerial,
		Subject:               pkix.Name{Organization: []string{"vectis-local dev CA"}},
		NotBefore:             now.Add(-1 * time.Hour),
		NotAfter:              now.Add(caValidity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
		MaxPathLenZero:        true,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("localpki: create CA cert: %w", err)
	}

	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		return fmt.Errorf("localpki: parse CA cert: %w", err)
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("localpki: generate server key: %w", err)
	}

	srvSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("localpki: server serial: %w", err)
	}

	srvTmpl := &x509.Certificate{
		SerialNumber: srvSerial,
		Subject:      pkix.Name{Organization: []string{"vectis-local"}, CommonName: "localhost"},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(serverCertLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("localpki: create server cert: %w", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caKeyDER, err := x509.MarshalECPrivateKey(caKey)
	if err != nil {
		return fmt.Errorf("localpki: marshal CA key: %w", err)
	}
	caKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: caKeyDER})

	srvCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER})
	srvKeyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		return fmt.Errorf("localpki: marshal server key: %w", err)
	}
	srvKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: srvKeyDER})

	if err := writeFileAtomic(caCertPath, caCertPEM, 0o644); err != nil {
		return err
	}

	if err := writeFileAtomic(caKeyPath, caKeyPEM, 0o600); err != nil {
		return err
	}

	if err := writeFileAtomic(srvCertPath, srvCertPEM, 0o644); err != nil {
		return err
	}

	if err := writeFileAtomic(srvKeyPath, srvKeyPEM, 0o600); err != nil {
		return err
	}

	return nil
}

func loadCAKeyPair(caCertPath, caKeyPath string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, nil, err
	}

	keyPEM, err := os.ReadFile(caKeyPath)
	if err != nil {
		return nil, nil, err
	}

	certBlock, _ := pem.Decode(certPEM)
	keyBlock, _ := pem.Decode(keyPEM)
	if certBlock == nil || keyBlock == nil {
		return nil, nil, fmt.Errorf("localpki: missing PEM block")
	}

	caCert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}

	if !caCert.IsCA {
		return nil, nil, fmt.Errorf("localpki: %q is not a CA certificate", caCertPath)
	}

	caKey, err := parseECPrivateKeyPEM(keyBlock)
	if err != nil {
		return nil, nil, err
	}

	return caCert, caKey, nil
}

func parseECPrivateKeyPEM(block *pem.Block) (*ecdsa.PrivateKey, error) {
	switch block.Type {
	case "EC PRIVATE KEY":
		return x509.ParseECPrivateKey(block.Bytes)
	case "PRIVATE KEY":
		k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, err
		}

		ek, ok := k.(*ecdsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("localpki: PKCS#8 key is not ECDSA")
		}

		return ek, nil
	default:
		return nil, fmt.Errorf("localpki: unsupported private key PEM type %q", block.Type)
	}
}

func renewServerCert(caCertPath, caKeyPath, srvCertPath, srvKeyPath string) error {
	caCert, caKey, err := loadCAKeyPair(caCertPath, caKeyPath)
	if err != nil {
		return generateFullChain(caCertPath, caKeyPath, srvCertPath, srvKeyPath)
	}

	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("localpki: generate server key: %w", err)
	}

	srvSerial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("localpki: server serial: %w", err)
	}

	now := time.Now()
	srvTmpl := &x509.Certificate{
		SerialNumber: srvSerial,
		Subject:      pkix.Name{Organization: []string{"vectis-local"}, CommonName: "localhost"},
		NotBefore:    now.Add(-1 * time.Hour),
		NotAfter:     now.Add(serverCertLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		return fmt.Errorf("localpki: create server cert: %w", err)
	}

	srvCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER})
	srvKeyDER, err := x509.MarshalECPrivateKey(srvKey)
	if err != nil {
		return fmt.Errorf("localpki: marshal server key: %w", err)
	}

	srvKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: srvKeyDER})

	if err := writeFileAtomic(srvCertPath, srvCertPEM, 0o644); err != nil {
		return err
	}

	if err := writeFileAtomic(srvKeyPath, srvKeyPEM, 0o600); err != nil {
		return err
	}

	return nil
}

func writeFileAtomic(path string, data []byte, mode os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".writetmp-*")
	if err != nil {
		return fmt.Errorf("localpki: temp file: %w", err)
	}

	tmpName := f.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("localpki: write %s: %w", tmpName, err)
	}

	if err := f.Chmod(mode); err != nil {
		_ = f.Close()
		return fmt.Errorf("localpki: chmod %s: %w", tmpName, err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("localpki: close %s: %w", tmpName, err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("localpki: rename to %q: %w", path, err)
	}

	return nil
}

func (m *Material) EnvVars() []string {
	if m == nil {
		return nil
	}

	return []string{
		"VECTIS_GRPC_TLS_INSECURE=false",
		"VECTIS_GRPC_TLS_CA_FILE=" + m.CAFile,
		"VECTIS_GRPC_TLS_CERT_FILE=" + m.ServerCert,
		"VECTIS_GRPC_TLS_KEY_FILE=" + m.ServerKey,
		// Resolver dials use 127.0.0.1:*; leaf SANs include localhost — set SNI/verify name.
		"VECTIS_GRPC_TLS_SERVER_NAME=localhost",
	}
}

func (m *Material) ApplyParentViper(set func(key string, value any)) {
	if m == nil {
		return
	}

	set("grpc_tls.insecure", false)
	set("grpc_tls.ca_file", m.CAFile)
	set("grpc_tls.cert_file", m.ServerCert)
	set("grpc_tls.key_file", m.ServerKey)
	set("grpc_tls.server_name", "localhost")
}

func ApplyPlaintextParentViper(set func(key string, value any)) {
	set("grpc_tls.insecure", true)
}
