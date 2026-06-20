package kubernetes

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"
)

const (
	DefaultManifestPath = "deploy/kubernetes/manifests.yaml.tmpl"
	DefaultOutputPath   = "artifacts/deploy/kubernetes/vectis.yaml"
	DefaultNamespace    = "vectis"
	DefaultImageTag     = "latest"

	defaultPostgresPassword = "change-me-vectis-postgres"
	defaultBootstrapToken   = "change-me-vectis-bootstrap-token"
	defaultEncryptedFSKey   = "change-me-vectis-encryptedfs-key"
)

type RenderOptions struct {
	ManifestPath     string
	Namespace        string
	ImageRegistry    string
	ImageTag         string
	PostgresPassword string
	BootstrapToken   string
	EncryptedFSKey   string
}

type RenderResult struct {
	Status        string `json:"status"`
	ManifestPath  string `json:"manifest_path"`
	Namespace     string `json:"namespace"`
	ImageRegistry string `json:"image_registry,omitempty"`
	ImageTag      string `json:"image_tag"`
	Bytes         int    `json:"bytes"`
}

type templateData struct {
	Namespace          string
	ImageRegistry      string
	ImageTag           string
	PostgresPassword   string
	BootstrapToken     string
	EncryptedFSKey     string
	DatabaseDSN        string
	GRPCCA             string
	GRPCServerCert     string
	GRPCServerKey      string
	GRPCClientCABundle string
	SPIFFECA           string
	SPIFFECAKey        string
}

func Render(opts RenderOptions) ([]byte, RenderResult, error) {
	opts = normalizeOptions(opts)
	if opts.Namespace == "" {
		return nil, RenderResult{}, fmt.Errorf("namespace is required")
	}
	if opts.ImageTag == "" {
		return nil, RenderResult{}, fmt.Errorf("image tag is required")
	}

	manifest, err := loadManifestTemplate(opts.ManifestPath)
	if err != nil {
		return nil, RenderResult{}, err
	}

	pki, err := generateDevPKI(opts.Namespace)
	if err != nil {
		return nil, RenderResult{}, err
	}

	data := templateData{
		Namespace:          opts.Namespace,
		ImageRegistry:      opts.ImageRegistry,
		ImageTag:           opts.ImageTag,
		PostgresPassword:   opts.PostgresPassword,
		BootstrapToken:     opts.BootstrapToken,
		EncryptedFSKey:     opts.EncryptedFSKey,
		DatabaseDSN:        postgresDSN(opts.PostgresPassword),
		GRPCCA:             string(pki.grpcCA),
		GRPCServerCert:     string(pki.grpcServerCert),
		GRPCServerKey:      string(pki.grpcServerKey),
		GRPCClientCABundle: string(bytes.Join([][]byte{pki.grpcCA, pki.spiffeCA}, nil)),
		SPIFFECA:           string(pki.spiffeCA),
		SPIFFECAKey:        string(pki.spiffeCAKey),
	}

	tmpl, err := template.New("kubernetes-manifest").
		Funcs(template.FuncMap{
			"image":     data.image,
			"yamlQuote": yamlQuote,
		}).
		Option("missingkey=error").
		Parse(string(manifest))
	if err != nil {
		return nil, RenderResult{}, fmt.Errorf("parse Kubernetes manifest template: %w", err)
	}

	var out bytes.Buffer
	if err := tmpl.Execute(&out, data); err != nil {
		return nil, RenderResult{}, fmt.Errorf("render Kubernetes manifest template: %w", err)
	}

	return out.Bytes(), RenderResult{
		Status:        "rendered",
		ManifestPath:  opts.ManifestPath,
		Namespace:     opts.Namespace,
		ImageRegistry: opts.ImageRegistry,
		ImageTag:      opts.ImageTag,
		Bytes:         out.Len(),
	}, nil
}

func RenderToFile(opts RenderOptions, outPath string) (RenderResult, error) {
	manifest, result, err := Render(opts)
	if err != nil {
		return RenderResult{}, err
	}

	if strings.TrimSpace(outPath) == "" {
		outPath = DefaultOutputPath
	}

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return RenderResult{}, err
	}

	if err := os.WriteFile(outPath, manifest, 0o600); err != nil {
		return RenderResult{}, err
	}

	return result, nil
}

func normalizeOptions(opts RenderOptions) RenderOptions {
	if strings.TrimSpace(opts.ManifestPath) == "" {
		opts.ManifestPath = DefaultManifestPath
	}
	opts.Namespace = strings.TrimSpace(opts.Namespace)
	if opts.Namespace == "" {
		opts.Namespace = DefaultNamespace
	}
	opts.ImageRegistry = strings.Trim(strings.TrimSpace(opts.ImageRegistry), "/")
	opts.ImageTag = strings.TrimSpace(opts.ImageTag)
	if opts.ImageTag == "" {
		opts.ImageTag = DefaultImageTag
	}
	if opts.PostgresPassword == "" {
		opts.PostgresPassword = defaultPostgresPassword
	}
	if opts.BootstrapToken == "" {
		opts.BootstrapToken = defaultBootstrapToken
	}
	if opts.EncryptedFSKey == "" {
		opts.EncryptedFSKey = defaultEncryptedFSKey
	}
	return opts
}

func loadManifestTemplate(path string) ([]byte, error) {
	if strings.TrimSpace(path) == "" || path == DefaultManifestPath {
		return embeddedManifestTemplate, nil
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return b, nil
}

func (d templateData) image(component string) string {
	name := "vectis-" + strings.TrimSpace(component)
	if d.ImageRegistry != "" {
		name = d.ImageRegistry + "/" + name
	}
	return name + ":" + d.ImageTag
}

func yamlQuote(s string) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return strings.TrimSpace(b.String())
}

func postgresDSN(password string) string {
	user := url.UserPassword("vectis", password).String()
	return fmt.Sprintf("postgres://%s@vectis-postgres:5432/vectis?sslmode=disable", user)
}

type devPKI struct {
	grpcCA         []byte
	grpcServerCert []byte
	grpcServerKey  []byte
	spiffeCA       []byte
	spiffeCAKey    []byte
}

func generateDevPKI(namespace string) (devPKI, error) {
	grpcCA, grpcCAKey, grpcCAPEM, _, err := generateDevCA("Vectis Kubernetes gRPC Dev CA", nil)
	if err != nil {
		return devPKI{}, err
	}

	serverCert, serverKey, err := generateDevServerCert(grpcCA, grpcCAKey, namespace)
	if err != nil {
		return devPKI{}, err
	}

	spiffeURI, err := url.Parse("spiffe://vectis.internal")
	if err != nil {
		return devPKI{}, err
	}

	_, _, spiffeCAPEM, spiffeKeyPEM, err := generateDevCA("Vectis Kubernetes SPIFFE Dev CA", []*url.URL{spiffeURI})
	if err != nil {
		return devPKI{}, err
	}

	return devPKI{
		grpcCA:         grpcCAPEM,
		grpcServerCert: serverCert,
		grpcServerKey:  serverKey,
		spiffeCA:       spiffeCAPEM,
		spiffeCAKey:    spiffeKeyPEM,
	}, nil
}

func generateDevCA(commonName string, uris []*url.URL) (*x509.Certificate, *ecdsa.PrivateKey, []byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("generate dev CA key: %w", err)
	}

	serial, err := randomCertificateSerial()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		URIs:                  uris,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("generate dev CA certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("parse generated dev CA certificate: %w", err)
	}

	keyPEM, err := encodePrivateKeyPEM(key)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return cert, key, encodeCertificatePEM(der), keyPEM, nil
}

func generateDevServerCert(ca *x509.Certificate, caKey *ecdsa.PrivateKey, namespace string) ([]byte, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate dev server key: %w", err)
	}

	serial, err := randomCertificateSerial()
	if err != nil {
		return nil, nil, err
	}

	ns := strings.TrimSpace(namespace)
	if ns == "" {
		ns = DefaultNamespace
	}

	dnsNames := []string{
		"vectis.internal",
		"localhost",
		"vectis-registry",
		"vectis-queue",
		"vectis-orchestrator",
		"vectis-log",
		"vectis-artifact",
		"vectis-secrets",
	}

	for _, service := range []string{"registry", "queue", "orchestrator", "log", "artifact", "secrets"} {
		base := "vectis-" + service
		dnsNames = append(dnsNames,
			base+"."+ns+".svc",
			base+"."+ns+".svc.cluster.local",
		)
	}

	now := time.Now().UTC()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "vectis.internal"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     dnsNames,
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca, key.Public(), caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("generate dev server certificate: %w", err)
	}

	keyPEM, err := encodePrivateKeyPEM(key)
	if err != nil {
		return nil, nil, err
	}

	return encodeCertificatePEM(der), keyPEM, nil
}

func encodeCertificatePEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func encodePrivateKeyPEM(key *ecdsa.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("marshal private key: %w", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
}

func randomCertificateSerial() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("generate certificate serial: %w", err)
	}

	return serial, nil
}
