package kubernetes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/template"
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
	Namespace        string
	ImageRegistry    string
	ImageTag         string
	PostgresPassword string
	BootstrapToken   string
	EncryptedFSKey   string
	DatabaseDSN      string
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

	data := templateData{
		Namespace:        opts.Namespace,
		ImageRegistry:    opts.ImageRegistry,
		ImageTag:         opts.ImageTag,
		PostgresPassword: opts.PostgresPassword,
		BootstrapToken:   opts.BootstrapToken,
		EncryptedFSKey:   opts.EncryptedFSKey,
		DatabaseDSN:      postgresDSN(opts.PostgresPassword),
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
