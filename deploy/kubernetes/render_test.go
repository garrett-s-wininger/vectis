package kubernetes

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderDefaultManifestContract(t *testing.T) {
	manifest, result, err := Render(RenderOptions{})
	if err != nil {
		t.Fatal(err)
	}

	text := string(manifest)
	if result.Namespace != DefaultNamespace || result.ImageTag != DefaultImageTag || result.Bytes != len(manifest) {
		t.Fatalf("unexpected render result: %+v len=%d", result, len(manifest))
	}

	for _, want := range []string{
		"kind: Namespace",
		"name: \"vectis\"",
		"name: vectis-postgres",
		"name: vectis-registry",
		"name: vectis-queue",
		"name: vectis-orchestrator",
		"name: vectis-log",
		"name: vectis-artifact",
		"name: vectis-secrets",
		"name: vectis-api",
		"name: vectis-worker",
		"VECTIS_DISCOVERY_REGISTRY_ADDRESS: vectis-registry:8082",
		"VECTIS_ACTION_REGISTRY_LOCAL_ROOTS: /app/examples/actions",
		"VECTIS_WORKER_REGISTER_WITH_REGISTRY",
		"VECTIS_ARTIFACT_STORAGE_READ_ONLY_MIN_FREE_BYTES",
		"VECTIS_LOG_STORAGE_READ_ONLY_MIN_FREE_BYTES",
		"VECTIS_WORKER_EXECUTION_IDENTITY_ENABLED",
		"mountPath: /var/lib/postgresql",
		"VECTIS_API_SERVER_PORT",
		"VECTIS_DOCS_PORT",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("manifest missing %q", want)
		}
	}

	if strings.Contains(text, "vectis-cell-ingress") {
		t.Fatalf("simple Kubernetes manifest should not expose cell ingress before mTLS is configured")
	}
}

func TestRenderHonorsImageAndSecretOptions(t *testing.T) {
	manifest, _, err := Render(RenderOptions{
		Namespace:        "ci-vectis",
		ImageRegistry:    "registry.example.com/acme/",
		ImageTag:         "test-sha",
		PostgresPassword: "p@ss word",
		BootstrapToken:   "bootstrap-for-tests",
		EncryptedFSKey:   "01234567890123456789012345678901",
	})

	if err != nil {
		t.Fatal(err)
	}

	text := string(manifest)
	for _, want := range []string{
		"name: \"ci-vectis\"",
		"image: registry.example.com/acme/vectis-api:test-sha",
		"image: registry.example.com/acme/vectis-worker-core:test-sha",
		"POSTGRES_PASSWORD: \"p@ss word\"",
		"postgres://vectis:p%40ss%20word@vectis-postgres:5432/vectis?sslmode=disable",
		"VECTIS_API_AUTH_BOOTSTRAP_TOKEN: \"bootstrap-for-tests\"",
		"encryptedfs.key: \"01234567890123456789012345678901\"",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("manifest missing %q", want)
		}
	}
}

func TestKubernetesSmokeJobContract(t *testing.T) {
	b, err := os.ReadFile("../../examples/e2e-kubernetes.json")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	for _, want := range []string{
		`"id": "e2e-kubernetes-smoke"`,
		`"uses": "examples/flaky-once@v1"`,
		`"name": "e2e-smoke-report"`,
		`"name": "e2e-registry-retry"`,
		"canonical-fanout-%s",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("smoke job missing %q", want)
		}
	}

	if strings.Contains(text, `"secrets"`) || strings.Contains(text, "canonical-secret-ok") {
		t.Fatal("Kubernetes smoke job should not require secrets before the manifest has mTLS/SPIFFE")
	}
}

func TestRenderToFile(t *testing.T) {
	out := filepath.Join(t.TempDir(), "vectis.yaml")
	result, err := RenderToFile(RenderOptions{Namespace: "render-file"}, out)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "rendered" {
		t.Fatalf("status = %q, want rendered", result.Status)
	}

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `name: "render-file"`) {
		t.Fatalf("rendered file did not contain namespace:\n%s", string(b))
	}
}
