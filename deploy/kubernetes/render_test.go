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
		"name: vectis-grpc-tls",
		"name: vectis-api",
		"name: vectis-worker",
		"VECTIS_DISCOVERY_REGISTRY_ADDRESS: vectis-registry:8082",
		"VECTIS_GRPC_TLS_INSECURE: \"false\"",
		"VECTIS_GRPC_TLS_CA_FILE: /run/vectis/grpc-tls/ca.pem",
		"VECTIS_GRPC_TLS_SERVER_NAME: vectis.internal",
		"vectis.dev/grpc-tls-checksum",
		"VECTIS_ACTION_REGISTRY_LOCAL_ROOTS: /app/examples/actions",
		"- name: VECTIS_WORKER_REGISTER_WITH_REGISTRY\n              value: \"true\"",
		"fieldPath: status.podIP",
		"VECTIS_WORKER_CONTROL_PUBLISH_ADDRESS",
		`value: "$(VECTIS_POD_IP):9084"`,
		"VECTIS_WORKER_SPIFFE_ENABLED",
		"VECTIS_WORKER_SPIFFE_WORKLOAD_API_ADDRESS",
		"VECTIS_GRPC_TLS_CLIENT_CA_FILE",
		"name: spiffe",
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
		t.Fatalf("simple Kubernetes manifest should not expose cell ingress")
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
	b, err := os.ReadFile("../../examples/e2e-canonical.json")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	for _, want := range []string{
		`"id": "e2e-canonical-smoke"`,
		`"ref": "encryptedfs://team/smoke-token"`,
		`"uses": "examples/flaky-once@v1"`,
		`"name": "e2e-smoke-report"`,
		`"name": "e2e-registry-retry"`,
		"canonical-control-%s",
		"canonical-secret-%s",
		"canonical-artifact-%s",
		"canonical-fanout-%s",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("smoke job missing %q", want)
		}
	}
}

func TestKubernetesCancelSmokeJobContract(t *testing.T) {
	b, err := os.ReadFile("../../examples/e2e-kubernetes-cancel.json")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	for _, want := range []string{
		`"id": "e2e-kubernetes-cancel-smoke"`,
		`"id": "cancel-long-running"`,
		"canonical-cancel-%s",
		"sleep 60",
		"unexpected-finished",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("cancel smoke job missing %q", want)
		}
	}
}

func TestKubernetesScaleSmokeJobContract(t *testing.T) {
	b, err := os.ReadFile("../../examples/e2e-kubernetes-scale.json")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	for _, want := range []string{
		`"id": "e2e-kubernetes-scale-smoke"`,
		`"id": "fanout-control"`,
		`"execution": "distributed"`,
		"scale-branch-a-started",
		"scale-branch-b-started",
		"scale-branch-c-started",
		"sleep 25",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("scale smoke job missing %q", want)
		}
	}
}

func TestSmokeDefaultsUseCanonicalSecretLane(t *testing.T) {
	opts := normalizeSmokeOptions(SmokeOptions{})
	if opts.JobPath != DefaultSmokeJobPath {
		t.Fatalf("job path = %q", opts.JobPath)
	}

	if opts.CancelJobPath != DefaultSmokeCancelJobPath {
		t.Fatalf("cancel job path = %q", opts.CancelJobPath)
	}

	if opts.ScaleJobPath != DefaultSmokeScaleJobPath {
		t.Fatalf("scale job path = %q", opts.ScaleJobPath)
	}

	if opts.ScaleWorkerReplicas != DefaultSmokeScaleWorkerReplicas {
		t.Fatalf("scale worker replicas = %d", opts.ScaleWorkerReplicas)
	}

	if opts.ScaleMinWorkers != DefaultSmokeScaleMinWorkers {
		t.Fatalf("scale min workers = %d", opts.ScaleMinWorkers)
	}

	if opts.CLIImage != DefaultSmokeCLIImage {
		t.Fatalf("cli image = %q", opts.CLIImage)
	}

	if !smokeSeedSecretEnabled(opts) {
		t.Fatal("secret seeding should be enabled by default")
	}
}

func TestSmokeSeedManifestMountsSecretStore(t *testing.T) {
	manifest := smokeSeedManifest("vectis-smoke-seed-test", "localhost/vectis-cli:dev-local")
	for _, want := range []string{
		"kind: Secret",
		"kind: Job",
		"claimName: vectis-secrets-data",
		"encryptedfs://team/smoke-token",
		"image: \"localhost/vectis-cli:dev-local\"",
		"mountPath: /data/vectis/secrets",
		"mountPath: /run/vectis/secrets",
		"mountPath: /run/vectis/smoke",
		"--from-file",
	} {
		if !strings.Contains(manifest, want) {
			t.Fatalf("manifest missing %q: %s", want, manifest)
		}
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
