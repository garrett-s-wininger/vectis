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
		"name: vectis-gerrit-stream-ssh",
		"name: vectis-api",
		"name: vectis-worker",
		"name: vectis-scm-gerrit-stream",
		"replicas: 0",
		"VECTIS_DISCOVERY_REGISTRY_ADDRESS: vectis-registry:8082",
		"VECTIS_GRPC_TLS_INSECURE: \"false\"",
		"VECTIS_GRPC_TLS_CA_FILE: /run/vectis/grpc-tls/ca.pem",
		"VECTIS_GRPC_TLS_SERVER_NAME: vectis.internal",
		"VECTIS_SCM_GERRIT_STREAM_URL: \"\"",
		"VECTIS_SCM_GERRIT_STREAM_TRANSPORT: ssh",
		"VECTIS_SCM_GERRIT_STREAM_SSH_KEY_FILE: /run/vectis/gerrit-stream/id_rsa",
		"VECTIS_SCM_GERRIT_STREAM_SSH_KNOWN_HOSTS_FILE: /run/vectis/gerrit-stream/known_hosts",
		"vectis.dev/grpc-tls-checksum",
		"VECTIS_ACTION_REGISTRY_LOCAL_ROOTS: /app/examples/actions",
		"- name: VECTIS_WORKER_REGISTER_WITH_REGISTRY\n              value: \"true\"",
		"- name: VECTIS_SCM_GERRIT_STREAM_INSTANCE_ID",
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
		"mountPath: /run/vectis/gerrit-stream",
		"name: worker-log-spool",
		"mountPath: /tmp/vectis-log-spool",
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
		"image: registry.example.com/acme/vectis-scm-gerrit-stream:test-sha",
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
		"canonical-control-start",
		"canonical-secret-ok",
		"canonical-artifact-written",
		"canonical-fanout-ok",
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

func TestKubernetesWorkerCoreSmokeJobContract(t *testing.T) {
	b, err := os.ReadFile("../../examples/e2e-kubernetes-worker-core.json")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	for _, want := range []string{
		`"id": "e2e-kubernetes-worker-core-smoke"`,
		`"uses": "builtins/script"`,
		"kubernetes-worker-core-smoke-ok",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("worker-core smoke job missing %q", want)
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

func TestKubernetesOrphanSmokeJobContract(t *testing.T) {
	b, err := os.ReadFile("../../examples/e2e-kubernetes-orphan.json")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	for _, want := range []string{
		`"id": "e2e-kubernetes-orphan-smoke"`,
		`"id": "orphan-long-running"`,
		"orphan-task-started",
		"sleep 300",
		"unexpected-orphan-finished",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("orphan smoke job missing %q", want)
		}
	}
}

func TestKubernetesRepairSmokeJobContract(t *testing.T) {
	b, err := os.ReadFile("../../examples/e2e-kubernetes-repair.json")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	for _, want := range []string{
		`"id": "e2e-kubernetes-repair-smoke"`,
		`"id": "repair-long-until-requeued"`,
		smokeRepairReadyAfterPlaceholder,
		"repair-task-started",
		"repair-task-requeued",
		"unexpected-repair-finished",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("repair smoke job missing %q", want)
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

	if opts.WorkerCoreJobPath != DefaultSmokeWorkerCoreJobPath {
		t.Fatalf("worker-core job path = %q", opts.WorkerCoreJobPath)
	}

	if opts.ScaleJobPath != DefaultSmokeScaleJobPath {
		t.Fatalf("scale job path = %q", opts.ScaleJobPath)
	}

	if opts.OrphanJobPath != DefaultSmokeOrphanJobPath {
		t.Fatalf("orphan job path = %q", opts.OrphanJobPath)
	}

	if opts.RepairJobPath != DefaultSmokeRepairJobPath {
		t.Fatalf("repair job path = %q", opts.RepairJobPath)
	}

	if opts.ScaleWorkerReplicas != DefaultSmokeScaleWorkerReplicas {
		t.Fatalf("scale worker replicas = %d", opts.ScaleWorkerReplicas)
	}

	if opts.ScaleMinWorkers != DefaultSmokeScaleMinWorkers {
		t.Fatalf("scale min workers = %d", opts.ScaleMinWorkers)
	}

	if opts.OrphanLeaseTTL != DefaultSmokeOrphanLeaseTTL {
		t.Fatalf("orphan lease ttl = %v", opts.OrphanLeaseTTL)
	}

	if opts.OrphanStability != DefaultSmokeOrphanStability {
		t.Fatalf("orphan stability = %v", opts.OrphanStability)
	}

	if opts.RepairLeaseTTL != DefaultSmokeRepairLeaseTTL {
		t.Fatalf("repair lease ttl = %v", opts.RepairLeaseTTL)
	}

	if opts.RepairReadyAfter != DefaultSmokeRepairReadyAfter {
		t.Fatalf("repair ready after = %v", opts.RepairReadyAfter)
	}

	if opts.CLIImage != DefaultSmokeCLIImage {
		t.Fatalf("cli image = %q", opts.CLIImage)
	}

	if opts.WorkerCoreImage != DefaultSmokeWorkerCoreImage {
		t.Fatalf("worker-core image = %q", opts.WorkerCoreImage)
	}

	if opts.WorkerCoreTaskImage != DefaultSmokeWorkerCoreTaskImage {
		t.Fatalf("worker-core task image = %q", opts.WorkerCoreTaskImage)
	}

	if !smokeSeedSecretEnabled(opts) {
		t.Fatal("secret seeding should be enabled by default")
	}
}

func TestKubernetesValidationEntrypointContract(t *testing.T) {
	b, err := os.ReadFile("validate/main.go")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	for _, want := range []string{
		"RunValidate",
		`"manifest"`,
		`"output"`,
		`"image-registry"`,
		`"image-tag"`,
		`"skip-smoke"`,
		`"job"`,
		`"cli-image"`,
		`"seed-secret"`,
		`"api-local-port"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("validate entrypoint missing %q", want)
		}
	}

	b, err = os.ReadFile("smoke/main.go")
	if err != nil {
		t.Fatal(err)
	}

	text = string(b)
	for _, want := range []string{
		"RunSmoke",
		`"worker-core-job"`,
		`"cancel-job"`,
		`"scale-job"`,
		`"orphan-job"`,
		`"repair-job"`,
		`"worker-core-only"`,
		`"cancel-only"`,
		`"scale-only"`,
		`"orphan-only"`,
		`"repair-only"`,
		`"worker-core-image"`,
		`"worker-core-task-image"`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("smoke entrypoint missing %q", want)
		}
	}
}

func TestKubernetesContainerBuildContextIncludesWorkerCoreProtos(t *testing.T) {
	b, err := os.ReadFile("../../.containerignore")
	if err != nil {
		t.Fatal(err)
	}

	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "api/proto/" || line == "api/proto" {
			t.Fatalf(".containerignore must not exclude api/proto; container builds need worker_core.proto for the worker-core Kubernetes binary")
		}
	}
}

func TestSmokeWorkerCoreDeploymentPatch(t *testing.T) {
	patch, err := smokeWorkerCoreDeploymentPatch(normalizeSmokeOptions(SmokeOptions{
		Namespace:           "ci-vectis",
		WorkerCoreImage:     "localhost/vectis-worker-core-kubernetes:test",
		WorkerCoreTaskImage: "localhost/vectis-worker:test",
	}))

	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		`"serviceAccountName":"vectis-worker-core-kubernetes"`,
		`"automountServiceAccountToken":true`,
		`"name":"worker-core"`,
		`"image":"localhost/vectis-worker-core-kubernetes:test"`,
		`"name":"KUBERNETES_NAMESPACE","value":"ci-vectis"`,
		`"name":"VECTIS_KUBERNETES_WORKER_CORE_IMAGE","value":"localhost/vectis-worker:test"`,
	} {
		if !strings.Contains(patch, want) {
			t.Fatalf("patch missing %q: %s", want, patch)
		}
	}
}

func TestSmokeKubernetesJobPhase(t *testing.T) {
	var job smokeKubernetesJob
	job.Metadata.Name = "task-job"
	job.Status.Conditions = append(job.Status.Conditions, struct {
		Type    string `json:"type,omitempty"`
		Status  string `json:"status,omitempty"`
		Reason  string `json:"reason,omitempty"`
		Message string `json:"message,omitempty"`
	}{Type: "Complete", Status: "True", Message: "done"})

	phase, message := smokeKubernetesJobPhase(job)
	if phase != "succeeded" || message != "done" {
		t.Fatalf("phase=%q message=%q, want succeeded/done", phase, message)
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
