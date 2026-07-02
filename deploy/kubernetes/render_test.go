package kubernetes

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
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

func TestKubernetesKnoxSmokeJobContract(t *testing.T) {
	b, err := os.ReadFile("../../examples/e2e-kubernetes-knox.json")
	if err != nil {
		t.Fatal(err)
	}

	text := string(b)
	for _, want := range []string{
		`"id": "e2e-kubernetes-knox-smoke"`,
		`"ref": "knox://team/smoke_token"`,
		`"uses": "examples/flaky-once@v1"`,
		"342534541 17",
		"canonical-secret-%s",
		"canonical-artifact-%s",
		"canonical-fanout-%s",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("Knox smoke job missing %q", want)
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

	if opts.GerritImage != DefaultSmokeGerritImage {
		t.Fatalf("gerrit image = %q", opts.GerritImage)
	}

	if opts.GerritHTTPPort != DefaultSmokeGerritHTTPPort {
		t.Fatalf("gerrit HTTP local port = %d", opts.GerritHTTPPort)
	}

	if opts.GerritSSHLocalPort != DefaultSmokeGerritSSHLocalPort {
		t.Fatalf("gerrit SSH local port = %d", opts.GerritSSHLocalPort)
	}

	if opts.GerritClusterURL != DefaultSmokeGerritClusterURL {
		t.Fatalf("gerrit cluster URL = %q", opts.GerritClusterURL)
	}

	if opts.GerritSSHHost != DefaultSmokeGerritSSHHost {
		t.Fatalf("gerrit SSH host = %q", opts.GerritSSHHost)
	}

	if opts.GerritSSHPort != DefaultSmokeGerritSSHPort {
		t.Fatalf("gerrit SSH port = %d", opts.GerritSSHPort)
	}

	if opts.GerritAccountID != DefaultSmokeGerritAccountID {
		t.Fatalf("gerrit account id = %q", opts.GerritAccountID)
	}

	if opts.GerritUsername != DefaultSmokeGerritUsername {
		t.Fatalf("gerrit username = %q", opts.GerritUsername)
	}

	if opts.GerritProjectPrefix != DefaultSmokeGerritProjectPrefix {
		t.Fatalf("gerrit project prefix = %q", opts.GerritProjectPrefix)
	}

	if opts.GerritGitBin != DefaultSmokeGerritGitBin {
		t.Fatalf("gerrit git executable = %q", opts.GerritGitBin)
	}

	if opts.GerritKeepFixture {
		t.Fatal("gerrit keep fixture should be disabled by default")
	}

	if opts.S3Image != DefaultSmokeS3Image {
		t.Fatalf("S3 image = %q", opts.S3Image)
	}

	if opts.S3LocalPort != DefaultSmokeS3LocalPort {
		t.Fatalf("S3 local port = %d", opts.S3LocalPort)
	}

	if opts.S3ClusterEndpoint != DefaultSmokeS3ClusterEndpoint {
		t.Fatalf("S3 cluster endpoint = %q", opts.S3ClusterEndpoint)
	}

	if opts.S3Bucket != DefaultSmokeS3Bucket {
		t.Fatalf("S3 bucket = %q", opts.S3Bucket)
	}

	if opts.S3Prefix != DefaultSmokeS3Prefix {
		t.Fatalf("S3 prefix = %q", opts.S3Prefix)
	}

	if opts.S3AccessKeyID != DefaultSmokeS3AccessKeyID {
		t.Fatalf("S3 access key id = %q", opts.S3AccessKeyID)
	}

	if opts.S3SecretAccessKey != DefaultSmokeS3SecretAccessKey {
		t.Fatalf("S3 secret access key = %q", opts.S3SecretAccessKey)
	}

	if opts.S3TempDir != DefaultSmokeS3TempDir {
		t.Fatalf("S3 temp dir = %q", opts.S3TempDir)
	}

	if opts.S3KeepFixture {
		t.Fatal("S3 keep fixture should be disabled by default")
	}

	if opts.KnoxImage != DefaultSmokeKnoxImage {
		t.Fatalf("Knox image = %q", opts.KnoxImage)
	}

	if opts.KnoxLocalPort != DefaultSmokeKnoxLocalPort {
		t.Fatalf("Knox local port = %d", opts.KnoxLocalPort)
	}

	if opts.KnoxClusterURL != DefaultSmokeKnoxClusterURL {
		t.Fatalf("Knox cluster URL = %q", opts.KnoxClusterURL)
	}

	if opts.KnoxCertMountPath != DefaultSmokeKnoxCertMountPath {
		t.Fatalf("Knox cert mount path = %q", opts.KnoxCertMountPath)
	}

	if opts.KnoxAuthToken != DefaultSmokeKnoxAuthToken {
		t.Fatalf("Knox auth token = %q", opts.KnoxAuthToken)
	}

	if opts.KnoxWrongAuthToken != DefaultSmokeKnoxWrongAuthToken {
		t.Fatalf("Knox wrong auth token = %q", opts.KnoxWrongAuthToken)
	}

	if opts.KnoxKeyID != DefaultSmokeKnoxKeyID {
		t.Fatalf("Knox key id = %q", opts.KnoxKeyID)
	}

	if opts.KnoxRef != DefaultSmokeKnoxRef {
		t.Fatalf("Knox ref = %q", opts.KnoxRef)
	}

	if opts.KnoxMissingRef != DefaultSmokeKnoxMissingRef {
		t.Fatalf("Knox missing ref = %q", opts.KnoxMissingRef)
	}

	if opts.KnoxExpectedData != DefaultSmokeKnoxSecret {
		t.Fatalf("Knox expected data = %q", opts.KnoxExpectedData)
	}

	if opts.KnoxKeepFixture {
		t.Fatal("Knox keep fixture should be disabled by default")
	}

	if opts.LDAPImage != DefaultSmokeLDAPImage {
		t.Fatalf("LDAP image = %q", opts.LDAPImage)
	}

	if opts.LDAPLocalPort != DefaultSmokeLDAPLocalPort {
		t.Fatalf("LDAP local port = %d", opts.LDAPLocalPort)
	}

	if opts.LDAPClusterURL != DefaultSmokeLDAPClusterURL {
		t.Fatalf("LDAP cluster URL = %q", opts.LDAPClusterURL)
	}

	if opts.LDAPBootstrapLDIF != DefaultSmokeLDAPBootstrapLDIF {
		t.Fatalf("LDAP bootstrap LDIF = %q", opts.LDAPBootstrapLDIF)
	}

	if opts.LDAPBaseDN != DefaultSmokeLDAPBaseDN {
		t.Fatalf("LDAP base DN = %q", opts.LDAPBaseDN)
	}

	if opts.LDAPBindDN != DefaultSmokeLDAPBindDN {
		t.Fatalf("LDAP bind DN = %q", opts.LDAPBindDN)
	}

	if opts.LDAPBindPassword != DefaultSmokeLDAPBindPassword {
		t.Fatalf("LDAP bind password = %q", opts.LDAPBindPassword)
	}

	if opts.LDAPUserFilter != DefaultSmokeLDAPUserFilter {
		t.Fatalf("LDAP user filter = %q", opts.LDAPUserFilter)
	}

	if opts.LDAPUsername != DefaultSmokeLDAPUsername || opts.LDAPPassword != DefaultSmokeLDAPPassword || opts.LDAPWrongPassword != DefaultSmokeLDAPWrongPassword {
		t.Fatalf("unexpected LDAP credentials: username=%q password=%q wrong=%q", opts.LDAPUsername, opts.LDAPPassword, opts.LDAPWrongPassword)
	}

	if opts.LDAPExpectedSubject != DefaultSmokeLDAPExpectedSubject || opts.LDAPExpectedName != DefaultSmokeLDAPExpectedName {
		t.Fatalf("unexpected LDAP expected identity: subject=%q display=%q", opts.LDAPExpectedSubject, opts.LDAPExpectedName)
	}

	if opts.LDAPBootstrapToken != DefaultSmokeLDAPBootstrapToken {
		t.Fatalf("LDAP bootstrap token = %q", opts.LDAPBootstrapToken)
	}

	if opts.LDAPKeepFixture {
		t.Fatal("LDAP keep fixture should be disabled by default")
	}

	knoxOpts := normalizeSmokeOptions(SmokeOptions{KnoxSecretsOnly: true})
	if knoxOpts.JobPath != DefaultSmokeKnoxJobPath {
		t.Fatalf("Knox job path = %q", knoxOpts.JobPath)
	}

	if !smokeSeedSecretEnabled(opts) {
		t.Fatal("secret seeding should be enabled by default")
	}
}

func TestSmokeRunWaitDiagnosticsSummarizeDispatchFailures(t *testing.T) {
	detail := smokeRunDetail{
		RunID:      "run-123",
		Status:     "queued",
		NextAction: "dispatch_pending",
		DispatchEvents: []smokeDispatchEvent{
			{Source: "cron", EventType: "attempt", Message: "ignored"},
			{Source: "scm_gerrit_stream", SourceInstance: "vectis-scm-gerrit-stream-0", EventType: "failure", Message: "decode execution envelope: missing job"},
		},
	}

	line := smokeRunProgressLine(detail)
	if !strings.Contains(line, "run_id=run-123 status=queued next_action=dispatch_pending") {
		t.Fatalf("progress line = %q", line)
	}

	diagnostics := smokeRunDispatchDiagnostics(detail)
	if !strings.Contains(diagnostics, "scm_gerrit_stream/vectis-scm-gerrit-stream-0 failure: decode execution envelope: missing job") {
		t.Fatalf("diagnostics = %q", diagnostics)
	}

	suffix := smokeRunWaitSuffix(detail)
	for _, want := range []string{
		"last_status=queued",
		"next_action=dispatch_pending",
		"dispatch=scm_gerrit_stream/vectis-scm-gerrit-stream-0 failure: decode execution envelope: missing job",
	} {
		if !strings.Contains(suffix, want) {
			t.Fatalf("suffix %q missing %q", suffix, want)
		}
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
		`"gerrit-stream-only"`,
		`"worker-core-image"`,
		`"worker-core-task-image"`,
		`"gerrit-image"`,
		`"gerrit-http-local-port"`,
		`"gerrit-ssh-local-port"`,
		`"gerrit-cluster-url"`,
		`"gerrit-ssh-host"`,
		`"gerrit-ssh-port"`,
		`"gerrit-account-id"`,
		`"gerrit-username"`,
		`"gerrit-project"`,
		`"gerrit-project-prefix"`,
		`"gerrit-git"`,
		`"gerrit-keep-fixture"`,
		`"s3-artifact-only"`,
		`"s3-image"`,
		`"s3-local-port"`,
		`"s3-cluster-endpoint"`,
		`"s3-bucket"`,
		`"s3-prefix"`,
		`"s3-access-key-id"`,
		`"s3-secret-access-key"`,
		`"s3-temp-dir"`,
		`"s3-keep-fixture"`,
		`"knox-secrets-only"`,
		`"knox-image"`,
		`"knox-local-port"`,
		`"knox-cluster-url"`,
		`"knox-cert-mount-path"`,
		`"knox-auth-token"`,
		`"knox-wrong-auth-token"`,
		`"knox-key-id"`,
		`"knox-ref"`,
		`"knox-missing-ref"`,
		`"knox-expected-data"`,
		`"knox-keep-fixture"`,
		`"ldap-auth-only"`,
		`"ldap-image"`,
		`"ldap-local-port"`,
		`"ldap-cluster-url"`,
		`"ldap-bootstrap-ldif"`,
		`"ldap-base-dn"`,
		`"ldap-bind-dn"`,
		`"ldap-bind-password"`,
		`"ldap-user-filter"`,
		`"ldap-subject-attribute"`,
		`"ldap-username-attribute"`,
		`"ldap-display-name-attribute"`,
		`"ldap-username"`,
		`"ldap-password"`,
		`"ldap-wrong-password"`,
		`"ldap-expected-subject"`,
		`"ldap-expected-display-name"`,
		`"ldap-bootstrap-token"`,
		`"ldap-admin-username"`,
		`"ldap-admin-password"`,
		`"ldap-keep-fixture"`,
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

	containerfile, err := os.ReadFile("../../build/Containerfile")
	if err != nil {
		t.Fatal(err)
	}

	text := string(containerfile)
	for _, stage := range []string{"scm-poller", "scm-gerrit-stream"} {
		stageStart := strings.Index(text, "FROM scratch AS "+stage)
		if stageStart < 0 {
			t.Fatalf("Containerfile missing %s stage", stage)
		}

		stageText := text[stageStart:]
		if nextStage := strings.Index(stageText[1:], "\nFROM "); nextStage >= 0 {
			stageText = stageText[:nextStage+1]
		}
		if !strings.Contains(stageText, "COPY --from=builder /app/examples/actions /app/examples/actions") {
			t.Fatalf("%s image must include /app/examples/actions for Kubernetes action registry defaults", stage)
		}
	}
}

func TestSmokeGerritFixtureManifestContract(t *testing.T) {
	manifest := smokeGerritFixtureManifest("registry.example.com/gerrit:test")
	for _, want := range []string{
		"kind: Deployment",
		"name: vectis-gerrit",
		"image: \"registry.example.com/gerrit:test\"",
		"name: CANONICAL_WEB_URL",
		"value: \"http://vectis-gerrit:8080/\"",
		"containerPort: 29418",
		"kind: Service",
		"targetPort: ssh",
	} {
		if !strings.Contains(manifest, want) {
			t.Fatalf("manifest missing %q: %s", want, manifest)
		}
	}

	for _, forbidden := range []string{
		"mountPath: /var/gerrit",
		"name: gerrit-site",
	} {
		if strings.Contains(manifest, forbidden) {
			t.Fatalf("manifest must not shadow Gerrit image contents with %q: %s", forbidden, manifest)
		}
	}
}

func TestSmokeS3ArtifactFixtureManifestContract(t *testing.T) {
	opts := normalizeSmokeOptions(SmokeOptions{
		S3Image:           "registry.example.com/seaweedfs:test",
		S3AccessKeyID:     `access "id"`,
		S3SecretAccessKey: `secret\value`,
	})

	manifest := smokeS3ArtifactFixtureManifest(opts)
	for _, want := range []string{
		"kind: ConfigMap",
		"name: vectis-s3-artifact-auth",
		"s3-auth.json:",
		"kind: Deployment",
		"name: vectis-s3-artifact",
		"image: \"registry.example.com/seaweedfs:test\"",
		"- -s3.config=/etc/seaweedfs/s3-auth.json",
		"automountServiceAccountToken: false",
		"kind: Service",
		"targetPort: 8333",
	} {
		if !strings.Contains(manifest, want) {
			t.Fatalf("manifest missing %q: %s", want, manifest)
		}
	}

	const prefix = "  s3-auth.json: "
	start := strings.Index(manifest, prefix)
	if start < 0 {
		t.Fatalf("manifest missing auth config: %s", manifest)
	}

	line := manifest[start+len(prefix):]
	line = line[:strings.IndexByte(line, '\n')]
	authJSON, err := strconv.Unquote(line)
	if err != nil {
		t.Fatal(err)
	}

	var auth struct {
		Identities []struct {
			Name        string `json:"name"`
			Credentials []struct {
				AccessKey string `json:"accessKey"`
				SecretKey string `json:"secretKey"`
			} `json:"credentials"`
			Actions []string `json:"actions"`
		} `json:"identities"`
	}

	if err := json.Unmarshal([]byte(authJSON), &auth); err != nil {
		t.Fatal(err)
	}

	if len(auth.Identities) != 1 || auth.Identities[0].Name != `access "id"` {
		t.Fatalf("unexpected auth identities: %+v", auth.Identities)
	}

	if len(auth.Identities[0].Credentials) != 1 ||
		auth.Identities[0].Credentials[0].AccessKey != `access "id"` ||
		auth.Identities[0].Credentials[0].SecretKey != `secret\value` {
		t.Fatalf("unexpected auth credentials: %+v", auth.Identities[0].Credentials)
	}

	if got := strings.Join(auth.Identities[0].Actions, ","); got != "Admin,Read,List,Tagging,Write" {
		t.Fatalf("actions = %q", got)
	}
}

func TestSmokeArtifactS3EnvContract(t *testing.T) {
	opts := normalizeSmokeOptions(SmokeOptions{
		S3ClusterEndpoint: "http://vectis-s3-artifact:8333/",
		S3Bucket:          "ci-artifacts",
		S3Prefix:          "/ci/smoke/",
		S3AccessKeyID:     "access",
		S3SecretAccessKey: "secret",
		S3TempDir:         "/data/vectis/artifact/s3-tmp",
	})

	env := smokeArtifactS3Env(opts)
	want := map[string]string{
		"VECTIS_ARTIFACT_STORAGE_BACKEND":              "s3",
		"VECTIS_ARTIFACT_STORAGE_S3_ENDPOINT":          "http://vectis-s3-artifact:8333",
		"VECTIS_ARTIFACT_STORAGE_S3_REGION":            "us-east-1",
		"VECTIS_ARTIFACT_STORAGE_S3_BUCKET":            "ci-artifacts",
		"VECTIS_ARTIFACT_STORAGE_S3_PREFIX":            "ci/smoke",
		"VECTIS_ARTIFACT_STORAGE_S3_ACCESS_KEY_ID":     "access",
		"VECTIS_ARTIFACT_STORAGE_S3_SECRET_ACCESS_KEY": "secret",
		"VECTIS_ARTIFACT_STORAGE_S3_PATH_STYLE":        "true",
		"VECTIS_ARTIFACT_STORAGE_S3_TEMP_DIR":          "/data/vectis/artifact/s3-tmp",
	}

	if len(env) != len(want) {
		t.Fatalf("env len = %d, want %d: %+v", len(env), len(want), env)
	}

	for key, value := range want {
		if env[key] != value {
			t.Fatalf("%s = %q, want %q", key, env[key], value)
		}
	}
}

func TestSmokeKnoxFixtureManifestContract(t *testing.T) {
	opts := normalizeSmokeOptions(SmokeOptions{
		KnoxImage:        "registry.example.com/knox:test",
		KnoxKeyID:        "team:ci_token",
		KnoxExpectedData: "secret value",
		KnoxAuthToken:    "0tci-principal",
	})

	certs := smokeKnoxCertBundle{
		CA:         []byte("ca"),
		ServerCert: []byte("server-cert"),
		ServerKey:  []byte("server-key"),
		ClientCert: []byte("client-cert"),
		ClientKey:  []byte("client-key"),
	}

	manifest := smokeKnoxFixtureManifest(opts, certs)
	for _, want := range []string{
		"kind: Secret",
		"name: \"vectis-knox-smoke-certs\"",
		"ca.crt: " + yamlQuote(base64.StdEncoding.EncodeToString([]byte("ca"))),
		"kind: Deployment",
		"name: vectis-knox",
		"image: \"registry.example.com/knox:test\"",
		"name: KNOX_SMOKE_KEY_ID",
		"value: \"team:ci_token\"",
		"name: KNOX_SMOKE_AUTH_TOKEN",
		"value: \"0tci-principal\"",
		"mountPath: /certs",
		"kind: Service",
		"targetPort: 9000",
	} {
		if !strings.Contains(manifest, want) {
			t.Fatalf("manifest missing %q: %s", want, manifest)
		}
	}
}

func TestSmokeKnoxSecretsEnvContract(t *testing.T) {
	opts := normalizeSmokeOptions(SmokeOptions{
		KnoxClusterURL:    "https://vectis-knox:9000/",
		KnoxAuthToken:     "0tci-principal",
		KnoxCertMountPath: "/run/vectis/knox-ci/",
	})

	env := smokeKnoxSecretsEnv(opts)
	want := map[string]string{
		"VECTIS_SECRETS_PROVIDERS_KNOX_URL":              "https://vectis-knox:9000",
		"VECTIS_SECRETS_PROVIDERS_KNOX_AUTH_TOKEN":       "0tci-principal",
		"VECTIS_SECRETS_PROVIDERS_KNOX_CA_FILE":          "/run/vectis/knox-ci/ca.crt",
		"VECTIS_SECRETS_PROVIDERS_KNOX_CLIENT_CERT_FILE": "/run/vectis/knox-ci/client.crt",
		"VECTIS_SECRETS_PROVIDERS_KNOX_CLIENT_KEY_FILE":  "/run/vectis/knox-ci/client.key",
		"VECTIS_SECRETS_POLICY_ALLOW":                    "namespace=*;job=*;task=*;ref=encryptedfs://*,namespace=*;job=*;task=*;ref=knox://*",
	}

	if len(env) != len(want) {
		t.Fatalf("env len = %d, want %d: %+v", len(env), len(want), env)
	}

	for key, value := range want {
		if env[key] != value {
			t.Fatalf("%s = %q, want %q", key, env[key], value)
		}
	}
}

func TestSmokeLDAPFixtureManifestContract(t *testing.T) {
	opts := normalizeSmokeOptions(SmokeOptions{
		LDAPImage:        "registry.example.com/openldap:test",
		LDAPBindPassword: "bind secret",
	})

	manifest := smokeLDAPFixtureManifest(opts, "dn: ou=people,dc=example,dc=org\nobjectClass: organizationalUnit\n")
	for _, want := range []string{
		"kind: ConfigMap",
		"name: vectis-ldap-bootstrap",
		"001-vectis-smoke.ldif:",
		"kind: Deployment",
		"name: vectis-ldap",
		"image: \"registry.example.com/openldap:test\"",
		"- --copy-service",
		"name: LDAP_READONLY_USER",
		"value: \"true\"",
		"name: LDAP_READONLY_USER_USERNAME",
		"value: \"vectis\"",
		"name: LDAP_READONLY_USER_PASSWORD",
		"value: \"bind secret\"",
		"name: LDAP_TLS",
		"mountPath: /container/service/slapd/assets/config/bootstrap/ldif/custom/001-vectis-smoke.ldif",
		"subPath: 001-vectis-smoke.ldif",
		"kind: Service",
		"targetPort: 389",
	} {
		if !strings.Contains(manifest, want) {
			t.Fatalf("manifest missing %q: %s", want, manifest)
		}
	}
}

func TestSmokeLDAPAPIEnvContract(t *testing.T) {
	opts := normalizeSmokeOptions(SmokeOptions{
		LDAPClusterURL:      "ldap://vectis-ldap:389/",
		LDAPBindDN:          "cn=svc,dc=example,dc=org",
		LDAPBindPassword:    "bind-secret",
		LDAPBaseDN:          "ou=users,dc=example,dc=org",
		LDAPUserFilter:      "(mail={username})",
		LDAPSubjectAttr:     "entryUUID",
		LDAPUsernameAttr:    "mail",
		LDAPDisplayNameAttr: "displayName",
	})

	env := smokeLDAPAPIEnv(opts)
	want := map[string]string{
		"VECTIS_API_AUTH_ENABLED":                     "true",
		"VECTIS_API_AUTHZ_ENGINE":                     "authenticated_full",
		"VECTIS_API_SESSION_ALLOW_INSECURE_COOKIES":   "true",
		"VECTIS_API_AUTH_LDAP_PROVIDER_ID":            "ldap",
		"VECTIS_API_AUTH_LDAP_URL":                    "ldap://vectis-ldap:389",
		"VECTIS_API_AUTH_LDAP_BIND_DN":                "cn=svc,dc=example,dc=org",
		"VECTIS_API_AUTH_LDAP_BIND_PASSWORD":          "bind-secret",
		"VECTIS_API_AUTH_LDAP_BASE_DN":                "ou=users,dc=example,dc=org",
		"VECTIS_API_AUTH_LDAP_USER_FILTER":            "(mail={username})",
		"VECTIS_API_AUTH_LDAP_SUBJECT_ATTRIBUTE":      "entryUUID",
		"VECTIS_API_AUTH_LDAP_USERNAME_ATTRIBUTE":     "mail",
		"VECTIS_API_AUTH_LDAP_DISPLAY_NAME_ATTRIBUTE": "displayName",
		"VECTIS_API_AUTH_LDAP_START_TLS":              "false",
		"VECTIS_API_AUTH_LDAP_TIMEOUT":                "30s",
		"VECTIS_API_AUTH_LDAP_AUTO_LINK_USERS":        "false",
		"VECTIS_API_AUTH_LDAP_AUTO_CREATE_USERS":      "false",
	}

	if len(env) != len(want) {
		t.Fatalf("env len = %d, want %d: %+v", len(env), len(want), env)
	}

	for key, value := range want {
		if env[key] != value {
			t.Fatalf("%s = %q, want %q", key, env[key], value)
		}
	}
}

func TestValidateLDAPAuthSmokeOptions(t *testing.T) {
	if err := validateLDAPAuthSmokeOptions(normalizeSmokeOptions(SmokeOptions{})); err != nil {
		t.Fatal(err)
	}

	opts := normalizeSmokeOptions(SmokeOptions{})
	opts.LDAPClusterURL = "http://vectis-ldap:389"
	if err := validateLDAPAuthSmokeOptions(opts); err == nil {
		t.Fatal("expected non-LDAP cluster URL to fail")
	}

	opts = normalizeSmokeOptions(SmokeOptions{})
	opts.LDAPBindPassword = ""
	if err := validateLDAPAuthSmokeOptions(opts); err == nil {
		t.Fatal("expected empty LDAP bind password to fail")
	}
}

func TestValidateKnoxSecretsSmokeOptions(t *testing.T) {
	if err := validateKnoxSecretsSmokeOptions(normalizeSmokeOptions(SmokeOptions{})); err != nil {
		t.Fatal(err)
	}

	opts := normalizeSmokeOptions(SmokeOptions{})
	opts.KnoxClusterURL = "http://vectis-knox:9000"
	if err := validateKnoxSecretsSmokeOptions(opts); err == nil {
		t.Fatal("expected non-https Knox cluster URL to fail")
	}

	opts = normalizeSmokeOptions(SmokeOptions{})
	opts.KnoxAuthToken = "token"
	if err := validateKnoxSecretsSmokeOptions(opts); err == nil {
		t.Fatal("expected invalid Knox auth token to fail")
	}
}

func TestValidateS3ArtifactSmokeOptions(t *testing.T) {
	if err := validateS3ArtifactSmokeOptions(normalizeSmokeOptions(SmokeOptions{})); err != nil {
		t.Fatal(err)
	}

	opts := normalizeSmokeOptions(SmokeOptions{})
	opts.S3LocalPort = 70000
	if err := validateS3ArtifactSmokeOptions(opts); err == nil {
		t.Fatal("expected invalid local port to fail")
	}

	opts = normalizeSmokeOptions(SmokeOptions{})
	opts.S3SecretAccessKey = ""
	if err := validateS3ArtifactSmokeOptions(opts); err == nil {
		t.Fatal("expected empty secret access key to fail")
	}
}

func TestSmokeGerritStreamJobPayloadContract(t *testing.T) {
	opts := normalizeSmokeOptions(SmokeOptions{GerritClusterURL: "http://vectis-gerrit:8080"})
	payload, err := smokeGerritStreamJobPayload(opts, "kubernetes-gerrit-stream-test", "project/test", "echo kubernetes-gerrit-stream-ok")
	if err != nil {
		t.Fatal(err)
	}

	var decoded struct {
		Namespace string `json:"namespace"`
		Job       struct {
			ID   string `json:"id"`
			Root struct {
				Uses string            `json:"uses"`
				With map[string]string `json:"with"`
			} `json:"root"`
			Triggers []struct {
				ID      string `json:"id"`
				SCMPoll struct {
					Provider        string `json:"provider"`
					BaseURL         string `json:"base_url"`
					Project         string `json:"project"`
					Branch          string `json:"branch"`
					Query           string `json:"query"`
					IntervalSeconds int    `json:"interval_seconds"`
				} `json:"scm_poll"`
			} `json:"triggers"`
		} `json:"job"`
	}

	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}

	if decoded.Namespace != "/" || decoded.Job.ID != "kubernetes-gerrit-stream-test" || decoded.Job.Root.Uses != "builtins/shell" {
		t.Fatalf("unexpected job payload: %+v", decoded)
	}

	if got := decoded.Job.Root.With["command"]; got != "echo kubernetes-gerrit-stream-ok" {
		t.Fatalf("command = %q", got)
	}

	if len(decoded.Job.Triggers) != 1 {
		t.Fatalf("triggers = %d, want 1", len(decoded.Job.Triggers))
	}

	trigger := decoded.Job.Triggers[0]
	if trigger.ID != smokeGerritStreamTriggerID || trigger.SCMPoll.Provider != "gerrit" ||
		trigger.SCMPoll.BaseURL != "http://vectis-gerrit:8080" ||
		trigger.SCMPoll.Project != "project/test" ||
		trigger.SCMPoll.Branch != smokeGerritStreamBranch ||
		trigger.SCMPoll.Query != smokeGerritStreamQuery ||
		trigger.SCMPoll.IntervalSeconds != smokeGerritStreamPollInterval {
		t.Fatalf("unexpected trigger payload: %+v", trigger)
	}
}

func TestValidateGerritStreamSmokeOptions(t *testing.T) {
	if err := validateGerritStreamSmokeOptions(normalizeSmokeOptions(SmokeOptions{})); err != nil {
		t.Fatal(err)
	}

	opts := normalizeSmokeOptions(SmokeOptions{})
	opts.GerritClusterURL = "not a url"
	if err := validateGerritStreamSmokeOptions(opts); err == nil {
		t.Fatal("expected invalid cluster URL to fail")
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
