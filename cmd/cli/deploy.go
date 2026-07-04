package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
	podmanassets "vectis/deploy/podman"
	"vectis/internal/database"
)

const (
	defaultPodmanKubeSpec    = "deploy/podman/kube-spec.yaml.tmpl"
	legacyPodmanKubeSpec     = "deploy/podman/kube-spec.yaml"
	defaultPodmanGrafanaSpec = "deploy/podman/grafana-configmaps.gen.yaml"
	podmanSecretName         = "vectis-podman-secrets"
	envDeployConfigDir       = "VECTIS_DEPLOY_CONFIG_DIR"
	podmanProfileSimple      = "simple"
	podmanProfileHA          = "ha"
)

type podmanSecrets struct {
	PostgresPassword string `json:"postgres_password"`
	BootstrapToken   string `json:"bootstrap_token"`
	EncryptedFSKey   string `json:"encryptedfs_key"`
	PodDSN           string `json:"pod_database_dsn"`
	HostDSN          string `json:"host_database_dsn"`
}

type podmanCommandResult struct {
	Status         string `json:"status"`
	Profile        string `json:"profile,omitempty"`
	SecretsPath    string `json:"secrets_path,omitempty"`
	ManifestPath   string `json:"manifest_path,omitempty"`
	Manifest       string `json:"manifest,omitempty"`
	SecretsCreated bool   `json:"secrets_created,omitempty"`
	BootstrapToken bool   `json:"bootstrap_token_generated,omitempty"`
	Network        string `json:"network,omitempty"`
	PodmanStdout   string `json:"podman_stdout,omitempty"`
	PodmanStderr   string `json:"podman_stderr,omitempty"`
}

func podmanDeployDir() (string, error) {
	if dir := os.Getenv(envDeployConfigDir); dir != "" {
		return filepath.Join(dir, "podman"), nil
	}

	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "vectis", "deploy", "podman"), nil
}

func podmanSecretsPath() (string, error) {
	dir, err := podmanDeployDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "secrets.json"), nil
}

func podmanRenderedPath() (string, error) {
	dir, err := podmanDeployDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(dir, "rendered.yaml"), nil
}

func randomSecret(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(b), nil
}

func loadOrCreatePodmanSecrets(rotate bool) (podmanSecrets, bool, error) {
	path, err := podmanSecretsPath()
	if err != nil {
		return podmanSecrets{}, false, err
	}

	if !rotate {
		b, err := os.ReadFile(path)
		if err == nil {
			var secrets podmanSecrets
			if err := json.Unmarshal(b, &secrets); err != nil {
				return podmanSecrets{}, false, fmt.Errorf("parse podman secrets %s: %w", path, err)
			}

			updated, err := ensurePodmanSecretFields(&secrets)
			if err != nil {
				return podmanSecrets{}, false, err
			}

			if updated {
				if err := writePodmanSecrets(path, secrets); err != nil {
					return podmanSecrets{}, false, err
				}
			}

			return secrets, false, nil
		}

		if !os.IsNotExist(err) {
			return podmanSecrets{}, false, err
		}
	}

	postgresPassword, err := randomSecret(32)
	if err != nil {
		return podmanSecrets{}, false, fmt.Errorf("generate postgres password: %w", err)
	}

	bootstrapToken, err := randomSecret(32)
	if err != nil {
		return podmanSecrets{}, false, fmt.Errorf("generate bootstrap token: %w", err)
	}

	secrets := podmanSecrets{
		PostgresPassword: postgresPassword,
		BootstrapToken:   bootstrapToken,
		PodDSN:           fmt.Sprintf("postgres://vectis:%s@127.0.0.1:5432/vectis?sslmode=verify-full&sslrootcert=/run/vectis/postgres-tls/ca.pem", postgresPassword),
		HostDSN:          fmt.Sprintf("postgres://vectis:%s@127.0.0.1:15432/vectis?sslmode=require", postgresPassword),
	}

	if _, err := ensurePodmanSecretFields(&secrets); err != nil {
		return podmanSecrets{}, false, err
	}

	if err := writePodmanSecrets(path, secrets); err != nil {
		return podmanSecrets{}, false, err
	}

	return secrets, true, os.Chmod(path, 0o600)
}

func ensurePodmanSecretFields(secrets *podmanSecrets) (bool, error) {
	if secrets == nil {
		return false, fmt.Errorf("podman secrets are required")
	}

	if strings.TrimSpace(secrets.EncryptedFSKey) != "" {
		return false, nil
	}

	key, err := randomSecret(32)
	if err != nil {
		return false, fmt.Errorf("generate encryptedfs key: %w", err)
	}

	secrets.EncryptedFSKey = key
	return true, nil
}

func writePodmanSecrets(path string, secrets podmanSecrets) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}

	b, err := json.MarshalIndent(secrets, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		return err
	}

	return os.Chmod(path, 0o600)
}

func yamlString(s string) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(s)
	return strings.TrimSpace(b.String())
}

func renderPodmanSecret(secrets podmanSecrets) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
type: Opaque
stringData:
  POSTGRES_PASSWORD: %s
  VECTIS_DATABASE_DSN: %s
  VECTIS_API_AUTH_BOOTSTRAP_TOKEN: %s
  encryptedfs.key: %s
---
`, podmanSecretName, yamlString(secrets.PostgresPassword), yamlString(secrets.PodDSN), yamlString(secrets.BootstrapToken), yamlString(secrets.EncryptedFSKey))
}

func readDeployFile(path string) ([]byte, error) {
	switch path {
	case "", defaultPodmanKubeSpec, legacyPodmanKubeSpec:
		return podmanassets.KubeSpecTemplate, nil
	case defaultPodmanGrafanaSpec:
		return podmanassets.GrafanaConfigMaps, nil
	}

	b, err := os.ReadFile(path)
	if err == nil {
		return b, nil
	}

	return nil, fmt.Errorf("read %s: %w", path, err)
}

func normalizedPodmanProfile() (string, error) {
	profile := strings.ToLower(strings.TrimSpace(podmanProfile))
	if profile == "" {
		profile = podmanProfileSimple
	}

	switch profile {
	case podmanProfileSimple, podmanProfileHA:
		return profile, nil
	default:
		return "", fmt.Errorf("invalid podman profile %q (must be simple or ha)", podmanProfile)
	}
}

type podmanTemplateData struct {
	Profile                       string
	RegistryAddresses             string
	PrometheusAPITargets          []string
	PrometheusQueueTargets        []string
	PrometheusOrchestratorTargets []string
	PrometheusWorkerTargets       []string
	PrometheusWorkerCoreTargets   []string
	PrometheusLogTargets          []string
	PrometheusArtifactTargets     []string
	PrometheusSecretsTargets      []string
	PrometheusReconcilerTargets   []string
	APIReplicas                   []podmanAPIReplica
	CronReplicas                  []podmanCronReplica
	LogShards                     []podmanLogShard
	ArtifactShards                []podmanArtifactShard
	OrchestratorReplicas          []podmanOrchestratorReplica
	QueueShards                   []podmanQueueShard
	ReconcilerReplicas            []podmanReconcilerReplica
	RegistryReplicas              []podmanRegistryReplica
	WorkerReplicas                []podmanWorkerReplica
}

type podmanAPIReplica struct {
	Name       string
	Port       int
	First      bool
	SetPortEnv bool
}

type podmanCronReplica struct {
	Name       string
	InstanceID string
}

type podmanLogShard struct {
	Name              string
	GRPCPort          int
	MetricsPort       int
	StorageDir        string
	InstanceID        string
	AdvertiseAddress  string
	SetGRPCPortEnv    bool
	SetMetricsPortEnv bool
}

type podmanArtifactShard struct {
	Name              string
	GRPCPort          int
	MetricsPort       int
	StorageDir        string
	InstanceID        string
	AdvertiseAddress  string
	SetGRPCPortEnv    bool
	SetMetricsPortEnv bool
}

type podmanQueueShard struct {
	Name              string
	Port              int
	MetricsPort       int
	PersistenceDir    string
	Pool              string
	InstanceID        string
	AdvertiseAddress  string
	SetPortEnv        bool
	SetMetricsPortEnv bool
}

type podmanOrchestratorReplica struct {
	Name              string
	Port              int
	MetricsPort       int
	AdvertiseAddress  string
	SetPortEnv        bool
	SetMetricsPortEnv bool
}

type podmanReconcilerReplica struct {
	Name              string
	MetricsPort       int
	SetMetricsPortEnv bool
}

type podmanRegistryReplica struct {
	Name             string
	Port             int
	First            bool
	SetPortEnv       bool
	ClusterNodeID    string
	AdvertiseAddress string
	PeerAddresses    string
}

type podmanWorkerReplica struct {
	Name              string
	MetricsPort       int
	ControlPort       int
	SetMetricsPortEnv bool
	SetControlPortEnv bool
}

func renderPodmanKubeSpec(profile string) ([]byte, error) {
	kubeTemplate, err := readDeployFile(podmanKubeSpec)
	if err != nil {
		return nil, err
	}

	tmpl, err := template.New("podman-kube-spec").
		Funcs(template.FuncMap{"quoteList": podmanQuoteList}).
		Option("missingkey=error").
		Parse(string(kubeTemplate))

	if err != nil {
		return nil, fmt.Errorf("parse podman kube template: %w", err)
	}

	var out bytes.Buffer
	if err := tmpl.Execute(&out, podmanTemplateDataForProfile(profile)); err != nil {
		return nil, fmt.Errorf("render podman kube template: %w", err)
	}

	return out.Bytes(), nil
}

func podmanQuoteList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, yamlString(value))
	}

	return "[" + strings.Join(quoted, ", ") + "]"
}

func podmanTemplateDataForProfile(profile string) podmanTemplateData {
	data := podmanTemplateData{
		Profile:                       profile,
		PrometheusAPITargets:          podmanTargets(8080),
		PrometheusQueueTargets:        podmanTargets(9081),
		PrometheusOrchestratorTargets: podmanTargets(9087),
		PrometheusWorkerTargets:       podmanTargets(9082),
		PrometheusWorkerCoreTargets:   podmanTargets(9092),
		PrometheusLogTargets:          podmanTargets(9083),
		PrometheusArtifactTargets:     podmanTargets(9089),
		PrometheusSecretsTargets:      podmanTargets(9091),
		PrometheusReconcilerTargets:   podmanTargets(9085),
		APIReplicas: []podmanAPIReplica{
			{Name: "api", Port: 8080, First: true},
		},
		CronReplicas: []podmanCronReplica{
			{Name: "cron"},
		},
		LogShards: []podmanLogShard{
			{Name: "log", GRPCPort: 8083, MetricsPort: 9083, StorageDir: "/data/vectis/jobs", InstanceID: "log-1", AdvertiseAddress: podmanAddr(8083)},
		},
		ArtifactShards: []podmanArtifactShard{
			{Name: "artifact", GRPCPort: 8086, MetricsPort: 9089, StorageDir: "/data/vectis/artifact", InstanceID: "artifact-1", AdvertiseAddress: podmanAddr(8086)},
		},
		QueueShards: []podmanQueueShard{
			{Name: "queue", Port: 8081, MetricsPort: 9081, InstanceID: "queue-1", AdvertiseAddress: podmanAddr(8081), PersistenceDir: "/data/vectis/queue"},
		},
		OrchestratorReplicas: []podmanOrchestratorReplica{
			{Name: "orchestrator", Port: 8087, MetricsPort: 9087, AdvertiseAddress: podmanAddr(8087), SetMetricsPortEnv: true},
		},
		ReconcilerReplicas: []podmanReconcilerReplica{
			{Name: "reconciler", MetricsPort: 9085},
		},
		RegistryReplicas: []podmanRegistryReplica{
			{Name: "registry", Port: 8082, First: true},
		},
		WorkerReplicas: []podmanWorkerReplica{
			{Name: "worker", MetricsPort: 9082, ControlPort: 9084},
		},
	}

	if profile != podmanProfileHA {
		return data
	}

	registryPorts := []int{8082, 8182, 8282}
	registryAddrs := make([]string, 0, len(registryPorts))
	for _, port := range registryPorts {
		registryAddrs = append(registryAddrs, podmanAddr(port))
	}

	data.RegistryAddresses = strings.Join(registryAddrs, ",")
	data.PrometheusAPITargets = podmanTargets(8080, 8180)
	data.PrometheusQueueTargets = podmanTargets(9081, 9181)
	data.PrometheusOrchestratorTargets = podmanTargets(9087)
	data.PrometheusWorkerTargets = podmanTargets(9082, 9182)
	data.PrometheusWorkerCoreTargets = podmanTargets(9092)
	data.PrometheusLogTargets = podmanTargets(9083, 9183)
	data.PrometheusArtifactTargets = podmanTargets(9089, 9189)
	data.PrometheusSecretsTargets = podmanTargets(9091)
	data.PrometheusReconcilerTargets = podmanTargets(9085, 9185)
	data.APIReplicas = []podmanAPIReplica{
		{Name: "api", Port: 8080, First: true},
		{Name: "api-2", Port: 8180, SetPortEnv: true},
	}

	data.CronReplicas = []podmanCronReplica{
		{Name: "cron", InstanceID: "cron-1"},
		{Name: "cron-2", InstanceID: "cron-2"},
	}

	data.LogShards = []podmanLogShard{
		{Name: "log", GRPCPort: 8083, MetricsPort: 9083, StorageDir: "/data/vectis/jobs/log-1", InstanceID: "log-1", AdvertiseAddress: podmanAddr(8083)},
		{Name: "log-2", GRPCPort: 8183, MetricsPort: 9183, StorageDir: "/data/vectis/jobs/log-2", InstanceID: "log-2", AdvertiseAddress: podmanAddr(8183), SetGRPCPortEnv: true, SetMetricsPortEnv: true},
	}

	data.ArtifactShards = []podmanArtifactShard{
		{Name: "artifact", GRPCPort: 8086, MetricsPort: 9089, StorageDir: "/data/vectis/artifact/artifact-1", InstanceID: "artifact-1", AdvertiseAddress: podmanAddr(8086)},
		{Name: "artifact-2", GRPCPort: 8186, MetricsPort: 9189, StorageDir: "/data/vectis/artifact/artifact-2", InstanceID: "artifact-2", AdvertiseAddress: podmanAddr(8186), SetGRPCPortEnv: true, SetMetricsPortEnv: true},
	}

	data.QueueShards = []podmanQueueShard{
		{Name: "queue", Port: 8081, MetricsPort: 9081, Pool: "local-ha", InstanceID: "queue-1", AdvertiseAddress: podmanAddr(8081), PersistenceDir: "/data/vectis/queue/local-ha/queue-1"},
		{Name: "queue-2", Port: 8181, MetricsPort: 9181, Pool: "local-ha", InstanceID: "queue-2", AdvertiseAddress: podmanAddr(8181), PersistenceDir: "/data/vectis/queue/local-ha/queue-2", SetPortEnv: true, SetMetricsPortEnv: true},
	}

	data.OrchestratorReplicas = []podmanOrchestratorReplica{
		{Name: "orchestrator", Port: 8087, MetricsPort: 9087, AdvertiseAddress: podmanAddr(8087), SetMetricsPortEnv: true},
	}

	data.ReconcilerReplicas = []podmanReconcilerReplica{
		{Name: "reconciler", MetricsPort: 9085},
		{Name: "reconciler-2", MetricsPort: 9185, SetMetricsPortEnv: true},
	}

	data.RegistryReplicas = make([]podmanRegistryReplica, 0, len(registryPorts))
	for i, port := range registryPorts {
		name := fmt.Sprintf("registry-%d", i+1)
		if i == 0 {
			name = "registry"
		}

		data.RegistryReplicas = append(data.RegistryReplicas, podmanRegistryReplica{
			Name:             name,
			Port:             port,
			First:            i == 0,
			SetPortEnv:       i != 0,
			ClusterNodeID:    fmt.Sprintf("registry-%d", i+1),
			AdvertiseAddress: registryAddrs[i],
			PeerAddresses:    podmanPeerAddresses(registryAddrs, i),
		})
	}

	data.WorkerReplicas = []podmanWorkerReplica{
		{Name: "worker", MetricsPort: 9082, ControlPort: 9084},
		{Name: "worker-2", MetricsPort: 9182, ControlPort: 9184, SetMetricsPortEnv: true, SetControlPortEnv: true},
	}

	return data
}

func podmanTargets(ports ...int) []string {
	targets := make([]string, 0, len(ports))
	for _, port := range ports {
		targets = append(targets, podmanAddr(port))
	}

	return targets
}

func podmanAddr(port int) string {
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func podmanPeerAddresses(addrs []string, self int) string {
	peers := make([]string, 0, len(addrs)-1)
	for i, addr := range addrs {
		if i != self {
			peers = append(peers, addr)
		}
	}

	return strings.Join(peers, ",")
}

func renderPodmanManifest(rotate bool) ([]byte, podmanSecrets, bool, error) {
	profile, err := normalizedPodmanProfile()
	if err != nil {
		return nil, podmanSecrets{}, false, err
	}

	secrets, created, err := loadOrCreatePodmanSecrets(rotate)
	if err != nil {
		return nil, podmanSecrets{}, false, err
	}

	kubeSpec, err := renderPodmanKubeSpec(profile)
	if err != nil {
		return nil, podmanSecrets{}, false, err
	}

	grafanaSpec, err := readDeployFile(podmanGrafanaSpec)
	if err != nil {
		return nil, podmanSecrets{}, false, err
	}

	var out bytes.Buffer
	out.WriteString(renderPodmanSecret(secrets))
	out.Write(kubeSpec)
	if !bytes.HasSuffix(kubeSpec, []byte("\n")) {
		out.WriteByte('\n')
	}

	out.WriteString("---\n")
	out.Write(grafanaSpec)
	if !bytes.HasSuffix(grafanaSpec, []byte("\n")) {
		out.WriteByte('\n')
	}

	return out.Bytes(), secrets, created, nil
}

func writePodmanRenderedManifest(rotate bool) (string, podmanSecrets, bool, error) {
	manifest, secrets, created, err := renderPodmanManifest(rotate)
	if err != nil {
		return "", podmanSecrets{}, false, err
	}

	path, err := podmanRenderedPath()
	if err != nil {
		return "", podmanSecrets{}, false, err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", podmanSecrets{}, false, err
	}

	if err := os.WriteFile(path, manifest, 0o600); err != nil {
		return "", podmanSecrets{}, false, err
	}

	return path, secrets, created, nil
}

func runDeployPodmanInit(cmd *cobra.Command, args []string) {
	rotate, _ := cmd.Flags().GetBool("rotate")
	secrets, created, err := loadOrCreatePodmanSecrets(rotate)
	runCLIError(err)

	path, _ := podmanSecretsPath()
	if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, podmanCommandResult{
			Status:         "initialized",
			SecretsPath:    path,
			SecretsCreated: created,
			BootstrapToken: secrets.BootstrapToken != "",
		}))
		return
	}

	if created {
		fmt.Printf("Generated Podman deployment secrets: %s\n", path)
	} else {
		fmt.Printf("Podman deployment secrets already exist: %s\n", path)
	}

	fmt.Println("Host migration DSN is stored locally for deploy commands.")
	if secrets.BootstrapToken != "" {
		fmt.Println("API bootstrap token generated for use when auth is enabled.")
	}
}

func runDeployPodmanRender(cmd *cobra.Command, args []string) {
	rotate, _ := cmd.Flags().GetBool("rotate")
	manifest, _, created, err := renderPodmanManifest(rotate)
	runCLIError(err)
	profile, err := normalizedPodmanProfile()
	runCLIError(err)

	if podmanRenderOut == "" || podmanRenderOut == "-" {
		if outputIsJSON() {
			runCLIError(writeJSON(os.Stdout, podmanCommandResult{
				Status:         "rendered",
				Profile:        profile,
				Manifest:       string(manifest),
				SecretsCreated: created,
			}))

			return
		}

		_, _ = os.Stdout.Write(manifest)
		return
	}

	if err := os.MkdirAll(filepath.Dir(podmanRenderOut), 0o700); err != nil {
		runCLIError(err)
	}

	if err := os.WriteFile(podmanRenderOut, manifest, 0o600); err != nil {
		runCLIError(err)
	}

	if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, podmanCommandResult{
			Status:         "rendered",
			Profile:        profile,
			ManifestPath:   podmanRenderOut,
			SecretsCreated: created,
		}))

		return
	}

	fmt.Printf("Rendered Podman %s manifest: %s\n", profile, podmanRenderOut)
}

func runDeployPodmanUp(cmd *cobra.Command, args []string) {
	manifestPath, secrets, created, err := writePodmanRenderedManifest(false)
	runCLIError(err)
	profile, err := normalizedPodmanProfile()
	runCLIError(err)

	if created && !outputIsJSON() {
		fmt.Println("Generated Podman deployment secrets.")
	}

	playArgs := []string{"play", "kube", "--replace", manifestPath}
	if podmanNetwork != "" {
		playArgs = append(playArgs, "--network", podmanNetwork)
	}

	// NOTE(garrett): playArgs are fixed podman subcommands plus manifestPath from our rendered output.
	podman := exec.CommandContext(cmd.Context(), "podman", playArgs...) //#nosec G204
	var podmanStdout, podmanStderr bytes.Buffer

	if outputIsJSON() {
		podman.Stdout = &podmanStdout
		podman.Stderr = &podmanStderr
	} else {
		podman.Stdout = os.Stdout
		podman.Stderr = os.Stderr
	}

	if err := podman.Run(); err != nil {
		runCLIError(fmt.Errorf("podman play kube failed: %w", err))
	}

	oldDriver, hadDriver := os.LookupEnv(database.EnvDatabaseDriver)
	oldDSN, hadDSN := os.LookupEnv(database.EnvDatabaseDSN)
	_ = os.Setenv(database.EnvDatabaseDriver, "pgx")
	_ = os.Setenv(database.EnvDatabaseDSN, secrets.HostDSN)
	defer func() {
		if hadDriver {
			_ = os.Setenv(database.EnvDatabaseDriver, oldDriver)
		} else {
			_ = os.Unsetenv(database.EnvDatabaseDriver)
		}

		if hadDSN {
			_ = os.Setenv(database.EnvDatabaseDSN, oldDSN)
		} else {
			_ = os.Unsetenv(database.EnvDatabaseDSN)
		}
	}()

	if err := database.Migrate(secrets.HostDSN); err != nil {
		runCLIError(fmt.Errorf("migrations failed: %w", err))
	}

	if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, podmanCommandResult{
			Status:         "up",
			Profile:        profile,
			ManifestPath:   manifestPath,
			SecretsCreated: created,
			Network:        podmanNetwork,
			PodmanStdout:   podmanStdout.String(),
			PodmanStderr:   podmanStderr.String(),
		}))

		return
	}

	fmt.Printf("Podman %s deployment is up and migrations are applied.\n", profile)
}

func runDeployPodmanDown(cmd *cobra.Command, args []string) {
	manifestPath, _, _, err := writePodmanRenderedManifest(false)
	runCLIError(err)
	profile, err := normalizedPodmanProfile()
	runCLIError(err)

	playArgs := []string{"play", "kube", "--down", manifestPath}
	podman := exec.CommandContext(cmd.Context(), "podman", playArgs...) //#nosec G204
	var podmanStdout, podmanStderr bytes.Buffer
	if outputIsJSON() {
		podman.Stdout = &podmanStdout
		podman.Stderr = &podmanStderr
	} else {
		podman.Stdout = os.Stdout
		podman.Stderr = os.Stderr
	}

	if err := podman.Run(); err != nil {
		runCLIError(fmt.Errorf("podman play kube --down failed: %w", err))
	}

	if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, podmanCommandResult{
			Status:       "down",
			Profile:      profile,
			ManifestPath: manifestPath,
			PodmanStdout: podmanStdout.String(),
			PodmanStderr: podmanStderr.String(),
		}))
	}
}

func runDeployPodmanStatus(cmd *cobra.Command, args []string) {
	podman := exec.CommandContext(cmd.Context(), "podman", "pod", "ps", "--filter", "name=vectis", "--format", "table {{.Name}}\t{{.Status}}")
	var podmanStdout, podmanStderr bytes.Buffer

	if outputIsJSON() {
		podman.Stdout = &podmanStdout
		podman.Stderr = &podmanStderr
	} else {
		podman.Stdout = os.Stdout
		podman.Stderr = os.Stderr
	}

	if err := podman.Run(); err != nil {
		runCLIError(fmt.Errorf("podman status failed: %w", err))
	}

	if outputIsJSON() {
		runCLIError(writeJSON(os.Stdout, podmanCommandResult{
			Status:       "status",
			PodmanStdout: podmanStdout.String(),
			PodmanStderr: podmanStderr.String(),
		}))
	}
}

var deployCmd = &cobra.Command{
	Use:     "deploy",
	Short:   "Manage first-class Vectis deployment paths",
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

var deployPodmanCmd = &cobra.Command{
	Use:   "podman",
	Short: "Manage the Podman staging/reference deployment",
	Run:   showCommandHelp,
}

var deployPodmanInitCmd = &cobra.Command{
	Use:   "init",
	Short: "Generate local Podman deployment secrets",
	Args:  cobra.NoArgs,
	Run:   runDeployPodmanInit,
}

var deployPodmanRenderCmd = &cobra.Command{
	Use:   "render",
	Short: "Render the Podman deployment manifest",
	Args:  cobra.NoArgs,
	Run:   runDeployPodmanRender,
}

var deployPodmanUpCmd = &cobra.Command{
	Use:   "up",
	Short: "Start or replace the Podman deployment and apply migrations",
	Args:  cobra.NoArgs,
	Run:   runDeployPodmanUp,
}

var deployPodmanDownCmd = &cobra.Command{
	Use:   "down",
	Short: "Stop the Podman deployment",
	Args:  cobra.NoArgs,
	Run:   runDeployPodmanDown,
}

var deployPodmanStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show Podman deployment status",
	Args:  cobra.NoArgs,
	Run:   runDeployPodmanStatus,
}
