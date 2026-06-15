package main

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/localpki"
	"vectis/internal/localspiffe"
	"vectis/internal/platform"
	secretstore "vectis/internal/secrets"
	"vectis/internal/serviceidentity"
	"vectis/internal/spire"
	"vectis/internal/supervisor"
	"vectis/internal/utils"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	_ "vectis/internal/dbdrivers"
)

type serviceStage struct {
	binary      string
	name        string
	stage       int
	checkHealth bool
	portFn      func() int
	healthName  string
	env         []string
}

type trackedCmd struct {
	cmd    *exec.Cmd
	binary string
	name   string
}

type localCell struct {
	ID                     string
	Index                  int
	QueuePort              int
	QueueMetricsPort       int
	SecretsPort            int
	SecretsMetricsPort     int
	CellIngressPort        int
	CellIngressMetricsPort int
	WorkerMetricsPort      int
	CellDB                 string
	QueueDir               string
	SecretsDir             string
	SecretsKeyFile         string
}

type localTopology struct {
	GlobalDB string
	Cells    []localCell
}

var (
	orderedSingletonServices = []serviceStage{
		{binary: "vectis-registry", stage: 0, checkHealth: true, portFn: config.RegistryEffectiveListenPort, healthName: "registry"},
		{binary: "vectis-log", stage: 1, checkHealth: true, portFn: config.LogGRPCPort, healthName: "log"},
		{binary: "vectis-artifact", stage: 1, checkHealth: true, portFn: config.ArtifactGRPCPort, healthName: "artifact"},
		{binary: "vectis-orchestrator", stage: 1, checkHealth: true, portFn: config.OrchestratorEffectiveListenPort, healthName: "orchestrator"},
		{binary: "vectis-worker-core", stage: 1, checkHealth: false},
		{binary: "vectis-cron", stage: 2, checkHealth: false},
		{binary: "vectis-reconciler", stage: 2, checkHealth: false},
		{binary: "vectis-catalog", stage: 2, checkHealth: false},
		{binary: "vectis-api", stage: 2, checkHealth: false},
		{binary: "vectis-docs", stage: 2, checkHealth: false},
	}

	allStarted     []*exec.Cmd
	allStartedMu   sync.Mutex
	tracked        []trackedCmd
	trackedMu      sync.Mutex
	shuttingDown   bool
	shuttingDownMu sync.Mutex
)

const (
	healthCheckInterval           = 50 * time.Millisecond
	healthCheckTimeout            = 10 * time.Second
	cellPortStride                = 100
	localSecretsDisabledAddress   = "disabled"
	localProfileSimple            = "simple"
	localProfileHA                = "ha"
	localHTTPSTLSAuto             = "auto"
	localHTTPSTLSOn               = "on"
	localHTTPSTLSOff              = "off"
	localSPIFFETrustDomainDefault = "vectis.internal"
)

func waitForHealthy(port int, serviceName string, timeout time.Duration) error {
	addr := fmt.Sprintf("localhost:%d", port)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for %s to be healthy", serviceName)
		case <-ticker.C:
			opts, err := config.GRPCClientDialOptions(addr)
			if err != nil {
				return fmt.Errorf("grpc tls for health check: %w", err)
			}

			conn, err := grpc.NewClient(addr, opts...)
			if err != nil {
				continue
			}

			client := healthpb.NewHealthClient(conn)
			resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: serviceName})
			_ = conn.Close()

			if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
				return nil
			}
		}
	}
}

func (svc serviceStage) label() string {
	if svc.name != "" {
		return svc.name
	}

	return svc.binary
}

func startService(logger interfaces.Logger, svc serviceStage, logLevel string, tlsEnv []string) (*exec.Cmd, error) {
	path, err := supervisor.FindBinary(svc.binary)
	if err != nil {
		return nil, fmt.Errorf("cannot find %s: %w", svc.label(), err)
	}

	// NOTE(garrett): Path comes from supervisor.FindBinary (installed vectis binaries), not arbitrary user input.
	command := exec.Command(path) //#nosec G204
	command.Stdin = nil
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	env := mergeEnv(os.Environ(), tlsEnv, svc.env)
	if logLevel != "" {
		env = mergeEnv(env, []string{logLevelEnvVar(svc.binary, logLevel)})
	}
	command.Env = env

	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("failed to start %s: %w", svc.label(), err)
	}

	return command, nil
}

func mergeEnv(groups ...[]string) []string {
	out := make([]string, 0)
	indexByKey := map[string]int{}

	for _, group := range groups {
		for _, entry := range group {
			key, _, ok := strings.Cut(entry, "=")
			if !ok {
				out = append(out, entry)
				continue
			}

			if index, exists := indexByKey[key]; exists {
				out[index] = entry
				continue
			}

			indexByKey[key] = len(out)
			out = append(out, entry)
		}
	}

	return out
}

func localServices(logger interfaces.Logger, topology localTopology) []serviceStage {
	switch localProfile() {
	case localProfileHA:
		return localHAProfileServices(logger, topology)
	default:
		return localSimpleProfileServices(logger, topology)
	}
}

func localSimpleProfileServices(logger interfaces.Logger, topology localTopology) []serviceStage {
	services := make([]serviceStage, 0, len(orderedSingletonServices)+len(topology.Cells)*4)
	for _, svc := range orderedSingletonServices {
		if svc.binary == "vectis-docs" {
			if !viper.GetBool("docs_enabled") {
				continue
			}

			if _, err := supervisor.FindBinary(svc.binary); err != nil {
				logger.Warn("Docs enabled, but %s was not found; continuing without local docs", svc.binary)
				continue
			}
		}

		services = append(services, svc)
	}

	for _, cell := range topology.Cells {
		services = append(services,
			serviceStage{
				binary:      "vectis-queue",
				name:        fmt.Sprintf("vectis-queue[%s]", cell.ID),
				stage:       1,
				checkHealth: true,
				portFn:      func() int { return cell.QueuePort },
				healthName:  "queue",
				env:         queueEnv(cell, topology.multiCell()),
			})

		if localSecretsEnabled() {
			services = append(services,
				serviceStage{
					binary:      "vectis-secrets",
					name:        fmt.Sprintf("vectis-secrets[%s]", cell.ID),
					stage:       1,
					checkHealth: true,
					portFn:      func() int { return cell.SecretsPort },
					healthName:  "secrets",
					env:         secretsEnv(cell),
				})
		}

		services = append(services,
			serviceStage{
				binary: "vectis-cell-ingress",
				name:   fmt.Sprintf("vectis-cell-ingress[%s]", cell.ID),
				stage:  2,
				env:    cellIngressEnv(cell),
			},
			serviceStage{
				binary: "vectis-worker",
				name:   fmt.Sprintf("vectis-worker[%s]", cell.ID),
				stage:  2,
				env:    workerEnv(cell, topology.multiCell()),
			},
		)
	}

	return services
}

func localHAProfileServices(logger interfaces.Logger, topology localTopology) []serviceStage {
	cell := newLocalCell(config.CellID(), 0, "")
	if len(topology.Cells) > 0 {
		cell = topology.Cells[0]
	}

	registryPorts := []int{8082, 8182, 8282}
	registryAddrs := make([]string, 0, len(registryPorts))
	for _, port := range registryPorts {
		registryAddrs = append(registryAddrs, netJoinLocal(port))
	}

	registryEnv := []string{"VECTIS_DISCOVERY_REGISTRY_ADDRESSES=" + strings.Join(registryAddrs, ",")}
	services := make([]serviceStage, 0, 20)
	for i, port := range registryPorts {
		name := fmt.Sprintf("registry-%d", i+1)
		peers := make([]string, 0, len(registryAddrs)-1)
		for j, addr := range registryAddrs {
			if j != i {
				peers = append(peers, addr)
			}
		}

		services = append(services, serviceStage{
			binary:      "vectis-registry",
			name:        name,
			stage:       0,
			checkHealth: true,
			portFn:      fixedPort(port),
			healthName:  "registry",
			env: []string{
				fmt.Sprintf("VECTIS_REGISTRY_PORT=%d", port),
				"VECTIS_REGISTRY_CLUSTER_NODE_ID=" + name,
				"VECTIS_REGISTRY_CLUSTER_ADVERTISE_ADDRESS=" + netJoinLocal(port),
				"VECTIS_REGISTRY_CLUSTER_PEER_ADDRESSES=" + strings.Join(peers, ","),
			},
		})
	}

	queuePorts := []int{cell.QueuePort, cell.QueuePort + cellPortStride}
	for i, port := range queuePorts {
		name := fmt.Sprintf("queue-%d", i+1)
		env := append([]string{}, registryEnv...)
		env = append(env,
			"VECTIS_CELL_ID="+cell.ID,
			fmt.Sprintf("VECTIS_QUEUE_PORT=%d", port),
			fmt.Sprintf("VECTIS_QUEUE_METRICS_PORT=%d", cell.QueueMetricsPort+(i*cellPortStride)),
			"VECTIS_QUEUE_POOL=local-ha",
			"VECTIS_QUEUE_INSTANCE_ID="+name,
			"VECTIS_QUEUE_ADVERTISE_ADDRESS="+netJoinLocal(port),
		)

		services = append(services, serviceStage{
			binary:      "vectis-queue",
			name:        name,
			stage:       1,
			checkHealth: true,
			portFn:      fixedPort(port),
			healthName:  "queue",
			env:         env,
		})
	}

	logPorts := []int{8083, 8183}
	for i, port := range logPorts {
		name := fmt.Sprintf("log-%d", i+1)
		env := append([]string{}, registryEnv...)
		env = append(env,
			fmt.Sprintf("VECTIS_LOG_GRPC_PORT=%d", port),
			fmt.Sprintf("VECTIS_LOG_METRICS_PORT=%d", 9083+(i*100)),
			"VECTIS_LOG_INSTANCE_ID="+name,
			"VECTIS_LOG_GRPC_ADVERTISE_ADDRESS="+netJoinLocal(port),
		)

		services = append(services, serviceStage{
			binary:      "vectis-log",
			name:        name,
			stage:       1,
			checkHealth: true,
			portFn:      fixedPort(port),
			healthName:  "log",
			env:         env,
		})
	}

	artifactPorts := []int{8086, 8186}
	for i, port := range artifactPorts {
		name := fmt.Sprintf("artifact-%d", i+1)
		env := append([]string{}, registryEnv...)
		env = append(env,
			fmt.Sprintf("VECTIS_ARTIFACT_GRPC_PORT=%d", port),
			fmt.Sprintf("VECTIS_ARTIFACT_METRICS_PORT=%d", 9089+(i*100)),
			"VECTIS_ARTIFACT_INSTANCE_ID="+name,
			"VECTIS_ARTIFACT_GRPC_ADVERTISE_ADDRESS="+netJoinLocal(port),
		)

		services = append(services, serviceStage{
			binary:      "vectis-artifact",
			name:        name,
			stage:       1,
			checkHealth: true,
			portFn:      fixedPort(port),
			healthName:  "artifact",
			env:         env,
		})
	}

	orchestratorEnv := append([]string{}, registryEnv...)
	orchestratorEnv = append(orchestratorEnv,
		fmt.Sprintf("VECTIS_ORCHESTRATOR_PORT=%d", config.OrchestratorPort()),
		fmt.Sprintf("VECTIS_ORCHESTRATOR_METRICS_PORT=%d", config.OrchestratorMetricsPort()),
		"VECTIS_ORCHESTRATOR_ADVERTISE_ADDRESS="+netJoinLocal(config.OrchestratorPort()),
	)

	services = append(services, serviceStage{
		binary:      "vectis-orchestrator",
		name:        "orchestrator",
		stage:       1,
		checkHealth: true,
		portFn:      fixedPort(config.OrchestratorPort()),
		healthName:  "orchestrator",
		env:         orchestratorEnv,
	})

	services = append(services, serviceStage{
		binary: "vectis-worker-core",
		name:   "worker-core",
		stage:  1,
	})

	if localSecretsEnabled() {
		services = append(services, serviceStage{
			binary:      "vectis-secrets",
			name:        fmt.Sprintf("vectis-secrets[%s]", cell.ID),
			stage:       1,
			checkHealth: true,
			portFn:      fixedPort(cell.SecretsPort),
			healthName:  "secrets",
			env:         secretsEnv(cell),
		})
	}

	for i, port := range []int{8080, 8180} {
		env := append([]string{}, registryEnv...)
		env = append(env,
			fmt.Sprintf("VECTIS_API_SERVER_PORT=%d", port),
			"VECTIS_API_SERVER_HOST="+localHost(),
		)

		services = append(services, serviceStage{
			binary: "vectis-api",
			name:   fmt.Sprintf("api-%d", i+1),
			stage:  2,
			env:    env,
		})
	}

	ingressEnv := append([]string{}, registryEnv...)
	ingressEnv = append(ingressEnv,
		"VECTIS_CELL_ID="+cell.ID,
		database.EnvCellDatabaseDSN+"="+cell.CellDB,
		"VECTIS_CELL_INGRESS_HOST="+localHost(),
		fmt.Sprintf("VECTIS_CELL_INGRESS_PORT=%d", cell.CellIngressPort),
		fmt.Sprintf("VECTIS_CELL_INGRESS_METRICS_PORT=%d", cell.CellIngressMetricsPort),
	)
	services = append(services, serviceStage{
		binary: "vectis-cell-ingress",
		name:   fmt.Sprintf("vectis-cell-ingress[%s]", cell.ID),
		stage:  2,
		env:    ingressEnv,
	})

	for i := range 2 {
		env := append([]string{}, registryEnv...)
		env = append(env,
			"VECTIS_CELL_ID="+cell.ID,
			database.EnvCellDatabaseDSN+"="+cell.CellDB,
			fmt.Sprintf("VECTIS_WORKER_METRICS_PORT=%d", cell.WorkerMetricsPort+(i*cellPortStride)),
			fmt.Sprintf("VECTIS_WORKER_CONTROL_PORT=%d", config.WorkerControlPort()+(i*cellPortStride)),
			"VECTIS_WORKER_CORE_SHELL_SOCKET="+localWorkerCoreShellSocket(fmt.Sprintf("worker-%d", i+1)),
			localWorkerSecretsEnv(cell),
		)

		services = append(services, serviceStage{
			binary: "vectis-worker",
			name:   fmt.Sprintf("worker-%d", i+1),
			stage:  2,
			env:    env,
		})
	}

	services = append(services, serviceStage{
		binary: "vectis-catalog",
		name:   "vectis-catalog",
		stage:  2,
		env:    registryEnv,
	})

	for i := range 2 {
		env := append([]string{}, registryEnv...)
		env = append(env, fmt.Sprintf("VECTIS_CRON_INSTANCE_ID=cron-%d", i+1))
		services = append(services, serviceStage{
			binary: "vectis-cron",
			name:   fmt.Sprintf("cron-%d", i+1),
			stage:  2,
			env:    env,
		})
	}

	for i := range 2 {
		env := append([]string{}, registryEnv...)
		env = append(env, fmt.Sprintf("VECTIS_RECONCILER_METRICS_PORT=%d", 9085+(i*100)))
		services = append(services, serviceStage{
			binary: "vectis-reconciler",
			name:   fmt.Sprintf("reconciler-%d", i+1),
			stage:  2,
			env:    env,
		})
	}

	if viper.GetBool("docs_enabled") {
		if _, err := supervisor.FindBinary("vectis-docs"); err != nil {
			logger.Warn("Docs enabled, but vectis-docs was not found; continuing without local docs")
		} else {
			services = append(services, serviceStage{binary: "vectis-docs", name: "docs", stage: 2})
		}
	}

	return services
}

func fixedPort(port int) func() int {
	return func() int { return port }
}

func netJoinLocal(port int) string {
	return fmt.Sprintf("localhost:%d", port)
}

func localProfile() string {
	profile := strings.ToLower(strings.TrimSpace(viper.GetString("profile")))
	if profile == "" {
		return localProfileSimple
	}

	return profile
}

func localHTTPSTLSMode() string {
	mode := strings.ToLower(strings.TrimSpace(viper.GetString("http_tls")))
	if mode == "" {
		return localHTTPSTLSAuto
	}

	return mode
}

func validLocalHTTPSTLSMode(mode string) bool {
	switch mode {
	case localHTTPSTLSAuto, localHTTPSTLSOn, localHTTPSTLSOff:
		return true
	default:
		return false
	}
}

func configuredLocalTLSDir(defaultDataHome string) string {
	if dir := strings.TrimSpace(viper.GetString("tls_dir")); dir != "" {
		return dir
	}

	return localpki.EnsureDir(defaultDataHome)
}

func defaultCertInstallDataHome() string {
	sudoUser := strings.TrimSpace(os.Getenv("SUDO_USER"))
	if sudoUser != "" && sudoUser != "root" {
		if u, err := user.Lookup(sudoUser); err == nil && strings.TrimSpace(u.HomeDir) != "" {
			return filepath.Join(u.HomeDir, ".local", "share")
		}
	}

	return utils.DataHome()
}

type localBrowserTLSConfig struct {
	Enabled bool
	Scheme  string
	Env     []string
}

func localBrowserTLS(material *localpki.Material, mode string, logger interfaces.Logger) localBrowserTLSConfig {
	cfg := localBrowserTLSConfig{Scheme: "http"}
	if mode == localHTTPSTLSOff || material == nil {
		return cfg
	}

	enabled := mode == localHTTPSTLSOn
	if mode == localHTTPSTLSAuto {
		trusted, err := material.ServerTrustedBySystem()
		if err != nil {
			logger.Warn("Could not verify local browser TLS trust: %v", err)
		}

		enabled = trusted
		if !trusted {
			logger.Warn("Local API and docs will use HTTP because the generated CA is not trusted. Run vectis-local init and then run vectis-local install-cert with elevated privileges, or set --http-tls=on.")
		}
	}

	if !enabled {
		return cfg
	}

	cfg.Enabled = true
	cfg.Scheme = "https"
	cfg.Env = []string{
		"VECTIS_API_TLS_CERT_FILE=" + material.ServerCert,
		"VECTIS_API_TLS_KEY_FILE=" + material.ServerKey,
		"VECTIS_API_SESSION_COOKIE_SECURE=true",
		"VECTIS_DOCS_TLS_CERT_FILE=" + material.ServerCert,
		"VECTIS_DOCS_TLS_KEY_FILE=" + material.ServerKey,
	}

	return cfg
}

type localSPIFFEConfig struct {
	Enabled            bool
	ClientCABundleFile string
	Env                []string
}

type localEmbeddedSPIFFEConfig struct {
	Enabled      bool
	TrustDomain  string
	DataDir      string
	RuntimeDir   string
	ServerSocket string
	AgentSocket  string
	BundleFile   string
	ParentID     string
	Selectors    []string
	Authority    *localspiffe.Authority
}

func startEmbeddedLocalSPIFFE(logger interfaces.Logger) (cfg localEmbeddedSPIFFEConfig, err error) {
	cfg, err = embeddedLocalSPIFFEConfig()
	if err != nil || !cfg.Enabled {
		return cfg, err
	}

	authority, err := localspiffe.Start(context.Background(), localspiffe.Config{
		TrustDomain:            cfg.TrustDomain,
		DataDir:                cfg.DataDir,
		RuntimeDir:             cfg.RuntimeDir,
		WorkloadSocketPath:     cfg.AgentSocket,
		RegistrationSocketPath: cfg.ServerSocket,
		BundleFile:             cfg.BundleFile,
		Selectors:              cfg.Selectors,
	})
	if err != nil {
		return cfg, fmt.Errorf("start embedded local SPIFFE authority: %w", err)
	}

	if logger != nil {
		logger.Info("Started embedded local SPIFFE authority for trust domain %s", cfg.TrustDomain)
	}

	cfg.Authority = authority
	applyEmbeddedLocalSPIFFEConfig(cfg)
	return cfg, nil
}

func embeddedLocalSPIFFEConfig() (localEmbeddedSPIFFEConfig, error) {
	if viper.GetBool("grpc_insecure") {
		return localEmbeddedSPIFFEConfig{}, nil
	}

	trustDomain := strings.TrimSpace(viper.GetString("spiffe_trust_domain"))
	if trustDomain == "" {
		trustDomain = localSPIFFETrustDomainDefault
	}

	dataDir := strings.TrimSpace(viper.GetString("spiffe_dir"))
	if dataDir == "" {
		dataDir = filepath.Join(utils.DataHome(), "vectis", "spiffe")
	}

	runtimeDir := strings.TrimSpace(viper.GetString("spiffe_runtime_dir"))
	if runtimeDir == "" {
		runtimeDir = filepath.Join(utils.RuntimeDir(), "spiffe")
	}

	selectors := localSPIFFERegistrationSelectors()
	if len(selectors) == 0 {
		selectors = []string{fmt.Sprintf("unix:uid:%d", os.Getuid())}
	}

	parentID := strings.TrimSpace(viper.GetString("spiffe_parent_id"))
	if parentID == "" {
		parentID = "spiffe://" + trustDomain + "/vectis-spiffe/agent/local"
	}

	serverSocket := filepath.Join(runtimeDir, "registration.sock")
	agentSocket := filepath.Join(runtimeDir, "workload.sock")

	return localEmbeddedSPIFFEConfig{
		Enabled:      true,
		TrustDomain:  trustDomain,
		DataDir:      dataDir,
		RuntimeDir:   runtimeDir,
		ServerSocket: serverSocket,
		AgentSocket:  agentSocket,
		BundleFile:   filepath.Join(dataDir, "bundle.pem"),
		ParentID:     parentID,
		Selectors:    selectors,
	}, nil
}

func applyEmbeddedLocalSPIFFEConfig(cfg localEmbeddedSPIFFEConfig) {
	viper.Set("spiffe_enabled", true)
	viper.Set("spiffe_trust_domain", cfg.TrustDomain)
	viper.Set("spiffe_workload_api_address", "unix://"+cfg.AgentSocket)
	viper.Set("spiffe_registration_server_address", "unix://"+cfg.ServerSocket)
	viper.Set("spiffe_parent_id", cfg.ParentID)
	viper.Set("spiffe_selectors", cfg.Selectors)
	viper.Set("spiffe_bundle_file", cfg.BundleFile)
}

func localSPIFFE(tlsDir string, material *localpki.Material) (localSPIFFEConfig, error) {
	if !viper.GetBool("spiffe_enabled") {
		return localSPIFFEConfig{}, nil
	}

	if viper.GetBool("grpc_insecure") {
		return localSPIFFEConfig{}, fmt.Errorf("local SPIFFE identity mode requires gRPC TLS; remove --grpc-insecure")
	}

	if material == nil {
		return localSPIFFEConfig{}, fmt.Errorf("local SPIFFE identity mode requires local TLS material")
	}

	trustDomain := strings.TrimSpace(viper.GetString("spiffe_trust_domain"))
	if trustDomain == "" {
		return localSPIFFEConfig{}, fmt.Errorf("local SPIFFE identity mode requires --spiffe-trust-domain")
	}

	workloadAPIAddress := strings.TrimSpace(viper.GetString("spiffe_workload_api_address"))
	if err := spire.ValidateWorkloadAPIAddress(workloadAPIAddress); err != nil {
		return localSPIFFEConfig{}, fmt.Errorf("local SPIFFE workload API: %w", err)
	}

	serverAPIAddress := strings.TrimSpace(viper.GetString("spiffe_registration_server_address"))
	if err := spire.ValidateServerAPIAddress(serverAPIAddress); err != nil {
		return localSPIFFEConfig{}, fmt.Errorf("local SPIFFE registration API: %w", err)
	}

	parentID := strings.TrimSpace(viper.GetString("spiffe_parent_id"))
	if parentID == "" {
		return localSPIFFEConfig{}, fmt.Errorf("local SPIFFE identity mode requires --spiffe-parent-id")
	}

	if _, err := serviceidentity.NormalizeSPIFFEAllowlist([]string{parentID}); err != nil {
		return localSPIFFEConfig{}, fmt.Errorf("local SPIFFE registration parent ID: %w", err)
	}

	selectors := localSPIFFERegistrationSelectors()
	if len(selectors) == 0 {
		return localSPIFFEConfig{}, fmt.Errorf("local SPIFFE identity mode requires at least one --spiffe-selector")
	}

	for _, selector := range selectors {
		if _, err := spire.ParseSelector(selector); err != nil {
			return localSPIFFEConfig{}, fmt.Errorf("local SPIFFE registration selector %q: %w", selector, err)
		}
	}

	spiffeBundle := strings.TrimSpace(viper.GetString("spiffe_bundle_file"))
	if spiffeBundle == "" {
		return localSPIFFEConfig{}, fmt.Errorf("local SPIFFE identity mode requires a SPIFFE bundle file")
	}

	clientCABundle, err := writeLocalSPIFFEClientCABundle(tlsDir, material.CAFile, spiffeBundle)
	if err != nil {
		return localSPIFFEConfig{}, err
	}

	env := []string{
		"VECTIS_GRPC_TLS_CLIENT_CA_FILE=" + clientCABundle,
		"VECTIS_WORKER_EXECUTION_IDENTITY_ENABLED=true",
		"VECTIS_WORKER_EXECUTION_IDENTITY_TRUST_DOMAIN=" + trustDomain,
		"VECTIS_WORKER_SPIFFE_ENABLED=true",
		"VECTIS_WORKER_SPIFFE_WORKLOAD_API_ADDRESS=" + workloadAPIAddress,
		"VECTIS_WORKER_SPIFFE_REGISTRATION_ENABLED=true",
		"VECTIS_WORKER_SPIFFE_REGISTRATION_SERVER_ADDRESS=" + serverAPIAddress,
		"VECTIS_WORKER_SPIFFE_REGISTRATION_PARENT_ID=" + parentID,
		"VECTIS_WORKER_SPIFFE_REGISTRATION_SELECTORS=" + strings.Join(selectors, ","),
	}

	if pathTemplate := strings.TrimSpace(viper.GetString("spiffe_path_template")); pathTemplate != "" {
		env = append(env, "VECTIS_WORKER_EXECUTION_IDENTITY_PATH_TEMPLATE="+pathTemplate)
	}

	for _, durationEnv := range []struct {
		key string
		env string
	}{
		{key: "spiffe_fetch_timeout", env: "VECTIS_WORKER_SPIFFE_FETCH_TIMEOUT"},
		{key: "spiffe_x509_svid_ttl", env: "VECTIS_WORKER_SPIFFE_REGISTRATION_X509_SVID_TTL"},
		{key: "spiffe_registration_min_ttl", env: "VECTIS_WORKER_SPIFFE_REGISTRATION_MIN_TTL"},
		{key: "spiffe_registration_max_ttl", env: "VECTIS_WORKER_SPIFFE_REGISTRATION_MAX_TTL"},
	} {
		if value := strings.TrimSpace(viper.GetString(durationEnv.key)); value != "" {
			env = append(env, durationEnv.env+"="+value)
		}
	}

	return localSPIFFEConfig{
		Enabled:            true,
		ClientCABundleFile: clientCABundle,
		Env:                env,
	}, nil
}

func localSPIFFERegistrationSelectors() []string {
	selectors := append([]string{}, viper.GetStringSlice("spiffe_selectors")...)
	if raw := strings.TrimSpace(viper.GetString("spiffe_selectors")); raw != "" {
		selectors = append(selectors, raw)
	}

	return cleanCommaSeparated(selectors)
}

func cleanCommaSeparated(values []string) []string {
	out := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			part = strings.TrimSpace(part)
			if part == "" || seen[part] {
				continue
			}

			seen[part] = true
			out = append(out, part)
		}
	}

	return out
}

func writeLocalSPIFFEClientCABundle(tlsDir, localCAFile, spiffeBundleFile string) (string, error) {
	localCA, err := readCertificateBundle(localCAFile, "local gRPC CA")
	if err != nil {
		return "", err
	}

	spiffeBundle, err := readCertificateBundle(spiffeBundleFile, "SPIFFE bundle")
	if err != nil {
		return "", err
	}

	if err := os.MkdirAll(tlsDir, 0o700); err != nil {
		return "", fmt.Errorf("create local TLS directory: %w", err)
	}

	path := filepath.Join(tlsDir, "client-ca-bundle.pem")
	var out bytes.Buffer
	out.Write(bytes.TrimSpace(localCA))
	out.WriteByte('\n')
	out.Write(bytes.TrimSpace(spiffeBundle))
	out.WriteByte('\n')

	if err := os.WriteFile(path, out.Bytes(), 0o644); err != nil {
		return "", fmt.Errorf("write combined local SPIFFE client CA bundle: %w", err)
	}

	return path, nil
}

func readCertificateBundle(path, label string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s %s: %w", label, path, err)
	}

	if count, err := countPEMCertificates(b); err != nil {
		return nil, fmt.Errorf("%s %s: %w", label, path, err)
	} else if count == 0 {
		return nil, fmt.Errorf("%s %s: no PEM certificates found", label, path)
	}

	return b, nil
}

func countPEMCertificates(b []byte) (int, error) {
	count := 0
	rest := bytes.TrimSpace(b)
	for len(rest) > 0 {
		block, remaining := pem.Decode(rest)
		if block == nil {
			return 0, fmt.Errorf("invalid PEM data")
		}

		if block.Type == "CERTIFICATE" {
			if _, err := x509.ParseCertificate(block.Bytes); err != nil {
				return 0, fmt.Errorf("parse certificate PEM: %w", err)
			}
			count++
		}

		rest = bytes.TrimSpace(remaining)
	}

	return count, nil
}

func docsEnv() []string {
	if !viper.GetBool("docs_enabled") {
		return nil
	}

	env := []string{
		"VECTIS_DOCS_HOST=" + localHost(),
		fmt.Sprintf("VECTIS_DOCS_PORT=%d", viper.GetInt("docs_port")),
	}

	if dir := strings.TrimSpace(viper.GetString("docs_dir")); dir != "" {
		env = append(env, "VECTIS_DOCS_DIR="+dir)
	}

	return env
}

func apiEnv() ([]string, error) {
	env := []string{"VECTIS_API_SERVER_HOST=" + localHost()}

	sourceRepos, err := localSourceRepositoryDeclarations()
	if err != nil {
		return nil, err
	}

	if len(sourceRepos) > 0 {
		payload, err := json.Marshal(sourceRepos)
		if err != nil {
			return nil, fmt.Errorf("encode local source repositories: %w", err)
		}

		env = append(env,
			"VECTIS_SOURCE_REPOSITORIES="+string(payload),
			"VECTIS_SOURCE_SYNC_CONFIGURED_REPOSITORIES_ON_STARTUP=true",
		)
	}

	return env, nil
}

func localSourceRepositoryDeclarations() ([]config.SourceRepositoryDeclaration, error) {
	values := localSourceRepositorySpecs()
	if len(values) == 0 {
		if viper.GetBool("config_as_code") {
			return nil, fmt.Errorf("config-as-code requires one or more --source-repository repository_id=checkout_path flags")
		}

		return nil, nil
	}

	repos := make([]config.SourceRepositoryDeclaration, 0, len(values))
	for _, value := range values {
		repositoryID, checkoutPath, ok := strings.Cut(value, "=")
		repositoryID = strings.TrimSpace(repositoryID)
		checkoutPath = strings.TrimSpace(checkoutPath)
		if !ok || repositoryID == "" || checkoutPath == "" {
			return nil, fmt.Errorf("local source repository %q must use repository_id=checkout_path", value)
		}

		if !filepath.IsAbs(checkoutPath) {
			abs, err := filepath.Abs(checkoutPath)
			if err != nil {
				return nil, fmt.Errorf("local source repository %q checkout path: %w", repositoryID, err)
			}

			checkoutPath = abs
		}

		repos = append(repos, config.SourceRepositoryDeclaration{
			RepositoryID: repositoryID,
			SourceKind:   "local_checkout",
			CheckoutMode: "external",
			CheckoutPath: checkoutPath,
		})
	}

	return repos, nil
}

func localSourceRepositorySpecs() []string {
	values := viper.GetStringSlice("source_repositories")
	if len(values) == 0 {
		if raw := strings.TrimSpace(viper.GetString("source_repositories")); raw != "" {
			values = []string{raw}
		}
	}

	return cleanCommaSeparated(values)
}

func queueEnv(cell localCell, multiCell bool) []string {
	env := []string{
		"VECTIS_CELL_ID=" + cell.ID,
		fmt.Sprintf("VECTIS_QUEUE_PORT=%d", cell.QueuePort),
		fmt.Sprintf("VECTIS_QUEUE_METRICS_PORT=%d", cell.QueueMetricsPort),
	}

	if multiCell {
		env = append(env, "VECTIS_QUEUE_PERSISTENCE_DIR="+cell.QueueDir)
	}

	if cell.Index > 0 {
		env = append(env, "VECTIS_QUEUE_REGISTER_WITH_REGISTRY=false")
	}

	return env
}

func cellIngressEnv(cell localCell) []string {
	return []string{
		"VECTIS_CELL_ID=" + cell.ID,
		database.EnvCellDatabaseDSN + "=" + cell.CellDB,
		"VECTIS_CELL_INGRESS_HOST=" + localHost(),
		fmt.Sprintf("VECTIS_CELL_INGRESS_PORT=%d", cell.CellIngressPort),
		fmt.Sprintf("VECTIS_CELL_INGRESS_METRICS_PORT=%d", cell.CellIngressMetricsPort),
		"VECTIS_CELL_INGRESS_QUEUE_ADDRESS=" + localQueueAddress(cell),
	}
}

func secretsEnv(cell localCell) []string {
	return []string{
		"VECTIS_CELL_ID=" + cell.ID,
		database.EnvCellDatabaseDSN + "=" + cell.CellDB,
		fmt.Sprintf("VECTIS_SECRETS_PORT=%d", cell.SecretsPort),
		fmt.Sprintf("VECTIS_SECRETS_METRICS_PORT=%d", cell.SecretsMetricsPort),
		"VECTIS_SECRETS_ENCRYPTEDFS_ROOT=" + cell.SecretsDir,
		"VECTIS_SECRETS_ENCRYPTEDFS_KEY_FILE=" + cell.SecretsKeyFile,
		"VECTIS_SECRETS_POLICY_ALLOW=namespace=*;job=*;task=*;ref=encryptedfs://*",
	}
}

func workerEnv(cell localCell, multiCell bool) []string {
	env := []string{
		"VECTIS_CELL_ID=" + cell.ID,
		database.EnvCellDatabaseDSN + "=" + cell.CellDB,
		fmt.Sprintf("VECTIS_WORKER_METRICS_PORT=%d", cell.WorkerMetricsPort),
		"VECTIS_WORKER_QUEUE_ADDRESS=" + localQueueAddress(cell),
		"VECTIS_WORKER_CORE_SHELL_SOCKET=" + localWorkerCoreShellSocket(cell.ID),
		localWorkerSecretsEnv(cell),
	}

	if multiCell {
		env = append(env, "VECTIS_WORKER_CONTROL_MODE=ephemeral")
	}

	return env
}

func localWorkerCoreShellSocket(name string) string {
	return filepath.Join(utils.RuntimeDir(), "worker-core-shell-"+safePathPart(name)+".sock")
}

func localSecretsEnabled() bool {
	return !viper.GetBool("grpc_insecure")
}

func localWorkerSecretsEnv(cell localCell) string {
	if !localSecretsEnabled() {
		return "VECTIS_WORKER_SECRETS_ADDRESS=" + localSecretsDisabledAddress
	}

	return "VECTIS_WORKER_SECRETS_ADDRESS=" + localSecretsAddress(cell)
}

func localQueueAddress(cell localCell) string {
	return net.JoinHostPort(localConnectHost(), fmt.Sprintf("%d", cell.QueuePort))
}

func localSecretsAddress(cell localCell) string {
	return net.JoinHostPort(localConnectHost(), fmt.Sprintf("%d", cell.SecretsPort))
}

func localCellIngressEndpointSpecs(cells []localCell) []string {
	specs := make([]string, 0, len(cells))
	scheme := localCellIngressEndpointScheme()
	for _, cell := range cells {
		endpoint := scheme + "://" + net.JoinHostPort(localConnectHost(), fmt.Sprintf("%d", cell.CellIngressPort))
		specs = append(specs, fmt.Sprintf("%s=%s", cell.ID, endpoint))
	}

	return specs
}

func localCellIngressEndpointScheme() string {
	if viper.GetBool("grpc_insecure") {
		return "http"
	}

	return "https"
}

func localCellIngressEndpointEnv(cells []localCell) []string {
	spec := strings.Join(localCellIngressEndpointSpecs(cells), ",")
	env := make([]string, 0, 2)
	if strings.TrimSpace(os.Getenv("VECTIS_CELL_INGRESS_ENDPOINTS")) == "" {
		env = append(env, "VECTIS_CELL_INGRESS_ENDPOINTS="+spec)
	}

	if strings.TrimSpace(os.Getenv("VECTIS_API_SERVER_CELL_INGRESS_ENDPOINTS")) == "" {
		env = append(env, "VECTIS_API_SERVER_CELL_INGRESS_ENDPOINTS="+spec)
	}

	return env
}

func localCatalogCellDatabaseEnv(cells []localCell) []string {
	if strings.TrimSpace(os.Getenv("VECTIS_CATALOG_CELL_DATABASE_DSNS")) != "" {
		return nil
	}

	specs := make([]string, 0, len(cells))
	for _, cell := range cells {
		specs = append(specs, fmt.Sprintf("%s=%s", cell.ID, cell.CellDB))
	}

	return []string{"VECTIS_CATALOG_CELL_DATABASE_DSNS=" + strings.Join(specs, ",")}
}

func (t localTopology) multiCell() bool {
	return len(t.Cells) > 1
}

func localDatabaseEnv(topology localTopology) []string {
	cellDSN := ""
	if len(topology.Cells) > 0 {
		cellDSN = topology.Cells[0].CellDB
	}

	return []string{
		database.EnvGlobalDatabaseDSN + "=" + topology.GlobalDB,
		database.EnvCellDatabaseDSN + "=" + cellDSN,
	}
}

func localDatabaseDSNs() (globalDSN, cellDSN string) {
	if localUsesManagedSQLiteDatabases() {
		return localManagedGlobalDB(), localManagedCellDB(config.CellID())
	}

	return database.GetDBPathForRole(database.RoleGlobal), database.GetDBPathForRole(database.RoleCell)
}

func localUsesManagedSQLiteDatabases() bool {
	return database.EffectiveDBDriver() == "sqlite3" &&
		strings.TrimSpace(os.Getenv(database.EnvDatabaseDSN)) == "" &&
		strings.TrimSpace(os.Getenv(database.EnvGlobalDatabaseDSN)) == "" &&
		strings.TrimSpace(os.Getenv(database.EnvCellDatabaseDSN)) == ""
}

func localManagedGlobalDB() string {
	return filepath.Join(utils.DataHome(), "vectis", "global", "db.sqlite3")
}

func localManagedCellDB(cellID string) string {
	return filepath.Join(utils.DataHome(), "vectis", "cells", safePathPart(cellID), "db.sqlite3")
}

func localManagedQueueDir(cellID string) string {
	return filepath.Join(utils.DataHome(), "vectis", "cells", safePathPart(cellID), "queue")
}

func localManagedSecretsDir(cellID string) string {
	return filepath.Join(utils.DataHome(), "vectis", "cells", safePathPart(cellID), "secrets")
}

func localManagedSecretsKeyFile(cellID string) string {
	return filepath.Join(utils.DataHome(), "vectis", "cells", safePathPart(cellID), "secrets.key")
}

func localExtraCellIDs() []string {
	values := viper.GetStringSlice("cells")
	if len(values) == 0 {
		if raw := strings.TrimSpace(viper.GetString("cells")); raw != "" {
			values = []string{raw}
		}
	}

	return cleanCellIDs(values)
}

func cleanCellIDs(values []string) []string {
	var out []string
	for _, value := range values {
		for part := range strings.SplitSeq(value, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	}

	return out
}

func buildLocalTopology() (localTopology, error) {
	globalDB, defaultCellDB := localDatabaseDSNs()
	defaultCellID := config.CellID()
	extraCells := localExtraCellIDs()
	if len(extraCells) > 0 && !localUsesManagedSQLiteDatabases() {
		return localTopology{}, fmt.Errorf("vectis-local multi-cell currently requires default SQLite database management; unset %s, %s, and %s", database.EnvDatabaseDSN, database.EnvGlobalDatabaseDSN, database.EnvCellDatabaseDSN)
	}

	cells := make([]localCell, 0, len(extraCells)+1)
	seen := map[string]bool{defaultCellID: true}
	cells = append(cells, newLocalCell(defaultCellID, 0, defaultCellDB))

	for i, cellID := range extraCells {
		if seen[cellID] {
			return localTopology{}, fmt.Errorf("duplicate local cell %q", cellID)
		}

		seen[cellID] = true
		cells = append(cells, newLocalCell(cellID, i+1, localManagedCellDB(cellID)))
	}

	for _, cell := range cells {
		if err := validateLocalCellPorts(cell); err != nil {
			return localTopology{}, err
		}
	}

	return localTopology{GlobalDB: globalDB, Cells: cells}, nil
}

func ensureLocalSecretsKeys(topology localTopology) error {
	for _, cell := range topology.Cells {
		if _, err := secretstore.EnsureEncryptedFSKeyFile(cell.SecretsKeyFile); err != nil {
			return fmt.Errorf("cell %s: %w", cell.ID, err)
		}
	}

	return nil
}

func newLocalCell(cellID string, index int, cellDB string) localCell {
	offset := index * cellPortStride
	return localCell{
		ID:                     cellID,
		Index:                  index,
		QueuePort:              config.QueuePort() + offset,
		QueueMetricsPort:       config.QueueMetricsPort() + offset,
		SecretsPort:            config.SecretsPort() + offset,
		SecretsMetricsPort:     config.SecretsMetricsPort() + offset,
		CellIngressPort:        config.CellIngressPort() + offset,
		CellIngressMetricsPort: config.CellIngressMetricsPort() + offset,
		WorkerMetricsPort:      config.WorkerMetricsPort() + offset,
		CellDB:                 cellDB,
		QueueDir:               localManagedQueueDir(cellID),
		SecretsDir:             localManagedSecretsDir(cellID),
		SecretsKeyFile:         localManagedSecretsKeyFile(cellID),
	}
}

func validateLocalCellPorts(cell localCell) error {
	for name, port := range map[string]int{
		"queue":                cell.QueuePort,
		"queue metrics":        cell.QueueMetricsPort,
		"secrets":              cell.SecretsPort,
		"secrets metrics":      cell.SecretsMetricsPort,
		"cell ingress":         cell.CellIngressPort,
		"cell ingress metrics": cell.CellIngressMetricsPort,
		"worker metrics":       cell.WorkerMetricsPort,
	} {
		if port <= 0 || port > 65535 {
			return fmt.Errorf("cell %q %s port %d is outside 1-65535", cell.ID, name, port)
		}
	}

	return nil
}

func localHost() string {
	if host := strings.TrimSpace(viper.GetString("host")); host != "" {
		return host
	}

	return "localhost"
}

func localConnectHost() string {
	host := localHost()
	switch host {
	case "", "0.0.0.0", "::":
		return "localhost"
	default:
		return host
	}
}

func safePathPart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "local"
	}

	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_")
	return replacer.Replace(s)
}

func logLevelEnvVar(binaryName, logLevel string) string {
	prefix := strings.ToUpper(strings.TrimPrefix(binaryName, "vectis-"))
	return fmt.Sprintf("VECTIS_%s_LOG_LEVEL=%s", prefix, logLevel)
}

func groupByStage(services []serviceStage) map[int][]serviceStage {
	byStage := make(map[int][]serviceStage)
	for _, svc := range services {
		byStage[svc.stage] = append(byStage[svc.stage], svc)
	}

	return byStage
}

func trackStarted(proc *exec.Cmd) {
	allStartedMu.Lock()
	defer allStartedMu.Unlock()
	allStarted = append(allStarted, proc)
}

func killAllStartedAndWait(logger interfaces.Logger) {
	allStartedMu.Lock()
	procs := make([]*exec.Cmd, len(allStarted))
	copy(procs, allStarted)
	allStartedMu.Unlock()

	signalProcessGroups(procs, syscall.SIGTERM)

	waitCh := make(chan struct{}, len(procs))
	for _, proc := range procs {
		go func(p *exec.Cmd) {
			_ = p.Wait()
			waitCh <- struct{}{}
		}(proc)
	}

	waitForProcessesOrKill(procs, waitCh, 5*time.Second)
}

func signalAllStarted(sig syscall.Signal) {
	allStartedMu.Lock()
	procs := make([]*exec.Cmd, len(allStarted))
	copy(procs, allStarted)
	allStartedMu.Unlock()

	signalProcessGroups(procs, sig)
}

func signalProcessGroups(procs []*exec.Cmd, sig syscall.Signal) {
	for _, proc := range procs {
		if proc.Process != nil {
			_ = syscall.Kill(-proc.Process.Pid, sig)
		}
	}
}

func waitForProcessesOrKill(procs []*exec.Cmd, waitCh <-chan struct{}, grace time.Duration) {
	timer := time.NewTimer(grace)
	defer timer.Stop()

	killed := false
	for received := 0; received < len(procs); {
		if !killed {
			select {
			case <-waitCh:
				received++
			case <-timer.C:
				killed = true
				for _, proc := range procs {
					if proc.Process != nil {
						_ = syscall.Kill(-proc.Process.Pid, syscall.SIGKILL)
					}
				}
			}
		} else {
			<-waitCh
			received++
		}
	}
}

func runVectis(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("cli")

	logLevel := viper.GetString("log_level")
	if !isValidLogLevel(logLevel) {
		logger.Fatal("invalid log level: %s (must be debug, info, warn, or error)", logLevel)
	}

	profile := localProfile()
	if profile != localProfileSimple && profile != localProfileHA {
		logger.Fatal("invalid profile: %s (must be simple or ha)", profile)
	}

	httpTLSMode := localHTTPSTLSMode()
	if !validLocalHTTPSTLSMode(httpTLSMode) {
		logger.Fatal("invalid http TLS mode: %s (must be auto, on, or off)", httpTLSMode)
	}

	setLoggerLevel(logger, logLevel)

	var tlsEnv []string
	var material *localpki.Material
	tlsDir := configuredLocalTLSDir(utils.DataHome())
	needsLocalPKI := !viper.GetBool("grpc_insecure") || httpTLSMode != localHTTPSTLSOff
	if needsLocalPKI {
		var err error
		material, err = localpki.Ensure(tlsDir)
		if err != nil {
			logger.Fatal("bootstrap local TLS: %v", err)
		}
	}

	if viper.GetBool("grpc_insecure") {
		logger.Info("gRPC plaintext mode (--grpc-insecure or VECTIS_LOCAL_GRPC_INSECURE=true)")

		_ = os.Setenv("VECTIS_GRPC_TLS_INSECURE", "true")
		localpki.ApplyPlaintextParentViper(viper.Set)
		tlsEnv = []string{"VECTIS_GRPC_TLS_INSECURE=true"}
	} else {
		tlsEnv = material.EnvVars()
		material.ApplyParentViper(viper.Set)
		_ = os.Setenv("VECTIS_GRPC_TLS_INSECURE", "false")
		_ = os.Setenv("VECTIS_GRPC_TLS_CA_FILE", material.CAFile)
		_ = os.Setenv("VECTIS_GRPC_TLS_CERT_FILE", material.ServerCert)
		_ = os.Setenv("VECTIS_GRPC_TLS_KEY_FILE", material.ServerKey)

		logger.Info("Bootstrapped gRPC TLS for local stack (material under %s)", tlsDir)
	}
	browserTLS := localBrowserTLS(material, httpTLSMode, logger)
	tlsEnv = append(tlsEnv, browserTLS.Env...)

	embeddedSPIFFE, err := startEmbeddedLocalSPIFFE(logger)
	if err != nil {
		logger.Fatal("%v", err)
	}

	if embeddedSPIFFE.Authority != nil {
		defer embeddedSPIFFE.Authority.Stop()
	}

	if embeddedSPIFFE.Enabled {
		logger.Info("Embedded local SPIFFE authority is running (registration socket %s, workload socket %s)", embeddedSPIFFE.ServerSocket, embeddedSPIFFE.AgentSocket)
	} else if viper.GetBool("grpc_insecure") {
		logger.Warn("Skipping embedded local SPIFFE authority because --grpc-insecure is enabled")
	}

	spiffeCfg, err := localSPIFFE(tlsDir, material)
	if err != nil {
		logger.Fatal("%v", err)
	}

	if spiffeCfg.Enabled {
		tlsEnv = append(tlsEnv, spiffeCfg.Env...)
		viper.Set("grpc_tls.client_ca_file", spiffeCfg.ClientCABundleFile)
		logger.Info("Enabled local SPIFFE execution identity using combined client CA bundle %s", spiffeCfg.ClientCABundleFile)
	}

	topology, err := buildLocalTopology()
	if err != nil {
		logger.Fatal("%v", err)
	}

	if profile == localProfileHA && topology.multiCell() {
		logger.Fatal("profile %q currently supports the default local execution cell only; use profile %q with --cell for multi-cell routing tests", localProfileHA, localProfileSimple)
	}

	if localSecretsEnabled() {
		if err := ensureLocalSecretsKeys(topology); err != nil {
			logger.Fatal("initialize local secrets keys: %v", err)
		}
	} else {
		logger.Warn("Skipping local secrets service because --grpc-insecure disables execution SVID client certificates")
	}

	if database.EffectiveDBDriver() == "sqlite3" {
		seen := map[string]bool{}
		dbPaths := []string{topology.GlobalDB}
		for _, cell := range topology.Cells {
			dbPaths = append(dbPaths, cell.CellDB)
		}

		for _, dbPath := range dbPaths {
			if seen[dbPath] {
				continue
			}

			seen[dbPath] = true

			logger.Info("Migrating SQLite database: %s", dbPath)
			if err := database.Migrate(dbPath); err != nil {
				logger.Fatal("database migrate failed: %v", err)
			}
		}
	}

	if topology.multiCell() {
		logger.Info("Using %d local cells: %s", len(topology.Cells), strings.Join(localCellIDs(topology.Cells), ", "))
	}

	if len(topology.Cells) > 0 && topology.GlobalDB != topology.Cells[0].CellDB {
		logger.Info("Using split local databases: global=%s default_cell=%s", topology.GlobalDB, topology.Cells[0].CellDB)
	} else {
		logger.Info("Using shared local database: %s", topology.GlobalDB)
	}

	services := localServices(logger, topology)

	localAPIEnv, err := apiEnv()
	if err != nil {
		logger.Fatal("%v", err)
	}

	tlsEnv = append(tlsEnv, localDatabaseEnv(topology)...)
	tlsEnv = append(tlsEnv, localAPIEnv...)
	tlsEnv = append(tlsEnv, localCellIngressEndpointEnv(topology.Cells)...)
	tlsEnv = append(tlsEnv, localCatalogCellDatabaseEnv(topology.Cells)...)
	tlsEnv = append(tlsEnv, docsEnv()...)

	logger.Info("Starting vectis-local profile: %s", profile)
	logger.Info("API will be available at %s://%s:%d", browserTLS.Scheme, localHost(), config.APIEffectiveListenPort())

	for _, cell := range topology.Cells {
		logger.Info("Cell %s ingress will be available at %s://%s:%d", cell.ID, localCellIngressEndpointScheme(), localHost(), cell.CellIngressPort)
	}

	if profile == localProfileHA {
		logger.Info("Additional HA API replica will be available at %s://%s:%d", browserTLS.Scheme, localHost(), 8180)
	}

	if hasService(services, "vectis-docs") {
		logger.Info("Docs will be available at %s://%s:%d", browserTLS.Scheme, localHost(), viper.GetInt("docs_port"))
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		shuttingDownMu.Lock()
		shuttingDown = true
		shuttingDownMu.Unlock()

		logger.Info("Received signal (%s), shutting down...", sig.String())
		killAllStartedAndWait(logger)
		os.Exit(0)
	}()

	byStage := groupByStage(services)

	stages := make([]int, 0, len(byStage))
	for stage := range byStage {
		stages = append(stages, stage)
	}
	sort.Ints(stages)

	for _, stage := range stages {
		svcs := byStage[stage]
		logger.Info("Starting stage %d: %v", stage, serviceNames(svcs))

		var wg sync.WaitGroup
		errCh := make(chan error, len(svcs))

		for _, svc := range svcs {
			wg.Add(1)
			go func(svc serviceStage) {
				defer wg.Done()

				proc, err := startService(logger, svc, logLevel, tlsEnv)
				if err != nil {
					errCh <- err
					return
				}

				trackStarted(proc)
				trackedMu.Lock()
				tracked = append(tracked, trackedCmd{cmd: proc, binary: svc.binary, name: svc.label()})
				trackedMu.Unlock()

				if svc.checkHealth {
					port := svc.portFn()
					logger.Info("Waiting for %s to be healthy (localhost:%d)...", svc.label(), port)
					if err := waitForHealthy(port, svc.healthName, healthCheckTimeout); err != nil {
						errCh <- err
						return
					}

					logger.Info("%s is healthy", svc.label())
				}
			}(svc)
		}

		wg.Wait()
		close(errCh)

		var firstErr error
		for err := range errCh {
			if firstErr == nil {
				firstErr = err
			}
		}

		if firstErr != nil {
			killAllStartedAndWait(logger)
			logger.Fatal("%v", firstErr)
		}

		logger.Info("Stage %d started successfully", stage)
	}

	trackedMu.Lock()
	toWait := make([]trackedCmd, len(tracked))
	copy(toWait, tracked)
	trackedMu.Unlock()

	exitCh := make(chan struct {
		name string
		err  error
	}, len(toWait))

	for _, t := range toWait {
		go func(t trackedCmd) {
			exitCh <- struct {
				name string
				err  error
			}{t.name, t.cmd.Wait()}
		}(t)
	}

	for range toWait {
		ex := <-exitCh
		if ex.err == nil {
			continue
		}

		shuttingDownMu.Lock()
		ok := shuttingDown
		shuttingDownMu.Unlock()

		if ok {
			return
		}

		signalAllStarted(syscall.SIGTERM)
		time.Sleep(2 * time.Second)
		signalAllStarted(syscall.SIGKILL)
		logger.Fatal("%s exited: %v", ex.name, ex.err)
	}
}

func serviceNames(svcs []serviceStage) []string {
	names := make([]string, len(svcs))
	for i, svc := range svcs {
		names[i] = svc.label()
	}

	return names
}

func localCellIDs(cells []localCell) []string {
	ids := make([]string, len(cells))
	for i, cell := range cells {
		ids[i] = cell.ID
	}

	return ids
}

func hasService(svcs []serviceStage, binary string) bool {
	for _, svc := range svcs {
		if svc.binary == binary {
			return true
		}
	}

	return false
}

func runLocalInit(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("cli")
	setLoggerLevel(logger, viper.GetString("log_level"))

	tlsDir := configuredLocalTLSDir(utils.DataHome())
	material, err := localpki.Ensure(tlsDir)
	if err != nil {
		logger.Fatal("initialize local TLS material: %v", err)
	}

	logger.Info("Local TLS material ready under %s", tlsDir)
	logger.Info("Local CA certificate: %s", material.CAFile)
	logger.Info("Local server certificate: %s", material.ServerCert)

	trusted, err := material.ServerTrustedBySystem()
	if err != nil {
		logger.Warn("Could not verify local CA trust: %v", err)
		return
	}

	if trusted {
		logger.Info("Local CA is already trusted by the system store")
		return
	}

	logger.Info("To trust the local CA, run with elevated privileges: vectis-local --tls-dir %q install-cert", tlsDir)
}

func runLocalInstallCert(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("cli")
	setLoggerLevel(logger, viper.GetString("log_level"))

	tlsDir := configuredLocalTLSDir(defaultCertInstallDataHome())
	material, err := localpki.Load(tlsDir)
	if err != nil {
		logger.Fatal("%v", err)
	}

	trusted, err := material.ServerTrustedBySystem()
	if err != nil {
		logger.Warn("Could not verify local CA trust before install: %v", err)
	}

	if trusted {
		logger.Info("Local CA is already trusted by the system store")
		return
	}

	if err := platform.InstallCertificateAuthority(cmd.Context(), material.CAFile); err != nil {
		logger.Fatal("install local CA into system trust store: %v", err)
	}

	trusted, err = material.ServerTrustedBySystem()
	if err != nil {
		logger.Warn("Installed local CA, but trust verification failed: %v", err)
		return
	}

	if !trusted {
		logger.Warn("Installed local CA, but the current process cannot verify it in the system trust store yet")
		return
	}

	logger.Info("Local CA installed and trusted")
}

var rootCmd = &cobra.Command{
	Use:   "vectis-local",
	Short: "Run Vectis services locally for development",
	Long: `Vectis Local runs all Vectis services locally for development and testing.

It starts the registry, queue, log service, artifact service, orchestrator,
worker-core, secrets service, cell ingress, worker, cron, reconciler, catalog,
API server, and docs site as child processes.

By default it bootstraps a dev CA and TLS certificates (under the XDG data directory)
and sets VECTIS_GRPC_TLS_* for child processes so internal gRPC and cell ingress
HTTP use TLS/mTLS. It also uses HTTPS for the local API and docs when the
generated CA is trusted by the system store, or when --http-tls=on is set. Use
--grpc-insecure or VECTIS_LOCAL_GRPC_INSECURE=true for plaintext internal gRPC
and loopback cell ingress HTTP; plaintext mode skips vectis-secrets because
execution secret resolution requires a verified SVID client certificate.

For local end-to-end secret resolution with SPIFFE identities, vectis-local
starts its embedded development authority for the current process user whenever
local gRPC TLS is enabled, and starts vectis-secrets with encryptedfs enabled.
This uses the same SPIFFE Workload API and Entry API contracts as the
worker, but runs through the bundled vectis-spiffe authority so the local demo
does not require external identity binaries. Identity material stays behind Unix
sockets while vectis-local combines the local Vectis CA and SPIFFE bundle for
client certificate verification.

Use "vectis-local init" to create or renew the local TLS material without
privileges. Use "vectis-local install-cert" with elevated privileges only when
you want to install that generated CA into the system trust store; that command
does not start services or perform any other setup.

The docs site is served from the vectis-docs binary on port 8088 by default.
If vectis-docs was not built, vectis-local logs a warning and continues without
local docs. Use --host=0.0.0.0 to expose the local API and docs outside the
development machine, or --docs=false to skip docs explicitly during local
development.

Use --cell repeatedly to add extra local execution cells over the default cell
from VECTIS_CELL_ID. Extra cells are intended for local multi-cell routing tests
and use per-cell SQLite databases, queues, secrets roots, ingress endpoints, and workers.

Use --config-as-code with one or more --source-repository repository_id=checkout_path
flags to start a local config-as-code instance without hand-authoring the JSON
source repository environment.

Use --profile=ha to start a local multi-instance exercise cell with multiple
registries, queue shards, log shards, artifact shards, API replicas, worker-core,
a secrets service, workers, cron instances, and reconcilers.`,
	Run: runVectis,
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize local TLS material without changing system trust",
	Long: `Create or renew vectis-local TLS material under the local data directory.

This command is intentionally unprivileged. It writes only the local CA, server
certificate, and private keys used by vectis-local. Run install-cert separately
with elevated privileges when you want to trust the generated CA system-wide.`,
	Run: runLocalInit,
}

var installCertCmd = &cobra.Command{
	Use:   "install-cert",
	Short: "Install the initialized vectis-local CA into the system trust store",
	Long: `Install the vectis-local CA certificate into the operating system trust store.

This command does not generate TLS material, migrate databases, or start any
Vectis services. Run vectis-local init as an unprivileged user first, then run
this command with elevated privileges if your OS requires them.`,
	Run: runLocalInstallCert,
}

func init() {
	cli.ConfigureVersion(rootCmd)

	rootCmd.PersistentFlags().String("log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().String("profile", localProfileSimple, "Local deployment profile: simple or ha")
	rootCmd.PersistentFlags().Bool("grpc-insecure", false, "Use plaintext gRPC instead of bootstrapped local TLS")
	rootCmd.PersistentFlags().String("http-tls", localHTTPSTLSAuto, "Local API/docs HTTPS mode: auto, on, or off")
	rootCmd.PersistentFlags().String("tls-dir", "", "Directory for vectis-local generated TLS material")
	rootCmd.PersistentFlags().String("host", "localhost", "Host/IP for the local API and docs sites to bind")
	rootCmd.PersistentFlags().Bool("docs", true, "Start the local docs site")
	rootCmd.PersistentFlags().Int("docs-port", 8088, "HTTP port for the local docs site")
	rootCmd.PersistentFlags().String("docs-dir", "", "Directory containing a docs build to serve instead of embedded docs")
	rootCmd.PersistentFlags().Bool("config-as-code", false, "Configure the local API server for config-as-code reusable jobs")
	rootCmd.PersistentFlags().StringArray("source-repository", nil, "Declare a local source repository as repository_id=checkout_path; may be repeated")
	rootCmd.PersistentFlags().StringArray("cell", nil, "Additional local execution cell ID to start; may be repeated")
	rootCmd.PersistentFlags().String("spiffe-dir", "", "Directory for local vectis-spiffe authority data")
	rootCmd.PersistentFlags().String("spiffe-runtime-dir", "", "Directory for local vectis-spiffe authority Unix sockets")
	rootCmd.PersistentFlags().String("spiffe-trust-domain", "", "SPIFFE trust domain for local execution IDs; defaults to vectis.internal")
	rootCmd.PersistentFlags().String("spiffe-path-template", "", "Optional execution SPIFFE path template for local identity mode")
	rootCmd.PersistentFlags().String("spiffe-parent-id", "", "Parent SPIFFE ID for worker-created execution registration entries")
	rootCmd.PersistentFlags().StringArray("spiffe-selector", nil, "Workload selector for worker-created execution registrations; may be repeated")
	rootCmd.PersistentFlags().String("spiffe-fetch-timeout", "", "Optional worker Workload API fetch timeout")
	rootCmd.PersistentFlags().String("spiffe-x509-svid-ttl", "", "Optional X.509-SVID TTL for worker-created registration entries")
	rootCmd.PersistentFlags().String("spiffe-registration-min-ttl", "", "Optional minimum lifetime for worker-created registration entries")
	rootCmd.PersistentFlags().String("spiffe-registration-max-ttl", "", "Optional maximum lifetime for worker-created registration entries")

	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("profile", rootCmd.PersistentFlags().Lookup("profile"))
	_ = viper.BindPFlag("grpc_insecure", rootCmd.PersistentFlags().Lookup("grpc-insecure"))
	_ = viper.BindPFlag("http_tls", rootCmd.PersistentFlags().Lookup("http-tls"))
	_ = viper.BindPFlag("tls_dir", rootCmd.PersistentFlags().Lookup("tls-dir"))
	_ = viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("docs_enabled", rootCmd.PersistentFlags().Lookup("docs"))
	_ = viper.BindPFlag("docs_port", rootCmd.PersistentFlags().Lookup("docs-port"))
	_ = viper.BindPFlag("docs_dir", rootCmd.PersistentFlags().Lookup("docs-dir"))
	_ = viper.BindPFlag("config_as_code", rootCmd.PersistentFlags().Lookup("config-as-code"))
	_ = viper.BindPFlag("source_repositories", rootCmd.PersistentFlags().Lookup("source-repository"))
	_ = viper.BindPFlag("cells", rootCmd.PersistentFlags().Lookup("cell"))
	_ = viper.BindPFlag("spiffe_dir", rootCmd.PersistentFlags().Lookup("spiffe-dir"))
	_ = viper.BindPFlag("spiffe_runtime_dir", rootCmd.PersistentFlags().Lookup("spiffe-runtime-dir"))
	_ = viper.BindPFlag("spiffe_trust_domain", rootCmd.PersistentFlags().Lookup("spiffe-trust-domain"))
	_ = viper.BindPFlag("spiffe_path_template", rootCmd.PersistentFlags().Lookup("spiffe-path-template"))
	_ = viper.BindPFlag("spiffe_parent_id", rootCmd.PersistentFlags().Lookup("spiffe-parent-id"))
	_ = viper.BindPFlag("spiffe_selectors", rootCmd.PersistentFlags().Lookup("spiffe-selector"))
	_ = viper.BindPFlag("spiffe_fetch_timeout", rootCmd.PersistentFlags().Lookup("spiffe-fetch-timeout"))
	_ = viper.BindPFlag("spiffe_x509_svid_ttl", rootCmd.PersistentFlags().Lookup("spiffe-x509-svid-ttl"))
	_ = viper.BindPFlag("spiffe_registration_min_ttl", rootCmd.PersistentFlags().Lookup("spiffe-registration-min-ttl"))
	_ = viper.BindPFlag("spiffe_registration_max_ttl", rootCmd.PersistentFlags().Lookup("spiffe-registration-max-ttl"))
	_ = viper.BindEnv("grpc_insecure", "VECTIS_LOCAL_GRPC_INSECURE")
	_ = viper.BindEnv("http_tls", "VECTIS_LOCAL_HTTP_TLS")
	_ = viper.BindEnv("tls_dir", "VECTIS_LOCAL_TLS_DIR")
	_ = viper.BindEnv("profile", "VECTIS_LOCAL_PROFILE")
	_ = viper.BindEnv("host", "VECTIS_LOCAL_HOST")
	_ = viper.BindEnv("docs_enabled", "VECTIS_LOCAL_DOCS_ENABLED")
	_ = viper.BindEnv("docs_port", "VECTIS_LOCAL_DOCS_PORT")
	_ = viper.BindEnv("docs_dir", "VECTIS_LOCAL_DOCS_DIR")
	_ = viper.BindEnv("config_as_code", "VECTIS_LOCAL_CONFIG_AS_CODE")
	_ = viper.BindEnv("source_repositories", "VECTIS_LOCAL_SOURCE_REPOSITORIES")
	_ = viper.BindEnv("cells", "VECTIS_LOCAL_CELLS")
	_ = viper.BindEnv("spiffe_dir", "VECTIS_LOCAL_SPIFFE_DIR")
	_ = viper.BindEnv("spiffe_runtime_dir", "VECTIS_LOCAL_SPIFFE_RUNTIME_DIR")
	_ = viper.BindEnv("spiffe_trust_domain", "VECTIS_LOCAL_SPIFFE_TRUST_DOMAIN")
	_ = viper.BindEnv("spiffe_path_template", "VECTIS_LOCAL_SPIFFE_PATH_TEMPLATE")
	_ = viper.BindEnv("spiffe_parent_id", "VECTIS_LOCAL_SPIFFE_PARENT_ID")
	_ = viper.BindEnv("spiffe_selectors", "VECTIS_LOCAL_SPIFFE_SELECTORS")
	_ = viper.BindEnv("spiffe_fetch_timeout", "VECTIS_LOCAL_SPIFFE_FETCH_TIMEOUT")
	_ = viper.BindEnv("spiffe_x509_svid_ttl", "VECTIS_LOCAL_SPIFFE_X509_SVID_TTL")
	_ = viper.BindEnv("spiffe_registration_min_ttl", "VECTIS_LOCAL_SPIFFE_REGISTRATION_MIN_TTL")
	_ = viper.BindEnv("spiffe_registration_max_ttl", "VECTIS_LOCAL_SPIFFE_REGISTRATION_MAX_TTL")

	viper.SetEnvPrefix("VECTIS_LOCAL")
	viper.AutomaticEnv()
	rootCmd.AddCommand(initCmd, installCertCmd)
}

func isValidLogLevel(level string) bool {
	_, err := interfaces.ParseLevel(level)
	return err == nil
}

func setLoggerLevel(logger interfaces.Logger, level string) {
	lvl, err := interfaces.ParseLevel(level)
	if err != nil {
		logger.Fatal("%v", err)
	}
	logger.SetLevel(lvl)
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
