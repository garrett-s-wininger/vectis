package main

import (
	"context"
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
	"vectis/internal/platform"
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
	CellIngressPort        int
	CellIngressMetricsPort int
	WorkerMetricsPort      int
	CellDB                 string
	QueueDir               string
}

type localTopology struct {
	GlobalDB string
	Cells    []localCell
}

var (
	orderedSingletonServices = []serviceStage{
		{binary: "vectis-registry", stage: 0, checkHealth: true, portFn: config.RegistryEffectiveListenPort, healthName: "registry"},
		{binary: "vectis-log", stage: 1, checkHealth: true, portFn: config.LogGRPCPort, healthName: "log"},
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
	healthCheckInterval = 50 * time.Millisecond
	healthCheckTimeout  = 10 * time.Second
	cellPortStride      = 100
	localProfileSimple  = "simple"
	localProfileHA      = "ha"
	localHTTPSTLSAuto   = "auto"
	localHTTPSTLSOn     = "on"
	localHTTPSTLSOff    = "off"
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
	services := make([]serviceStage, 0, len(orderedSingletonServices)+len(topology.Cells)*3)
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
		cell := cell
		services = append(services,
			serviceStage{
				binary:      "vectis-queue",
				name:        fmt.Sprintf("vectis-queue[%s]", cell.ID),
				stage:       1,
				checkHealth: true,
				portFn:      func() int { return cell.QueuePort },
				healthName:  "queue",
				env:         queueEnv(cell, topology.multiCell()),
			},
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

	for i := 0; i < 2; i++ {
		env := append([]string{}, registryEnv...)
		env = append(env,
			"VECTIS_CELL_ID="+cell.ID,
			database.EnvCellDatabaseDSN+"="+cell.CellDB,
			fmt.Sprintf("VECTIS_WORKER_METRICS_PORT=%d", cell.WorkerMetricsPort+(i*cellPortStride)),
			fmt.Sprintf("VECTIS_WORKER_CONTROL_PORT=%d", config.WorkerControlPort()+(i*cellPortStride)),
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

	for i := 0; i < 2; i++ {
		env := append([]string{}, registryEnv...)
		env = append(env, fmt.Sprintf("VECTIS_CRON_INSTANCE_ID=cron-%d", i+1))
		services = append(services, serviceStage{
			binary: "vectis-cron",
			name:   fmt.Sprintf("cron-%d", i+1),
			stage:  2,
			env:    env,
		})
	}

	for i := 0; i < 2; i++ {
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

func apiEnv() []string {
	return []string{"VECTIS_API_SERVER_HOST=" + localHost()}
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

func workerEnv(cell localCell, multiCell bool) []string {
	env := []string{
		"VECTIS_CELL_ID=" + cell.ID,
		database.EnvCellDatabaseDSN + "=" + cell.CellDB,
		fmt.Sprintf("VECTIS_WORKER_METRICS_PORT=%d", cell.WorkerMetricsPort),
		"VECTIS_WORKER_QUEUE_ADDRESS=" + localQueueAddress(cell),
	}

	if multiCell {
		env = append(env, "VECTIS_WORKER_CONTROL_MODE=ephemeral")
	}

	return env
}

func localQueueAddress(cell localCell) string {
	return net.JoinHostPort(localConnectHost(), fmt.Sprintf("%d", cell.QueuePort))
}

func localCellIngressEndpointSpecs(cells []localCell) []string {
	specs := make([]string, 0, len(cells))
	for _, cell := range cells {
		endpoint := "http://" + net.JoinHostPort(localConnectHost(), fmt.Sprintf("%d", cell.CellIngressPort))
		specs = append(specs, fmt.Sprintf("%s=%s", cell.ID, endpoint))
	}

	return specs
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
		for _, part := range strings.Split(value, ",") {
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

func newLocalCell(cellID string, index int, cellDB string) localCell {
	offset := index * cellPortStride
	return localCell{
		ID:                     cellID,
		Index:                  index,
		QueuePort:              config.QueuePort() + offset,
		QueueMetricsPort:       config.QueueMetricsPort() + offset,
		CellIngressPort:        config.CellIngressPort() + offset,
		CellIngressMetricsPort: config.CellIngressMetricsPort() + offset,
		WorkerMetricsPort:      config.WorkerMetricsPort() + offset,
		CellDB:                 cellDB,
		QueueDir:               localManagedQueueDir(cellID),
	}
}

func validateLocalCellPorts(cell localCell) error {
	for name, port := range map[string]int{
		"queue":                cell.QueuePort,
		"queue metrics":        cell.QueueMetricsPort,
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

	for _, proc := range procs {
		if proc.Process != nil {
			_ = syscall.Kill(-proc.Process.Pid, syscall.SIGTERM)
		}
	}

	waitCh := make(chan struct{}, len(procs))
	for _, proc := range procs {
		go func(p *exec.Cmd) {
			_ = p.Wait()
			waitCh <- struct{}{}
		}(proc)
	}

	timer := time.NewTimer(5 * time.Second)
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

	topology, err := buildLocalTopology()
	if err != nil {
		logger.Fatal("%v", err)
	}
	if profile == localProfileHA && topology.multiCell() {
		logger.Fatal("profile %q currently supports the default local execution cell only; use profile %q with --cell for multi-cell routing tests", localProfileHA, localProfileSimple)
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

	tlsEnv = append(tlsEnv, localDatabaseEnv(topology)...)
	tlsEnv = append(tlsEnv, apiEnv()...)
	tlsEnv = append(tlsEnv, localCellIngressEndpointEnv(topology.Cells)...)
	tlsEnv = append(tlsEnv, localCatalogCellDatabaseEnv(topology.Cells)...)
	tlsEnv = append(tlsEnv, docsEnv()...)

	logger.Info("Starting vectis-local profile: %s", profile)
	logger.Info("API will be available at %s://%s:%d", browserTLS.Scheme, localHost(), config.APIEffectiveListenPort())

	for _, cell := range topology.Cells {
		logger.Info("Cell %s ingress will be available at http://%s:%d", cell.ID, localHost(), cell.CellIngressPort)
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

It starts the registry, queue, log service, cell ingress, worker, cron,
reconciler, catalog, API server, and docs site as child processes.

By default it bootstraps a dev CA and TLS certificates (under the XDG data directory)
and sets VECTIS_GRPC_TLS_* for child processes so internal gRPC uses TLS. It also
uses HTTPS for the local API and docs when the generated CA is trusted by the
system store, or when --http-tls=on is set. Use --grpc-insecure or
VECTIS_LOCAL_GRPC_INSECURE=true for plaintext gRPC.

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
and use per-cell SQLite databases, queues, ingress endpoints, and workers.

Use --profile=ha to start a local multi-instance exercise cell with multiple
registries, queue shards, log shards, API replicas, workers, cron instances,
and reconcilers.`,
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
	rootCmd.PersistentFlags().StringArray("cell", nil, "Additional local execution cell ID to start; may be repeated")
	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("profile", rootCmd.PersistentFlags().Lookup("profile"))
	_ = viper.BindPFlag("grpc_insecure", rootCmd.PersistentFlags().Lookup("grpc-insecure"))
	_ = viper.BindPFlag("http_tls", rootCmd.PersistentFlags().Lookup("http-tls"))
	_ = viper.BindPFlag("tls_dir", rootCmd.PersistentFlags().Lookup("tls-dir"))
	_ = viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("docs_enabled", rootCmd.PersistentFlags().Lookup("docs"))
	_ = viper.BindPFlag("docs_port", rootCmd.PersistentFlags().Lookup("docs-port"))
	_ = viper.BindPFlag("docs_dir", rootCmd.PersistentFlags().Lookup("docs-dir"))
	_ = viper.BindPFlag("cells", rootCmd.PersistentFlags().Lookup("cell"))
	_ = viper.BindEnv("grpc_insecure", "VECTIS_LOCAL_GRPC_INSECURE")
	_ = viper.BindEnv("http_tls", "VECTIS_LOCAL_HTTP_TLS")
	_ = viper.BindEnv("tls_dir", "VECTIS_LOCAL_TLS_DIR")
	_ = viper.BindEnv("profile", "VECTIS_LOCAL_PROFILE")
	_ = viper.BindEnv("host", "VECTIS_LOCAL_HOST")
	_ = viper.BindEnv("docs_enabled", "VECTIS_LOCAL_DOCS_ENABLED")
	_ = viper.BindEnv("docs_port", "VECTIS_LOCAL_DOCS_PORT")
	_ = viper.BindEnv("docs_dir", "VECTIS_LOCAL_DOCS_DIR")
	_ = viper.BindEnv("cells", "VECTIS_LOCAL_CELLS")
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
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
