package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
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

func (svc serviceStage) label() string {
	if svc.name != "" {
		return svc.name
	}

	return svc.binary
}

func localServices(logger interfaces.Logger, topology localTopology) []serviceStage {
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

	setLoggerLevel(logger, logLevel)

	var tlsEnv []string
	if viper.GetBool("grpc_insecure") {
		logger.Info("gRPC plaintext mode (--grpc-insecure or VECTIS_LOCAL_GRPC_INSECURE=true)")

		_ = os.Setenv("VECTIS_GRPC_TLS_INSECURE", "true")
		localpki.ApplyPlaintextParentViper(viper.Set)
		tlsEnv = []string{"VECTIS_GRPC_TLS_INSECURE=true"}
	} else {
		tlsDir := localpki.EnsureDir(utils.DataHome())
		material, err := localpki.Ensure(tlsDir)
		if err != nil {
			logger.Fatal("bootstrap local gRPC TLS: %v", err)
		}

		tlsEnv = material.EnvVars()
		material.ApplyParentViper(viper.Set)
		_ = os.Setenv("VECTIS_GRPC_TLS_INSECURE", "false")
		_ = os.Setenv("VECTIS_GRPC_TLS_CA_FILE", material.CAFile)
		_ = os.Setenv("VECTIS_GRPC_TLS_CERT_FILE", material.ServerCert)
		_ = os.Setenv("VECTIS_GRPC_TLS_KEY_FILE", material.ServerKey)

		logger.Info("Bootstrapped gRPC TLS for local stack (material under %s)", tlsDir)
	}

	topology, err := buildLocalTopology()
	if err != nil {
		logger.Fatal("%v", err)
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
	logger.Info("API will be available at http://%s:%d", localHost(), config.APIEffectiveListenPort())
	for _, cell := range topology.Cells {
		logger.Info("Cell %s ingress will be available at http://%s:%d", cell.ID, localHost(), cell.CellIngressPort)
	}

	if hasService(services, "vectis-docs") {
		logger.Info("Docs will be available at http://%s:%d", localHost(), viper.GetInt("docs_port"))
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
				tracked = append(tracked, trackedCmd{cmd: proc, binary: svc.label()})
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
		binary string
		err    error
	}, len(toWait))

	for _, t := range toWait {
		go func() {
			exitCh <- struct {
				binary string
				err    error
			}{t.binary, t.cmd.Wait()}
		}()
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

		logger.Fatal("%s exited: %v", ex.binary, ex.err)
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

var rootCmd = &cobra.Command{
	Use:   "vectis-local",
	Short: "Run Vectis services locally for development",
	Long: `Vectis Local runs all Vectis services locally for development and testing.

It starts the registry, queue, log service, cell ingress, worker, cron,
reconciler, catalog, API server, and docs site as child processes.

By default it bootstraps a dev CA and TLS certificates (under the XDG data directory)
and sets VECTIS_GRPC_TLS_* for child processes so internal gRPC uses TLS. Use
--grpc-insecure or VECTIS_LOCAL_GRPC_INSECURE=true for plaintext gRPC.

The docs site is served from the vectis-docs binary on port 8088 by default.
If vectis-docs was not built, vectis-local logs a warning and continues without
local docs. Use --host=0.0.0.0 to expose the local API and docs outside the
development machine, or --docs=false to skip docs explicitly during local
development.

Use --cell repeatedly to add extra local execution cells over the default cell
from VECTIS_CELL_ID. Extra cells are intended for local multi-cell routing tests
and use per-cell SQLite databases, queues, ingress endpoints, and workers.`,
	Run: runVectis,
}

func init() {
	cli.ConfigureVersion(rootCmd)

	rootCmd.PersistentFlags().String("log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().Bool("grpc-insecure", false, "Use plaintext gRPC instead of bootstrapped local TLS")
	rootCmd.PersistentFlags().String("host", "localhost", "Host/IP for the local API and docs sites to bind")
	rootCmd.PersistentFlags().Bool("docs", true, "Start the local docs site")
	rootCmd.PersistentFlags().Int("docs-port", 8088, "HTTP port for the local docs site")
	rootCmd.PersistentFlags().String("docs-dir", "", "Directory containing a docs build to serve instead of embedded docs")
	rootCmd.PersistentFlags().StringArray("cell", nil, "Additional local execution cell ID to start; may be repeated")
	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("grpc_insecure", rootCmd.PersistentFlags().Lookup("grpc-insecure"))
	_ = viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	_ = viper.BindPFlag("docs_enabled", rootCmd.PersistentFlags().Lookup("docs"))
	_ = viper.BindPFlag("docs_port", rootCmd.PersistentFlags().Lookup("docs-port"))
	_ = viper.BindPFlag("docs_dir", rootCmd.PersistentFlags().Lookup("docs-dir"))
	_ = viper.BindPFlag("cells", rootCmd.PersistentFlags().Lookup("cell"))
	_ = viper.BindEnv("grpc_insecure", "VECTIS_LOCAL_GRPC_INSECURE")
	_ = viper.BindEnv("host", "VECTIS_LOCAL_HOST")
	_ = viper.BindEnv("docs_enabled", "VECTIS_LOCAL_DOCS_ENABLED")
	_ = viper.BindEnv("docs_port", "VECTIS_LOCAL_DOCS_PORT")
	_ = viper.BindEnv("docs_dir", "VECTIS_LOCAL_DOCS_DIR")
	_ = viper.BindEnv("cells", "VECTIS_LOCAL_CELLS")
	viper.SetEnvPrefix("VECTIS_LOCAL")
	viper.AutomaticEnv()
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
