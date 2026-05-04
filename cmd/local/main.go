package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
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
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	_ "vectis/internal/dbdrivers"
)

type serviceStage struct {
	binary      string
	stage       int
	checkHealth bool
	portFn      func() int
	healthName  string
}

type trackedCmd struct {
	cmd    *exec.Cmd
	binary string
}

var (
	orderedServices = []serviceStage{
		{binary: "vectis-registry", stage: 0, checkHealth: true, portFn: config.RegistryEffectiveListenPort, healthName: "registry"},
		{binary: "vectis-queue", stage: 1, checkHealth: true, portFn: config.QueueEffectiveListenPort, healthName: "queue"},
		{binary: "vectis-log", stage: 1, checkHealth: true, portFn: config.LogGRPCPort, healthName: "log"},
		{binary: "vectis-worker", stage: 2, checkHealth: false},
		{binary: "vectis-cron", stage: 2, checkHealth: false},
		{binary: "vectis-reconciler", stage: 2, checkHealth: false},
		{binary: "vectis-api", stage: 2, checkHealth: false},
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

			client := healthgrpc.NewHealthClient(conn)
			resp, err := client.Check(ctx, &healthgrpc.HealthCheckRequest{Service: serviceName})
			conn.Close()

			if err == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING {
				return nil
			}
		}
	}
}

func startService(logger interfaces.Logger, svc serviceStage, logLevel string, tlsEnv []string) (*exec.Cmd, error) {
	path, err := supervisor.FindBinary(svc.binary)
	if err != nil {
		return nil, fmt.Errorf("cannot find %s: %w", svc.binary, err)
	}

	command := exec.Command(path)
	command.Stdin = nil
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	command.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	env := append([]string(nil), os.Environ()...)
	env = append(env, tlsEnv...)
	if logLevel != "" {
		env = append(env, logLevelEnvVar(svc.binary, logLevel))
	}
	command.Env = env

	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("failed to start %s: %w", svc.binary, err)
	}

	return command, nil
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

	if database.EffectiveDBDriver() == "sqlite3" {
		dbPath := database.GetDBPath()
		logger.Info("Migrating SQLite database: %s", dbPath)
		if err := database.Migrate(dbPath); err != nil {
			logger.Fatal("database migrate failed: %v", err)
		}
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

	byStage := groupByStage(orderedServices)

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
				tracked = append(tracked, trackedCmd{cmd: proc, binary: svc.binary})
				trackedMu.Unlock()

				if svc.checkHealth {
					port := svc.portFn()
					logger.Info("Waiting for %s to be healthy (localhost:%d)...", svc.binary, port)
					if err := waitForHealthy(port, svc.healthName, healthCheckTimeout); err != nil {
						errCh <- err
						return
					}

					logger.Info("%s is healthy", svc.binary)
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
		names[i] = svc.binary
	}

	return names
}

var rootCmd = &cobra.Command{
	Use:   "vectis-local",
	Short: "Run Vectis services locally for development",
	Long: `Vectis Local runs all Vectis services locally for development and testing.

It starts the registry, queue, worker, and API server as child processes.

By default it bootstraps a dev CA and TLS certificates (under the XDG data directory)
and sets VECTIS_GRPC_TLS_* for child processes so internal gRPC uses TLS. Use
--grpc-insecure or VECTIS_LOCAL_GRPC_INSECURE=true for plaintext gRPC.`,
	Run: runVectis,
}

func init() {
	cli.ConfigureVersion(rootCmd)

	rootCmd.PersistentFlags().String("log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().Bool("grpc-insecure", false, "Use plaintext gRPC instead of bootstrapped local TLS")
	_ = viper.BindPFlag("log_level", rootCmd.PersistentFlags().Lookup("log-level"))
	_ = viper.BindPFlag("grpc_insecure", rootCmd.PersistentFlags().Lookup("grpc-insecure"))
	_ = viper.BindEnv("grpc_insecure", "VECTIS_LOCAL_GRPC_INSECURE")
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
