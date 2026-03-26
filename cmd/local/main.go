package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"vectis/internal/config"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/supervisor"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	_ "github.com/mattn/go-sqlite3"
)

type serviceStage struct {
	binary      string
	stage       int
	checkHealth bool
	portFn      func() int
	healthName  string
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
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func startService(logger interfaces.Logger, svc serviceStage) (*exec.Cmd, error) {
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

	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("failed to start %s: %w", svc.binary, err)
	}

	return command, nil
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
	defer allStartedMu.Unlock()

	for _, proc := range allStarted {
		if proc.Process != nil {
			syscall.Kill(-proc.Process.Pid, syscall.SIGTERM)
		}
	}

	waitCh := make(chan struct{}, len(allStarted))
	for _, proc := range allStarted {
		go func(p *exec.Cmd) {
			p.Wait()
			waitCh <- struct{}{}
		}(proc)
	}

	timeout := time.After(5 * time.Second)
	for range allStarted {
		select {
		case <-waitCh:
		case <-timeout:
			for _, proc := range allStarted {
				if proc.Process != nil {
					syscall.Kill(-proc.Process.Pid, syscall.SIGKILL)
				}
			}

			for _, proc := range allStarted {
				proc.Wait()
			}

			return
		}
	}
}

func runVectis(cmd *cobra.Command, args []string) {
	logger := interfaces.NewLogger("cli")
	dbPath := database.GetDBPath()
	logger.Info("Migrating database: %s", dbPath)
	if err := database.Migrate(dbPath); err != nil {
		logger.Fatal("database migrate failed: %v", err)
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

	commands := make([]*exec.Cmd, 0, len(orderedServices))
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

				proc, err := startService(logger, svc)
				if err != nil {
					errCh <- err
					return
				}

				trackStarted(proc)
				commands = append(commands, proc)

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

	for i, c := range commands {
		if err := c.Wait(); err != nil {
			shuttingDownMu.Lock()
			ok := shuttingDown
			shuttingDownMu.Unlock()

			if ok {
				return
			}

			logger.Fatal("%s exited: %v", orderedServices[i].binary, err)
		}
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

It starts the registry, queue, worker, and API server as child processes.`,
	Run: runVectis,
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
