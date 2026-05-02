package main

import (
	"context"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/logforwarder"
	"vectis/internal/utils"
)

const (
	defaultSocketName = "log-forwarder.sock"
	defaultLockName   = "log-forwarder.lock"
)

func socketPath() string {
	return filepath.Join(utils.RuntimeDir(), defaultSocketName)
}

func lockPath() string {
	return filepath.Join(utils.RuntimeDir(), defaultLockName)
}

func runLogForwarder(cmd *cobra.Command, args []string) {
	ctx := cmd.Context()
	logger := interfaces.NewLogger("log-forwarder")
	cli.SetLogLevel(logger)

	sock := socketPath()
	lock := lockPath()

	if override := viper.GetString("socket"); override != "" {
		sock = override
	}

	if override := viper.GetString("lockfile"); override != "" {
		lock = override
	}

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonClientOnly); err != nil {
		logger.Fatal("%v", err)
	}

	config.StartGRPCTLSReloadLoop(ctx)

	logger.Info("Starting vectis-log-forwarder...")
	logger.Info("Socket: %s", sock)
	logger.Info("Lockfile: %s", lock)

	// Acquire lock
	lockFd, err := logforwarder.AcquireLock(lock)
	if err != nil {
		logger.Fatal("Failed to acquire lock: %v", err)
	}
	defer logforwarder.ReleaseLock(lockFd, lock)

	logger.Info("Lock acquired")

	// Resolve vectis-log gRPC client
	logClient, logCleanup, err := logforwarder.ResolveLogClient(ctx, logger)
	if err != nil {
		logger.Fatal("Failed to resolve log service: %v", err)
	}
	defer logCleanup()

	logger.Info("Connected to vectis-log")

	// Create Unix socket server
	bufferSize := viper.GetInt("buffer_size")
	if bufferSize <= 0 {
		bufferSize = 1024
	}

	server, err := logforwarder.NewSocketServer(sock, bufferSize)
	if err != nil {
		logger.Fatal("Failed to create socket server: %v", err)
	}
	defer server.Close()
	server.SetLogger(logger)

	logger.Info("Listening on Unix socket: %s", sock)

	// Create forwarder
	spoolDir := viper.GetString("spool_dir")
	if spoolDir == "" {
		dataHome := utils.DataHome()
		spoolDir = filepath.Join(dataHome, "vectis", "log-forwarder", "spool")
	}

	fwd := logforwarder.NewForwarder(
		server.Chunks(),
		logger,
		spoolDir,
		viper.GetInt("batch_size"),
		viper.GetInt("max_chunks_per_sec"),
	)
	fwd.SetLogClient(logClient)

	// Run server and forwarder concurrently
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	wg.Go(func() {
		if err := server.Serve(); err != nil {
			logger.Error("Socket server error: %v", err)
		}
	})

	wg.Go(func() {
		fwd.Run(ctx)
	})

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-ctx.Done():
		logger.Info("Context cancelled; shutting down...")
	case sig := <-sigCh:
		logger.Info("Received signal %s; shutting down...", sig)
	}

	server.Close()
	fwd.Shutdown()
	cancel()

	// Wait for goroutines to finish, with a timeout backstop.
	shutdownTimeout := viper.GetDuration("shutdown_timeout")
	if shutdownTimeout <= 0 {
		shutdownTimeout = 10 * time.Second
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Shutdown complete")
	case <-time.After(shutdownTimeout):
		logger.Warn("Shutdown timeout exceeded; exiting with pending work")
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-log-forwarder",
	Short: "Box-level log forwarding daemon for Vectis workers",
	Long: `The vectis-log-forwarder receives log chunks from local workers
over a Unix domain socket, batches them, and forwards to vectis-log
via gRPC.  When vectis-log is unavailable it writes to a local spool
directory for later retry.`,
	Run: runLogForwarder,
}

func init() {
	v := viper.GetViper()
	v.SetDefault("socket", "")
	v.SetDefault("lockfile", "")
	v.SetDefault("spool_dir", "")
	v.SetDefault("batch_size", 100)
	v.SetDefault("max_chunks_per_sec", 10000)
	v.SetDefault("buffer_size", 1024)
	v.SetDefault("shutdown_timeout", "10s")

	rootCmd.PersistentFlags().String("socket", "", "Unix socket path (default: $XDG_RUNTIME_DIR/vectis/log-forwarder.sock)")
	rootCmd.PersistentFlags().String("lockfile", "", "Lockfile path (default: $XDG_RUNTIME_DIR/vectis/log-forwarder.lock)")
	rootCmd.PersistentFlags().String("spool-dir", "", "Spool directory (default: $XDG_DATA_HOME/vectis/log-forwarder/spool)")
	rootCmd.PersistentFlags().Int("batch-size", 100, "Max chunks per batch")
	rootCmd.PersistentFlags().Int("max-chunks-per-sec", 10000, "Rate limit for forwarding")
	rootCmd.PersistentFlags().Int("buffer-size", 1024, "Unix socket receive buffer size")
	rootCmd.PersistentFlags().Duration("shutdown-timeout", 10*time.Second, "Graceful shutdown timeout")

	_ = v.BindPFlag("socket", rootCmd.PersistentFlags().Lookup("socket"))
	_ = v.BindPFlag("lockfile", rootCmd.PersistentFlags().Lookup("lockfile"))
	_ = v.BindPFlag("spool_dir", rootCmd.PersistentFlags().Lookup("spool-dir"))
	_ = v.BindPFlag("batch_size", rootCmd.PersistentFlags().Lookup("batch-size"))
	_ = v.BindPFlag("max_chunks_per_sec", rootCmd.PersistentFlags().Lookup("max-chunks-per-sec"))
	_ = v.BindPFlag("buffer_size", rootCmd.PersistentFlags().Lookup("buffer-size"))
	_ = v.BindPFlag("shutdown_timeout", rootCmd.PersistentFlags().Lookup("shutdown-timeout"))

	viper.SetEnvPrefix("VECTIS_LOG_FORWARDER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
