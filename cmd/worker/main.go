package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/job"
	"vectis/internal/multidial"
	"vectis/internal/observability"
	"vectis/internal/queueclient"
	"vectis/internal/registry"
	"vectis/internal/runpolicy"
	"vectis/internal/utils"

	_ "vectis/internal/dbdrivers"
)

const (
	maxFailureReasonLen = 4096
	dequeueBackoffBase  = 500 * time.Millisecond
	dequeueBackoffMax   = 30 * time.Second
	longPollTimeout     = 30 * time.Second
	ackMaxAttempts      = 4
	ackBackoffBase      = 150 * time.Millisecond
	ackBackoffMax       = 2 * time.Second
	finalizeMaxAttempts = 4
	finalizeBackoffBase = 150 * time.Millisecond
	finalizeBackoffMax  = 2 * time.Second
)

func runWorker(cmd *cobra.Command, args []string) {
	shutdownCtx := cmd.Context()
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}

	runCtx := context.Background()
	logger := interfaces.NewLogger("worker")
	cli.SetLogLevel(logger)

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonClientOnly); err != nil {
		logger.Fatal("%v", err)
	}

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("%v", err)
	}

	config.StartGRPCTLSReloadLoop(shutdownCtx)
	config.StartMetricsTLSReloadLoop(shutdownCtx)

	workerID := uuid.New().String()
	logger.Info("Worker ID: %s", workerID)

	dbPath := database.GetDBPath()
	logger.Info("Using database: %s", dbPath)
	db, err := database.OpenDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to open database: %v", err)
	}
	defer db.Close()

	if err := database.WaitForMigrations(db, logger); err != nil {
		logger.Fatal("database wait for migrations failed: %v", err)
	}

	metricsHandler, shutdownMetrics, err := observability.InitServiceMetrics(shutdownCtx, "vectis-worker")
	if err != nil {
		logger.Fatal("Failed to initialize metrics: %v", err)
	}

	if err := observability.RegisterSQLDBPoolMetrics(db); err != nil {
		logger.Fatal("Failed to register DB pool metrics: %v", err)
	}

	workerMetrics, err := observability.NewWorkerMetrics()
	if err != nil {
		logger.Fatal("Failed to register worker metrics: %v", err)
	}

	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := shutdownMetrics(shutCtx); err != nil {
			logger.Warn("Metrics shutdown: %v", err)
		}
	}()

	metricsPort := config.WorkerMetricsEffectiveListenPort()
	metricsAddr := fmt.Sprintf(":%d", metricsPort)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", metricsHandler)
	metricsSrv := &http.Server{
		Addr:    metricsAddr,
		Handler: metricsMux,
	}

	metricsLn, err := net.Listen("tcp", metricsAddr)
	if err != nil {
		logger.Fatal("Failed to listen for metrics: %v", err)
	}

	metricsLn, err = config.MetricsHTTPSListener(metricsLn)
	if err != nil {
		logger.Fatal("metrics tls: %v", err)
	}

	go func() {
		if err := metricsSrv.Serve(metricsLn); err != nil && err != http.ErrServerClosed {
			logger.Error("Metrics server: %v", err)
		}
	}()

	defer func() {
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsSrv.Shutdown(shutCtx); err != nil {
			logger.Warn("Metrics HTTP shutdown: %v", err)
		}
	}()

	if !config.MetricsTLSInsecure() {
		logger.Info("Worker metrics listening on %s (HTTPS /metrics)", metricsAddr)
	} else {
		logger.Info("Worker metrics listening on %s (/metrics)", metricsAddr)
	}

	dial := func(ctx context.Context) (interfaces.QueueClient, interfaces.LogClient, func(), error) {
		q, l, cleanup, err := multidial.DialQueueAndLog(ctx, logger)
		return q, l, cleanup, err
	}

	clients, err := queueclient.NewManagingWorkerDial(shutdownCtx, logger, dial)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("Worker graceful shutdown before connecting to queue or log service")
			return
		}

		logger.Fatal("Failed to connect to queue or log service: %v", err)
	}
	defer func() { _ = clients.Close() }()

	logClient := interfaces.LogClient(clients)

	// Prefer the local log-forwarder Unix socket when available.
	// The PreferForwarderLogClient dynamically checks the socket before
	// each StreamLogs, so if the forwarder crashes the worker falls back
	// to direct gRPC automatically.
	forwarderSocket := forwarderSocketPath()
	logClient = interfaces.NewPreferForwarderLogClient(forwarderSocket, logClient)

	w := &worker{
		ctx:           shutdownCtx,
		runCtx:        runCtx,
		logger:        logger,
		workerID:      workerID,
		clock:         interfaces.SystemClock{},
		renewInterval: dal.DefaultRenewInterval,
		queue:         clients,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         dal.NewSQLRepositories(db).Runs(),
		metrics:       workerMetrics,
		cancelCh:      make(chan string, 1),
	}

	// Start worker control server for remote cancellation.
	controlListener, controlAddr, err := startControlListener(logger)
	if err != nil {
		logger.Warn("Failed to start worker control listener: %v", err)
	} else {
		controlServer := newWorkerControlServer(workerID, w.cancelCh, w.getCurrentRunInfo, logger)
		startWorkerControlServer(shutdownCtx, controlListener, controlServer, logger)

		if config.WorkerRegisterWithRegistry() {
			regAddr := config.WorkerRegistryAddress()
			if regAddr == "" {
				regAddr = config.RegistryListenAddr()
			}

			registryClient, err := registry.New(shutdownCtx, regAddr, logger, interfaces.SystemClock{})
			if err != nil {
				logger.Warn("Failed to create registry client for worker registration: %v", err)
			} else {
				defer registryClient.Close()
				if err := registryClient.RegisterInstance(shutdownCtx, api.Component_COMPONENT_WORKER, workerID, controlAddr); err != nil {
					logger.Warn("Failed to register worker with registry: %v", err)
				} else {
					stopHeartbeat := registry.StartRegistrationHeartbeat(
						shutdownCtx, registryClient, api.Component_COMPONENT_WORKER, controlAddr,
						config.RegistryRegistrationRefresh(), logger,
					)

					defer stopHeartbeat()
					logger.Info("Registered worker %s with registry at %s", workerID, controlAddr)
				}
			}
		}
	}

	forwarder := job.NewLogSpoolForwarder(logClient, logger, 5*time.Second)
	forwarderDone := make(chan struct{})
	go func() {
		defer close(forwarderDone)
		forwarder.Run(shutdownCtx)
	}()

	w.run()

	// Wait for the forwarder to finish before closing clients.
	<-forwarderDone
	logger.Info("Worker graceful shutdown complete")
}

func forwarderSocketPath() string {
	return filepath.Join(utils.RuntimeDir(), "log-forwarder.sock")
}

func startControlListener(logger interfaces.Logger) (net.Listener, string, error) {
	mode := config.WorkerControlMode()
	port := config.WorkerControlPort()

	switch mode {
	case "ephemeral":
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, "", fmt.Errorf("listen ephemeral: %w", err)
		}

		return ln, ln.Addr().String(), nil
	case "range":
		minPort := config.WorkerControlPortMin()
		maxPort := config.WorkerControlPortMax()
		for p := minPort; p <= maxPort; p++ {
			addr := fmt.Sprintf(":%d", p)
			ln, err := net.Listen("tcp", addr)

			if err == nil {
				return ln, addr, nil
			}
		}

		return nil, "", fmt.Errorf("no available port in range %d-%d", minPort, maxPort)
	default: // "static"
		addr := fmt.Sprintf(":%d", port)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return nil, "", fmt.Errorf("listen %s: %w", addr, err)
		}

		return ln, addr, nil
	}
}

type worker struct {
	ctx                context.Context // canceled on SIGINT/SIGTERM; dequeue and between-job backoff only
	runCtx             context.Context // Background; execution, lease renew, ack, finalize survive SIGTERM until dequeue stops
	logger             interfaces.Logger
	workerID           string
	clock              interfaces.Clock
	renewInterval      time.Duration
	queue              interfaces.QueueClient
	logClient          interfaces.LogClient
	executor           *job.Executor
	store              dal.RunsRepository
	metrics            *observability.WorkerMetrics
	dequeueFailAttempt int
	dbUnavailable      bool
	dbFailAttempt      int
	cancelCh           chan string
	currentRunID       string
	currentClaimToken  string
	currentMu          sync.Mutex
}

func (w *worker) run() {
	for {
		job, keepGoing := w.dequeueNext()
		if !keepGoing {
			return
		}

		if job == nil {
			continue
		}

		w.handleJob(job)
	}
}

func (w *worker) dequeueNext() (*api.Job, bool) {
	w.logger.Debug("Initiating long poll from queue...")
	pollCtx, cancelPoll := context.WithTimeout(w.ctx, longPollTimeout)
	job, err := w.queue.Dequeue(pollCtx)
	cancelPoll()

	if err != nil {
		return w.handleDequeueError(err)
	}

	w.dequeueFailAttempt = 0
	if job == nil {
		w.logger.Debug("Dequeue returned nil job, skipping")
		return nil, true
	}

	return job, true
}

func (w *worker) logGracefulDequeueStop(cause error) {
	w.logger.Info("Worker graceful shutdown; dequeue loop stopped")
	if cause != nil {
		w.logger.Debug("Dequeue shutdown detail: %v", cause)
	}
}

func (w *worker) handleDequeueError(err error) (*api.Job, bool) {
	if err != nil && errors.Is(err, context.Canceled) {
		w.logGracefulDequeueStop(err)
		return nil, false
	}

	if st, ok := status.FromError(err); ok && st.Code() == codes.Canceled {
		w.logGracefulDequeueStop(err)
		return nil, false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		w.logger.Debug("Long poll timed out. Retrying...")
		w.dequeueFailAttempt = 0
		return nil, true
	}

	if st, ok := status.FromError(err); ok && st.Code() == codes.DeadlineExceeded {
		w.logger.Debug("Long poll timed out. Retrying...")
		w.dequeueFailAttempt = 0
		return nil, true
	}

	delay := backoff.ExponentialDelay(dequeueBackoffBase, w.dequeueFailAttempt, dequeueBackoffMax)
	if queueclient.IsTransientDequeueError(err) {
		w.logger.Warn("Failed to dequeue job: %v; retrying in %v", err, delay)
	} else {
		w.logger.Error("Dequeue failed with unexpected gRPC code; backing off for self-healing: %v; retry in %v", err, delay)
	}

	if sleepErr := w.clock.Sleep(w.ctx, delay); sleepErr != nil {
		if errors.Is(sleepErr, context.Canceled) {
			w.logGracefulDequeueStop(sleepErr)
		} else {
			w.logger.Info("Stopping worker dequeue loop: %v", sleepErr)
		}

		return nil, false
	}

	w.dequeueFailAttempt++
	return nil, true
}

func (w *worker) noteDBError(err error) {
	if !database.IsUnavailableError(err) {
		return
	}

	if !w.dbUnavailable {
		w.dbUnavailable = true
		w.logger.Warn("Database unavailable; DB-backed run transitions will retry/backoff until recovery: %v", err)
	}
}

func (w *worker) noteDBRecovered() {
	if w.dbUnavailable {
		w.dbUnavailable = false
		w.dbFailAttempt = 0
		w.logger.Info("Database connectivity recovered; DB-backed run transitions resumed")
	}
}

func (w *worker) sleepDBBackoff() error {
	delay := backoff.ExponentialDelay(dequeueBackoffBase, w.dbFailAttempt, dequeueBackoffMax)
	w.dbFailAttempt++
	return w.clock.Sleep(w.runCtx, delay)
}

func (w *worker) handleJob(job *api.Job) {
	start := w.clock.Now()
	if w.metrics != nil {
		w.metrics.RecordJobReceived(context.Background())
	}

	var outcome string
	defer func() {
		if w.metrics != nil && outcome != "" {
			w.metrics.RecordJobFinished(context.Background(), outcome, w.clock.Now().Sub(start))
		}
	}()

	jobID := job.GetId()
	runID := job.GetRunId()
	deliveryID := job.GetDeliveryId()
	w.logger.Info("Dequeued job: %s (run %s)", jobID, runID)

	if runID != "" {
		outcome = w.runClaimedJob(job, jobID, runID, deliveryID)
		return
	}

	if err := w.ackDelivery(deliveryID); err != nil {
		w.logger.Error("Ack delivery %s failed for job %s: %v", deliveryID, jobID, err)
		outcome = observability.WorkerOutcomeFailed
		return
	}

	if err := w.executor.ExecuteJob(w.runCtx, job, w.logClient, w.logger); err != nil {
		w.logger.Error("Job %s failed: %v", jobID, err)
		outcome = observability.WorkerOutcomeFailed
		return
	}

	w.logger.Info("Job completed successfully: %s", jobID)
	outcome = observability.WorkerOutcomeSuccess
}

func (w *worker) runClaimedJob(job *api.Job, jobID, runID, deliveryID string) string {
	leaseUntil := time.Now().Add(dal.DefaultLeaseTTL)
	claimed, claimToken, claimErr := w.store.TryClaim(w.runCtx, runID, w.workerID, leaseUntil)
	if claimErr != nil {
		w.noteDBError(claimErr)
		_ = w.sleepDBBackoff()
		w.logger.Error("TryClaim %s: %v", runID, claimErr)
		return observability.WorkerOutcomeFailed
	}
	w.noteDBRecovered()

	if !claimed {
		w.logger.Debug("Run %s not claimed (other worker or not queued); dropping message", runID)
		if err := w.ackDelivery(deliveryID); err != nil {
			w.logger.Warn("Ack delivery %s for unclaimed run %s failed: %v", deliveryID, runID, err)
		}

		return observability.WorkerOutcomeSkippedUnclaimed
	}

	if ackFailure := w.ackDeliveryWithRetry(deliveryID); ackFailure != nil {
		w.logger.Error("Ack delivery %s failed for claimed run %s: %v (reason_code=%s)",
			deliveryID, runID, ackFailure.err, ackFailure.decision.ReasonCode)

		if markErr := w.markRunOrphanedWithRetry(runID, claimToken, ackFailure.decision.OrphanReason); markErr != nil {
			w.logger.Error("Failed to mark run %s orphaned after ack error (%s): %v", runID, ackFailure.decision.ReasonCode, markErr)
		}

		return observability.WorkerOutcomeFailed
	}

	execErr := w.executeWithLeaseRenewal(runID, claimToken, job)
	if execErr != nil {
		w.logger.Error("Job %s failed: %v", jobID, execErr)
		decision := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerExecutionResult})
		reason := truncateFailureReason(execErr.Error())
		if err := w.markRunFailedWithRetry(runID, claimToken, decision.FailureCode, reason); err != nil {
			w.logger.Error("Failed to mark run %s failed: %v", runID, err)
		}

		return observability.WorkerOutcomeFailed
	}

	if err := w.markRunSucceededWithRetry(runID, claimToken); err != nil {
		w.logger.Error("Failed to mark run %s succeeded: %v", runID, err)
		return observability.WorkerOutcomeFailed
	}

	w.logger.Info("Job completed successfully: %s", jobID)
	return observability.WorkerOutcomeSuccess
}

func (w *worker) ackDelivery(deliveryID string) error {
	if deliveryID == "" {
		return nil
	}

	return w.queue.Ack(w.runCtx, deliveryID)
}

type ackDeliveryFailure struct {
	err      error
	attempt  int
	decision runpolicy.Decision
}

func (w *worker) ackDeliveryWithRetry(deliveryID string) *ackDeliveryFailure {
	for attempt := 1; attempt <= ackMaxAttempts; attempt++ {
		err := w.ackDelivery(deliveryID)
		if err == nil {
			return nil
		}

		decision := runpolicy.Decide(runpolicy.Input{
			Trigger:     runpolicy.TriggerAckResult,
			Attempt:     attempt,
			MaxAttempts: ackMaxAttempts,
			Transient:   queueclient.IsTransientRPCError(err),
		})

		if decision.Outcome != runpolicy.OutcomeRetry {
			return &ackDeliveryFailure{err: err, attempt: attempt, decision: decision}
		}

		delay := backoff.ExponentialDelay(ackBackoffBase, attempt-1, ackBackoffMax)
		w.logger.Warn("Ack delivery %s transient failure (attempt %d/%d): %v; retrying in %v",
			deliveryID, attempt, ackMaxAttempts, err, delay)

		if sleepErr := w.clock.Sleep(w.runCtx, delay); sleepErr != nil {
			decision := runpolicy.Decide(runpolicy.Input{
				Trigger:     runpolicy.TriggerAckResult,
				Attempt:     attempt,
				MaxAttempts: ackMaxAttempts,
				Transient:   false,
			})

			return &ackDeliveryFailure{err: sleepErr, attempt: attempt, decision: decision}
		}
	}

	decision := runpolicy.Decide(runpolicy.Input{
		Trigger:     runpolicy.TriggerAckResult,
		Attempt:     ackMaxAttempts,
		MaxAttempts: ackMaxAttempts,
		Transient:   true,
	})

	return &ackDeliveryFailure{err: status.Error(codes.Unavailable, "ack retries exhausted"), attempt: ackMaxAttempts, decision: decision}
}

func (w *worker) markRunSucceededWithRetry(runID, claimToken string) error {
	var lastErr error
	for attempt := 1; attempt <= finalizeMaxAttempts; attempt++ {
		err := w.store.MarkRunSucceeded(w.runCtx, runID, claimToken)
		if err == nil {
			w.noteDBRecovered()
			return nil
		}
		w.noteDBError(err)

		lastErr = err
		if !database.IsUnavailableError(err) {
			break
		}

		if attempt == finalizeMaxAttempts {
			break
		}

		delay := backoff.ExponentialDelay(finalizeBackoffBase, attempt-1, finalizeBackoffMax)
		w.logger.Warn("MarkRunSucceeded run %s failed (attempt %d/%d): %v; retrying in %v",
			runID, attempt, finalizeMaxAttempts, err, delay)

		if sleepErr := w.clock.Sleep(w.runCtx, delay); sleepErr != nil {
			return sleepErr
		}
	}

	return lastErr
}

func (w *worker) markRunFailedWithRetry(runID, claimToken, failureCode, reason string) error {
	var lastErr error
	for attempt := 1; attempt <= finalizeMaxAttempts; attempt++ {
		err := w.store.MarkRunFailed(w.runCtx, runID, claimToken, failureCode, reason)
		if err == nil {
			w.noteDBRecovered()
			return nil
		}
		w.noteDBError(err)

		lastErr = err
		if !database.IsUnavailableError(err) {
			break
		}

		if attempt == finalizeMaxAttempts {
			break
		}

		delay := backoff.ExponentialDelay(finalizeBackoffBase, attempt-1, finalizeBackoffMax)
		w.logger.Warn("MarkRunFailed run %s failed (attempt %d/%d): %v; retrying in %v",
			runID, attempt, finalizeMaxAttempts, err, delay)

		if sleepErr := w.clock.Sleep(w.runCtx, delay); sleepErr != nil {
			return sleepErr
		}
	}

	return lastErr
}

func (w *worker) markRunOrphanedWithRetry(runID, claimToken, reason string) error {
	var lastErr error
	for attempt := 1; attempt <= finalizeMaxAttempts; attempt++ {
		err := w.store.MarkRunOrphaned(w.runCtx, runID, claimToken, reason)
		if err == nil {
			w.noteDBRecovered()
			return nil
		}
		w.noteDBError(err)

		lastErr = err
		if !database.IsUnavailableError(err) {
			break
		}

		if attempt == finalizeMaxAttempts {
			break
		}

		delay := backoff.ExponentialDelay(finalizeBackoffBase, attempt-1, finalizeBackoffMax)
		w.logger.Warn("MarkRunOrphaned run %s failed (attempt %d/%d): %v; retrying in %v",
			runID, attempt, finalizeMaxAttempts, err, delay)

		if sleepErr := w.clock.Sleep(w.runCtx, delay); sleepErr != nil {
			return sleepErr
		}
	}

	return lastErr
}

func (w *worker) setCurrentRun(runID, claimToken string) {
	w.currentMu.Lock()
	w.currentRunID = runID
	w.currentClaimToken = claimToken
	w.currentMu.Unlock()
}

func (w *worker) clearCurrentRun() {
	w.currentMu.Lock()
	w.currentRunID = ""
	w.currentClaimToken = ""
	w.currentMu.Unlock()
}

func (w *worker) getCurrentRunInfo() (string, string) {
	w.currentMu.Lock()
	defer w.currentMu.Unlock()
	return w.currentRunID, w.currentClaimToken
}

func (w *worker) executeWithLeaseRenewal(runID, claimToken string, job *api.Job) error {
	w.setCurrentRun(runID, claimToken)
	defer w.clearCurrentRun()

	execCtx, execCancel := context.WithCancel(w.runCtx)
	defer execCancel()

	stopRenew := make(chan struct{})
	doneRenew := make(chan struct{})

	go w.leaseRenewalLoop(execCtx, runID, claimToken, stopRenew, doneRenew)

	// Listen for remote cancel requests.
	stopCancel := make(chan struct{})
	go func() {
		for {
			select {
			case cancelledRunID := <-w.cancelCh:
				if cancelledRunID == runID {
					w.logger.Info("Cancelling run %s via remote request", runID)
					execCancel()
				}
			case <-stopCancel:
				return
			}
		}
	}()

	err := w.executor.ExecuteJob(execCtx, job, w.logClient, w.logger)
	close(stopRenew)
	<-doneRenew
	close(stopCancel)

	return err
}

func (w *worker) leaseRenewalLoop(
	execCtx context.Context,
	runID string,
	claimToken string,
	stopRenew <-chan struct{},
	doneRenew chan<- struct{},
) {
	defer close(doneRenew)

	interval := w.renewInterval
	if interval <= 0 {
		interval = dal.DefaultRenewInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	renewFailed := false

	for {
		select {
		case <-stopRenew:
			return
		case <-execCtx.Done():
			return
		case <-ticker.C:
			next := time.Now().Add(dal.DefaultLeaseTTL)
			if err := w.store.RenewLease(w.runCtx, runID, w.workerID, claimToken, next); err != nil {
				w.noteDBError(err)
				renewFailed = true
				w.logger.Warn("Run %s: lease renew failed (will retry): %v", runID, err)
				continue
			}
			w.noteDBRecovered()

			if renewFailed {
				w.logger.Info("Run %s: lease renew recovered", runID)
				renewFailed = false
			}
		}
	}
}

func truncateFailureReason(reason string) string {
	if len(reason) <= maxFailureReasonLen {
		return reason
	}

	return reason[:maxFailureReasonLen] + "..."
}

var rootCmd = &cobra.Command{
	Use:   "vectis-worker",
	Short: "Vectis Worker",
	Long:  `The Vectis Worker executes jobs from the queue using the action system.`,
	Run:   runWorker,
}

func init() {
	viper.SetDefault("metrics_port", config.WorkerMetricsPort())

	rootCmd.PersistentFlags().Int("metrics-port", config.WorkerMetricsPort(), "HTTP port for Prometheus /metrics")
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))

	viper.SetEnvPrefix("VECTIS_WORKER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
