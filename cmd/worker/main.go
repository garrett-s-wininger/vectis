package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/cell"
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
	"vectis/internal/taskdispatch"
	"vectis/internal/taskfinalize"
	"vectis/internal/taskreduce"
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
	cancelPollInterval  = 5 * time.Second
)

var errRunCancelled = errors.New("run cancelled")

func runWorker(cmd *cobra.Command, args []string) {
	shutdownCtx := cmd.Context()
	if shutdownCtx == nil {
		shutdownCtx = context.Background()
	}

	// runCtx intentionally survives SIGINT/SIGTERM so a claimed run can finish
	// its action, ack, lease, and terminal DB update during graceful drain.
	runCtx := context.Background()
	logger := interfaces.NewAsyncLogger("worker")
	defer logger.Close()

	cli.SetLogLevel(logger)

	if err := config.ValidateGRPCTLSForRole(config.GRPCTLSDaemonWorker); err != nil {
		logger.Fatal("%v", err)
	}

	if err := config.ValidateMetricsTLS(); err != nil {
		logger.Fatal("%v", err)
	}

	config.StartGRPCTLSReloadLoop(shutdownCtx)
	config.StartMetricsTLSReloadLoop(shutdownCtx)

	workerID := uuid.New().String()
	logger.Info("Worker ID: %s", workerID)

	db, _, err := database.OpenReadyDBForRole(logger, database.RoleCell)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	shutdownTracer, err := observability.InitTracer(shutdownCtx, "vectis-worker")
	if err != nil {
		logger.Fatal("Failed to initialize tracer: %v", err)
	}
	defer cli.DeferShutdown(logger, "Tracer", shutdownTracer)()

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

	retryMetrics, err := observability.NewRetryMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize retry metrics: %v", err)
	}

	logRoutingMetrics, err := observability.NewLogRoutingMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize log routing metrics: %v", err)
	}

	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	metricsPort := config.WorkerMetricsEffectiveListenPort()
	metricsAddr := fmt.Sprintf(":%d", metricsPort)
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Worker", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	repos := dal.NewSQLRepositoriesWithCellID(db, config.CellID())
	runsRepo := repos.Runs()
	dial := func(ctx context.Context) (interfaces.QueueClient, interfaces.LogClient, func(), error) {
		q, l, cleanup, err := multidial.DialQueueAndLog(ctx, logger, retryMetrics, runsRepo, logRoutingMetrics)
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

	taskDispatcher := taskdispatch.New(runsRepo, repos.TaskDispatchIntents(), repos.DispatchEvents(), cell.NewQueueExecutionIngress(queueClientServiceAdapter{queue: clients}, logger), interfaces.SystemClock{})
	w := &worker{
		ctx:                   shutdownCtx,
		runCtx:                runCtx,
		logger:                logger,
		workerID:              workerID,
		cellID:                config.CellID(),
		clock:                 interfaces.SystemClock{},
		renewInterval:         dal.DefaultRenewInterval,
		queue:                 clients,
		logClient:             logClient,
		executor:              job.NewExecutor(),
		store:                 runsRepo,
		catalog:               cell.NewCatalogEventPublisher(config.CellID(), repos.CatalogEvents()),
		metrics:               workerMetrics,
		taskDispatchService:   taskdispatch.NewService(logger, taskDispatcher),
		taskReduceService:     taskreduce.NewService(taskreduce.New(runsRepo)),
		taskCompletionService: job.NewTaskCompletionService(runsRepo),
		taskCompletionFanout:  config.WorkerTaskCompletionFanout(),
		cancelCh:              make(chan string, 1),
	}

	// Start worker control server for remote cancellation.
	controlListener, controlAddr, err := startControlListener()
	if err != nil {
		logger.Warn("Failed to start worker control listener: %v", err)
	} else {
		controlServer := newWorkerControlServer(workerID, w.cancelCh, w.getCurrentRunInfo, logger)
		if err := startWorkerControlServer(shutdownCtx, controlListener, controlServer, logger); err != nil {
			logger.Warn("Failed to start worker control server: %v", err)
			_ = controlListener.Close()
			controlAddr = ""
		}

		if controlAddr != "" && config.WorkerRegisterWithRegistry() {
			stopRegistration, err := registry.RegisterWithHeartbeat(shutdownCtx, registry.RegistrationOptions{
				RegistryAddress: config.WorkerRegistrationRegistryAddress(),
				Component:       api.Component_COMPONENT_WORKER,
				InstanceID:      workerID,
				PublishAddress:  controlAddr,
				Metadata:        registry.DefaultServiceMetadataForCell(config.CellID()),
				RefreshInterval: config.RegistryRegistrationRefresh(),
				Logger:          logger,
				Metrics:         retryMetrics,
			})

			if err != nil {
				logger.Warn("Failed to register worker with registry: %v", err)
			} else {
				defer stopRegistration()
				logger.Info("Registered worker %s with registry at %s", workerID, controlAddr)
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

func startControlListener() (net.Listener, string, error) {
	return startControlListenerWithListen(net.Listen)
}

type controlListenFunc func(network, address string) (net.Listener, error)

func startControlListenerWithListen(listen controlListenFunc) (net.Listener, string, error) {
	mode := config.WorkerControlMode()
	port := config.WorkerControlPort()

	switch mode {
	case "ephemeral":
		ln, err := listen("tcp", ":0")
		if err != nil {
			return nil, "", fmt.Errorf("listen ephemeral: %w", err)
		}

		return ln, controlPublishAddress(ln.Addr().String()), nil
	case "range":
		minPort := config.WorkerControlPortMin()
		maxPort := config.WorkerControlPortMax()
		for p := minPort; p <= maxPort; p++ {
			addr := fmt.Sprintf(":%d", p)
			ln, err := listen("tcp", addr)

			if err == nil {
				return ln, controlPublishAddress(ln.Addr().String()), nil
			}
		}

		return nil, "", fmt.Errorf("no available port in range %d-%d", minPort, maxPort)
	default: // "static"
		addr := fmt.Sprintf(":%d", port)
		ln, err := listen("tcp", addr)
		if err != nil {
			return nil, "", fmt.Errorf("listen %s: %w", addr, err)
		}

		return ln, controlPublishAddress(ln.Addr().String()), nil
	}
}

func controlPublishAddress(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	if host == "" || host == "::" || host == "0.0.0.0" {
		return net.JoinHostPort("localhost", port)
	}

	return addr
}

type worker struct {
	ctx                   context.Context // canceled on SIGINT/SIGTERM; dequeue and between-job backoff only
	runCtx                context.Context // Background; execution, lease renew, ack, finalize survive SIGTERM until dequeue stops
	logger                interfaces.Logger
	workerID              string
	cellID                string
	clock                 interfaces.Clock
	renewInterval         time.Duration
	cancelPollInterval    time.Duration
	queue                 interfaces.QueueClient
	logClient             interfaces.LogClient
	executor              *job.Executor
	store                 dal.RunsRepository
	catalog               cell.CatalogEventPublisher
	metrics               *observability.WorkerMetrics
	taskDispatchService   *taskdispatch.Service
	taskReduceService     *taskreduce.Service
	taskCompletionService job.TaskCompleter
	taskCompletionFanout  bool
	dequeueFailAttempt    int
	dbUnavailable         bool
	dbFailAttempt         int
	dbMu                  sync.Mutex
	cancelCh              chan string
	currentRunID          string
	currentClaimToken     string
	currentMu             sync.Mutex
}

type queueClientServiceAdapter struct {
	queue interfaces.QueueClient
}

func (a queueClientServiceAdapter) Enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error) {
	if a.queue == nil {
		return nil, errors.New("queue client is required")
	}

	if err := a.queue.Enqueue(ctx, req); err != nil {
		return nil, err
	}

	return &api.Empty{}, nil
}

func (w *worker) now() time.Time {
	if w.clock != nil {
		return w.clock.Now()
	}

	return time.Now()
}

func (w *worker) run() {
	w.setLifecyclePhase(observability.WorkerPhaseIdle)
	w.setDraining(false)
	stopDrainObserver := w.startDrainObserver()
	defer close(stopDrainObserver)

	for {
		jobReq, keepGoing := w.dequeueNext()
		if !keepGoing {
			return
		}

		if jobReq == nil {
			continue
		}

		w.handleJob(jobReq)
	}
}

func (w *worker) dequeueNext() (*api.JobRequest, bool) {
	w.logger.Debug("Initiating long poll from queue...")
	w.setLifecyclePhase(observability.WorkerPhaseDequeuing)
	pollCtx, cancelPoll := context.WithTimeout(w.ctx, longPollTimeout)
	job, err := w.queue.Dequeue(pollCtx)
	cancelPoll()

	if err != nil {
		return w.handleDequeueError(err)
	}

	w.dequeueFailAttempt = 0
	if job == nil {
		w.logger.Debug("Dequeue returned nil job, skipping")
		w.setLifecyclePhase(observability.WorkerPhaseIdle)
		return nil, true
	}

	return job, true
}

func (w *worker) startDrainObserver() chan struct{} {
	stop := make(chan struct{})
	if w.ctx == nil {
		return stop
	}

	go func() {
		select {
		case <-w.ctx.Done():
			w.setDraining(true)
			if w.logger != nil {
				w.logger.Info("Worker drain requested; dequeue loop will stop after active work")
			}
		case <-stop:
		}
	}()

	return stop
}

func (w *worker) setLifecyclePhase(phase string) {
	if w.metrics != nil {
		w.metrics.SetLifecyclePhase(phase)
	}
}

func (w *worker) setDraining(draining bool) {
	if w.metrics != nil {
		w.metrics.SetDraining(draining)
	}
}

func (w *worker) logGracefulDequeueStop(cause error) {
	w.setDraining(true)
	w.setLifecyclePhase(observability.WorkerPhaseIdle)
	w.logger.Info("Worker graceful shutdown; dequeue loop stopped")
	if cause != nil {
		w.logger.Debug("Dequeue shutdown detail: %v", cause)
	}
}

func (w *worker) handleDequeueError(err error) (*api.JobRequest, bool) {
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

	if w.metrics != nil {
		w.metrics.SetDBUnavailable(true)
	}

	w.dbMu.Lock()
	defer w.dbMu.Unlock()
	if !w.dbUnavailable {
		w.dbUnavailable = true
		w.logger.Warn("Database unavailable; DB-backed run transitions will retry/backoff until recovery: %v", err)
	}
}

func (w *worker) noteDBRecovered() {
	if w.metrics != nil {
		w.metrics.SetDBUnavailable(false)
	}

	w.dbMu.Lock()
	defer w.dbMu.Unlock()
	if w.dbUnavailable {
		w.dbUnavailable = false
		w.dbFailAttempt = 0
		w.logger.Info("Database connectivity recovered; DB-backed run transitions resumed")
	}
}

func (w *worker) recordRunCatalogEvent(update dal.RunStatusUpdate) {
	if err := w.catalog.RecordRunStatus(w.runCtx, update); err != nil {
		w.noteDBError(err)
		w.logger.Warn("Record catalog run event %s status %s failed: %v", update.RunID, update.Status, err)
		return
	}

	w.noteDBRecovered()
}

func (w *worker) recordExecutionCatalogEvent(ctx context.Context, env *cell.ExecutionEnvelope, status string) {
	if env == nil {
		return
	}

	if err := w.catalog.RecordExecutionStatus(w.runCtx, dal.ExecutionStatusUpdate{ExecutionID: env.ExecutionID, Status: status}); err != nil {
		w.noteDBError(err)
		w.logger.Warn("Record catalog execution event %s status %s failed: %v", env.ExecutionID, status, err)
		trace.SpanFromContext(ctx).RecordError(err)
		return
	}

	w.noteDBRecovered()
}

func (w *worker) sleepDBBackoff() error {
	w.dbMu.Lock()
	delay := backoff.ExponentialDelay(dequeueBackoffBase, w.dbFailAttempt, dequeueBackoffMax)
	w.dbFailAttempt++
	w.dbMu.Unlock()
	return w.clock.Sleep(w.runCtx, delay)
}

func (w *worker) handleJob(jobReq *api.JobRequest) {
	jobCtx := observability.ExtractJobTraceContext(w.runCtx, jobReq)
	job := jobReq.GetJob()
	if job == nil {
		w.logger.Error("Dequeued empty job request")
		return
	}

	jobID := job.GetId()
	runID := job.GetRunId()
	executionEnvelope := w.executionEnvelopeFromRequest(jobReq)
	consumeCtx, span := observability.Tracer("vectis/worker").Start(jobCtx, "worker.job.consume", trace.WithSpanKind(trace.SpanKindConsumer))
	span.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
	span.SetAttributes(attribute.Bool("vectis.run.claimed", runID != ""))
	span.SetAttributes(attribute.String("run.phase", "consume"))
	if executionEnvelope != nil {
		span.SetAttributes(executionEnvelopeAttrs(executionEnvelope)...)
	}
	if enqueuedRaw := jobReq.GetMetadata()[observability.JobEnqueueAcceptedUnixNanoKey]; enqueuedRaw != "" {
		if enqueuedAtUnixNano, err := strconv.ParseInt(enqueuedRaw, 10, 64); err == nil {
			handoff := float64(time.Now().UnixNano()-enqueuedAtUnixNano) / float64(time.Millisecond)
			if handoff >= 0 {
				span.SetAttributes(attribute.Float64("queue.handoff.ms", handoff))
			}
		}
	} else if enqueuedRaw := jobReq.GetMetadata()[observability.JobEnqueuedAtUnixNanoKey]; enqueuedRaw != "" {
		if enqueuedAtUnixNano, err := strconv.ParseInt(enqueuedRaw, 10, 64); err == nil {
			handoff := float64(time.Now().UnixNano()-enqueuedAtUnixNano) / float64(time.Millisecond)
			if handoff >= 0 {
				span.SetAttributes(attribute.Float64("queue.handoff.ms", handoff))
			}
		}
	}

	span.AddEvent("queue.long_poll.delivered")

	start := w.now()
	if w.metrics != nil {
		w.metrics.RecordJobReceived(consumeCtx)
	}

	deliveryID := job.GetDeliveryId()
	w.logger.Info("Dequeued job: %s (run %s)", jobID, runID)

	if runID != "" {
		span.SetAttributes(attribute.String("vectis.worker.outcome", "consumed"))
		span.End()
		outcome := w.runClaimedJob(jobCtx, job, jobID, runID, deliveryID, executionEnvelope)
		if w.metrics != nil && outcome != "" {
			w.metrics.RecordJobFinished(jobCtx, outcome, w.now().Sub(start))
		}

		return
	}

	w.setLifecyclePhase(observability.WorkerPhaseAcking)
	if err := w.ackDelivery(deliveryID); err != nil {
		w.logger.Error("Ack delivery %s failed for job %s: %v", deliveryID, jobID, err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "ack delivery")
		span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeFailed))
		span.End()
		w.setLifecyclePhase(observability.WorkerPhaseIdle)
		if w.metrics != nil {
			w.metrics.RecordJobFinished(jobCtx, observability.WorkerOutcomeFailed, w.now().Sub(start))
		}

		return
	}

	span.SetAttributes(attribute.String("vectis.worker.outcome", "consumed"))
	span.End()

	w.setLifecyclePhase(observability.WorkerPhaseExecuting)
	if err := w.executor.ExecuteJob(jobCtx, job, w.logClient, w.logger); err != nil {
		w.logger.Error("Job %s failed: %v", jobID, err)
		w.setLifecyclePhase(observability.WorkerPhaseIdle)
		if w.metrics != nil {
			w.metrics.RecordJobFinished(jobCtx, observability.WorkerOutcomeFailed, w.now().Sub(start))
		}

		return
	}

	w.logger.Info("Job completed successfully: %s", jobID)
	w.setLifecyclePhase(observability.WorkerPhaseIdle)
	if w.metrics != nil {
		w.metrics.RecordJobFinished(jobCtx, observability.WorkerOutcomeSuccess, w.now().Sub(start))
	}
}

func (w *worker) executionEnvelopeFromRequest(jobReq *api.JobRequest) *cell.ExecutionEnvelope {
	env, ok, err := cell.ExecutionEnvelopeFromRequest(jobReq)
	if err != nil {
		w.logger.Error("Invalid execution envelope metadata: %v", err)
		return nil
	}

	if !ok {
		return nil
	}

	w.logger.Debug("Decoded execution envelope: run=%s segment=%s execution=%s cell=%s",
		env.RunID, env.SegmentID, env.ExecutionID, env.CellID)
	return env
}

func (w *worker) runClaimedJob(ctx context.Context, job *api.Job, jobID, runID, deliveryID string, envelopes ...*cell.ExecutionEnvelope) string {
	var executionEnvelope *cell.ExecutionEnvelope
	if len(envelopes) > 0 {
		executionEnvelope = envelopes[0]
	}
	w.setLifecyclePhase(observability.WorkerPhaseClaiming)
	defer w.setLifecyclePhase(observability.WorkerPhaseIdle)

	ctx, span := observability.Tracer("vectis/worker").Start(ctx, "worker.run.execute", trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
	span.SetAttributes(observability.DeliveryAttrs(deliveryID)...)
	span.SetAttributes(attribute.String("run.phase", "execute"))
	if executionEnvelope != nil {
		span.SetAttributes(executionEnvelopeAttrs(executionEnvelope)...)
	}
	defer span.End()

	leaseUntil := w.now().Add(dal.DefaultLeaseTTL)
	span.AddEvent("run.claim.attempt", trace.WithAttributes(attribute.Int("attempt", 1)))
	claimed, claimToken, claimErr := w.store.TryClaim(w.runCtx, runID, w.workerID, leaseUntil)
	if claimErr != nil {
		w.noteDBError(claimErr)
		_ = w.sleepDBBackoff()
		w.logger.Error("TryClaim %s: %v", runID, claimErr)
		span.RecordError(claimErr)
		span.SetStatus(otelcodes.Error, "try claim")
		span.AddEvent("run.claim.error", trace.WithAttributes(attribute.String("error", claimErr.Error())))
		return observability.WorkerOutcomeFailed
	}
	w.noteDBRecovered()

	if !claimed {
		w.logger.Debug("Run %s not claimed (other worker or not queued); dropping message", runID)
		w.setLifecyclePhase(observability.WorkerPhaseAcking)
		if err := w.ackDelivery(deliveryID); err != nil {
			w.logger.Warn("Ack delivery %s for unclaimed run %s failed: %v", deliveryID, runID, err)
		}
		span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeSkippedUnclaimed))

		return observability.WorkerOutcomeSkippedUnclaimed
	}

	w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusRunning})

	w.setLifecyclePhase(observability.WorkerPhaseAcking)
	if ackFailure := w.ackDeliveryWithRetry(ctx, deliveryID); ackFailure != nil {
		w.logger.Error("Ack delivery %s failed for claimed run %s: %v (reason_code=%s)",
			deliveryID, runID, ackFailure.err, ackFailure.decision.ReasonCode)
		span.RecordError(ackFailure.err)
		span.SetStatus(otelcodes.Error, "ack delivery retry exhausted")

		w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
		if markErr := w.markRunOrphanedWithRetry(runID, claimToken, ackFailure.decision.OrphanReason); markErr != nil {
			w.logger.Error("Failed to mark run %s orphaned after ack error (%s): %v", runID, ackFailure.decision.ReasonCode, markErr)
			span.RecordError(markErr)
		}

		return observability.WorkerOutcomeFailed
	}

	w.markExecutionAccepted(ctx, executionEnvelope)
	w.markExecutionStarted(ctx, executionEnvelope)
	w.setLifecyclePhase(observability.WorkerPhaseExecuting)
	execErr := w.executeWithLeaseRenewal(ctx, runID, claimToken, job, executionEnvelope)
	if execErr != nil {
		if errors.Is(execErr, errRunCancelled) {
			span.AddEvent("run.cancelled")
			span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeAborted))
			w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
			if w.taskCompletionFanout && executionEnvelope != nil {
				if _, ok := w.completeExecutionTerminal(ctx, executionEnvelope, dal.ExecutionStatusAborted); !ok {
					span.SetStatus(otelcodes.Error, "complete aborted task execution")
					return observability.WorkerOutcomeFailed
				}
			}

			if err := w.markRunAbortedWithRetry(runID, claimToken, dal.CancelReasonAPI); err != nil {
				w.logger.Error("Failed to mark run %s cancelled: %v", runID, err)
				span.RecordError(err)
			}
			if !(w.taskCompletionFanout && executionEnvelope != nil) {
				w.markExecutionTerminal(ctx, executionEnvelope, dal.ExecutionStatusAborted)
			}

			return observability.WorkerOutcomeAborted
		}

		w.logger.Error("Job %s failed: %v", jobID, execErr)
		span.RecordError(execErr)
		span.SetStatus(otelcodes.Error, "execute with lease renewal")
		decision := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerExecutionResult})
		reason := truncateFailureReason(execErr.Error())
		w.setLifecyclePhase(observability.WorkerPhaseFinalizing)

		if w.taskCompletionFanout && executionEnvelope != nil {
			if _, ok := w.completeExecutionTerminal(ctx, executionEnvelope, dal.ExecutionStatusFailed); !ok {
				span.SetStatus(otelcodes.Error, "complete failed task execution")
				return observability.WorkerOutcomeFailed
			}

			_, err := w.reduceTaskRun(ctx, runID)
			if err != nil {
				w.logger.Error("Failed to reduce failed task run %s: %v", runID, err)
				span.RecordError(err)
				span.SetStatus(otelcodes.Error, "reduce failed task run")
				return observability.WorkerOutcomeFailed
			}

			if err := w.markRunFailedWithRetry(runID, claimToken, decision.FailureCode, reason); err != nil {
				w.logger.Error("Failed to mark run %s failed after task execution failure: %v", runID, err)
				span.RecordError(err)
				span.SetStatus(otelcodes.Error, "mark failed task run failed")
				return observability.WorkerOutcomeFailed
			}

			span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeFailed))
			return observability.WorkerOutcomeFailed
		}

		if err := w.markRunFailedWithRetry(runID, claimToken, decision.FailureCode, reason); err != nil {
			w.logger.Error("Failed to mark run %s failed: %v", runID, err)
			span.RecordError(err)
		}
		w.markExecutionTerminal(ctx, executionEnvelope, dal.ExecutionStatusFailed)

		return observability.WorkerOutcomeFailed
	}

	w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
	if w.taskCompletionFanout && executionEnvelope != nil {
		completion, ok := w.completeExecutionTerminal(ctx, executionEnvelope, dal.ExecutionStatusSucceeded)
		if !ok {
			span.SetStatus(otelcodes.Error, "complete task execution")
			return observability.WorkerOutcomeFailed
		}

		continued, err := w.continueTaskRun(ctx, runID, claimToken, completion.dispatchableChildren > 0)
		if err != nil {
			w.logger.Error("Failed to continue task run %s: %v", runID, err)
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "continue task run")
			return observability.WorkerOutcomeFailed
		}

		if continued {
			finalizeDecision := taskfinalize.Decide(true, taskreduce.Decision{})
			taskfinalize.RecordDecision(ctx, finalizeDecision)
			span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeSuccess))
			w.logger.Info("Task completed successfully; run queued for continuation: %s", jobID)
			return observability.WorkerOutcomeSuccess
		}

		reduceDecision, err := w.reduceTaskRun(ctx, runID)
		if err != nil {
			w.logger.Error("Failed to reduce task run %s: %v", runID, err)
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "reduce task run")
			return observability.WorkerOutcomeFailed
		}

		finalizeDecision := taskfinalize.Decide(false, reduceDecision)
		taskfinalize.RecordDecision(ctx, finalizeDecision)

		switch finalizeDecision.Outcome {
		case taskfinalize.OutcomeReduceSucceeded:
		case taskfinalize.OutcomeReduceFailed:
			decision := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerExecutionResult})
			reason := truncateFailureReason(taskfinalize.FailureReason(finalizeDecision))
			if err := w.markRunFailedWithRetry(runID, claimToken, decision.FailureCode, reason); err != nil {
				w.logger.Error("Failed to mark run %s failed after task reduction: %v", runID, err)
				span.RecordError(err)
				span.SetStatus(otelcodes.Error, "mark reduced run failed")
				return observability.WorkerOutcomeFailed
			}

			span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeFailed))
			w.logger.Info("Task run reduced to failed: %s", jobID)
			return observability.WorkerOutcomeFailed
		case taskfinalize.OutcomeIncomplete:
			if err := w.markRunQueuedForContinuationWithRetry(runID, claimToken); err != nil {
				w.logger.Error("Failed to queue incomplete task run %s for continuation: %v", runID, err)
				span.RecordError(err)
				span.SetStatus(otelcodes.Error, "queue reduced run continuation")
				return observability.WorkerOutcomeFailed
			}

			span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeSuccess))
			w.logger.Info("Task run has incomplete work; run queued for continuation: %s", jobID)
			return observability.WorkerOutcomeSuccess
		default:
			span.RecordError(fmt.Errorf("unsupported task finalize outcome %q", finalizeDecision.Outcome))
			span.SetStatus(otelcodes.Error, "unsupported task finalize outcome")
			return observability.WorkerOutcomeFailed
		}
	}

	if err := w.markRunSucceededWithRetry(runID, claimToken); err != nil {
		w.logger.Error("Failed to mark run %s succeeded: %v", runID, err)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "mark run succeeded")
		return observability.WorkerOutcomeFailed
	}

	if !(w.taskCompletionFanout && executionEnvelope != nil) {
		w.markExecutionTerminal(ctx, executionEnvelope, dal.ExecutionStatusSucceeded)
	}

	span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeSuccess))
	w.logger.Info("Job completed successfully: %s", jobID)
	return observability.WorkerOutcomeSuccess
}

func (w *worker) reduceTaskRun(ctx context.Context, runID string) (taskreduce.Decision, error) {
	service := w.taskReduceService
	if service == nil {
		service = taskreduce.NewService(taskreduce.New(w.store))
	}

	reduceCtx := trace.ContextWithSpan(w.runCtx, trace.SpanFromContext(ctx))
	decision, err := service.Process(reduceCtx, runID)
	if err != nil {
		return taskreduce.Decision{}, err
	}

	return decision, nil
}

func executionEnvelopeAttrs(env *cell.ExecutionEnvelope) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("vectis.cell.id", env.CellID),
		attribute.String("vectis.task.id", env.TaskID),
		attribute.String("vectis.task.key", env.TaskKey),
		attribute.String("vectis.task.attempt.id", env.TaskAttemptID),
		attribute.Int("vectis.task.attempt", env.TaskAttempt),
		attribute.String("vectis.segment.id", env.SegmentID),
		attribute.String("vectis.execution.id", env.ExecutionID),
		attribute.Int("vectis.definition.version", env.DefinitionVersion),
		attribute.String("vectis.definition.hash", env.DefinitionHash),
	}
}

func (w *worker) markExecutionAccepted(ctx context.Context, env *cell.ExecutionEnvelope) {
	if env == nil {
		return
	}

	if err := w.store.MarkExecutionAccepted(w.runCtx, env.ExecutionID); err != nil {
		w.noteDBError(err)
		w.logger.Warn("MarkExecutionAccepted execution %s failed: %v", env.ExecutionID, err)
		trace.SpanFromContext(ctx).RecordError(err)
		return
	}

	w.noteDBRecovered()
	w.recordExecutionCatalogEvent(ctx, env, dal.ExecutionStatusAccepted)
	trace.SpanFromContext(ctx).AddEvent("execution.accepted", trace.WithAttributes(executionEnvelopeAttrs(env)...))
}

func (w *worker) markExecutionStarted(ctx context.Context, env *cell.ExecutionEnvelope) {
	if env == nil {
		return
	}

	if err := w.store.MarkExecutionStarted(w.runCtx, env.ExecutionID); err != nil {
		w.noteDBError(err)
		w.logger.Warn("MarkExecutionStarted execution %s failed: %v", env.ExecutionID, err)
		trace.SpanFromContext(ctx).RecordError(err)
		return
	}

	w.noteDBRecovered()
	w.recordExecutionCatalogEvent(ctx, env, dal.ExecutionStatusRunning)
	trace.SpanFromContext(ctx).AddEvent("execution.started", trace.WithAttributes(executionEnvelopeAttrs(env)...))
}

type executionTerminalResult struct {
	activatedChildren    int
	dispatchableChildren int
}

func (w *worker) markExecutionTerminal(ctx context.Context, env *cell.ExecutionEnvelope, status string) {
	_, _ = w.completeExecutionTerminal(ctx, env, status)
}

func (w *worker) completeExecutionTerminal(ctx context.Context, env *cell.ExecutionEnvelope, status string) (executionTerminalResult, bool) {
	if env == nil {
		return executionTerminalResult{}, true
	}

	var result executionTerminalResult
	if w.taskCompletionFanout {
		service := w.taskCompletionService
		if service == nil {
			service = job.NewTaskCompletionService(w.store)
		}

		completionCtx := trace.ContextWithSpan(w.runCtx, trace.SpanFromContext(ctx))
		completion, err := service.CompleteTaskExecution(completionCtx, env.ExecutionID, status)
		if err != nil {
			w.noteDBError(err)
			w.logger.Warn("CompleteTaskExecution execution %s status %s failed: %v", env.ExecutionID, status, err)
			trace.SpanFromContext(ctx).RecordError(err)
			return executionTerminalResult{}, false
		}

		result.activatedChildren = completion.Activated
		result.dispatchableChildren = len(completion.Children)
	} else if err := w.store.MarkExecutionTerminal(w.runCtx, env.ExecutionID, status); err != nil {
		w.noteDBError(err)
		w.logger.Warn("MarkExecutionTerminal execution %s status %s failed: %v", env.ExecutionID, status, err)
		trace.SpanFromContext(ctx).RecordError(err)
		return executionTerminalResult{}, false
	}

	w.noteDBRecovered()
	w.recordExecutionCatalogEvent(ctx, env, status)
	trace.SpanFromContext(ctx).AddEvent("execution.terminal", trace.WithAttributes(
		append(
			executionEnvelopeAttrs(env),
			attribute.String("vectis.execution.status", status),
			attribute.Int("vectis.task.children.activated", result.activatedChildren),
			attribute.Int("vectis.task.children.dispatchable", result.dispatchableChildren),
		)...,
	))
	return result, true
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

func (w *worker) ackDeliveryWithRetry(ctx context.Context, deliveryID string) *ackDeliveryFailure {
	for attempt := 1; attempt <= ackMaxAttempts; attempt++ {
		trace.SpanFromContext(ctx).AddEvent("queue.ack.attempt", trace.WithAttributes(
			attribute.Int("attempt", attempt),
			attribute.Int("max_attempts", ackMaxAttempts),
		))

		err := w.ackDelivery(deliveryID)
		if err == nil {
			trace.SpanFromContext(ctx).AddEvent("queue.ack.success", trace.WithAttributes(
				attribute.Int("attempt", attempt),
			))
			return nil
		}

		decision := runpolicy.Decide(runpolicy.Input{
			Trigger:     runpolicy.TriggerAckResult,
			Attempt:     attempt,
			MaxAttempts: ackMaxAttempts,
			Transient:   queueclient.IsTransientRPCError(err),
		})

		if decision.Outcome != runpolicy.OutcomeRetry {
			trace.SpanFromContext(ctx).AddEvent("queue.ack.error", trace.WithAttributes(
				attribute.Int("attempt", attempt),
				attribute.String("error", err.Error()),
				attribute.String("decision.outcome", fmt.Sprintf("%v", decision.Outcome)),
				attribute.String("decision.reason_code", decision.ReasonCode),
			))

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

func (w *worker) continueTaskRun(ctx context.Context, runID, claimToken string, knownPending bool) (bool, error) {
	if w.taskDispatchService == nil {
		return false, nil
	}

	opts := taskdispatch.DrainOptions{CellID: w.cellID, RunID: runID, Limit: 1}
	if !knownPending {
		pending, err := w.taskDispatchService.HasPending(w.runCtx, opts)
		if err != nil {
			return false, err
		}

		if !pending {
			return false, nil
		}
	}

	if err := w.markRunQueuedForContinuationWithRetry(runID, claimToken); err != nil {
		return false, err
	}

	result, err := w.taskDispatchService.Process(w.runCtx, opts)
	if err != nil {
		return true, err
	}

	trace.SpanFromContext(ctx).AddEvent("task.dispatch.drain", trace.WithAttributes(
		attribute.Int("vectis.task.dispatch.listed", result.Listed),
		attribute.Int("vectis.task.dispatch.enqueued", result.Enqueued),
		attribute.Int("vectis.task.dispatch.failed", result.Failed),
	))

	if result.Enqueued == 0 {
		w.logger.Warn("Task run %s queued for continuation, but no task dispatch intent was enqueued (listed=%d failed=%d)", runID, result.Listed, result.Failed)
	}

	return true, nil
}

func (w *worker) markRunSucceededWithRetry(runID, claimToken string) error {
	var lastErr error
	for attempt := 1; attempt <= finalizeMaxAttempts; attempt++ {
		err := w.store.MarkRunSucceeded(w.runCtx, runID, claimToken)
		if err == nil {
			w.noteDBRecovered()
			w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusSucceeded})
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

func (w *worker) markRunQueuedForContinuationWithRetry(runID, claimToken string) error {
	var lastErr error
	for attempt := 1; attempt <= finalizeMaxAttempts; attempt++ {
		err := w.store.MarkRunQueuedForContinuation(w.runCtx, runID, claimToken)
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
		w.logger.Warn("MarkRunQueuedForContinuation run %s failed (attempt %d/%d): %v; retrying in %v",
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
			w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusFailed, FailureCode: failureCode, Reason: reason})
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

func (w *worker) markRunAbortedWithRetry(runID, claimToken, reason string) error {
	var lastErr error
	for attempt := 1; attempt <= finalizeMaxAttempts; attempt++ {
		err := w.store.MarkRunAborted(w.runCtx, runID, claimToken, reason)
		if err == nil {
			w.noteDBRecovered()
			w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusCancelled, Reason: reason})
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
		w.logger.Warn("MarkRunAborted run %s failed (attempt %d/%d): %v; retrying in %v",
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
			w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusOrphaned, Reason: reason})
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

func (w *worker) executeWithLeaseRenewal(ctx context.Context, runID, claimToken string, job *api.Job, executionEnvelope *cell.ExecutionEnvelope) error {
	w.setCurrentRun(runID, claimToken)
	defer w.clearCurrentRun()

	execCtx, execCancel := context.WithCancel(ctx)
	defer execCancel()
	cancelled := make(chan struct{})
	var cancelOnce sync.Once
	cancelRun := func(source string) {
		cancelOnce.Do(func() {
			w.logger.Info("Cancelling run %s via %s", runID, source)
			close(cancelled)
			execCancel()
		})
	}

	stopRenew := make(chan struct{})
	doneRenew := make(chan struct{})

	go w.leaseRenewalLoop(execCtx, runID, claimToken, stopRenew, doneRenew)

	// Listen for remote cancel requests.
	// Drain any stale cancel from a previous job so the buffer is free for this run.
	select {
	case <-w.cancelCh:
	default:
	}
	stopCancel := make(chan struct{})
	defer close(stopCancel)
	go func() {
		for {
			select {
			case cancelledRunID := <-w.cancelCh:
				if cancelledRunID == runID {
					cancelRun("remote request")
				}
			case <-stopCancel:
				return
			case <-execCtx.Done():
				return
			}
		}
	}()

	if w.store != nil {
		go w.cancelRequestLoop(execCtx, runID, claimToken, stopCancel, cancelRun)
	}

	var err error
	if w.taskCompletionFanout && executionEnvelope != nil {
		err = w.executor.ExecuteTask(execCtx, job, executionEnvelope.TaskKey, w.logClient, w.logger)
	} else {
		err = w.executor.ExecuteJob(execCtx, job, w.logClient, w.logger)
	}
	close(stopRenew)
	<-doneRenew

	select {
	case <-cancelled:
		if err != nil {
			return fmt.Errorf("%w: %v", errRunCancelled, err)
		}
		return errRunCancelled
	default:
	}

	return err
}

func (w *worker) cancelRequestLoop(
	execCtx context.Context,
	runID string,
	claimToken string,
	stopCancel <-chan struct{},
	cancelRun func(string),
) {
	interval := w.cancelPollInterval
	if interval <= 0 {
		interval = cancelPollInterval
	}

	check := func() {
		requested, err := w.store.RunCancelRequested(w.runCtx, runID, claimToken)
		if err != nil {
			w.noteDBError(err)
			w.logger.Warn("Run %s: cancel request poll failed (will retry): %v", runID, err)
			return
		}
		w.noteDBRecovered()

		if requested {
			cancelRun("durable request")
		}
	}

	check()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCancel:
			return
		case <-execCtx.Done():
			return
		case <-ticker.C:
			check()
		}
	}
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
			next := w.now().Add(dal.DefaultLeaseTTL)
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
	cli.ConfigureVersion(rootCmd)
	viper.SetDefault("metrics_port", config.WorkerMetricsPort())

	rootCmd.PersistentFlags().Int("metrics-port", config.WorkerMetricsPort(), "HTTP port for Prometheus /metrics")
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindEnv("worker.queue.address", "VECTIS_WORKER_QUEUE_ADDRESS")
	_ = viper.BindEnv("worker.log.address", "VECTIS_WORKER_LOG_ADDRESS")
	_ = viper.BindEnv("worker.registry.address", "VECTIS_WORKER_REGISTRY_ADDRESS")
	_ = viper.BindEnv("worker.control.mode", "VECTIS_WORKER_CONTROL_MODE")
	_ = viper.BindEnv("worker.task_completion_fanout", "VECTIS_WORKER_TASK_COMPLETION_FANOUT")
	_ = viper.BindEnv("control_port", "VECTIS_WORKER_CONTROL_PORT")
	_ = viper.BindEnv("control_port_min", "VECTIS_WORKER_CONTROL_PORT_MIN")
	_ = viper.BindEnv("control_port_max", "VECTIS_WORKER_CONTROL_PORT_MAX")

	viper.SetEnvPrefix("VECTIS_WORKER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
