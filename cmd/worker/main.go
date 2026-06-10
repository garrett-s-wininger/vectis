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
	"vectis/internal/action"
	"vectis/internal/action/actionconfig"
	"vectis/internal/action/actionregistry"
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
	"vectis/internal/platform"
	"vectis/internal/queueclient"
	"vectis/internal/registry"
	"vectis/internal/runpolicy"
	"vectis/internal/spire"
	"vectis/internal/taskdispatch"
	"vectis/internal/taskfinalize"
	"vectis/internal/taskreduce"
	"vectis/internal/utils"
	"vectis/internal/workloadidentity"

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

	// runCtx intentionally survives SIGINT/SIGTERM so the active task execution
	// can finish its action, lease, and terminal DB update during graceful drain.
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

	if err := config.ValidateWorkerExecutionIdentityConfig(); err != nil {
		logger.Fatal("%v", err)
	}

	if err := config.ValidateWorkerSPIREConfig(); err != nil {
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

	dispatchMetrics, err := observability.NewDispatchMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize dispatch metrics: %v", err)
	}

	taskDispatchMetrics, err := observability.NewTaskDispatchMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize task dispatch metrics: %v", err)
	}

	taskFinalizeMetrics, err := observability.NewTaskFinalizeMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize task finalize metrics: %v", err)
	}

	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	metricsAddr := config.WorkerMetricsListenAddr()
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Worker", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	repos := dal.NewSQLRepositoriesWithCellID(db, config.CellID())
	runsRepo := repos.Runs()
	_, _, dequeueSupportedIsolation := workerExecutionCapabilitiesForBackend(config.WorkerExecutionBackend())
	dialOptions := multidial.DialOptions{QueueDequeueSupportedIsolation: dequeueSupportedIsolation}
	dial := func(ctx context.Context) (interfaces.QueueClient, interfaces.LogClient, func(), error) {
		q, l, cleanup, err := multidial.DialQueueAndLogWithOptions(ctx, logger, retryMetrics, runsRepo, logRoutingMetrics, dialOptions)
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
	taskDispatcher.SetDispatchMetrics(dispatchMetrics)
	taskDispatchService := taskdispatch.NewService(logger, taskDispatcher)
	taskDispatchService.SetMetrics(taskDispatchMetrics)
	var spireSVIDSource spire.X509SVIDSource
	if config.WorkerSPIREEnabled() {
		src, err := spire.NewWorkloadAPISource(config.WorkerSPIREWorkloadAPIAddress())
		if err != nil {
			logger.Fatal("Failed to configure SPIRE Workload API source: %v", err)
		}

		spireSVIDSource = src
	}

	executor, err := configuredJobExecutor(logger)
	if err != nil {
		logger.Fatal("Invalid worker execution backend: %v", err)
	}

	actionResolver, err := actionconfig.DescriptorResolver()
	if err != nil {
		logger.Fatal("Invalid action registry config: %v", err)
	}

	w := &worker{
		ctx:                 shutdownCtx,
		runCtx:              runCtx,
		logger:              logger,
		workerID:            workerID,
		cellID:              config.CellID(),
		clock:               interfaces.SystemClock{},
		renewInterval:       dal.DefaultRenewInterval,
		queue:               clients,
		logClient:           logClient,
		executor:            executor,
		actionResolver:      actionResolver,
		store:               runsRepo,
		catalog:             cell.NewCatalogEventPublisher(config.CellID(), repos.CatalogEvents()),
		metrics:             workerMetrics,
		taskDispatchService: taskDispatchService,
		taskFinalizeMetrics: taskFinalizeMetrics,
		spireSVIDSource:     spireSVIDSource,
		cancelCh:            make(chan string, 1),
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
				Metadata:        workerRegistryMetadata(),
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

func configuredJobExecutor(logger interfaces.Logger) (*job.Executor, error) {
	processExecutor, backend, err := configuredProcessExecutor()
	if err != nil {
		return nil, err
	}

	if logger != nil {
		logger.Info("Worker execution backend: %s", backend)
	}

	options := []job.ExecutorOption{}
	if workspaceRoot := config.WorkerExecutionWorkspaceRoot(); workspaceRoot != "" {
		options = append(options, job.WithWorkspaceRoot(workspaceRoot))
	}
	if processExecutor != nil {
		options = append(options,
			job.WithVMProcessExecutor(processExecutor),
			job.WithDefaultIsolation(action.IsolationVM),
		)
	}
	return job.NewExecutor(options...), nil
}

func workerRegistryMetadata() map[string]string {
	backend, defaultIsolation, supportedIsolation := workerExecutionCapabilitiesForBackend(config.WorkerExecutionBackend())
	return registry.WorkerExecutionMetadataForCell(config.CellID(), backend, defaultIsolation, supportedIsolation)
}

func workerExecutionCapabilitiesForBackend(backend string) (string, string, []string) {
	switch backend {
	case "", "host":
		return "host", action.IsolationHost, []string{action.IsolationHost}
	case "lima":
		return "lima", action.IsolationVM, []string{action.IsolationHost, action.IsolationVM}
	default:
		return backend, "", nil
	}
}

func configuredProcessExecutor() (interfaces.ExecExecutor, string, error) {
	switch backend := config.WorkerExecutionBackend(); backend {
	case "", "host":
		return nil, "host", nil
	case "lima":
		executor, err := platform.NewVirtualMachineCommandExecutor(platform.VirtualMachineConfig{
			Provider:           platform.VirtualMachineProviderLima,
			Instance:           config.WorkerExecutionLimaInstance(),
			ProviderPath:       config.WorkerExecutionLimaPath(),
			GuestWorkspaceRoot: config.WorkerExecutionLimaGuestWorkspaceRoot(),
			Start:              config.WorkerExecutionLimaStart(),
			PreserveEnv:        config.WorkerExecutionLimaPreserveEnv(),
		})
		if err != nil {
			return nil, "", err
		}
		return executor, "lima", nil
	default:
		return nil, "", fmt.Errorf("unknown execution backend %q", backend)
	}
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
	ctx                 context.Context // canceled on SIGINT/SIGTERM; dequeue and between-job backoff only
	runCtx              context.Context // Background; execution, lease renew, ack, finalize survive SIGTERM until dequeue stops
	logger              interfaces.Logger
	workerID            string
	cellID              string
	clock               interfaces.Clock
	renewInterval       time.Duration
	cancelPollInterval  time.Duration
	queue               interfaces.QueueClient
	logClient           interfaces.LogClient
	executor            *job.Executor
	actionResolver      actionregistry.Resolver
	store               dal.RunsRepository
	catalog             cell.CatalogEventPublisher
	metrics             *observability.WorkerMetrics
	taskDispatchService *taskdispatch.Service
	taskFinalizeMetrics *observability.TaskFinalizeMetrics
	spireSVIDSource     spire.X509SVIDSource
	dequeueFailAttempt  int
	dbUnavailable       bool
	dbFailAttempt       int
	dbMu                sync.Mutex
	cancelCh            chan string
	currentRunID        string
	currentClaimToken   string
	currentMu           sync.Mutex
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

func (w *worker) leaseDeadline() time.Time {
	now := w.now().UTC()
	realNow := time.Now().UTC()
	if now.Before(realNow) {
		now = realNow
	}

	return now.Add(dal.DefaultLeaseTTL)
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
		outcome := w.runTaskExecution(jobCtx, job, jobID, runID, deliveryID, executionEnvelope)
		if w.metrics != nil && outcome != "" {
			w.metrics.RecordJobFinished(jobCtx, outcome, w.now().Sub(start))
		}

		return
	}

	if config.WorkerExecutionIdentityEnabled() {
		w.logger.Error("Job %s missing run context; worker execution identity requires execution envelope", jobID)
	}
	w.logger.Error("Dropping malformed queue delivery for job %s: missing run_id", jobID)
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

	span.AddEvent("queue.delivery.malformed", trace.WithAttributes(attribute.String("reason", "missing_run_id")))
	span.SetStatus(otelcodes.Error, "missing run_id")
	span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeMalformed))
	span.End()

	w.setLifecyclePhase(observability.WorkerPhaseIdle)
	if w.metrics != nil {
		w.metrics.RecordJobFinished(jobCtx, observability.WorkerOutcomeMalformed, w.now().Sub(start))
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

func (w *worker) runTaskExecution(ctx context.Context, job *api.Job, jobID, runID, deliveryID string, envelopes ...*cell.ExecutionEnvelope) string {
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

	leaseUntil := w.leaseDeadline()
	w.setLifecyclePhase(observability.WorkerPhaseAcking)
	if ackFailure := w.ackDeliveryWithRetry(ctx, deliveryID); ackFailure != nil {
		w.logger.Error("Ack delivery %s failed for run %s: %v (reason_code=%s)",
			deliveryID, runID, ackFailure.err, ackFailure.decision.ReasonCode)
		span.RecordError(ackFailure.err)
		span.SetStatus(otelcodes.Error, "ack delivery retry exhausted")

		w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
		if markErr := w.markRunOrphanedWithRetry(runID, ackFailure.decision.OrphanReason); markErr != nil {
			w.logger.Error("Failed to mark run %s orphaned after ack error (%s): %v", runID, ackFailure.decision.ReasonCode, markErr)
			span.RecordError(markErr)
		}

		return observability.WorkerOutcomeFailed
	}

	if executionEnvelope == nil {
		span.AddEvent("execution.envelope.missing")
		span.SetStatus(otelcodes.Error, "missing or invalid execution envelope")
		w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
		reason := "missing or invalid execution envelope for persisted run"
		if err := w.markRunFailedWithRetry(runID, dal.FailureCodeInvalidEnvelope, reason); err != nil {
			w.logger.Error("Failed to mark run %s failed after missing execution envelope: %v", runID, err)
			span.RecordError(err)
		}

		span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeFailed))
		return observability.WorkerOutcomeFailed
	}

	executionClaimToken, executionClaimed, executionClaimErr := w.tryClaimExecution(ctx, executionEnvelope, leaseUntil)
	if executionClaimErr != nil {
		span.SetStatus(otelcodes.Error, "claim execution")
		w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
		if err := w.markRunOrphanedWithRetry(runID, dal.OrphanReasonAckUncertain); err != nil {
			w.logger.Error("Failed to mark run %s orphaned after execution claim failure: %v", runID, err)
			span.RecordError(err)
		}

		span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeFailed))
		return observability.WorkerOutcomeFailed
	}
	if !executionClaimed {
		span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeSkippedUnclaimed))
		return observability.WorkerOutcomeSkippedUnclaimed
	}

	w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusRunning})
	w.setLifecyclePhase(observability.WorkerPhaseExecuting)
	execErr := w.executeWithLeaseRenewal(ctx, runID, executionClaimToken, job, executionEnvelope)
	if execErr != nil {
		if errors.Is(execErr, errRunCancelled) {
			span.AddEvent("run.cancelled")
			span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeAborted))
			w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
			return w.finalizeAbortedTaskRunByExecutionClaim(ctx, executionClaimToken, dal.CancelReasonAPI, executionEnvelope)
		}

		w.logger.Error("Job %s failed: %v", jobID, execErr)
		span.RecordError(execErr)
		span.SetStatus(otelcodes.Error, "execute with lease renewal")
		decision := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerExecutionResult})
		reason := truncateFailureReason(execErr.Error())
		w.setLifecyclePhase(observability.WorkerPhaseFinalizing)

		return w.finalizeFailedTaskRunByExecutionClaim(ctx, executionClaimToken, decision.FailureCode, reason, executionEnvelope)
	}

	w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
	return w.finalizeSucceededTaskRunByExecutionClaim(ctx, jobID, runID, executionClaimToken, executionEnvelope)
}

func (w *worker) finalizeFailedTaskRunByExecutionClaim(ctx context.Context, executionClaimToken, failureCode, reason string, executionEnvelope *cell.ExecutionEnvelope) string {
	span := trace.SpanFromContext(ctx)

	result, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, executionEnvelope, executionClaimToken, dal.ExecutionStatusFailed, failureCode, reason)
	if !ok {
		span.SetStatus(otelcodes.Error, "complete failed task execution by claim")
		return observability.WorkerOutcomeFailed
	}

	reduceDecision := taskreduce.Decide(result.Summary)
	w.recordTaskReduceDecision(ctx, reduceDecision, nil)
	w.recordTaskFinalizeDecision(ctx, taskfinalize.ExecutionFailed(reduceDecision))

	if result.Outcome != dal.ExecutionFinalizationOutcomeRunFailed {
		err := fmt.Errorf("failed execution finalization produced outcome %q", result.Outcome)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "unexpected failed execution finalization outcome")
		return observability.WorkerOutcomeFailed
	}

	span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeFailed))
	return observability.WorkerOutcomeFailed
}

func (w *worker) finalizeAbortedTaskRunByExecutionClaim(ctx context.Context, executionClaimToken, reason string, executionEnvelope *cell.ExecutionEnvelope) string {
	span := trace.SpanFromContext(ctx)

	result, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, executionEnvelope, executionClaimToken, dal.ExecutionStatusAborted, "", reason)
	if !ok {
		span.SetStatus(otelcodes.Error, "complete aborted task execution by claim")
		return observability.WorkerOutcomeFailed
	}

	w.recordTaskFinalizeDecision(ctx, taskfinalize.ExecutionAborted())

	if result.Outcome != dal.ExecutionFinalizationOutcomeRunCancelled {
		err := fmt.Errorf("aborted execution finalization produced outcome %q", result.Outcome)
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, "unexpected aborted execution finalization outcome")
		return observability.WorkerOutcomeFailed
	}

	return observability.WorkerOutcomeAborted
}

func (w *worker) finalizeSucceededTaskRunByExecutionClaim(ctx context.Context, jobID, runID, executionClaimToken string, executionEnvelope *cell.ExecutionEnvelope) string {
	span := trace.SpanFromContext(ctx)

	result, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, executionEnvelope, executionClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if !ok {
		span.SetStatus(otelcodes.Error, "complete succeeded task execution by claim")
		return observability.WorkerOutcomeFailed
	}

	reduceDecision := taskreduce.Decide(result.Summary)
	w.recordTaskReduceDecision(ctx, reduceDecision, nil)
	switch result.Outcome {
	case dal.ExecutionFinalizationOutcomeRunSucceeded:
		finalizeDecision := taskfinalize.Decide(false, reduceDecision)
		w.recordTaskFinalizeDecision(ctx, finalizeDecision)
		span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeSuccess))
		w.logger.Info("Job completed successfully: %s", jobID)
		return observability.WorkerOutcomeSuccess
	case dal.ExecutionFinalizationOutcomeRunFailed:
		finalizeDecision := taskfinalize.Decide(false, reduceDecision)
		w.recordTaskFinalizeDecision(ctx, finalizeDecision)
		span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeFailed))
		w.logger.Info("Task run reduced to failed: %s", jobID)
		return observability.WorkerOutcomeFailed
	case dal.ExecutionFinalizationOutcomeContinued, dal.ExecutionFinalizationOutcomeWaiting:
		knownPending := result.Outcome == dal.ExecutionFinalizationOutcomeContinued
		continued, err := w.drainQueuedTaskRunContinuation(ctx, runID, knownPending)
		if err != nil {
			w.logger.Error("Failed to continue task run %s: %v", runID, err)
			span.RecordError(err)
			span.SetStatus(otelcodes.Error, "continue task run")
			return observability.WorkerOutcomeFailed
		}

		finalizeDecision := taskfinalize.Decide(continued, reduceDecision)
		w.recordTaskFinalizeDecision(ctx, finalizeDecision)
		span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeSuccess))
		w.logger.Info("Task run has incomplete work; run queued for continuation: %s", jobID)
		return observability.WorkerOutcomeSuccess
	default:
		span.RecordError(fmt.Errorf("unsupported execution finalization outcome %q", result.Outcome))
		span.SetStatus(otelcodes.Error, "unsupported execution finalization outcome")
		return observability.WorkerOutcomeFailed
	}
}

func (w *worker) recordTaskReduceDecision(ctx context.Context, decision taskreduce.Decision, err error) {
	if w.taskFinalizeMetrics == nil {
		return
	}

	w.taskFinalizeMetrics.RecordReduce(ctx, decision, err)
}

func (w *worker) recordTaskFinalizeDecision(ctx context.Context, decision taskfinalize.Decision) {
	taskfinalize.RecordDecision(ctx, decision)
	if w.taskFinalizeMetrics == nil {
		return
	}

	w.taskFinalizeMetrics.RecordFinalize(ctx, decision)
}

func executionEnvelopeAttrs(env *cell.ExecutionEnvelope) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("vectis.cell.id", env.CellID),
		attribute.String("vectis.namespace.path", env.NamespacePath),
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

func executionWorkloadIdentity(env *cell.ExecutionEnvelope) (*workloadidentity.Identity, error) {
	if !config.WorkerExecutionIdentityEnabled() {
		return nil, nil
	}

	if env == nil {
		return nil, fmt.Errorf("worker execution identity is enabled but execution envelope is missing")
	}

	return workloadidentity.NewIdentity(
		config.WorkerExecutionIdentityTrustDomain(),
		config.WorkerExecutionIdentityPathTemplate(),
		workloadidentity.Execution{
			CellID:            env.CellID,
			NamespacePath:     env.NamespacePath,
			JobID:             env.Job.GetId(),
			RunID:             env.RunID,
			RunIndex:          env.RunIndex,
			SegmentID:         env.SegmentID,
			ExecutionID:       env.ExecutionID,
			Attempt:           env.Attempt,
			DefinitionVersion: env.DefinitionVersion,
			DefinitionHash:    env.DefinitionHash,
		},
	)
}

func (w *worker) acquireExecutionSVID(ctx context.Context, identity *workloadidentity.Identity) (*workloadidentity.Identity, error) {
	if !config.WorkerSPIREEnabled() {
		return identity, nil
	}

	if identity == nil {
		if w.metrics != nil {
			w.metrics.RecordSPIRESVIDCheck(ctx, observability.WorkerSPIRESVIDOutcomeFailed, observability.WorkerSPIRESVIDReasonMissingIdentity)
		}

		return identity, fmt.Errorf("worker SPIRE execution SVID is required but execution identity is missing")
	}

	source := w.spireSVIDSource
	if source == nil {
		if w.metrics != nil {
			w.metrics.RecordSPIRESVIDCheck(ctx, observability.WorkerSPIRESVIDOutcomeFailed, observability.WorkerSPIRESVIDReasonMissingSource)
		}

		return identity, fmt.Errorf("worker SPIRE execution SVID is required but SPIRE source is not configured")
	}

	checkCtx := ctx
	cancel := func() {}
	if timeout := config.WorkerSPIREFetchTimeout(); timeout > 0 {
		checkCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	svid, err := spire.FetchX509SVID(checkCtx, source, identity.SPIFFEID)
	if err != nil {
		if w.metrics != nil {
			w.metrics.RecordSPIRESVIDCheck(ctx, observability.WorkerSPIRESVIDOutcomeFailed, workerSPIRESVIDFailureReason(err))
		}

		return identity, fmt.Errorf("worker SPIRE execution SVID: %w", err)
	}

	if w.metrics != nil {
		w.metrics.RecordSPIRESVIDCheck(ctx, observability.WorkerSPIRESVIDOutcomeSuccess, observability.WorkerSPIRESVIDReasonMatched)
	}

	return identity.WithX509SVID(workloadidentity.X509SVID{SPIFFEID: svid.SPIFFEID}), nil
}

func workerSPIRESVIDFailureReason(err error) string {
	switch {
	case errors.Is(err, spire.ErrExpectedSPIFFEIDInvalid):
		return observability.WorkerSPIRESVIDReasonInvalidExpectedID
	case errors.Is(err, spire.ErrNoMatchingX509SVID):
		return observability.WorkerSPIRESVIDReasonMismatch
	case errors.Is(err, spire.ErrX509SVIDSourceRequired):
		return observability.WorkerSPIRESVIDReasonMissingSource
	case errors.Is(err, context.DeadlineExceeded):
		return observability.WorkerSPIRESVIDReasonSourceTimeout
	case errors.Is(err, context.Canceled):
		return observability.WorkerSPIRESVIDReasonCanceled
	default:
		return observability.WorkerSPIRESVIDReasonSourceError
	}
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

func (w *worker) completeExecutionAndFinalizeRunByClaim(ctx context.Context, env *cell.ExecutionEnvelope, executionClaimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, bool) {
	if env == nil {
		return dal.ExecutionFinalizationResult{}, true
	}

	completionCtx := trace.ContextWithSpan(w.runCtx, trace.SpanFromContext(ctx))
	result, err := w.completeExecutionAndFinalizeRunByClaimWithRetry(completionCtx, env.ExecutionID, executionClaimToken, status, failureCode, reason)
	if err != nil {
		w.logger.Warn("CompleteExecutionAndFinalizeRunByClaim execution %s status %s failed: %v", env.ExecutionID, status, err)
		trace.SpanFromContext(ctx).RecordError(err)
		return dal.ExecutionFinalizationResult{}, false
	}

	job.RecordTaskCompletion(ctx, job.TaskCompletionResult{
		ExecutionID: env.ExecutionID,
		Status:      status,
		Children:    result.Children,
		Activated:   result.Activated,
	})

	w.recordExecutionCatalogEvent(ctx, env, status)
	w.recordRunCatalogEventForExecutionFinalization(result, failureCode, reason)
	trace.SpanFromContext(ctx).AddEvent("execution.finalized", trace.WithAttributes(
		append(
			executionEnvelopeAttrs(env),
			attribute.String("vectis.execution.status", status),
			attribute.String("vectis.execution.finalization.outcome", string(result.Outcome)),
			attribute.Int("vectis.task.children.activated", result.Activated),
			attribute.Int("vectis.task.children.dispatchable", len(result.Children)),
			attribute.Int("vectis.task.total", result.Summary.Total),
			attribute.Int("vectis.task.succeeded", result.Summary.Succeeded),
			attribute.Int("vectis.task.terminal_failed", result.Summary.TerminalFailed),
			attribute.Int("vectis.task.incomplete", result.Summary.Incomplete),
		)...,
	))

	return result, true
}

func (w *worker) completeExecutionAndFinalizeRunByClaimWithRetry(ctx context.Context, executionID, executionClaimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	var lastErr error
	for attempt := 1; attempt <= finalizeMaxAttempts; attempt++ {
		result, err := w.store.CompleteExecutionAndFinalizeRunByClaim(ctx, executionID, w.workerID, executionClaimToken, status, failureCode, reason)
		if err == nil {
			w.noteDBRecovered()
			return result, nil
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
		w.logger.Warn("CompleteExecutionAndFinalizeRunByClaim execution %s status %s failed (attempt %d/%d): %v; retrying in %v",
			executionID, status, attempt, finalizeMaxAttempts, err, delay)

		if sleepErr := w.clock.Sleep(w.runCtx, delay); sleepErr != nil {
			return dal.ExecutionFinalizationResult{}, sleepErr
		}
	}

	return dal.ExecutionFinalizationResult{}, lastErr
}

func (w *worker) recordRunCatalogEventForExecutionFinalization(result dal.ExecutionFinalizationResult, failureCode, reason string) {
	switch result.Outcome {
	case dal.ExecutionFinalizationOutcomeRunSucceeded:
		w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: result.RunID, Status: dal.RunStatusSucceeded})
	case dal.ExecutionFinalizationOutcomeRunFailed:
		if failureCode == "" {
			failureCode = dal.FailureCodeExecution
		}

		if reason == "" {
			reason = taskfinalize.FailureReason(taskfinalize.Decision{Reduce: taskreduce.Decide(result.Summary)})
		}

		w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: result.RunID, Status: dal.RunStatusFailed, FailureCode: failureCode, Reason: reason})
	case dal.ExecutionFinalizationOutcomeRunCancelled:
		if reason == "" {
			reason = dal.CancelReasonAPI
		}

		w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: result.RunID, Status: dal.RunStatusCancelled, Reason: reason})
	}
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

func (w *worker) drainQueuedTaskRunContinuation(ctx context.Context, runID string, knownPending bool) (bool, error) {
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

func (w *worker) markRunFailedWithRetry(runID, failureCode, reason string) error {
	var lastErr error
	for attempt := 1; attempt <= finalizeMaxAttempts; attempt++ {
		err := w.store.MarkRunFailed(w.runCtx, runID, failureCode, reason)
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

func (w *worker) markRunOrphanedWithRetry(runID, reason string) error {
	var lastErr error
	for attempt := 1; attempt <= finalizeMaxAttempts; attempt++ {
		err := w.store.MarkRunOrphaned(w.runCtx, runID, reason)
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

func (w *worker) tryClaimExecution(ctx context.Context, executionEnvelope *cell.ExecutionEnvelope, leaseUntil time.Time) (string, bool, error) {
	if executionEnvelope == nil || w.store == nil {
		return "", false, nil
	}

	span := trace.SpanFromContext(ctx)
	span.AddEvent("execution.claim.attempt", trace.WithAttributes(executionEnvelopeAttrs(executionEnvelope)...))
	claim, err := w.store.TryClaimExecution(w.runCtx, executionEnvelope.ExecutionID, w.workerID, leaseUntil)
	if err != nil {
		w.noteDBError(err)
		w.logger.Warn("TryClaimExecution %s failed; stopping before task execution: %v", executionEnvelope.ExecutionID, err)
		span.RecordError(err)
		span.AddEvent("execution.claim.error", trace.WithAttributes(attribute.String("error", err.Error())))
		return "", false, err
	}

	w.noteDBRecovered()
	if claim.Expired {
		w.logger.Warn("Execution %s expired before claim; stopping before task execution", executionEnvelope.ExecutionID)
		w.recordExecutionCatalogEvent(ctx, executionEnvelope, dal.ExecutionStatusFailed)
		w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: executionEnvelope.RunID, Status: dal.RunStatusFailed, FailureCode: dal.FailureCodeDispatchExpired, Reason: "execution was not started before dispatch deadline"})
		span.AddEvent("execution.claim.expired")
		return "", false, nil
	}

	if !claim.Claimed {
		w.logger.Warn("Execution %s not claimed; stopping before task execution", executionEnvelope.ExecutionID)
		span.AddEvent("execution.claim.skipped")
		return "", false, nil
	}

	span.AddEvent("execution.claim.success")
	if claim.TransitionedToAccepted {
		w.recordExecutionCatalogEvent(ctx, executionEnvelope, dal.ExecutionStatusAccepted)
		span.AddEvent("execution.accepted", trace.WithAttributes(executionEnvelopeAttrs(executionEnvelope)...))
	}

	return claim.ClaimToken, true, nil
}

func (w *worker) executeWithLeaseRenewal(ctx context.Context, runID, executionClaimToken string, runJob *api.Job, env *cell.ExecutionEnvelope) error {
	w.setCurrentRun(runID, executionClaimToken)
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

	go w.leaseRenewalLoop(execCtx, runID, env, executionClaimToken, stopRenew, doneRenew)

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
		go w.cancelRequestLoop(execCtx, runID, stopCancel, cancelRun)
	}

	workloadIdentity, err := executionWorkloadIdentity(env)
	if err == nil {
		workloadIdentity, err = w.acquireExecutionSVID(execCtx, workloadIdentity)
	}

	if err == nil {
		w.markExecutionStarted(ctx, env)
		err = w.executor.ExecuteTaskWithOptions(execCtx, runJob, env.TaskKey, w.logClient, w.logger, job.ExecuteOptions{
			WorkloadIdentity: workloadIdentity,
			ActionResolver:   w.actionResolver,
			ActionLocks:      env.ActionLocks,
		})
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
	stopCancel <-chan struct{},
	cancelRun func(string),
) {
	interval := w.cancelPollInterval
	if interval <= 0 {
		interval = cancelPollInterval
	}

	check := func() {
		requested, err := w.store.RunCancelRequested(w.runCtx, runID)
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
	executionEnvelope *cell.ExecutionEnvelope,
	executionClaimToken string,
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
			next := w.leaseDeadline()
			if executionEnvelope != nil && executionClaimToken != "" {
				if err := w.store.RenewExecutionLease(w.runCtx, executionEnvelope.ExecutionID, w.workerID, executionClaimToken, next); err != nil {
					w.noteDBError(err)
					renewFailed = true
					w.logger.Warn("Execution %s: lease renew failed (will retry): %v", executionEnvelope.ExecutionID, err)
					continue
				}
				w.noteDBRecovered()
			}

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
	Long:  `The Vectis Worker executes envelope-backed task deliveries from the queue using the action system.`,
	Run:   runWorker,
}

func init() {
	cli.ConfigureVersion(rootCmd)
	viper.SetDefault("metrics_host", config.WorkerMetricsHost())
	viper.SetDefault("metrics_port", config.WorkerMetricsPort())

	rootCmd.PersistentFlags().String("metrics-host", config.WorkerMetricsHost(), "Host/IP for the Prometheus /metrics HTTP server to bind")
	rootCmd.PersistentFlags().Int("metrics-port", config.WorkerMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().String("execution-backend", config.WorkerExecutionBackend(), "Command execution backend: host or lima")
	rootCmd.PersistentFlags().String("workspace-root", config.WorkerExecutionWorkspaceRoot(), "Parent directory for automatically-created run workspaces")
	rootCmd.PersistentFlags().String("lima-path", config.WorkerExecutionLimaPath(), "Path to limactl when --execution-backend=lima")
	rootCmd.PersistentFlags().String("lima-instance", config.WorkerExecutionLimaInstance(), "Lima instance name when --execution-backend=lima")
	rootCmd.PersistentFlags().String("lima-guest-workspace-root", config.WorkerExecutionLimaGuestWorkspaceRoot(), "Guest-side parent directory for Lima workspaces")
	rootCmd.PersistentFlags().Bool("lima-start", config.WorkerExecutionLimaStart(), "Start the Lima instance before each command when --execution-backend=lima")
	rootCmd.PersistentFlags().Bool("lima-preserve-env", config.WorkerExecutionLimaPreserveEnv(), "Preserve host environment variables in Lima shell commands")
	_ = viper.BindPFlag("metrics_host", rootCmd.PersistentFlags().Lookup("metrics-host"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("worker.execution.backend", rootCmd.PersistentFlags().Lookup("execution-backend"))
	_ = viper.BindPFlag("worker.execution.workspace_root", rootCmd.PersistentFlags().Lookup("workspace-root"))
	_ = viper.BindPFlag("worker.execution.lima.path", rootCmd.PersistentFlags().Lookup("lima-path"))
	_ = viper.BindPFlag("worker.execution.lima.instance", rootCmd.PersistentFlags().Lookup("lima-instance"))
	_ = viper.BindPFlag("worker.execution.lima.guest_workspace_root", rootCmd.PersistentFlags().Lookup("lima-guest-workspace-root"))
	_ = viper.BindPFlag("worker.execution.lima.start", rootCmd.PersistentFlags().Lookup("lima-start"))
	_ = viper.BindPFlag("worker.execution.lima.preserve_env", rootCmd.PersistentFlags().Lookup("lima-preserve-env"))
	_ = viper.BindEnv("worker.queue.address", "VECTIS_WORKER_QUEUE_ADDRESS")
	_ = viper.BindEnv("worker.log.address", "VECTIS_WORKER_LOG_ADDRESS")
	_ = viper.BindEnv("worker.registry.address", "VECTIS_WORKER_REGISTRY_ADDRESS")
	_ = viper.BindEnv("worker.control.mode", "VECTIS_WORKER_CONTROL_MODE")
	_ = viper.BindEnv("control_port", "VECTIS_WORKER_CONTROL_PORT")
	_ = viper.BindEnv("control_port_min", "VECTIS_WORKER_CONTROL_PORT_MIN")
	_ = viper.BindEnv("control_port_max", "VECTIS_WORKER_CONTROL_PORT_MAX")
	_ = viper.BindEnv("worker.execution.backend", "VECTIS_WORKER_EXECUTION_BACKEND")
	_ = viper.BindEnv("worker.execution.workspace_root", "VECTIS_WORKER_WORKSPACE_ROOT")
	_ = viper.BindEnv("worker.execution.lima.path", "VECTIS_WORKER_LIMA_PATH")
	_ = viper.BindEnv("worker.execution.lima.instance", "VECTIS_WORKER_LIMA_INSTANCE")
	_ = viper.BindEnv("worker.execution.lima.guest_workspace_root", "VECTIS_WORKER_LIMA_GUEST_WORKSPACE_ROOT")
	_ = viper.BindEnv("worker.execution.lima.start", "VECTIS_WORKER_LIMA_START")
	_ = viper.BindEnv("worker.execution.lima.preserve_env", "VECTIS_WORKER_LIMA_PRESERVE_ENV")

	viper.SetEnvPrefix("VECTIS_WORKER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
