package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
	"google.golang.org/protobuf/proto"

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
	"vectis/internal/orchestrator"
	"vectis/internal/queueclient"
	"vectis/internal/registry"
	"vectis/internal/resolver"
	"vectis/internal/runpolicy"
	"vectis/internal/secrets"
	"vectis/internal/spire"
	"vectis/internal/taskfinalize"
	"vectis/internal/taskreduce"
	"vectis/internal/utils"
	"vectis/internal/workercore"
	"vectis/internal/workloadidentity"
	workersdk "vectis/sdk/workercore"

	"google.golang.org/grpc"
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
	coreCancelTimeout   = 5 * time.Second
)

var errRunCancelled = errors.New("run cancelled")

type executionErrorDisposition string

const (
	executionErrorFailed    executionErrorDisposition = "failed"
	executionErrorCancelled executionErrorDisposition = "cancelled"
	executionErrorOrphaned  executionErrorDisposition = "orphaned"
)

type executionErrorDecision struct {
	disposition         executionErrorDisposition
	failureCode         string
	reason              string
	workerCoreOutcome   string
	workerCoreReason    string
	workerCoreResultErr *workercore.TaskResultError
}

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

	if err := config.ValidateWorkerSPIFFEConfig(); err != nil {
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

	taskFinalizeMetrics, err := observability.NewTaskFinalizeMetrics()
	if err != nil {
		logger.Fatal("Failed to initialize task finalize metrics: %v", err)
	}

	executionCore, coreDescription, coreCleanup, err := configuredWorkerCore(shutdownCtx, logger)
	if err != nil {
		logger.Fatal("Failed to configure worker core: %v", err)
	}
	defer coreCleanup()

	coreShell, coreShellEndpoint, coreShellCleanup, err := startWorkerCoreShell(shutdownCtx, logger)
	if err != nil {
		logger.Fatal("Failed to start worker core shell: %v", err)
	}
	defer coreShellCleanup()

	defer cli.DeferShutdown(logger, "Metrics", shutdownMetrics)()

	metricsAddr := config.WorkerMetricsListenAddr()
	metricsSrv, err := cli.StartMetricsHTTPServer(metricsHandler, metricsAddr, "Worker", logger)
	if err != nil {
		logger.Fatal("%v", err)
	}
	defer metricsSrv.Shutdown()

	repos := dal.NewSQLRepositoriesWithCellID(db, config.CellID())
	runsRepo := repos.Runs()
	dequeueSupportedIsolation := coreDescription.SupportedIsolation
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

	orchestratorConn, stopOrchestrator, err := resolver.DialOrchestrator(shutdownCtx, logger, config.PinnedOrchestratorAddress(), config.WorkerRegistryDialAddress(), retryMetrics)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			logger.Info("Worker graceful shutdown before connecting to orchestrator service")
			return
		}

		logger.Fatal("Failed to connect to orchestrator service: %v", err)
	}
	defer stopOrchestrator()

	logClient := interfaces.LogClient(clients)

	// Prefer the local log-forwarder Unix socket when available.
	// The PreferForwarderLogClient dynamically checks the socket before
	// each StreamLogs, so if the forwarder crashes the worker falls back
	// to direct gRPC automatically.
	forwarderSocket := forwarderSocketPath()
	logClient = interfaces.NewPreferForwarderLogClient(forwarderSocket, logClient)

	secretResolverForWorkload, err := newSecretsResolverFactory(logger)
	if err != nil {
		logger.Fatal("Failed to configure secrets service client: %v", err)
	}

	var spiffeSVIDSource spire.X509SVIDSource
	if config.WorkerSPIFFEEnabled() {
		src, err := spire.NewWorkloadAPISource(config.WorkerSPIFFEWorkloadAPIAddress())
		if err != nil {
			logger.Fatal("Failed to configure SPIFFE Workload API source: %v", err)
		}

		spiffeSVIDSource = src
	}

	actionResolver, err := actionconfig.DescriptorResolver()
	if err != nil {
		logger.Fatal("Invalid action registry config: %v", err)
	}

	var spiffeRegistrar spire.Registrar
	var spiffeRegistrarCleanup func()
	var spiffeRegistrationSelectors []spire.Selector
	if config.WorkerSPIFFERegistrationEnabled() {
		selectors, err := config.WorkerSPIFFERegistrationSelectors()
		if err != nil {
			logger.Fatal("Failed to configure SPIFFE registration selectors: %v", err)
		}

		registrar, cleanup, err := spire.DialSPIREServerRegistrar(
			config.WorkerSPIFFERegistrationServerAddress(),
			spire.WithSPIREServerX509SVIDTTL(config.WorkerSPIFFERegistrationX509SVIDTTL()),
		)

		if err != nil {
			logger.Fatal("Failed to configure SPIFFE registration server client: %v", err)
		}

		spiffeRegistrar = registrar
		spiffeRegistrarCleanup = cleanup
		spiffeRegistrationSelectors = selectors
		logger.Info("Configured SPIFFE registration via Entry API at %s", config.WorkerSPIFFERegistrationServerAddress())
	}

	if spiffeRegistrarCleanup != nil {
		defer spiffeRegistrarCleanup()
	}

	w := &worker{
		ctx:                         shutdownCtx,
		runCtx:                      runCtx,
		logger:                      logger,
		workerID:                    workerID,
		cellID:                      config.CellID(),
		clock:                       interfaces.SystemClock{},
		renewInterval:               dal.DefaultRenewInterval,
		queue:                       clients,
		logClient:                   logClient,
		core:                        executionCore,
		coreShell:                   coreShell,
		coreShellEndpoint:           coreShellEndpoint,
		actionResolver:              actionResolver,
		store:                       runsRepo,
		artifactManifests:           repos.Artifacts(),
		artifactMaxBytes:            config.WorkerArtifactMaxBytes(),
		artifactMaxRunBytes:         config.WorkerArtifactMaxRunBytes(),
		artifactMaxCount:            config.WorkerArtifactMaxCount(),
		retryMetrics:                retryMetrics,
		choreographer:               newGRPCExecutionChoreographer(api.NewOrchestratorServiceClient(orchestratorConn)),
		secretResolverForWorkload:   secretResolverForWorkload,
		catalog:                     cell.NewCatalogEventPublisher(config.CellID(), repos.CatalogEvents()),
		metrics:                     workerMetrics,
		taskFinalizeMetrics:         taskFinalizeMetrics,
		spiffeSVIDSource:            spiffeSVIDSource,
		spiffeRegistrar:             spiffeRegistrar,
		spiffeRegistrationParentID:  config.WorkerSPIFFERegistrationParentID(),
		spiffeRegistrationSelectors: spiffeRegistrationSelectors,
		spiffeRegistrationMinTTL:    config.WorkerSPIFFERegistrationMinTTL(),
		spiffeRegistrationMaxTTL:    config.WorkerSPIFFERegistrationMaxTTL(),
		cancelCh:                    make(chan string, 1),
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
				Metadata:        workerRegistryMetadata(coreDescription),
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

func configuredWorkerCore(ctx context.Context, logger interfaces.Logger) (workercore.Core, workercore.CoreDescription, func(), error) {
	socketPath := strings.TrimSpace(viper.GetString("worker.core.socket"))
	if socketPath == "" {
		socketPath = workercore.DefaultCoreSocketPath()
	}

	connectTimeout := viper.GetDuration("worker.core.connect_timeout")
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}

	dialCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	core, cleanup, err := workercore.DialUnixCore(dialCtx, socketPath)
	if err != nil {
		return nil, workercore.CoreDescription{}, nil, err
	}

	desc, err := core.Describe(dialCtx)
	if err != nil {
		cleanup()
		return nil, workercore.CoreDescription{}, nil, err
	}

	if err := workercore.ValidateCoreDescription(desc, workercore.RequiredWorkerCoreCapabilities()); err != nil {
		cleanup()
		return nil, workercore.CoreDescription{}, nil, err
	}

	if logger != nil {
		logger.Info("Worker core: socket=%s protocol=%s", socketPath, desc.ProtocolVersion)
	}

	return core, desc, cleanup, nil
}

func workerRegistryMetadata(desc workercore.CoreDescription) map[string]string {
	backend := desc.Metadata[registry.MetadataWorkerExecutionBackend]
	defaultIsolation := desc.Metadata[registry.MetadataWorkerDefaultIsolation]
	return registry.WorkerExecutionMetadataForCell(config.CellID(), backend, defaultIsolation, desc.SupportedIsolation)
}

func startWorkerCoreShell(ctx context.Context, logger interfaces.Logger) (*workercore.ShellServer, string, func(), error) {
	socketPath := strings.TrimSpace(viper.GetString("worker.core.shell_socket"))
	if socketPath == "" {
		socketPath = workercore.DefaultShellSocketPath()
	}

	socketPath, err := workercore.SocketPathFromEndpoint(socketPath)
	if err != nil {
		return nil, "", nil, err
	}

	shell := workercore.NewShellServer()
	grpcServer, listener, err := workercore.NewUnixShellServer(socketPath, shell)
	if err != nil {
		return nil, "", nil, err
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil && ctx.Err() == nil {
			logger.Warn("Worker core shell server stopped: %v", err)
		}
	}()

	cleanup := func() {
		grpcServer.Stop()
		_ = os.Remove(socketPath)
	}

	if logger != nil {
		logger.Info("Worker core shell listening on %s", socketPath)
	}

	return shell, workercore.UnixEndpoint(socketPath), cleanup, nil
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
	ctx                         context.Context // canceled on SIGINT/SIGTERM; dequeue and between-job backoff only
	runCtx                      context.Context // Background; execution, lease renew, ack, finalize survive SIGTERM until dequeue stops
	logger                      interfaces.Logger
	workerID                    string
	cellID                      string
	clock                       interfaces.Clock
	renewInterval               time.Duration
	cancelPollInterval          time.Duration
	queue                       interfaces.QueueClient
	logClient                   interfaces.LogClient
	core                        workercore.Core
	coreShell                   *workercore.ShellServer
	coreShellEndpoint           string
	actionResolver              actionregistry.Resolver
	store                       dal.RunsRepository
	artifactManifests           dal.ArtifactsRepository
	artifactMaxBytes            int64
	artifactMaxRunBytes         int64
	artifactMaxCount            int64
	retryMetrics                backoff.RetryMetrics
	choreographer               executionChoreographer
	secretResolver              secrets.Resolver
	secretResolverForWorkload   secretResolverFactory
	catalog                     cell.CatalogEventPublisher
	metrics                     *observability.WorkerMetrics
	taskFinalizeMetrics         *observability.TaskFinalizeMetrics
	spiffeSVIDSource            spire.X509SVIDSource
	spiffeRegistrar             spire.Registrar
	spiffeRegistrationParentID  string
	spiffeRegistrationSelectors []spire.Selector
	spiffeRegistrationMinTTL    time.Duration
	spiffeRegistrationMaxTTL    time.Duration
	dequeueFailAttempt          int
	dbUnavailable               bool
	dbFailAttempt               int
	dbMu                        sync.Mutex
	cancelCh                    chan string
	currentRunID                string
	currentClaimToken           string
	currentMu                   sync.Mutex
}

type secretResolverFactory func(*workloadidentity.Identity) (secrets.Resolver, func(), error)

type executionSPIFFERegistration struct {
	identity *workloadidentity.Identity
	env      *cell.ExecutionEnvelope
	handle   spire.RegistrationHandle
}

func newSecretsResolverFactory(logger interfaces.Logger) (secretResolverFactory, error) {
	addr := strings.TrimSpace(config.WorkerSecretsAddress())
	if secretsAddressDisabled(addr) {
		return nil, nil
	}

	if logger != nil {
		logger.Info("Configured workload-authenticated secrets service client for %s", addr)
	}

	return func(workload *workloadidentity.Identity) (secrets.Resolver, func(), error) {
		if workload == nil || workload.X509SVID == nil {
			return nil, nil, fmt.Errorf("workload X.509-SVID is required for secret resolution")
		}

		clientCert, err := workload.X509SVID.TLSCertificate()
		if err != nil {
			return nil, nil, err
		}

		dialOpts, err := config.GRPCClientDialOptionsWithClientCertificate(addr, *clientCert)
		if err != nil {
			return nil, nil, fmt.Errorf("grpc tls: %w", err)
		}

		conn, err := grpc.NewClient(addr, dialOpts...)
		if err != nil {
			return nil, nil, fmt.Errorf("dial %s: %w", addr, err)
		}

		return secrets.NewGRPCResolver(conn), func() { _ = conn.Close() }, nil
	}, nil
}

func secretsAddressDisabled(addr string) bool {
	switch strings.ToLower(strings.TrimSpace(addr)) {
	case "", "disabled", "none", "off", "-":
		return true
	default:
		return false
	}
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

func (w *worker) executionChoreographer() executionChoreographer {
	if w.choreographer != nil {
		return w.choreographer
	}

	return missingExecutionChoreographer{}
}

type missingExecutionChoreographer struct{}

func (missingExecutionChoreographer) LoadRun(context.Context, *api.Job, *cell.ExecutionEnvelope, []orchestrator.TaskExecutionSnapshot) error {
	return errors.New("execution choreographer is not configured")
}

func (missingExecutionChoreographer) ClaimAndStartExecution(context.Context, *cell.ExecutionEnvelope, string, time.Time) (dal.ExecutionClaimResult, error) {
	return dal.ExecutionClaimResult{}, errors.New("execution choreographer is not configured")
}

func (missingExecutionChoreographer) RenewExecutionLease(context.Context, *cell.ExecutionEnvelope, string, string, time.Time) error {
	return errors.New("execution choreographer is not configured")
}

func (missingExecutionChoreographer) CompleteExecution(context.Context, *cell.ExecutionEnvelope, string, string, string, string, string) (dal.ExecutionFinalizationResult, error) {
	return dal.ExecutionFinalizationResult{}, errors.New("execution choreographer is not configured")
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

	if err := w.prepareRunForExecution(ctx, job, executionEnvelope); err != nil {
		span.SetStatus(otelcodes.Error, "prepare run")
		span.RecordError(err)
		w.logger.Error("Failed to prepare run %s for orchestrator execution: %v", runID, err)
		w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
		if markErr := w.markRunOrphanedWithRetry(runID, dal.OrphanReasonAckUncertain); markErr != nil {
			w.logger.Error("Failed to mark run %s orphaned after orchestrator prepare failure: %v", runID, markErr)
			span.RecordError(markErr)
		}

		span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeFailed))
		return observability.WorkerOutcomeFailed
	}

	executionClaimToken, executionClaimed, executionStarted, executionClaimErr := w.tryClaimExecution(ctx, executionEnvelope, leaseUntil)
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

	if !executionClaimed && executionEnvelope.TaskKey != dal.RootTaskKey {
		recoveredClaim := newExecutionClaimState("")
		recovered, recoverErr := w.recoverOrchestratorExecutionClaim(ctx, job, executionEnvelope, recoveredClaim, leaseUntil, "claim")
		if recoverErr != nil && !errors.Is(recoverErr, dal.ErrConflict) {
			span.SetStatus(otelcodes.Error, "recover execution claim")
			span.RecordError(recoverErr)
			w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
			if err := w.markRunOrphanedWithRetry(runID, dal.OrphanReasonAckUncertain); err != nil {
				w.logger.Error("Failed to mark run %s orphaned after orchestrator claim recovery failure: %v", runID, err)
				span.RecordError(err)
			}

			span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeFailed))
			return observability.WorkerOutcomeFailed
		}

		if recovered {
			executionClaimToken = recoveredClaim.get()
			executionClaimed = true
			executionStarted = false
		}
	}

	if !executionClaimed {
		span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeSkippedUnclaimed))
		return observability.WorkerOutcomeSkippedUnclaimed
	}

	w.recordRunCatalogEvent(dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusRunning})
	if executionStarted {
		w.recordExecutionStarted(ctx, executionEnvelope)
	} else {
		w.markExecutionStarted(ctx, executionEnvelope)
	}
	executionClaim := newExecutionClaimState(executionClaimToken)
	w.setLifecyclePhase(observability.WorkerPhaseExecuting)
	execErr := w.executeWithLeaseRenewal(ctx, runID, executionClaim, job, executionEnvelope)
	if execErr != nil {
		if errors.Is(execErr, errRunCancelled) {
			span.AddEvent("run.cancelled")
			span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeAborted))
			w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
			return w.finalizeAbortedTaskRunByExecutionClaim(ctx, job, executionClaim, dal.CancelReasonAPI, executionEnvelope)
		}

		execDecision := decideExecutionError(execErr)
		if execDecision.workerCoreResultErr != nil {
			span.SetAttributes(
				attribute.String("worker_core.outcome", execDecision.workerCoreOutcome),
				attribute.String("worker_core.reason_code", execDecision.workerCoreReason),
			)
		}

		switch execDecision.disposition {
		case executionErrorCancelled:
			span.AddEvent("worker_core.cancelled")
			span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeAborted))
			w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
			return w.finalizeAbortedTaskRunByExecutionClaim(ctx, job, executionClaim, execDecision.reason, executionEnvelope)
		case executionErrorOrphaned:
			w.logger.Warn("Job %s worker core outcome is unknown: %v", jobID, execErr)
			span.RecordError(execErr)
			span.SetStatus(otelcodes.Error, "worker core outcome unknown")
			w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
			if err := w.markRunOrphanedWithRetry(runID, execDecision.reason); err != nil {
				w.logger.Error("Failed to mark run %s orphaned after worker core unknown outcome: %v", runID, err)
				span.RecordError(err)
			}

			span.SetAttributes(attribute.String("vectis.worker.outcome", observability.WorkerOutcomeFailed))
			return observability.WorkerOutcomeFailed
		}

		w.logger.Error("Job %s failed: %v", jobID, execErr)
		span.RecordError(execErr)
		span.SetStatus(otelcodes.Error, "execute with lease renewal")
		w.setLifecyclePhase(observability.WorkerPhaseFinalizing)

		return w.finalizeFailedTaskRunByExecutionClaim(ctx, job, executionClaim, execDecision.failureCode, execDecision.reason, executionEnvelope)
	}

	w.setLifecyclePhase(observability.WorkerPhaseFinalizing)
	return w.finalizeSucceededTaskRunByExecutionClaim(ctx, job, jobID, runID, executionClaim, executionEnvelope)
}

func (w *worker) prepareRunForExecution(ctx context.Context, j *api.Job, env *cell.ExecutionEnvelope) error {
	if j == nil || env == nil {
		return nil
	}

	plan, err := job.PlanTaskExecutions(j)
	if err != nil {
		return err
	}

	if len(plan) > 0 && w.store != nil && env.TaskKey == dal.RootTaskKey {
		if _, err := job.EnsurePlannedTaskExecutions(w.runCtx, w.store, env.RunID, plan, env.CellID); err != nil {
			w.noteDBError(err)
			return fmt.Errorf("materialize planned task executions: %w", err)
		}
		w.noteDBRecovered()
	}

	if err := w.executionChoreographer().LoadRun(w.runCtx, j, env, nil); err != nil {
		return fmt.Errorf("load orchestrator run: %w", err)
	}

	trace.SpanFromContext(ctx).AddEvent("orchestrator.run.loaded")
	return nil
}

func (w *worker) finalizeFailedTaskRunByExecutionClaim(ctx context.Context, j *api.Job, executionClaim *executionClaimState, failureCode, reason string, executionEnvelope *cell.ExecutionEnvelope) string {
	span := trace.SpanFromContext(ctx)

	result, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, j, executionEnvelope, executionClaim, dal.ExecutionStatusFailed, failureCode, reason)
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

func decideExecutionError(err error) executionErrorDecision {
	decision := runpolicy.Decide(runpolicy.Input{Trigger: runpolicy.TriggerExecutionResult})
	out := executionErrorDecision{
		disposition: executionErrorFailed,
		failureCode: decision.FailureCode,
		reason:      truncateFailureReason(err.Error()),
	}

	var resultErr *workercore.TaskResultError
	if !errors.As(err, &resultErr) {
		return out
	}

	out.workerCoreResultErr = resultErr
	out.workerCoreOutcome = resultErr.Outcome.String()
	out.workerCoreReason = strings.TrimSpace(resultErr.ReasonCode)

	switch resultErr.Outcome {
	case api.RunOutcome_RUN_OUTCOME_UNKNOWN:
		out.failureCode = ""
		if out.workerCoreReason == workersdk.ReasonCancelled {
			out.disposition = executionErrorCancelled
			out.reason = truncateFailureReason(workerCoreResultReason(resultErr, err))
			return out
		}

		out.disposition = executionErrorOrphaned
		out.reason = truncateFailureReason(workerCoreUnknownOrphanReason(resultErr, err))
	case api.RunOutcome_RUN_OUTCOME_FAILURE:
		out.reason = truncateFailureReason(workerCoreResultReason(resultErr, err))
	}

	return out
}

func workerCoreUnknownOrphanReason(resultErr *workercore.TaskResultError, fallback error) string {
	detail := workerCoreResultReason(resultErr, fallback)
	if detail == "" {
		return dal.OrphanReasonWorkerCoreUnknown
	}

	return dal.OrphanReasonWorkerCoreUnknown + ": " + detail
}

func workerCoreResultReason(resultErr *workercore.TaskResultError, fallback error) string {
	if resultErr == nil {
		if fallback == nil {
			return ""
		}

		return fallback.Error()
	}

	reasonCode := strings.TrimSpace(resultErr.ReasonCode)
	message := strings.TrimSpace(resultErr.Message)

	switch {
	case reasonCode != "" && message != "":
		return reasonCode + ": " + message
	case reasonCode != "":
		return reasonCode
	case message != "":
		return message
	default:
		if fallback == nil {
			return ""
		}

		return fallback.Error()
	}
}

func (w *worker) finalizeAbortedTaskRunByExecutionClaim(ctx context.Context, j *api.Job, executionClaim *executionClaimState, reason string, executionEnvelope *cell.ExecutionEnvelope) string {
	span := trace.SpanFromContext(ctx)

	result, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, j, executionEnvelope, executionClaim, dal.ExecutionStatusAborted, "", reason)
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

func (w *worker) finalizeSucceededTaskRunByExecutionClaim(ctx context.Context, j *api.Job, jobID, runID string, executionClaim *executionClaimState, executionEnvelope *cell.ExecutionEnvelope) string {
	span := trace.SpanFromContext(ctx)

	result, ok := w.completeExecutionAndFinalizeRunByClaim(ctx, j, executionEnvelope, executionClaim, dal.ExecutionStatusSucceeded, "", "")
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
		continued, err := w.dispatchOrchestratorContinuation(ctx, j, executionEnvelope, result, knownPending)
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

func (w *worker) dispatchOrchestratorContinuation(ctx context.Context, j *api.Job, source *cell.ExecutionEnvelope, result dal.ExecutionFinalizationResult, knownPending bool) (bool, error) {
	if len(result.Children) == 0 {
		return false, nil
	}

	if j == nil || source == nil {
		return false, fmt.Errorf("job and source execution envelope are required")
	}

	if w.queue == nil {
		return false, fmt.Errorf("queue client is required")
	}

	enqueued := 0
	for _, child := range result.Children {
		if child.ExecutionID == "" {
			continue
		}

		req := &api.JobRequest{
			Job:      cloneJobForWorker(j),
			Metadata: cloneMetadataForWorker(source.Metadata),
		}

		dispatch := executionDispatchRecordFromTaskExecution(j, source, child)
		if _, err := cell.AttachExecutionEnvelope(req, dispatch, w.now().UnixNano()); err != nil {
			return enqueued > 0, fmt.Errorf("attach child execution envelope %s: %w", child.ExecutionID, err)
		}

		if err := w.queue.Enqueue(w.runCtx, req); err != nil {
			return enqueued > 0, fmt.Errorf("enqueue child execution %s: %w", child.ExecutionID, err)
		}

		enqueued++
	}

	trace.SpanFromContext(ctx).AddEvent("task.dispatch.direct", trace.WithAttributes(
		attribute.Int("vectis.task.dispatch.enqueued", enqueued),
		attribute.Int("vectis.task.children.dispatchable", len(result.Children)),
		attribute.Bool("vectis.task.dispatch.known_pending", knownPending),
	))

	if enqueued == 0 && knownPending {
		return false, fmt.Errorf("orchestrator returned continuation without dispatchable children")
	}

	return enqueued > 0, nil
}

func cloneMetadataForWorker(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}

	return out
}

func executionDispatchRecordFromTaskExecution(j *api.Job, source *cell.ExecutionEnvelope, rec dal.TaskExecutionRecord) dal.ExecutionDispatchRecord {
	return dal.ExecutionDispatchRecord{
		RunID:             rec.RunID,
		JobID:             j.GetId(),
		RunIndex:          source.RunIndex,
		TaskID:            rec.TaskID,
		TaskKey:           rec.TaskKey,
		TaskName:          rec.Name,
		TaskAttemptID:     rec.TaskAttemptID,
		SegmentID:         rec.SegmentID,
		SegmentName:       rec.SegmentName,
		SegmentStatus:     dal.SegmentStatusPending,
		ExecutionID:       rec.ExecutionID,
		ExecutionStatus:   dal.ExecutionStatusPending,
		CellID:            rec.CellID,
		Attempt:           rec.Attempt,
		DefinitionVersion: source.DefinitionVersion,
		DefinitionHash:    source.DefinitionHash,
		OwningCell:        source.CellID,
	}
}

func cloneJobForWorker(j *api.Job) *api.Job {
	if j == nil {
		return nil
	}

	cloned, ok := proto.Clone(j).(*api.Job)
	if !ok {
		return j
	}

	return cloned
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

func executionFromEnvelope(env *cell.ExecutionEnvelope) workloadidentity.Execution {
	if env == nil {
		return workloadidentity.Execution{}
	}

	return workloadidentity.Execution{
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
		executionFromEnvelope(env),
	)
}

func (w *worker) ensureExecutionSPIFFERegistration(ctx context.Context, identity *workloadidentity.Identity, env *cell.ExecutionEnvelope, expiresAt time.Time) (spire.RegistrationHandle, bool, error) {
	if w == nil || w.spiffeRegistrar == nil {
		return spire.RegistrationHandle{}, false, nil
	}

	if ctx == nil {
		ctx = context.Background()
	}

	if identity == nil {
		return spire.RegistrationHandle{}, false, fmt.Errorf("worker SPIFFE registration requires execution identity")
	}

	if env == nil {
		return spire.RegistrationHandle{}, false, fmt.Errorf("worker SPIFFE registration requires execution envelope")
	}

	if expiresAt.IsZero() {
		expiresAt = w.leaseDeadline()
	}

	intent, err := spire.NewExecutionRegistrationIntent(identity.SPIFFEID, executionFromEnvelope(env), spire.ExecutionRegistrationOptions{
		ParentSPIFFEID: w.spiffeRegistrationParentID,
		Selectors:      w.spiffeRegistrationSelectors,
		ExpiresAt:      expiresAt,
		Now:            w.now().UTC(),
		MinTTL:         w.spiffeRegistrationMinTTL,
		MaxTTL:         w.spiffeRegistrationMaxTTL,
	})

	if err != nil {
		return spire.RegistrationHandle{}, false, err
	}

	result, err := w.spiffeRegistrar.EnsureRegistration(ctx, intent)
	if err != nil {
		return spire.RegistrationHandle{}, false, err
	}

	handle := result.Handle
	if handle.Key == "" {
		handle.Key = intent.Key
	}

	if handle.SPIFFEID == "" {
		handle.SPIFFEID = intent.SPIFFEID
	}

	if handle.ExpiresAt.IsZero() {
		handle.ExpiresAt = intent.ExpiresAt
	}
	if result.Created {
		handle.Managed = true
	}

	return handle, true, nil
}

func (w *worker) releaseExecutionSPIFFERegistration(registration *executionSPIFFERegistration) {
	if w == nil || w.spiffeRegistrar == nil || registration == nil {
		return
	}

	handle := registration.handle
	if handle.EntryID == "" && handle.Key == "" && handle.SPIFFEID == "" {
		return
	}

	ctx := w.runCtx
	if ctx == nil {
		ctx = context.Background()
	}

	if err := w.spiffeRegistrar.ReleaseRegistration(ctx, handle); err != nil && w.logger != nil {
		executionID := ""
		if registration.env != nil {
			executionID = registration.env.ExecutionID
		}

		w.logger.Warn("Failed to release SPIFFE registration for execution %s: %v", executionID, err)
	}
}

func (w *worker) acquireExecutionSVID(ctx context.Context, identity *workloadidentity.Identity) (*workloadidentity.Identity, error) {
	if !config.WorkerSPIFFEEnabled() {
		return identity, nil
	}

	if identity == nil {
		if w.metrics != nil {
			w.metrics.RecordSPIFFESVIDCheck(ctx, observability.WorkerSPIFFESVIDOutcomeFailed, observability.WorkerSPIFFESVIDReasonMissingIdentity)
		}

		return identity, fmt.Errorf("worker SPIFFE execution SVID is required but execution identity is missing")
	}

	source := w.spiffeSVIDSource
	if source == nil {
		if w.metrics != nil {
			w.metrics.RecordSPIFFESVIDCheck(ctx, observability.WorkerSPIFFESVIDOutcomeFailed, observability.WorkerSPIFFESVIDReasonMissingSource)
		}

		return identity, fmt.Errorf("worker SPIFFE execution SVID is required but SPIFFE source is not configured")
	}

	checkCtx := ctx
	cancel := func() {}
	if timeout := config.WorkerSPIFFEFetchTimeout(); timeout > 0 {
		checkCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	svid, err := spire.FetchX509SVID(checkCtx, source, identity.SPIFFEID)
	if err != nil {
		if w.metrics != nil {
			w.metrics.RecordSPIFFESVIDCheck(ctx, observability.WorkerSPIFFESVIDOutcomeFailed, workerSPIFFESVIDFailureReason(err))
		}

		return identity, fmt.Errorf("worker SPIFFE execution SVID: %w", err)
	}

	if w.metrics != nil {
		w.metrics.RecordSPIFFESVIDCheck(ctx, observability.WorkerSPIFFESVIDOutcomeSuccess, observability.WorkerSPIFFESVIDReasonMatched)
	}

	return identity.WithX509SVID(workloadidentity.X509SVID{
		SPIFFEID:     svid.SPIFFEID,
		Certificates: svid.Certificates,
		PrivateKey:   svid.PrivateKey,
	}), nil
}

func workerSPIFFESVIDFailureReason(err error) string {
	switch {
	case errors.Is(err, spire.ErrExpectedSPIFFEIDInvalid):
		return observability.WorkerSPIFFESVIDReasonInvalidExpectedID
	case errors.Is(err, spire.ErrNoMatchingX509SVID):
		return observability.WorkerSPIFFESVIDReasonMismatch
	case errors.Is(err, spire.ErrX509SVIDSourceRequired):
		return observability.WorkerSPIFFESVIDReasonMissingSource
	case errors.Is(err, context.DeadlineExceeded):
		return observability.WorkerSPIFFESVIDReasonSourceTimeout
	case errors.Is(err, context.Canceled):
		return observability.WorkerSPIFFESVIDReasonCanceled
	default:
		return observability.WorkerSPIFFESVIDReasonSourceError
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
	w.recordExecutionStarted(ctx, env)
}

func (w *worker) recordExecutionStarted(ctx context.Context, env *cell.ExecutionEnvelope) {
	if env == nil {
		return
	}

	w.recordExecutionCatalogEvent(ctx, env, dal.ExecutionStatusRunning)
	trace.SpanFromContext(ctx).AddEvent("execution.started", trace.WithAttributes(executionEnvelopeAttrs(env)...))
}

func (w *worker) completeExecutionAndFinalizeRunByClaim(ctx context.Context, j *api.Job, env *cell.ExecutionEnvelope, executionClaim *executionClaimState, status, failureCode, reason string) (dal.ExecutionFinalizationResult, bool) {
	if env == nil {
		return dal.ExecutionFinalizationResult{}, true
	}

	completionCtx := trace.ContextWithSpan(w.runCtx, trace.SpanFromContext(ctx))
	result, err := w.completeExecutionEnvelopeWithRetry(completionCtx, j, env, executionClaim, status, failureCode, reason)
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

func (w *worker) completeExecutionEnvelopeWithRetry(ctx context.Context, j *api.Job, env *cell.ExecutionEnvelope, executionClaim *executionClaimState, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	var lastErr error
	recoveries := 0
	for attempt := 1; attempt <= finalizeMaxAttempts; {
		result, err := w.executionChoreographer().CompleteExecution(ctx, env, w.workerID, executionClaim.get(), status, failureCode, reason)
		if err == nil {
			return result, nil
		}

		lastErr = err
		if isOrchestratorNotFound(err) && recoveries < finalizeMaxAttempts {
			recoveries++
			recovered, recoverErr := w.recoverOrchestratorExecutionClaim(ctx, j, env, executionClaim, w.leaseDeadline(), "complete")
			if recoverErr != nil {
				lastErr = fmt.Errorf("recover orchestrator execution claim: %w", recoverErr)
			} else if recovered {
				w.logger.Info("Execution %s: recovered orchestrator claim after missing completion state", env.ExecutionID)
				continue
			}
		}

		if attempt == finalizeMaxAttempts {
			break
		}

		delay := backoff.ExponentialDelay(finalizeBackoffBase, attempt-1, finalizeBackoffMax)
		w.logger.Warn("CompleteExecutionAndFinalizeRunByClaim execution %s status %s failed (attempt %d/%d): %v; retrying in %v",
			env.ExecutionID, status, attempt, finalizeMaxAttempts, err, delay)

		if sleepErr := w.clock.Sleep(w.runCtx, delay); sleepErr != nil {
			return dal.ExecutionFinalizationResult{}, sleepErr
		}
		attempt++
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

type executionClaimState struct {
	mu    sync.Mutex
	token string
}

func newExecutionClaimState(token string) *executionClaimState {
	return &executionClaimState{token: token}
}

func (s *executionClaimState) get() string {
	if s == nil {
		return ""
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.token
}

func (s *executionClaimState) set(token string) {
	if s == nil {
		return
	}

	s.mu.Lock()
	s.token = token
	s.mu.Unlock()
}

func (w *worker) tryClaimExecution(ctx context.Context, executionEnvelope *cell.ExecutionEnvelope, leaseUntil time.Time) (string, bool, bool, error) {
	if executionEnvelope == nil {
		return "", false, false, nil
	}

	span := trace.SpanFromContext(ctx)
	span.AddEvent("execution.claim.attempt", trace.WithAttributes(executionEnvelopeAttrs(executionEnvelope)...))
	claim, err := w.executionChoreographer().ClaimAndStartExecution(w.runCtx, executionEnvelope, w.workerID, leaseUntil)
	if err != nil {
		w.logger.Warn("ClaimAndStartExecution %s failed; stopping before task execution: %v", executionEnvelope.ExecutionID, err)
		span.RecordError(err)
		span.AddEvent("execution.claim.error", trace.WithAttributes(attribute.String("error", err.Error())))
		return "", false, false, err
	}

	if !claim.Claimed {
		w.logger.Warn("Execution %s not claimed; stopping before task execution", executionEnvelope.ExecutionID)
		span.AddEvent("execution.claim.skipped")
		return "", false, false, nil
	}

	if err := w.mirrorExecutionClaim(ctx, executionEnvelope, claim.ClaimToken, leaseUntil); err != nil {
		w.logger.Warn("MirrorExecutionClaim %s failed; stopping before task execution: %v", executionEnvelope.ExecutionID, err)
		span.RecordError(err)
		span.AddEvent("execution.claim.mirror_error", trace.WithAttributes(attribute.String("error", err.Error())))
		return "", false, false, err
	}

	span.AddEvent("execution.claim.success")
	if claim.TransitionedToAccepted {
		w.recordExecutionCatalogEvent(ctx, executionEnvelope, dal.ExecutionStatusAccepted)
		span.AddEvent("execution.accepted", trace.WithAttributes(executionEnvelopeAttrs(executionEnvelope)...))
	}

	return claim.ClaimToken, true, claim.ExecutionStarted, nil
}

func (w *worker) recoverOrchestratorExecutionClaim(ctx context.Context, j *api.Job, env *cell.ExecutionEnvelope, executionClaim *executionClaimState, leaseUntil time.Time, stage string) (bool, error) {
	if j == nil || env == nil {
		return false, fmt.Errorf("job and execution envelope are required")
	}

	snapshots, err := w.orchestratorRecoverySnapshots(ctx, j, env)
	if err != nil {
		return false, err
	}

	if err := w.executionChoreographer().LoadRun(w.runCtx, j, env, snapshots); err != nil {
		return false, fmt.Errorf("load orchestrator run: %w", err)
	}

	claim, err := w.executionChoreographer().ClaimAndStartExecution(w.runCtx, env, w.workerID, leaseUntil)
	if err != nil {
		return false, fmt.Errorf("claim execution: %w", err)
	}

	if !claim.Claimed {
		return false, fmt.Errorf("%w: execution %s was not claimable after orchestrator recovery", dal.ErrConflict, env.ExecutionID)
	}

	if err := w.mirrorExecutionClaim(ctx, env, claim.ClaimToken, leaseUntil); err != nil {
		return false, fmt.Errorf("mirror recovered execution claim: %w", err)
	}

	executionClaim.set(claim.ClaimToken)
	w.setCurrentRun(env.RunID, claim.ClaimToken)
	trace.SpanFromContext(ctx).AddEvent("orchestrator.execution.claim_recovered", trace.WithAttributes(executionEnvelopeAttrs(env)...))
	w.metrics.RecordOrchestratorRecovery(ctx, stage)

	return true, nil
}

func (w *worker) mirrorExecutionClaim(ctx context.Context, env *cell.ExecutionEnvelope, claimToken string, leaseUntil time.Time) error {
	if w.store == nil || env == nil {
		return nil
	}

	if strings.TrimSpace(claimToken) == "" {
		return fmt.Errorf("%w: execution claim token is required", dal.ErrConflict)
	}

	clock := w.clock
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	var lastErr error
	for attempt := 1; attempt <= finalizeMaxAttempts; attempt++ {
		err := w.store.MirrorExecutionClaim(w.runCtx, env.ExecutionID, w.workerID, claimToken, leaseUntil)
		if err == nil {
			w.noteDBRecovered()
			return nil
		}

		lastErr = err
		w.noteDBError(err)
		trace.SpanFromContext(ctx).RecordError(err)
		if attempt == finalizeMaxAttempts {
			break
		}

		delay := backoff.ExponentialDelay(finalizeBackoffBase, attempt-1, finalizeBackoffMax)
		w.logger.Warn("MirrorExecutionClaim %s failed (attempt %d/%d): %v; retrying in %v",
			env.ExecutionID, attempt, finalizeMaxAttempts, err, delay)

		if sleepErr := clock.Sleep(w.runCtx, delay); sleepErr != nil {
			return sleepErr
		}
	}

	return lastErr
}

func (w *worker) renewMirroredExecutionClaim(ctx context.Context, env *cell.ExecutionEnvelope, claimToken string, leaseUntil time.Time) error {
	if w.store == nil || env == nil || strings.TrimSpace(claimToken) == "" {
		return nil
	}

	if err := w.store.RenewExecutionLease(w.runCtx, env.ExecutionID, w.workerID, claimToken, leaseUntil); err != nil {
		w.noteDBError(err)
		trace.SpanFromContext(ctx).RecordError(err)
		return err
	}

	w.noteDBRecovered()
	return nil
}

func (w *worker) orchestratorRecoverySnapshots(ctx context.Context, j *api.Job, env *cell.ExecutionEnvelope) ([]orchestrator.TaskExecutionSnapshot, error) {
	if env == nil {
		return nil, nil
	}

	snapshots := make([]orchestrator.TaskExecutionSnapshot, 0)
	byTaskKey := map[string]orchestrator.TaskExecutionSnapshot{}

	if w.store != nil {
		storeSnapshots, err := w.orchestratorSnapshotsFromStore(ctx, env.RunID)
		if err != nil {
			return nil, err
		}

		for _, snapshot := range storeSnapshots {
			snapshots = append(snapshots, snapshot)
			if snapshot.Record.TaskKey != "" {
				byTaskKey[snapshot.Record.TaskKey] = snapshot
			}
		}
	}

	ancestorSnapshots, err := inferredAncestorSnapshots(j, env, byTaskKey)
	if err != nil {
		return nil, err
	}

	snapshots = append(snapshots, ancestorSnapshots...)
	snapshots = append(snapshots, orchestrator.TaskExecutionSnapshot{
		Record: orchestrator.TaskExecutionRecordFromEnvelope(env),
		Status: dal.ExecutionStatusRunning,
	})

	return snapshots, nil
}

func (w *worker) orchestratorSnapshotsFromStore(ctx context.Context, runID string) ([]orchestrator.TaskExecutionSnapshot, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, nil
	}

	const pageLimit = 500
	cursor := int64(0)
	snapshots := make([]orchestrator.TaskExecutionSnapshot, 0)
	for {
		tasks, nextCursor, err := w.store.ListRunTasks(w.runCtx, runID, cursor, pageLimit)
		if err != nil {
			w.noteDBError(err)
			trace.SpanFromContext(ctx).RecordError(err)
			return nil, fmt.Errorf("list run tasks: %w", err)
		}
		w.noteDBRecovered()

		for _, task := range tasks {
			snapshot, ok := orchestratorSnapshotFromTaskRecord(task)
			if ok {
				snapshots = append(snapshots, snapshot)
			}
		}

		if nextCursor == 0 {
			break
		}

		cursor = nextCursor
	}

	return snapshots, nil
}

func orchestratorSnapshotFromTaskRecord(task dal.TaskRecord) (orchestrator.TaskExecutionSnapshot, bool) {
	record := dal.TaskExecutionRecord{
		RunID:        task.RunID,
		TaskID:       task.TaskID,
		TaskKey:      task.TaskKey,
		Name:         task.Name,
		ParentTaskID: stringPtrValue(task.ParentTaskID),
	}

	status := task.Status
	if attempt, ok := latestTaskAttempt(task.Attempts); ok {
		record.TaskAttemptID = attempt.AttemptID
		record.ExecutionID = attempt.ExecutionID
		record.CellID = attempt.CellID
		record.Attempt = attempt.Attempt
		status = attempt.ExecutionStatus
	}

	if record.TaskKey == "" || status == "" {
		return orchestrator.TaskExecutionSnapshot{}, false
	}

	return orchestrator.TaskExecutionSnapshot{
		Record: record,
		Status: status,
	}, true
}

func latestTaskAttempt(attempts []dal.TaskAttemptRecord) (dal.TaskAttemptRecord, bool) {
	if len(attempts) == 0 {
		return dal.TaskAttemptRecord{}, false
	}

	latest := attempts[0]
	for _, attempt := range attempts[1:] {
		if attempt.Attempt > latest.Attempt {
			latest = attempt
		}
	}

	return latest, true
}

func inferredAncestorSnapshots(j *api.Job, env *cell.ExecutionEnvelope, byTaskKey map[string]orchestrator.TaskExecutionSnapshot) ([]orchestrator.TaskExecutionSnapshot, error) {
	if j == nil || env == nil || env.TaskKey == dal.RootTaskKey {
		return nil, nil
	}

	plan, err := job.PlanTaskExecutions(j)
	if err != nil {
		return nil, err
	}

	parents := map[string]string{}
	for _, entry := range plan {
		parent := strings.TrimSpace(entry.ParentTaskKey)
		if parent == "" {
			parent = dal.RootTaskKey
		}

		parents[entry.TaskKey] = parent
	}

	var snapshots []orchestrator.TaskExecutionSnapshot
	seen := map[string]struct{}{}
	for taskKey := env.TaskKey; ; {
		parent, ok := parents[taskKey]
		if !ok || parent == "" {
			break
		}

		if _, ok := seen[parent]; ok {
			break
		}

		seen[parent] = struct{}{}
		snapshot, ok := byTaskKey[parent]
		if !ok {
			snapshot = orchestrator.TaskExecutionSnapshot{
				Record: defaultTaskExecutionRecord(env.RunID, parent, parents[parent], parent, env.CellID),
			}
		}

		snapshot.Status = dal.ExecutionStatusSucceeded
		snapshots = append(snapshots, snapshot)

		if parent == dal.RootTaskKey {
			break
		}

		taskKey = parent
	}

	return snapshots, nil
}

func defaultTaskExecutionRecord(runID, taskKey, parentTaskKey, name, cellID string) dal.TaskExecutionRecord {
	if taskKey == "" {
		taskKey = dal.RootTaskKey
	}

	if name == "" {
		name = taskKey
	}

	taskID := runID + ":" + taskKey
	parentTaskID := ""
	if parentTaskKey != "" {
		parentTaskID = runID + ":" + parentTaskKey
	}

	return dal.TaskExecutionRecord{
		RunID:         runID,
		TaskID:        taskID,
		ParentTaskID:  parentTaskID,
		TaskKey:       taskKey,
		Name:          name,
		TaskAttemptID: taskID + ":attempt:1",
		SegmentID:     taskID + ":segment",
		SegmentName:   name,
		ExecutionID:   taskID + ":attempt:1:execution",
		CellID:        cellID,
		Attempt:       1,
	}
}

func stringPtrValue(value *string) string {
	if value == nil {
		return ""
	}

	return *value
}

func isOrchestratorNotFound(err error) bool {
	return errors.Is(err, dal.ErrNotFound) || status.Code(err) == codes.NotFound
}

func (w *worker) executeWithLeaseRenewal(ctx context.Context, runID string, executionClaim *executionClaimState, runJob *api.Job, env *cell.ExecutionEnvelope) error {
	w.setCurrentRun(runID, executionClaim.get())
	defer w.clearCurrentRun()

	execCtx, execCancel := context.WithCancel(ctx)
	defer execCancel()
	cancelled := make(chan struct{})
	var cancelOnce sync.Once

	cancelRun := func(source string) {
		cancelOnce.Do(func() {
			w.logger.Info("Cancelling run %s via %s", runID, source)
			close(cancelled)
			go w.cancelCoreTask(runID, env, source)
			execCancel()
		})
	}

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
	var spiffeRegistration *executionSPIFFERegistration
	if err == nil {
		handle, registered, registerErr := w.ensureExecutionSPIFFERegistration(execCtx, workloadIdentity, env, w.leaseDeadline())
		if registerErr != nil {
			err = fmt.Errorf("ensure SPIFFE execution registration: %w", registerErr)
		} else if registered {
			spiffeRegistration = &executionSPIFFERegistration{
				identity: workloadIdentity,
				env:      env,
				handle:   handle,
			}
			defer w.releaseExecutionSPIFFERegistration(spiffeRegistration)
		}
	}

	if err == nil {
		workloadIdentity, err = w.acquireExecutionSVID(execCtx, workloadIdentity)
	}

	stopRenew := make(chan struct{})
	doneRenew := make(chan struct{})
	renewStarted := false
	startRenewal := func() {
		if renewStarted {
			return
		}
		renewStarted = true
		go w.leaseRenewalLoop(execCtx, runID, runJob, env, executionClaim, spiffeRegistration, stopRenew, doneRenew)
	}
	stopRenewal := func() {
		if !renewStarted {
			return
		}
		close(stopRenew)
		<-doneRenew
	}

	if err == nil {
		startRenewal()

		var secretFiles []secrets.FileMaterial
		secretFiles, err = w.resolveExecutionSecrets(execCtx, runJob, env, executionClaim.get(), workloadIdentity)
		if err != nil {
			err = fmt.Errorf("resolve execution secrets: %w", err)
		}
		if err != nil {
			stopRenewal()
			return err
		}

		artifactPublisher := w.newArtifactPublisher(env)
		if artifactPublisher != nil {
			defer artifactPublisher.Close()
		}

		execSessionOpts := workercore.TaskSessionOptions{
			SessionID:        env.ExecutionID,
			ShellEndpoint:    w.coreShellEndpoint,
			LogClient:        w.logClient,
			Logger:           w.logger,
			WorkloadIdentity: workloadIdentity,
			ActionResolver:   w.actionResolver,
			ActionLocks:      env.ActionLocks,
			SecretFiles:      secretFiles,
		}
		if artifactPublisher != nil {
			execSessionOpts.ArtifactPublisher = action.ArtifactPublisher(artifactPublisher)
		}

		execSession := workercore.NewTaskSession(execSessionOpts)
		if w.coreShell != nil {
			unregister, sessionErr := w.coreShell.RegisterSession(execSession)
			if sessionErr != nil {
				err = sessionErr
			} else {
				defer unregister()
			}
		}

		if err == nil {
			w.markExecutionStarted(ctx, env)
		}

		execReq := workercore.ExecuteTaskRequest{
			Job:     runJob,
			TaskKey: env.TaskKey,
			Session: execSession,
		}

		if err != nil {
			// Keep the shell-owned setup error as the execution result.
		} else if w.core == nil {
			err = fmt.Errorf("worker execution core is not configured")
		} else {
			err = w.core.ExecuteTask(execCtx, execReq)
		}
	}

	stopRenewal()

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

func (w *worker) resolveExecutionSecrets(ctx context.Context, runJob *api.Job, env *cell.ExecutionEnvelope, executionClaimToken string, workloadIdentity *workloadidentity.Identity) ([]secrets.FileMaterial, error) {
	if env == nil {
		return nil, nil
	}

	refs := secrets.ReferencesForTask(runJob, env.TaskKey)
	if len(refs) == 0 {
		return nil, nil
	}

	if w == nil {
		return nil, fmt.Errorf("job declares secrets but worker secrets resolver is not configured")
	}

	resolver := w.secretResolver
	cleanup := func() {}
	if w.secretResolverForWorkload != nil {
		workloadResolver, workloadCleanup, err := w.secretResolverForWorkload(workloadIdentity)
		if err != nil {
			return nil, err
		}

		resolver = workloadResolver
		if workloadCleanup != nil {
			cleanup = workloadCleanup
		}
	}

	if resolver == nil {
		return nil, fmt.Errorf("job declares secrets but worker secrets resolver is not configured")
	}
	defer cleanup()

	bundle, err := resolver.Resolve(ctx, secrets.ResolveRequest{
		RunID:               env.RunID,
		ExecutionID:         env.ExecutionID,
		ExecutionClaimToken: executionClaimToken,
		Workload:            workloadIdentity,
		Secrets:             refs,
	})

	if err != nil {
		return nil, err
	}

	return bundle.Files, nil
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

func (w *worker) cancelCoreTask(runID string, env *cell.ExecutionEnvelope, reason string) {
	cancellable, ok := w.core.(workercore.CancellableCore)
	if !ok || cancellable == nil || env == nil || strings.TrimSpace(env.ExecutionID) == "" {
		return
	}

	ctx, cancel := context.WithTimeout(w.runCtx, coreCancelTimeout)
	defer cancel()

	if err := cancellable.CancelTask(ctx, workercore.CancelTaskRequest{
		SessionID: env.ExecutionID,
		RunID:     runID,
		TaskKey:   env.TaskKey,
		Reason:    reason,
	}); err != nil {
		w.logger.Warn("Cancel worker core task for run %s execution %s failed: %v", runID, env.ExecutionID, err)
	}
}

func (w *worker) leaseRenewalLoop(
	execCtx context.Context,
	runID string,
	j *api.Job,
	executionEnvelope *cell.ExecutionEnvelope,
	executionClaim *executionClaimState,
	spiffeRegistration *executionSPIFFERegistration,
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
			claimToken := executionClaim.get()
			if executionEnvelope != nil && claimToken != "" {
				if err := w.executionChoreographer().RenewExecutionLease(w.runCtx, executionEnvelope, w.workerID, claimToken, next); err != nil {
					renewFailed = true
					if isOrchestratorNotFound(err) {
						recovered, recoverErr := w.recoverOrchestratorExecutionClaim(execCtx, j, executionEnvelope, executionClaim, next, "renew")
						if recoverErr == nil && recovered {
							w.logger.Info("Execution %s: recovered orchestrator claim after missing lease state", executionEnvelope.ExecutionID)
							continue
						}
						if recoverErr != nil {
							err = fmt.Errorf("%w; recovery failed: %v", err, recoverErr)
						}
					}
					w.logger.Warn("Execution %s: lease renew failed (will retry): %v", executionEnvelope.ExecutionID, err)
					continue
				}

				if err := w.renewMirroredExecutionClaim(execCtx, executionEnvelope, claimToken, next); err != nil {
					renewFailed = true
					w.logger.Warn("Execution %s: mirrored lease renew failed (will retry): %v", executionEnvelope.ExecutionID, err)
					continue
				}
			}

			if spiffeRegistration != nil {
				handle, registered, err := w.ensureExecutionSPIFFERegistration(w.runCtx, spiffeRegistration.identity, spiffeRegistration.env, next)
				if err != nil {
					w.logger.Warn("Execution %s: SPIFFE registration renew failed (will retry): %v", executionEnvelope.ExecutionID, err)
				} else if registered {
					spiffeRegistration.handle = handle
				}
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
	viper.SetDefault("worker.secrets.address", config.WorkerSecretsAddress())

	rootCmd.PersistentFlags().String("metrics-host", config.WorkerMetricsHost(), "Host/IP for the Prometheus /metrics HTTP server to bind")
	rootCmd.PersistentFlags().Int("metrics-port", config.WorkerMetricsPort(), "HTTP port for Prometheus /metrics")
	rootCmd.PersistentFlags().Int64("artifact-max-bytes", config.WorkerArtifactMaxBytes(), "Maximum bytes for a worker artifact upload (0 disables)")
	rootCmd.PersistentFlags().Int64("artifact-max-run-bytes", config.WorkerArtifactMaxRunBytes(), "Maximum total artifact bytes recorded for one run (0 disables)")
	rootCmd.PersistentFlags().Int64("artifact-max-count", config.WorkerArtifactMaxCount(), "Maximum artifact manifests recorded for one run (0 disables)")
	rootCmd.PersistentFlags().String("core-socket", workercore.DefaultCoreSocketPath(), "Unix socket for the remote worker core")
	rootCmd.PersistentFlags().String("core-shell-socket", workercore.DefaultShellSocketPath(), "Unix socket exposed by the worker shell for core callbacks")
	rootCmd.PersistentFlags().Duration("core-connect-timeout", 10*time.Second, "Timeout for connecting to the remote worker core")
	rootCmd.PersistentFlags().String("secrets-address", config.WorkerSecretsAddress(), "gRPC address for the cell-local secrets service")

	_ = viper.BindPFlag("metrics_host", rootCmd.PersistentFlags().Lookup("metrics-host"))
	_ = viper.BindPFlag("metrics_port", rootCmd.PersistentFlags().Lookup("metrics-port"))
	_ = viper.BindPFlag("worker.artifact_max_bytes", rootCmd.PersistentFlags().Lookup("artifact-max-bytes"))
	_ = viper.BindPFlag("worker.artifact_max_run_bytes", rootCmd.PersistentFlags().Lookup("artifact-max-run-bytes"))
	_ = viper.BindPFlag("worker.artifact_max_count", rootCmd.PersistentFlags().Lookup("artifact-max-count"))
	_ = viper.BindPFlag("worker.core.socket", rootCmd.PersistentFlags().Lookup("core-socket"))
	_ = viper.BindPFlag("worker.core.shell_socket", rootCmd.PersistentFlags().Lookup("core-shell-socket"))
	_ = viper.BindPFlag("worker.core.connect_timeout", rootCmd.PersistentFlags().Lookup("core-connect-timeout"))
	_ = viper.BindPFlag("worker.secrets.address", rootCmd.PersistentFlags().Lookup("secrets-address"))

	_ = viper.BindEnv("worker.artifact_max_bytes", "VECTIS_WORKER_ARTIFACT_MAX_BYTES")
	_ = viper.BindEnv("worker.artifact_max_run_bytes", "VECTIS_WORKER_ARTIFACT_MAX_RUN_BYTES")
	_ = viper.BindEnv("worker.artifact_max_count", "VECTIS_WORKER_ARTIFACT_MAX_COUNT")
	_ = viper.BindEnv("worker.queue.address", "VECTIS_WORKER_QUEUE_ADDRESS")
	_ = viper.BindEnv("worker.log.address", "VECTIS_WORKER_LOG_ADDRESS")
	_ = viper.BindEnv("worker.orchestrator.address", "VECTIS_WORKER_ORCHESTRATOR_ADDRESS")
	_ = viper.BindEnv("worker.secrets.address", "VECTIS_WORKER_SECRETS_ADDRESS")
	_ = viper.BindEnv("worker.registry.address", "VECTIS_WORKER_REGISTRY_ADDRESS")
	_ = viper.BindEnv("worker.control.mode", "VECTIS_WORKER_CONTROL_MODE")
	_ = viper.BindEnv("control_port", "VECTIS_WORKER_CONTROL_PORT")
	_ = viper.BindEnv("control_port_min", "VECTIS_WORKER_CONTROL_PORT_MIN")
	_ = viper.BindEnv("control_port_max", "VECTIS_WORKER_CONTROL_PORT_MAX")
	_ = viper.BindEnv("worker.core.socket", "VECTIS_WORKER_CORE_SOCKET")
	_ = viper.BindEnv("worker.core.shell_socket", "VECTIS_WORKER_CORE_SHELL_SOCKET")
	_ = viper.BindEnv("worker.core.connect_timeout", "VECTIS_WORKER_CORE_CONNECT_TIMEOUT")

	viper.SetEnvPrefix("VECTIS_WORKER")
	viper.AutomaticEnv()
}

func main() {
	if err := cli.ExecuteWithShutdownSignals(rootCmd); err != nil {
		os.Exit(1)
	}
}
