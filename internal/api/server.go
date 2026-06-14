package api

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/api/audit"
	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/api/ratelimit"
	"vectis/internal/backoff"
	"vectis/internal/cache"
	"vectis/internal/cell"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/httpsecurity"
	"vectis/internal/interfaces"
	"vectis/internal/logclient"
	"vectis/internal/observability"
	"vectis/internal/queueclient"
	sourcepkg "vectis/internal/source"
	"vectis/internal/version"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	defaultShutdownTimeout   = 30 * time.Second
	healthDBPingTimeout      = 2 * time.Second
	defaultReadHeaderTimeout = 10 * time.Second
	defaultReadTimeout       = 5 * time.Minute
	defaultWriteTimeout      = 3 * time.Minute
	defaultIdleTimeout       = 120 * time.Second
	defaultHandlerDBTimeout  = 60 * time.Second
)

type queueClientHolder struct {
	client interfaces.QueueService
	close  func()
}

type logClientHolder struct {
	client logReaderClient
	state  func() connectivity.State
	close  func()
}

type logReaderClient interface {
	GetLogs(ctx context.Context, in *api.GetLogsRequest, opts ...grpc.CallOption) (api.LogService_GetLogsClient, error)
}

type ctxHolder struct {
	ctx context.Context
}

type APIServer struct {
	jobs                     dal.JobsRepository
	runs                     dal.RunsRepository
	ephemeralRuns            dal.EphemeralRunStarter
	artifacts                dal.ArtifactsRepository
	authRepo                 dal.AuthRepository
	namespaces               dal.NamespacesRepository
	roleBindings             dal.RoleBindingsRepository
	idempotency              dal.IdempotencyRepository
	dispatchEvents           dal.DispatchEventsRepository
	triggerEvents            dal.TriggerInvocationsRepository
	catalogEvents            dal.CatalogEventsRepository
	schedules                dal.SchedulesRepository
	sources                  dal.SourcesRepository
	logger                   interfaces.Logger
	actionResolver           action.Resolver
	actionDescriptorResolver actionregistry.Resolver
	queueClient              atomic.Pointer[queueClientHolder]
	logClient                atomic.Pointer[logClientHolder]
	runBroadcaster           *RunBroadcaster
	dbUnavailable            atomic.Bool
	draining                 atomic.Bool
	healthDB                 *sql.DB
	MetricsHandler           http.Handler

	// AccessLogger, when set, writes one structured slog record per HTTP request
	// (typically JSON on stderr). Health probes are excluded.
	AccessLogger       *slog.Logger
	logRoutingMetrics  logclient.RoutingMetrics
	apiDispatchMetrics *observability.APIDispatchMetrics
	apiSecurityMetrics securityRejectionMetrics
	sourceSyncMetrics  sourceRepositorySyncMetrics

	// authzOverride, if non-nil, replaces SelectAuthorizer(complete) in middleware (tests).
	authzOverride authz.Authorizer

	// rateLimiter, when set, applies rate limiting to API routes.
	rateLimiter ratelimit.RateLimiter

	// cacheService stores shared API sessions and rate-limit buckets.
	cacheService cache.Service

	// auditor, when set, logs audit events for auth operations.
	auditor audit.Auditor
	// auditPolicy resolves event-specific audit durability.
	auditPolicy audit.Policy

	// retryMetrics, when set, records retry/backoff metrics for gRPC dials.
	retryMetrics backoff.RetryMetrics

	// dispatchMetrics, when set, records dispatch event counters.
	dispatchMetrics dispatchMetrics

	// ResolveWorkerAddress, when set, resolves a worker_id to a control address via the registry.
	ResolveWorkerAddress func(ctx context.Context, workerID string) (string, error)

	mu                       sync.RWMutex
	executionIngress         cell.ExecutionIngress
	sourceSyncMu             sync.Mutex
	sourceSyncRunning        map[string]struct{}
	sourceSyncCheckoutStatus func(context.Context, dal.SourceRepositoryRecord, string) sourcepkg.GitCheckoutStatus
	sourceDefinitionAuthor   SourceDefinitionAuthorFactory
	sourceAuthoring          SourceAuthoringCapabilityResolver
	srvCtx                   atomic.Pointer[ctxHolder]
}

type SourceDefinitionAuthorFactory func(dal.SourceRepositoryRecord) (sourcepkg.DefinitionAuthor, error)

type SourceAuthoringCapabilityResolver func(dal.SourceRepositoryRecord) sourcepkg.AuthoringCapability

type routeSpec struct {
	Pattern   string
	Handler   http.Handler
	Auth      routeAuthPolicy
	Cache     routeCachePolicy
	Body      routeBodyPolicy
	Accept    routeAcceptPolicy
	Query     routeQueryPolicy
	Headers   routeHeaderPolicy
	RateLimit ratelimit.Rule
}

type dispatchMetrics interface {
	RecordDispatchEvent(ctx context.Context, source, eventType, targetCell string)
}

type sourceRepositorySyncMetrics interface {
	RecordSourceRepositorySync(ctx context.Context, trigger, sourceKind, checkoutMode, outcome, reason string, d time.Duration)
}

func NewAPIServer(logger interfaces.Logger, db *sql.DB) *APIServer {
	repos := dal.NewSQLRepositoriesWithCellID(db, config.CellID())
	s := NewAPIServerWithRepositories(logger, repos.Jobs(), repos.Runs(), repos)
	s.healthDB = db
	s.authRepo = repos.Auth()
	s.namespaces = repos.Namespaces()
	s.roleBindings = repos.RoleBindings()
	s.idempotency = repos.Idempotency()
	s.dispatchEvents = repos.DispatchEvents()
	s.triggerEvents = repos.TriggerInvocations()
	s.catalogEvents = repos.CatalogEvents()
	s.schedules = repos.Schedules()
	s.sources = repos.Sources()
	s.artifacts = repos.Artifacts()
	s.cacheService = cache.NewSQLService(db, database.EffectiveDBDriver())
	return s
}

func NewAPIServerWithRepositories(
	logger interfaces.Logger,
	jobs dal.JobsRepository,
	runs dal.RunsRepository,
	ephemeralRuns dal.EphemeralRunStarter,
) *APIServer {
	var artifacts dal.ArtifactsRepository
	if repos, ok := ephemeralRuns.(interface {
		Artifacts() dal.ArtifactsRepository
	}); ok {
		artifacts = repos.Artifacts()
	}

	s := &APIServer{
		jobs:                   jobs,
		runs:                   runs,
		ephemeralRuns:          ephemeralRuns,
		artifacts:              artifacts,
		logger:                 logger,
		runBroadcaster:         NewRunBroadcaster(logger),
		auditPolicy:            audit.DefaultPolicy(),
		sourceDefinitionAuthor: sourcepkg.NewDefinitionAuthorFromRecord,
		sourceAuthoring:        sourcepkg.AuthoringCapabilityFromRecord,
	}

	return s
}

func (s *APIServer) SetSourceDefinitionAuthoring(factory SourceDefinitionAuthorFactory, capabilities SourceAuthoringCapabilityResolver) {
	if factory == nil {
		factory = sourcepkg.NewDefinitionAuthorFromRecord
	}

	if capabilities == nil {
		capabilities = sourcepkg.AuthoringCapabilityFromRecord
	}

	s.sourceDefinitionAuthor = factory
	s.sourceAuthoring = capabilities
}

func (s *APIServer) markDBUnavailable(err error) {
	if database.IsUnavailableError(err) && s.dbUnavailable.CompareAndSwap(false, true) {
		s.logger.Warn("Database unavailable; API returning 503 for affected routes until recovery: %v", err)
	}
}

func (s *APIServer) markDBRecovered() {
	if s.dbUnavailable.CompareAndSwap(true, false) {
		s.logger.Info("Database connectivity recovered; API database operations resumed")
	}
}

func idempotencyKeyFromRequest(r *http.Request) string {
	return r.Header.Get(idempotencyKeyHeaderName)
}

func hashIdempotencyRequest(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}

	return hex.EncodeToString(h.Sum(nil))
}

func principalIdempotencyScope(prefix string, p *authn.Principal) string {
	if p == nil {
		return prefix + ":anonymous"
	}

	return prefix + ":user:" + strconv.FormatInt(p.LocalUserID, 10)
}

func (s *APIServer) reserveIdempotency(w http.ResponseWriter, ctx context.Context, scope, key, requestHash string) (record dal.IdempotencyRecord, reserved bool, ok bool) {
	record, reserved, _, ok = s.reserveIdempotencyState(w, ctx, scope, key, requestHash, false)
	return record, reserved, ok
}

func (s *APIServer) reserveRecoverableIdempotency(w http.ResponseWriter, ctx context.Context, scope, key, requestHash string) (record dal.IdempotencyRecord, reserved bool, inProgress bool, ok bool) {
	return s.reserveIdempotencyState(w, ctx, scope, key, requestHash, true)
}

func (s *APIServer) reserveIdempotencyState(w http.ResponseWriter, ctx context.Context, scope, key, requestHash string, allowInProgress bool) (record dal.IdempotencyRecord, reserved bool, inProgress bool, ok bool) {
	if key == "" || s.idempotency == nil {
		return dal.IdempotencyRecord{}, false, false, true
	}

	record, created, err := s.idempotency.Reserve(ctx, scope, key, requestHash)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return dal.IdempotencyRecord{}, false, false, false
		}

		s.logger.Error("Database error reserving idempotency key: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return dal.IdempotencyRecord{}, false, false, false
	}

	if !created && record.RequestHash != requestHash {
		writeAPIError(w, http.StatusConflict, "idempotency_key_reused", "idempotency key reused for different request", nil)
		return dal.IdempotencyRecord{}, false, false, false
	}

	if !created && record.ResponseJSON == nil {
		if allowInProgress {
			return record, false, true, true
		}

		writeAPIError(w, http.StatusConflict, "idempotency_in_progress", "idempotent request is still in progress", nil)
		return dal.IdempotencyRecord{}, false, false, false
	}

	return record, created, false, true
}

func (s *APIServer) completeIdempotency(ctx context.Context, scope, key string, response []byte) {
	if key == "" || s.idempotency == nil {
		return
	}

	if err := s.idempotency.Complete(ctx, scope, key, string(response)); err != nil {
		s.logger.Error("Failed to complete idempotency key: %v", err)
	}
}

func (s *APIServer) releaseIdempotency(ctx context.Context, scope, key string) {
	if key == "" || s.idempotency == nil {
		return
	}

	if err := s.idempotency.Release(ctx, scope, key); err != nil {
		s.logger.Error("Failed to release idempotency key: %v", err)
	}
}

func (s *APIServer) attachIdempotencyResource(ctx context.Context, scope, key, resourceType, resourceID string) error {
	if key == "" || s.idempotency == nil || resourceID == "" {
		return nil
	}

	return s.idempotency.AttachResource(ctx, scope, key, resourceType, resourceID)
}

func (s *APIServer) recordDispatchEvent(ctx context.Context, runID, source, eventType, targetCell string, message *string) {
	if s.dispatchMetrics != nil {
		s.dispatchMetrics.RecordDispatchEvent(ctx, source, eventType, targetCell)
	}

	if s.dispatchEvents != nil {
		if err := s.dispatchEvents.Record(ctx, runID, source, eventType, message); err != nil {
			s.logger.Error("Failed to record dispatch event for run %s: %v", runID, err)
		}
	}
}

func (s *APIServer) recordAPIEnqueueMetric(ctx context.Context, runKind, outcome string) {
	if s.apiDispatchMetrics == nil {
		return
	}

	s.apiDispatchMetrics.RecordRunEnqueue(ctx, runKind, outcome)
}

func (s *APIServer) handleDBUnavailableError(w http.ResponseWriter, err error) bool {
	if database.IsUnavailableError(err) {
		s.markDBUnavailable(err)
		writeAPIError(w, http.StatusServiceUnavailable, "database_unavailable", "database unavailable", nil)
		return true
	}

	return false
}

type grpcQueueConnectivity interface {
	GRPCConnectivityState() connectivity.State
}

func queueRPCReady(q interfaces.QueueService) bool {
	if q == nil {
		return false
	}

	wc, ok := q.(grpcQueueConnectivity)
	if !ok {
		return true
	}

	return wc.GRPCConnectivityState() == connectivity.Ready
}

func (s *APIServer) handlerDBCtx(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), defaultHandlerDBTimeout)
}

func (s *APIServer) requireNamespaces(w http.ResponseWriter) bool {
	if s.namespaces == nil {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrNamespacesNotConfigured)
		return false
	}
	return true
}

func (s *APIServer) requireRoleBindings(w http.ResponseWriter) bool {
	if s.roleBindings == nil {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrRoleBindingsNotConfigured)
		return false
	}
	return true
}

func (s *APIServer) requireSources(w http.ResponseWriter) bool {
	if s.sources == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "source_repositories_not_configured", "source repositories not configured", nil)
		return false
	}

	return true
}

func (s *APIServer) requireSchedules(w http.ResponseWriter) bool {
	if s.schedules == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "schedules_not_configured", "schedules not configured", nil)
		return false
	}

	return true
}

func (s *APIServer) requireStoredJobs(w http.ResponseWriter) bool {
	if !config.SourceStoredJobsEnabled() {
		writeAPIError(w, http.StatusConflict, "stored_jobs_disabled", "stored job APIs are disabled", nil)
		return false
	}

	return true
}

func (s *APIServer) requireAuthRepo(w http.ResponseWriter) bool {
	if s.authRepo == nil {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrAuthNotConfigured)
		return false
	}
	return true
}

func (s *APIServer) requirePrincipal(w http.ResponseWriter, r *http.Request) (*authn.Principal, bool) {
	p, ok := authn.PrincipalFromContext(r.Context())
	if ok {
		return p, true
	}

	if !config.APIAuthEnabled() {
		return nil, true
	}

	writeAPIErrorCode(w, http.StatusUnauthorized, apiErrAuthenticationRequired)
	return nil, false
}

func (s *APIServer) authorizeNamespace(ctx context.Context, w http.ResponseWriter, p *authn.Principal, action authz.Action, namespacePath string) bool {
	if !config.APIAuthEnabled() {
		return true
	}

	z := s.effectiveAuthorizer(true)
	if !z.Allow(ctx, p, action, authz.Resource{NamespacePath: namespacePath}) {
		writeAPIErrorCode(w, http.StatusForbidden, apiErrAuthorizationDenied)
		return false
	}

	return true
}

func (s *APIServer) authorizeAction(ctx context.Context, w http.ResponseWriter, p *authn.Principal, action authz.Action, res authz.Resource) bool {
	if !config.APIAuthEnabled() {
		return true
	}

	z := s.effectiveAuthorizer(true)
	if !z.Allow(ctx, p, action, res) {
		writeAPIErrorCode(w, http.StatusForbidden, apiErrAuthorizationDenied)
		return false
	}

	return true
}

func (s *APIServer) checkNamespaceAuth(ctx context.Context, p *authn.Principal, action authz.Action, namespacePath string) bool {
	if !config.APIAuthEnabled() {
		return true
	}

	z := s.effectiveAuthorizer(true)
	return z.Allow(ctx, p, action, authz.Resource{NamespacePath: namespacePath})
}

func (s *APIServer) getJobNamespacePath(ctx context.Context, jobID string) (string, error) {
	nsID, err := s.jobs.GetNamespaceID(ctx, jobID)
	if err != nil {
		return "", err
	}

	if s.namespaces == nil {
		return "/", nil
	}

	ns, err := s.namespaces.GetByID(ctx, nsID)
	if err != nil {
		return "", err
	}

	return ns.Path, nil
}

func (s *APIServer) getRunJobNamespacePath(ctx context.Context, runID string) (string, error) {
	jobID, err := s.runs.GetRunJobID(ctx, runID)
	if err != nil {
		return "", err
	}

	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			nsPath, err := s.getSourceRunNamespacePath(ctx, runID, jobID)
			if err != nil {
				return "", err
			}

			if nsPath != "" {
				return nsPath, nil
			}

			// Ephemeral runs don't have stored_jobs entries;
			// they run in the default namespace.
			return "/", nil
		}
		return "", err
	}

	return nsPath, nil
}

func (s *APIServer) getSourceRunNamespacePath(ctx context.Context, runID, jobID string) (string, error) {
	if s.sources == nil || s.namespaces == nil {
		return "", nil
	}

	rec, err := s.runs.GetRun(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			return "", nil
		}

		return "", err
	}

	source, err := s.sources.GetDefinitionSource(ctx, jobID, rec.DefinitionVersion)
	if err != nil {
		if dal.IsNotFound(err) {
			return "", nil
		}

		return "", err
	}

	repo, err := s.sources.GetRepository(ctx, source.RepositoryID)
	if err != nil {
		if dal.IsNotFound(err) {
			return "", nil
		}

		return "", err
	}

	ns, err := s.namespaces.GetByID(ctx, repo.NamespaceID)
	if err != nil {
		if dal.IsNotFound(err) {
			return "", nil
		}

		return "", err
	}

	return ns.Path, nil
}

func writeVersionHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Vectis-Build-Version", version.Version)
	w.Header().Set("X-Vectis-Build-Commit", version.Commit)
	w.Header().Set("X-Vectis-Build-Date", version.BuildDate)
	w.Header().Set("X-Vectis-Cell-ID", config.CellID())
}

func (s *APIServer) HealthLive(w http.ResponseWriter, _ *http.Request) {
	writeVersionHeaders(w)
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) HealthReady(w http.ResponseWriter, r *http.Request) {
	writeVersionHeaders(w)

	if s.draining.Load() {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrServerShuttingDown)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), healthDBPingTimeout)
	defer cancel()

	if s.healthDB != nil {
		if err := s.healthDB.PingContext(ctx); err != nil {
			writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrDatabaseNotReady)
			return
		}
	}

	holder := s.queueClient.Load()
	var qc interfaces.QueueService
	if holder != nil {
		qc = holder.client
	}

	if !queueRPCReady(qc) {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrQueueNotReady)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) SetQueueClient(client interfaces.QueueService) {
	old := s.queueClient.Swap(&queueClientHolder{client: client})
	if old != nil && old.close != nil {
		old.close()
	}
}

func (s *APIServer) SetExecutionIngress(ingress cell.ExecutionIngress) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.executionIngress = ingress
}

func (s *APIServer) SetLogClient(client api.LogServiceClient) {
	old := s.logClient.Swap(&logClientHolder{client: client})
	if old != nil && old.close != nil {
		old.close()
	}
}

func (s *APIServer) SetCatalogEventsRepository(repo dal.CatalogEventsRepository) {
	s.catalogEvents = repo
}

func (s *APIServer) SetRateLimiter(limiter ratelimit.RateLimiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rateLimiter = limiter
}

func (s *APIServer) SetCacheService(cacheService cache.Service) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cacheService = cacheService
}

func (s *APIServer) SetActionResolver(resolver action.Resolver) {
	s.actionResolver = resolver
}

func (s *APIServer) SetActionDescriptorResolver(resolver actionregistry.Resolver) {
	s.actionDescriptorResolver = resolver
}

func (s *APIServer) SetAuditor(auditor audit.Auditor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auditor = auditor
}

func (s *APIServer) SetAuditPolicy(policy audit.Policy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auditPolicy = policy
}

func (s *APIServer) SetRetryMetrics(m backoff.RetryMetrics) {
	s.retryMetrics = m
}

func (s *APIServer) SetDispatchMetrics(m dispatchMetrics) {
	s.dispatchMetrics = m
}

func (s *APIServer) SetLogRoutingMetrics(m logclient.RoutingMetrics) {
	s.logRoutingMetrics = m
}

func (s *APIServer) SetAPIDispatchMetrics(m *observability.APIDispatchMetrics) {
	s.apiDispatchMetrics = m
}

func (s *APIServer) SetAPISecurityMetrics(m securityRejectionMetrics) {
	s.apiSecurityMetrics = m
}

func (s *APIServer) SetSourceSyncMetrics(m sourceRepositorySyncMetrics) {
	s.sourceSyncMetrics = m
}

func (s *APIServer) SetSourceSyncCheckoutStatus(fn func(context.Context, dal.SourceRepositoryRecord, string) sourcepkg.GitCheckoutStatus) {
	s.sourceSyncCheckoutStatus = fn
}

func (s *APIServer) auditLog(ctx context.Context, eventType string, actorID, targetID int64, metadata map[string]any) error {
	s.mu.RLock()
	auditor := s.auditor
	policy := s.auditPolicy
	s.mu.RUnlock()

	if auditor == nil {
		return nil
	}

	ip := ""
	if req, ok := ctx.Value(httpRequestKey{}).(*http.Request); ok && req != nil {
		ip = config.HTTPClientIP(req)
	}

	durability := policy.DurabilityFor(eventType)
	err := auditor.Log(ctx, audit.Event{
		Type:          eventType,
		Durability:    durability,
		ActorID:       actorID,
		TargetID:      targetID,
		Metadata:      metadata,
		IPAddress:     ip,
		CorrelationID: observability.CorrelationID(ctx),
	})
	if err != nil {
		s.logger.Error("Audit event emission failed: event_type=%s durability=%s error=%v", eventType, durability, err)
	}

	return err
}

type httpRequestKey struct{}

func (s *APIServer) ConnectToQueue(ctx context.Context) error {
	old := s.queueClient.Swap(nil)
	if old != nil && old.close != nil {
		old.close()
	}

	mq, err := queueclient.NewManagingQueuePoolService(ctx, s.logger, queueclient.QueuePoolOptions{
		PinnedAddress:   config.PinnedQueueAddress(),
		RegistryAddress: config.APIRegistryDialAddress(),
		RetryMetrics:    s.retryMetrics,
	})

	if err != nil {
		return fmt.Errorf("failed to connect to queue: %w", err)
	}

	s.queueClient.Store(&queueClientHolder{client: mq, close: func() { _ = mq.Close() }})
	return nil
}

func (s *APIServer) ConnectToLog(ctx context.Context) error {
	old := s.logClient.Swap(nil)
	if old != nil && old.close != nil {
		old.close()
	}

	client, err := logclient.NewManagingLogClient(ctx, s.logger, logclient.PoolOptions{
		PinnedAddress:   config.APILogAddress(),
		RegistryAddress: config.APIRegistryDialAddress(),
		RetryMetrics:    s.retryMetrics,
		AssignmentStore: s.runs,
		Metrics:         s.logRoutingMetrics,
	})

	if err != nil {
		return fmt.Errorf("failed to connect to log service: %w", err)
	}

	s.logClient.Store(&logClientHolder{
		client: client,
		state:  client.GRPCConnectivityState,
		close:  func() { _ = client.Close() },
	})

	return nil
}

func (s *APIServer) runHTTPServer(ctx context.Context, srv *http.Server, serve func() error) error {
	s.draining.Store(false)
	errCh := make(chan error, 1)
	go func() {
		errCh <- serve()
	}()

	select {
	case <-ctx.Done():
		s.draining.Store(true)
		shutCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil && s.logger != nil {
			s.logger.Warn("API server shutdown: %v", err)
		}

		err := <-errCh
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}

		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}

		return err
	}
}

func (s *APIServer) Run(ctx context.Context, addr string) error {
	srvCtx, srvCancel := context.WithCancel(ctx)
	defer srvCancel()
	s.srvCtx.Store(&ctxHolder{ctx: srvCtx})

	srv := s.newHTTPServer(addr)
	s.logger.Info("API server listening on %s", addr)
	return s.runHTTPServer(ctx, srv, srv.ListenAndServe)
}

func (s *APIServer) Serve(ctx context.Context, l net.Listener) error {
	srvCtx, srvCancel := context.WithCancel(ctx)
	defer srvCancel()
	s.srvCtx.Store(&ctxHolder{ctx: srvCtx})

	srv := s.newHTTPServer("")
	s.logger.Info("API server serving on %s", l.Addr().String())
	return s.runHTTPServer(ctx, srv, func() error { return srv.Serve(l) })
}

func (s *APIServer) newHTTPServer(addr string) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
		MaxHeaderBytes:    httpsecurity.DefaultMaxHeaderBytes,
	}
}
