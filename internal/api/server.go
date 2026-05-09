package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/api/audit"
	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/api/ratelimit"
	"vectis/internal/backoff"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	jobvalidation "vectis/internal/job/validation"
	"vectis/internal/observability"
	"vectis/internal/queueclient"
	"vectis/internal/resolver"
	"vectis/internal/version"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	defaultForceFailReason = "manually failed via API"

	defaultShutdownTimeout   = 30 * time.Second
	healthDBPingTimeout      = 2 * time.Second
	defaultReadHeaderTimeout = 10 * time.Second
	defaultIdleTimeout       = 120 * time.Second
	defaultHandlerDBTimeout  = 60 * time.Second
)

type queueClientHolder struct {
	client interfaces.QueueService
	close  func()
}

type logClientHolder struct {
	client api.LogServiceClient
	close  func()
}

type ctxHolder struct {
	ctx context.Context
}

type APIServer struct {
	jobs           dal.JobsRepository
	runs           dal.RunsRepository
	ephemeralRuns  dal.EphemeralRunStarter
	authRepo       dal.AuthRepository
	namespaces     dal.NamespacesRepository
	roleBindings   dal.RoleBindingsRepository
	idempotency    dal.IdempotencyRepository
	dispatchEvents dal.DispatchEventsRepository
	logger         interfaces.Logger
	queueClient    atomic.Pointer[queueClientHolder]
	logClient      atomic.Pointer[logClientHolder]
	runBroadcaster *RunBroadcaster
	dbUnavailable  atomic.Bool
	healthDB       *sql.DB
	MetricsHandler http.Handler
	// AccessLogger, when set, writes one structured slog record per HTTP request
	// (typically JSON on stderr). Health and /metrics are excluded.
	AccessLogger *slog.Logger

	// authzOverride, if non-nil, replaces SelectAuthorizer(complete) in middleware (tests).
	authzOverride authz.Authorizer

	// rateLimiter, when set, applies rate limiting to API routes.
	rateLimiter ratelimit.RateLimiter

	// auditor, when set, logs audit events for auth operations.
	auditor audit.Auditor

	// retryMetrics, when set, records retry/backoff metrics for gRPC dials.
	retryMetrics backoff.RetryMetrics

	// ResolveWorkerAddress, when set, resolves a worker_id to a control address via the registry.
	ResolveWorkerAddress func(ctx context.Context, workerID string) (string, error)

	mu     sync.RWMutex
	srvCtx atomic.Pointer[ctxHolder]
}

type routeSpec struct {
	Pattern   string
	Handler   http.Handler
	Auth      routeAuthPolicy
	RateLimit ratelimit.Rule
}

func NewAPIServer(logger interfaces.Logger, db *sql.DB) *APIServer {
	repos := dal.NewSQLRepositories(db)
	s := NewAPIServerWithRepositories(logger, repos.Jobs(), repos.Runs(), repos)
	s.healthDB = db
	s.authRepo = repos.Auth()
	s.namespaces = repos.Namespaces()
	s.roleBindings = repos.RoleBindings()
	s.idempotency = repos.Idempotency()
	s.dispatchEvents = repos.DispatchEvents()
	return s
}

func NewAPIServerWithRepositories(
	logger interfaces.Logger,
	jobs dal.JobsRepository,
	runs dal.RunsRepository,
	ephemeralRuns dal.EphemeralRunStarter,
) *APIServer {
	return &APIServer{
		jobs:           jobs,
		runs:           runs,
		ephemeralRuns:  ephemeralRuns,
		logger:         logger,
		runBroadcaster: NewRunBroadcaster(logger),
	}
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
	return r.Header.Get("Idempotency-Key")
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
	if key == "" || s.idempotency == nil {
		return dal.IdempotencyRecord{}, false, true
	}

	record, created, err := s.idempotency.Reserve(ctx, scope, key, requestHash)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return dal.IdempotencyRecord{}, false, false
		}

		s.logger.Error("Database error reserving idempotency key: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return dal.IdempotencyRecord{}, false, false
	}

	if !created && record.RequestHash != requestHash {
		writeAPIError(w, http.StatusConflict, "idempotency_key_reused", "idempotency key reused for different request", nil)
		return dal.IdempotencyRecord{}, false, false
	}

	if !created && record.ResponseJSON == nil {
		writeAPIError(w, http.StatusConflict, "idempotency_in_progress", "idempotent request is still in progress", nil)
		return dal.IdempotencyRecord{}, false, false
	}

	return record, created, true
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

func (s *APIServer) recordDispatchEvent(ctx context.Context, runID, source, eventType string, message *string) {
	if s.dispatchEvents == nil {
		return
	}

	if err := s.dispatchEvents.Record(ctx, runID, source, eventType, message); err != nil {
		s.logger.Error("Failed to record dispatch event for run %s: %v", runID, err)
	}
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

	writeAuthJSON(w, http.StatusUnauthorized, authAPIError{Error: AuthJSONAuthenticationRequired})
	return nil, false
}

func (s *APIServer) authorizeNamespace(ctx context.Context, w http.ResponseWriter, p *authn.Principal, action authz.Action, namespacePath string) bool {
	if !config.APIAuthEnabled() {
		return true
	}

	z := s.effectiveAuthorizer(true)
	if !z.Allow(ctx, p, action, authz.Resource{NamespacePath: namespacePath}) {
		writeAuthJSON(w, http.StatusForbidden, authAPIError{Error: AuthJSONAuthorizationDenied})
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
		writeAuthJSON(w, http.StatusForbidden, authAPIError{Error: AuthJSONAuthorizationDenied})
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
			// Ephemeral runs don't have stored_jobs entries;
			// they run in the default namespace.
			return "/", nil
		}
		return "", err
	}

	return nsPath, nil
}

func writeVersionHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Vectis-Build-Version", version.Version)
	w.Header().Set("X-Vectis-Build-Commit", version.Commit)
	w.Header().Set("X-Vectis-Build-Date", version.BuildDate)
}

func (s *APIServer) HealthLive(w http.ResponseWriter, _ *http.Request) {
	writeVersionHeaders(w)
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) HealthReady(w http.ResponseWriter, r *http.Request) {
	writeVersionHeaders(w)

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

func (s *APIServer) SetRateLimiter(limiter ratelimit.RateLimiter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rateLimiter = limiter
}

func (s *APIServer) SetAuditor(auditor audit.Auditor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auditor = auditor
}

func (s *APIServer) SetRetryMetrics(m backoff.RetryMetrics) {
	s.retryMetrics = m
}

func (s *APIServer) auditLog(ctx context.Context, eventType string, actorID, targetID int64, metadata map[string]any) {
	s.mu.RLock()
	auditor := s.auditor
	s.mu.RUnlock()

	if auditor == nil {
		return
	}

	ip := ""
	if req, ok := ctx.Value(httpRequestKey{}).(*http.Request); ok && req != nil {
		ip, _, _ = net.SplitHostPort(req.RemoteAddr)
		if ip == "" {
			ip = req.RemoteAddr
		}
	}

	_ = auditor.Log(ctx, audit.Event{
		Type:          eventType,
		ActorID:       actorID,
		TargetID:      targetID,
		Metadata:      metadata,
		IPAddress:     ip,
		CorrelationID: observability.CorrelationID(ctx),
	})
}

type httpRequestKey struct{}

func (s *APIServer) ConnectToQueue(ctx context.Context) error {
	old := s.queueClient.Swap(nil)
	if old != nil && old.close != nil {
		old.close()
	}

	mq, err := queueclient.NewManagingQueueService(ctx, s.logger, func(ctx context.Context) (*grpc.ClientConn, func(), error) {
		return resolver.DialQueue(ctx, s.logger, config.PinnedQueueAddress(), config.APIRegistryDialAddress(), s.retryMetrics)
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

	conn, cleanup, err := resolver.DialLog(ctx, s.logger, config.APILogAddress(), config.APIRegistryDialAddress(), s.retryMetrics)
	if err != nil {
		return fmt.Errorf("failed to connect to log service: %w", err)
	}

	client := api.NewLogServiceClient(conn)
	s.logClient.Store(&logClientHolder{client: client, close: cleanup})
	return nil
}

func (s *APIServer) ForceFailRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunOperator, nsPath) {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return
	}

	_, found, err := s.runs.GetRunStatus(ctx, runID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	if !found {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return
	}

	reason := defaultForceFailReason
	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "request_read_failed", "failed to read request body", nil)
		return
	}

	if len(body) > 0 {
		var req struct {
			Reason string `json:"reason"`
		}

		if err := json.Unmarshal(body, &req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request_body", "invalid request body", nil)
			return
		}

		if req.Reason != "" {
			reason = req.Reason
		}
	}

	if err := s.runs.MarkRunFailed(ctx, runID, "", dal.FailureCodeForceFailed, reason); err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Force-fail run %s failed: %v", runID, err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventRunForceFailed, actorID, 0, map[string]any{
		"run_id":    runID,
		"namespace": nsPath,
		"reason":    reason,
	})

	s.logger.Warn("Run force-failed via API: %s", runID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *APIServer) ForceRequeueRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunOperator, nsPath) {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return
	}

	status, found, err := s.runs.GetRunStatus(ctx, runID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	if !found {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return
	}

	if status == "succeeded" {
		writeAPIError(w, http.StatusConflict, "run_requeue_forbidden", "cannot requeue succeeded run", map[string]any{"status": status})
		return
	}

	if err := s.runs.RequeueRunForRetry(ctx, runID); err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusConflict, "run_requeue_conflict", "run cannot be requeued from current status", nil)
			return
		}

		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		s.logger.Error("Force-requeue run %s failed: %v", runID, err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventRunForceRequeued, actorID, 0, map[string]any{
		"run_id":    runID,
		"namespace": nsPath,
	})

	s.logger.Warn("Run force-requeued via API: %s", runID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *APIServer) CancelRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunOperator, nsPath) {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return
	}

	rec, err := s.runs.GetRunForCancel(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	if rec.Status != "running" {
		writeAPIError(w, http.StatusConflict, "run_not_executing", "run is not executing", map[string]any{"status": rec.Status})
		return
	}

	if rec.LeaseOwner == "" {
		writeAPIError(w, http.StatusConflict, "run_worker_missing", "run has no assigned worker", nil)
		return
	}

	if s.ResolveWorkerAddress == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "worker_resolution_unavailable", "worker resolution not configured", nil)
		return
	}

	workerAddr, err := s.ResolveWorkerAddress(ctx, rec.LeaseOwner)
	if err != nil {
		s.logger.Error("Failed to resolve worker %s for run %s: %v", rec.LeaseOwner, runID, err)
		writeAPIError(w, http.StatusBadGateway, "worker_not_reachable", "worker not reachable", nil)
		return
	}

	if err := s.sendCancelToWorker(ctx, workerAddr, runID, rec.CancelToken); err != nil {
		s.logger.Error("Failed to send cancel to worker %s for run %s: %v", rec.LeaseOwner, runID, err)
		writeAPIError(w, http.StatusBadGateway, "worker_cancel_failed", "failed to send cancel to worker", nil)
		return
	}

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventRunCancelled, actorID, 0, map[string]any{
		"run_id":    runID,
		"namespace": nsPath,
	})

	s.logger.Warn("Run cancel sent to worker: %s", runID)
	w.WriteHeader(http.StatusNoContent)
}

const workerCancelRPCTimeout = 10 * time.Second

func (s *APIServer) sendCancelToWorker(ctx context.Context, workerAddr, runID, cancelToken string) error {
	dialOpts, err := config.GRPCClientDialOptions(workerAddr)
	if err != nil {
		return fmt.Errorf("worker tls config: %w", err)
	}

	conn, err := grpc.NewClient(workerAddr, dialOpts...)
	if err != nil {
		return fmt.Errorf("dial worker: %w", err)
	}
	defer conn.Close()

	client := api.NewWorkerControlServiceClient(conn)

	rpcCtx, cancel := context.WithTimeout(ctx, workerCancelRPCTimeout)
	defer cancel()

	_, err = client.CancelRun(rpcCtx, &api.CancelRunRequest{
		RunId:       &runID,
		CancelToken: &cancelToken,
	})

	return err
}

func (s *APIServer) CreateJob(w http.ResponseWriter, r *http.Request) {
	if !requestContentTypeIsJSON(r) {
		writeAPIError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "content type must be application/json", nil)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "request_read_failed", "failed to read request body", nil)
		return
	}

	var req struct {
		Namespace string          `json:"namespace"`
		Job       json.RawMessage `json:"job"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		// Fallback to legacy format: the body is the job definition itself
		req.Job = body
	}

	if req.Job == nil {
		req.Job = body
	}

	var job api.Job
	if err := json.Unmarshal(req.Job, &job); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", nil)
		return
	}

	if job.Id == nil || *job.Id == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_job_id", "job id is required", nil)
		return
	}

	if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{RequireJobID: true}); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", jobvalidation.ErrorDetails(err))
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) {
		return
	}

	namespacePath := "/"
	if req.Namespace != "" {
		namespacePath = req.Namespace
	}

	ns, err := s.namespaces.GetByPath(ctx, namespacePath)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "namespace_not_found", "namespace not found", nil)
			return
		}
		if s.handleDBUnavailableError(w, err) {
			return
		}
		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionJobWrite, ns.Path) {
		return
	}

	err = s.jobs.Create(ctx, *job.Id, string(req.Job), ns.ID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusConflict, "job_already_exists", "job already exists", nil)
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventJobCreated, actorID, 0, map[string]any{
		"job_id":    *job.Id,
		"namespace": ns.Path,
	})

	s.logger.Info("Stored job: %s", *job.Id)
	w.WriteHeader(http.StatusCreated)
}

func (s *APIServer) DeleteJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionJobWrite, nsPath) {
		writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
		return
	}

	err = s.jobs.Delete(ctx, jobID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventJobDeleted, actorID, 0, map[string]any{
		"job_id":    jobID,
		"namespace": nsPath,
	})

	s.logger.Info("Deleted job: %s", jobID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *APIServer) GetJobs(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	params := parsePageParams(r)
	records, nextCursor, err := s.jobs.List(ctx, params.Cursor, params.Limit)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	var jobs []map[string]any
	z := s.effectiveAuthorizer(true)
	for _, rec := range records {
		var nsPath string
		if s.namespaces != nil && config.APIAuthEnabled() {
			ns, err := s.namespaces.GetByID(ctx, rec.NamespaceID)
			if err != nil {
				continue
			}

			nsPath = ns.Path
			if !z.Allow(ctx, p, authz.ActionJobRead, authz.Resource{NamespacePath: nsPath}) {
				continue
			}
		}

		var definition any
		if err := json.Unmarshal([]byte(rec.DefinitionJSON), &definition); err != nil {
			s.logger.Error("Failed to parse job definition for job %s: %v", rec.JobID, err)
			continue
		}

		job := map[string]any{
			"name":       rec.JobID,
			"definition": definition,
		}

		if nsPath != "" {
			job["namespace"] = nsPath
		}

		jobs = append(jobs, job)
	}

	w.Header().Set("Content-Type", "application/json")
	if jobs == nil {
		jobs = make([]map[string]any, 0)
	}

	resp := buildPaginatedResponse(jobs, nextCursor)
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(resp); err != nil {
		s.logger.Error("Failed to encode jobs as JSON: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	_, _ = w.Write(buf.Bytes())
}

func (s *APIServer) GetJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionJobRead, nsPath) {
		writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
		return
	}

	var definitionJSON string
	var version int

	if versionParam := r.URL.Query().Get("version"); versionParam != "" {
		v, err := strconv.Atoi(versionParam)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_version", "invalid version parameter", nil)
			return
		}

		definitionJSON, err = s.jobs.GetDefinitionVersion(ctx, jobID, v)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAPIError(w, http.StatusNotFound, "job_version_not_found", "job version not found", nil)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
			return
		}

		version = v
	} else {
		definitionJSON, version, err = s.jobs.GetDefinition(ctx, jobID)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
			return
		}
	}
	s.markDBRecovered()

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Vectis-Version", strconv.Itoa(version))
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, definitionJSON); err != nil {
		s.logger.Error("Failed to write job definition: %v", err)
	}
}

func (s *APIServer) TriggerJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunTrigger, nsPath) {
		writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
		return
	}

	definitionJSON, _, err := s.jobs.GetDefinition(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	var job api.Job
	if err := protojson.Unmarshal([]byte(definitionJSON), &job); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "invalid_stored_job_definition", "invalid job definition stored", nil)
		return
	}

	job.Id = &jobID

	idempotencyKey := idempotencyKeyFromRequest(r)
	idempotencyScope := principalIdempotencyScope("trigger:"+jobID, p)
	idempotencyHash := hashIdempotencyRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID)
	idempotencyRecord, idempotencyReserved, ok := s.reserveIdempotency(w, ctx, idempotencyScope, idempotencyKey, idempotencyHash)
	if !ok {
		return
	}

	if idempotencyRecord.ResponseJSON != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, *idempotencyRecord.ResponseJSON)
		return
	}

	runID, runIndex, err := s.runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error creating job run: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	s.runBroadcaster.Broadcast(jobID, runID, runIndex)
	job.RunId = &runID

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventRunTriggered, actorID, 0, map[string]any{
		"job_id":    jobID,
		"run_id":    runID,
		"run_index": runIndex,
		"namespace": nsPath,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(map[string]any{
		"job_id":    jobID,
		"run_id":    runID,
		"run_index": runIndex,
	}); err != nil {
		s.logger.Error("Failed to encode trigger response: %v", err)
		return
	}

	_, _ = w.Write(buf.Bytes())
	s.completeIdempotency(ctx, idempotencyScope, idempotencyKey, buf.Bytes())

	// NOTE(garrett): We finish the enqueue asynchronously so that we can response immediately to the client,
	// rather than them waiting for the enqueue to complete (dual enqueue is idempotent by worker claim).
	bgCtx := detachedTraceContextFromRequest(r)
	go s.finishTriggerEnqueue(bgCtx, jobID, runID, runIndex, &job)
}

func (s *APIServer) finishTriggerEnqueue(ctx context.Context, jobID, runID string, runIndex int, job *api.Job) {
	ctx, span := observability.Tracer("vectis/api").Start(ctx, "run.enqueue.trigger.async", trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
	span.SetAttributes(observability.RunIndexAttrs(runIndex)...)
	span.SetAttributes(attribute.String("run.phase", "enqueue"))

	holder := s.queueClient.Load()
	var qc interfaces.QueueService
	if holder != nil {
		qc = holder.client
	}

	req := &api.JobRequest{Job: job}
	if req.Metadata == nil {
		req.Metadata = map[string]string{}
	}

	req.Metadata[observability.JobEnqueuedAtUnixNanoKey] = strconv.FormatInt(time.Now().UnixNano(), 10)
	observability.InjectJobTraceContext(ctx, req)

	s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventAttempt, nil)
	if err := enqueueWithRetry(ctx, qc, req, s.logger); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue")
		span.End()
		s.logger.Error("Failed to enqueue job (run %s): %v", runID, err)
		msg := err.Error()
		s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, &msg)
		return
	}
	span.SetAttributes(attribute.String("vectis.enqueue.outcome", "success"))
	span.End()

	if err := s.runs.TouchDispatched(ctx, runID); err != nil {
		_, tdSpan := observability.Tracer("vectis/api").Start(ctx, "run.touch_dispatched", trace.WithSpanKind(trace.SpanKindInternal))
		tdSpan.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
		tdSpan.RecordError(err)
		tdSpan.SetStatus(codes.Error, "touch dispatched")
		tdSpan.End()
		s.logger.Error("TouchDispatched after enqueue (run %s): %v", runID, err)
		msg := "touch dispatched: " + err.Error()
		s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, &msg)
		return
	}
	s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventSuccess, nil)

	_, tdSpan := observability.Tracer("vectis/api").Start(ctx, "run.touch_dispatched", trace.WithSpanKind(trace.SpanKindInternal))
	tdSpan.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
	if runIndex > 0 {
		tdSpan.SetAttributes(observability.RunIndexAttrs(runIndex)...)
	}
	tdSpan.End()

	s.logger.Info("Triggered job: %s (run %s, index %d)", jobID, runID, runIndex)
}

func (s *APIServer) UpdateJobDefinition(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	if !requestContentTypeIsJSON(r) {
		writeAPIError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "content type must be application/json", nil)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "request_read_failed", "failed to read request body", nil)
		return
	}

	var job api.Job
	if err := json.Unmarshal(body, &job); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", nil)
		return
	}

	if job.Id == nil || *job.Id != jobID {
		writeAPIError(w, http.StatusBadRequest, "job_id_mismatch", "job id mismatch", nil)
		return
	}

	if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{RequireJobID: true}); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", jobvalidation.ErrorDetails(err))
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionJobWrite, nsPath) {
		writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
		return
	}

	newVersion, err := s.jobs.UpdateDefinition(ctx, jobID, string(body))
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	w.Header().Set("X-Vectis-Version", strconv.Itoa(newVersion))

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventJobUpdated, actorID, 0, map[string]any{
		"job_id":    jobID,
		"namespace": nsPath,
	})

	s.logger.Info("Updated job definition: %s", jobID)
	w.WriteHeader(http.StatusNoContent)
}

// Ephemeral runs persist definition version 1 in job_definitions so the reconciler can re-enqueue if the queue drops work.
func (s *APIServer) RunJob(w http.ResponseWriter, r *http.Request) {
	if !requestContentTypeIsJSON(r) {
		writeAPIError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "content type must be application/json", nil)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "request_read_failed", "failed to read request body", nil)
		return
	}

	var req struct {
		Namespace string          `json:"namespace"`
		Job       json.RawMessage `json:"job"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		req.Job = body
	}

	if req.Job == nil {
		req.Job = body
	}

	var job api.Job
	if err := json.Unmarshal(req.Job, &job); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", nil)
		return
	}

	if job.Id == nil || *job.Id == "" {
		generatedID := uuid.New().String()
		job.Id = &generatedID
	}

	if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{}); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", jobvalidation.ErrorDetails(err))
		return
	}

	generatedID := uuid.New().String()
	job.Id = &generatedID

	definitionJSON, err := json.Marshal(&job)
	if err != nil {
		s.logger.Error("Failed to marshal job definition: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	runIndexOne := 1
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) {
		return
	}

	namespacePath := "/"
	if req.Namespace != "" {
		namespacePath = req.Namespace
	}

	ns, err := s.namespaces.GetByPath(ctx, namespacePath)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "namespace_not_found", "namespace not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionRunTrigger, ns.Path) {
		return
	}

	idempotencyKey := idempotencyKeyFromRequest(r)
	idempotencyScope := principalIdempotencyScope("run:"+ns.Path, p)
	idempotencyHash := hashIdempotencyRequest(http.MethodPost, "/api/v1/jobs/run", string(body))
	idempotencyRecord, idempotencyReserved, ok := s.reserveIdempotency(w, ctx, idempotencyScope, idempotencyKey, idempotencyHash)
	if !ok {
		return
	}

	if idempotencyRecord.ResponseJSON != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, *idempotencyRecord.ResponseJSON)
		return
	}

	runID, _, err := s.ephemeralRuns.CreateDefinitionAndRun(ctx, generatedID, string(definitionJSON), &runIndexOne)
	if err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error creating ephemeral job run: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	job.RunId = &runID

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventRunTriggered, actorID, 0, map[string]any{
		"job_id":    generatedID,
		"run_id":    runID,
		"namespace": ns.Path,
		"ephemeral": true,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(map[string]string{
		"id":     generatedID,
		"run_id": runID,
	}); err != nil {
		s.logger.Error("Failed to encode response: %v", err)
		return
	}

	_, _ = w.Write(buf.Bytes())
	s.completeIdempotency(ctx, idempotencyScope, idempotencyKey, buf.Bytes())

	bgCtx := detachedTraceContextFromRequest(r)

	go s.finishRunJobEnqueue(bgCtx, generatedID, runID, &job)
}

func (s *APIServer) finishRunJobEnqueue(ctx context.Context, generatedID, runID string, job *api.Job) {
	ctx, span := observability.Tracer("vectis/api").Start(ctx, "run.enqueue.ephemeral.async", trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(observability.JobRunAttrs(generatedID, runID)...)
	span.SetAttributes(attribute.Bool("vectis.run.ephemeral", true))
	span.SetAttributes(attribute.String("run.phase", "enqueue"))

	holder := s.queueClient.Load()
	var qc interfaces.QueueService
	if holder != nil {
		qc = holder.client
	}

	req := &api.JobRequest{Job: job}
	if req.Metadata == nil {
		req.Metadata = map[string]string{}
	}

	req.Metadata[observability.JobEnqueuedAtUnixNanoKey] = strconv.FormatInt(time.Now().UnixNano(), 10)
	observability.InjectJobTraceContext(ctx, req)

	s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventAttempt, nil)
	if err := enqueueWithRetry(ctx, qc, req, s.logger); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue")
		span.End()
		s.logger.Error("Failed to enqueue job (run %s): %v", runID, err)
		msg := err.Error()
		s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, &msg)
		return
	}
	span.SetAttributes(attribute.String("vectis.enqueue.outcome", "success"))
	span.End()

	if err := s.runs.TouchDispatched(ctx, runID); err != nil {
		_, tdSpan := observability.Tracer("vectis/api").Start(ctx, "run.touch_dispatched", trace.WithSpanKind(trace.SpanKindInternal))
		tdSpan.SetAttributes(observability.JobRunAttrs(generatedID, runID)...)
		tdSpan.RecordError(err)
		tdSpan.SetStatus(codes.Error, "touch dispatched")
		tdSpan.End()
		s.logger.Error("TouchDispatched after enqueue (run %s): %v", runID, err)
		msg := "touch dispatched: " + err.Error()
		s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, &msg)
		return
	}
	s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventSuccess, nil)

	_, tdSpan := observability.Tracer("vectis/api").Start(ctx, "run.touch_dispatched", trace.WithSpanKind(trace.SpanKindInternal))
	tdSpan.SetAttributes(observability.JobRunAttrs(generatedID, runID)...)
	tdSpan.End()

	s.logger.Info("Enqueued ephemeral job: %s (run %s)", generatedID, runID)
}

func detachedTraceContextFromRequest(r *http.Request) context.Context {
	if r == nil {
		return context.Background()
	}

	sc := trace.SpanFromContext(r.Context()).SpanContext()
	if !sc.IsValid() {
		return context.Background()
	}

	// Preserve trace linkage for post-202 enqueue work without inheriting the
	// HTTP request cancellation/deadline; the reconciler is the durable backstop.
	return trace.ContextWithSpanContext(context.Background(), sc)
}

func (s *APIServer) GetJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	sinceStr := r.URL.Query().Get("since")
	var since *int
	if sinceStr != "" {
		parsedSince, err := strconv.Atoi(sinceStr)
		if err != nil || parsedSince < 0 {
			writeAPIError(w, http.StatusBadRequest, "invalid_since", "since must be a non-negative integer", nil)
			return
		}
		since = &parsedSince
	}

	params := parsePageParams(r)

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
		return
	}

	runRows, nextCursor, err := s.runs.ListByJob(ctx, jobID, since, params.Cursor, params.Limit)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	type runRow struct {
		RunID         string  `json:"run_id"`
		RunIndex      int     `json:"run_index"`
		Status        string  `json:"status"`
		OrphanReason  *string `json:"orphan_reason,omitempty"`
		FailureCode   *string `json:"failure_code,omitempty"`
		StartedAt     *string `json:"started_at,omitempty"`
		FinishedAt    *string `json:"finished_at,omitempty"`
		FailureReason *string `json:"failure_reason,omitempty"`
	}

	var runs []runRow
	for _, rec := range runRows {
		runs = append(runs, runRow{
			RunID:         rec.RunID,
			RunIndex:      rec.RunIndex,
			Status:        rec.Status,
			OrphanReason:  rec.OrphanReason,
			FailureCode:   rec.FailureCode,
			StartedAt:     rec.StartedAt,
			FinishedAt:    rec.FinishedAt,
			FailureReason: rec.FailureReason,
		})
	}

	if runs == nil {
		runs = []runRow{}
	}

	w.Header().Set("Content-Type", "application/json")
	resp := buildPaginatedResponse(runs, nextCursor)
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(resp); err != nil {
		s.logger.Error("Failed to encode runs: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	_, _ = w.Write(buf.Bytes())
}

func (s *APIServer) GetRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	rec, err := s.runs.GetRun(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}
		if s.handleDBUnavailableError(w, err) {
			return
		}
		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	nsPath, err := s.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return
	}

	dispatchEvents := []dal.DispatchEvent{}
	if s.dispatchEvents != nil {
		dispatchEvents, err = s.dispatchEvents.ListByRun(ctx, runID)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
			return
		}
	}

	type dispatchEventRow struct {
		ID        int64   `json:"id"`
		Source    string  `json:"source"`
		EventType string  `json:"event_type"`
		Message   *string `json:"message,omitempty"`
		CreatedAt int64   `json:"created_at"`
	}

	type runRow struct {
		RunID          string             `json:"run_id"`
		RunIndex       int                `json:"run_index"`
		Status         string             `json:"status"`
		OrphanReason   *string            `json:"orphan_reason,omitempty"`
		FailureCode    *string            `json:"failure_code,omitempty"`
		StartedAt      *string            `json:"started_at,omitempty"`
		FinishedAt     *string            `json:"finished_at,omitempty"`
		FailureReason  *string            `json:"failure_reason,omitempty"`
		DispatchEvents []dispatchEventRow `json:"dispatch_events"`
	}

	resp := runRow{
		RunID:          rec.RunID,
		RunIndex:       rec.RunIndex,
		Status:         rec.Status,
		OrphanReason:   rec.OrphanReason,
		FailureCode:    rec.FailureCode,
		StartedAt:      rec.StartedAt,
		FinishedAt:     rec.FinishedAt,
		FailureReason:  rec.FailureReason,
		DispatchEvents: []dispatchEventRow{},
	}

	for _, event := range dispatchEvents {
		resp.DispatchEvents = append(resp.DispatchEvents, dispatchEventRow{
			ID:        event.ID,
			Source:    event.Source,
			EventType: event.EventType,
			Message:   event.Message,
			CreatedAt: event.CreatedAt,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(resp); err != nil {
		s.logger.Error("Failed to encode run: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	_, _ = w.Write(buf.Bytes())
}

func (s *APIServer) HandleSSEJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// NOTE(garrett): Prevent buffering behind various proxies for lower latency.
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrStreamingUnsupported)
		return
	}

	ch := s.runBroadcaster.Subscribe(jobID)
	defer s.runBroadcaster.Unsubscribe(jobID, ch)

	s.logger.Info("SSE client subscribed to runs for job: %s", jobID)

	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			_, _ = w.Write([]byte(": keep-alive\n\n"))
			flusher.Flush()
		case payload, ok := <-ch:
			if !ok {
				return
			}
			if _, err := w.Write([]byte("data: ")); err != nil {
				return
			}
			if _, err := w.Write(payload); err != nil {
				return
			}
			if _, err := w.Write([]byte("\n\n")); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *APIServer) GetRunLogs(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, err := s.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return
	}

	holder := s.logClient.Load()
	if holder == nil || holder.client == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "log_service_unavailable", "log service not connected", nil)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		writeAPIError(w, http.StatusInternalServerError, "streaming_unsupported", "streaming unsupported", nil)
		return
	}

	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	stream, err := holder.client.GetLogs(r.Context(), &api.GetLogsRequest{RunId: &runID})
	if err != nil {
		s.logger.Warn("Failed to connect to log service: %v", err)
		writeAPIError(w, http.StatusBadGateway, "log_service_error", "failed to connect to log service", nil)
		return
	}

	sawCompletion := false
	for {
		chunk, err := stream.Recv()
		if err != nil {
			break
		}

		jsonData := formatLogChunkSSE(chunk)
		if _, err := w.Write([]byte("data: ")); err != nil {
			return
		}
		if _, err := w.Write(jsonData); err != nil {
			return
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return
		}
		flusher.Flush()

		if chunk.GetCompleted() != api.RunOutcome_RUN_OUTCOME_UNSPECIFIED {
			sawCompletion = true
		}
	}

	if !sawCompletion {
		// Use a fresh context for the one-shot fallback — the handler's DB context
		// may have expired if the SSE stream has been alive longer than the timeout.
		fallbackCtx, fallbackCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer fallbackCancel()
		status, found, err := s.runs.GetRunStatus(fallbackCtx, runID)
		if err != nil {
			s.logger.Warn("Log completion fallback DB lookup failed for run %s: %v", runID, err)
		} else if found && (status == "succeeded" || status == "failed") {
			completedStatus := "unknown"
			if status == "succeeded" {
				completedStatus = "success"
			} else if status == "failed" {
				completedStatus = "failure"
			}

			inner, _ := json.Marshal(struct {
				Event     string `json:"event"`
				Status    string `json:"status"`
				Synthetic bool   `json:"synthetic"`
			}{"completed", completedStatus, true})

			outer, _ := json.Marshal(struct {
				Timestamp string         `json:"timestamp"`
				Stream    api.Stream     `json:"stream"`
				Sequence  int64          `json:"sequence"`
				Data      string         `json:"data"`
				Completed api.RunOutcome `json:"completed,omitempty"`
			}{time.Now().Format(time.RFC3339Nano), api.Stream_STREAM_CONTROL, -1, string(inner), api.RunOutcome_RUN_OUTCOME_UNKNOWN})

			w.Write([]byte("data: "))
			w.Write(outer)
			w.Write([]byte("\n\n"))
			flusher.Flush()
		}
	}
}

func formatLogChunkSSE(chunk *api.LogChunk) []byte {
	// The log server always sets Timestamp on GetLogs chunks. time.Now() is
	// a safety net for any future code path that leaves the field nil.
	ts := time.Now().Format(time.RFC3339Nano)
	if t := chunk.GetTimestamp(); t != nil {
		ts = t.AsTime().Format(time.RFC3339Nano)
	}

	data := string(chunk.GetData())
	b, err := json.Marshal(struct {
		Timestamp string         `json:"timestamp"`
		Stream    api.Stream     `json:"stream"`
		Sequence  int64          `json:"sequence"`
		Data      string         `json:"data"`
		Completed api.RunOutcome `json:"completed,omitempty"`
	}{
		Timestamp: ts,
		Stream:    chunk.GetStream(),
		Sequence:  chunk.GetSequence(),
		Data:      data,
		Completed: chunk.GetCompleted(),
	})
	if err != nil {
		b = []byte(`{}`)
	}
	return b
}

func (s *APIServer) Handler() http.Handler {
	mux := http.NewServeMux()

	for _, spec := range s.routeSpecs(true) {
		s.registerRoute(mux, spec)
	}

	h := http.Handler(mux)
	h = accessLogMiddleware(s.AccessLogger, apiHTTPExcludedFromAuxLogging, h)
	h = observability.CorrelationMiddleware(h)
	h = panicRecoveryMiddleware(s.logger, h)
	return instrumentHTTPServer(h)
}

func (s *APIServer) routeSpecs(includeMetrics bool) []routeSpec {
	defaultLimits := ratelimit.DefaultCategory()
	specs := make([]routeSpec, 0, 36)

	if includeMetrics && s.MetricsHandler != nil {
		specs = append(specs, routeSpec{
			Pattern: "GET /metrics",
			Handler: s.MetricsHandler,
			Auth:    routeAuthPolicy{Public: true},
		})
	}

	specs = append(specs,
		routeSpec{Pattern: "GET /health/live", Handler: http.HandlerFunc(s.HealthLive), Auth: routeAuthPolicy{Public: true}},
		routeSpec{Pattern: "GET /health/ready", Handler: http.HandlerFunc(s.HealthReady), Auth: routeAuthPolicy{Public: true}},
		routeSpec{Pattern: "GET /api/v1/jobs", Handler: http.HandlerFunc(s.GetJobs), Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/jobs/{id}", Handler: http.HandlerFunc(s.GetJob), Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/jobs", Handler: http.HandlerFunc(s.CreateJob), Auth: routeAuthPolicy{Action: authz.ActionJobWrite}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/jobs/run", Handler: http.HandlerFunc(s.RunJob), Auth: routeAuthPolicy{Action: authz.ActionRunTrigger}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "DELETE /api/v1/jobs/{id}", Handler: http.HandlerFunc(s.DeleteJob), Auth: routeAuthPolicy{Action: authz.ActionJobWrite}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "PUT /api/v1/jobs/{id}", Handler: http.HandlerFunc(s.UpdateJobDefinition), Auth: routeAuthPolicy{Action: authz.ActionJobWrite}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/jobs/trigger/{id}", Handler: http.HandlerFunc(s.TriggerJob), Auth: routeAuthPolicy{Action: authz.ActionRunTrigger}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/jobs/{id}/runs", Handler: http.HandlerFunc(s.GetJobRuns), Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/sse/jobs/{id}/runs", Handler: http.HandlerFunc(s.HandleSSEJobRuns), Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/runs/{id}", Handler: http.HandlerFunc(s.GetRun), Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/cancel", Handler: http.HandlerFunc(s.CancelRun), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/force-fail", Handler: http.HandlerFunc(s.ForceFailRun), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/runs/{id}/force-requeue", Handler: http.HandlerFunc(s.ForceRequeueRun), Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/runs/{id}/logs", Handler: http.HandlerFunc(s.GetRunLogs), Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/setup/status", Handler: http.HandlerFunc(s.GetSetupStatus), Auth: routeAuthPolicy{Action: authz.ActionSetupStatus}, RateLimit: defaultLimits.Auth},
		routeSpec{Pattern: "POST /api/v1/setup/complete", Handler: http.HandlerFunc(s.PostSetupComplete), Auth: routeAuthPolicy{Action: authz.ActionSetupComplete}, RateLimit: defaultLimits.Auth},
		routeSpec{Pattern: "POST /api/v1/login", Handler: http.HandlerFunc(s.Login), Auth: routeAuthPolicy{Public: true}, RateLimit: defaultLimits.Auth},
		routeSpec{Pattern: "GET /api/v1/tokens", Handler: http.HandlerFunc(s.ListTokens), Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Token},
		routeSpec{Pattern: "POST /api/v1/tokens", Handler: http.HandlerFunc(s.CreateToken), Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Token},
		routeSpec{Pattern: "DELETE /api/v1/tokens/{id}", Handler: http.HandlerFunc(s.DeleteToken), Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Token},
		routeSpec{Pattern: "POST /api/v1/users/change-password", Handler: http.HandlerFunc(s.ChangePassword), Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Auth},
		routeSpec{Pattern: "POST /api/v1/users", Handler: http.HandlerFunc(s.CreateUser), Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/users", Handler: http.HandlerFunc(s.ListUsers), Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/users/{id}", Handler: http.HandlerFunc(s.GetUser), Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "PUT /api/v1/users/{id}", Handler: http.HandlerFunc(s.UpdateUser), Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "DELETE /api/v1/users/{id}", Handler: http.HandlerFunc(s.DeleteUser), Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/namespaces", Handler: http.HandlerFunc(s.ListNamespaces), Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/namespaces", Handler: http.HandlerFunc(s.CreateNamespace), Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/namespaces/{id}", Handler: http.HandlerFunc(s.GetNamespace), Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "DELETE /api/v1/namespaces/{id}", Handler: http.HandlerFunc(s.DeleteNamespace), Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "GET /api/v1/namespaces/{id}/bindings", Handler: http.HandlerFunc(s.ListBindings), Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "POST /api/v1/namespaces/{id}/bindings", Handler: http.HandlerFunc(s.CreateBinding), Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General},
		routeSpec{Pattern: "DELETE /api/v1/namespaces/{id}/bindings/{user_id}", Handler: http.HandlerFunc(s.DeleteBinding), Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General},
	)

	return specs
}

func panicRecoveryMiddleware(log interfaces.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic in HTTP handler: %v", rec)

				// Only write an error response if headers have not yet been sent.
				// If they have, the client will see a truncated response; we log it here.
				if sw, ok := w.(*statusResponseWriter); !ok || !sw.wroteHeader {
					writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
				}
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func (s *APIServer) registerRoute(mux *http.ServeMux, spec routeSpec) {
	if spec.Pattern == "" {
		panic("api route pattern must not be empty")
	}
	if spec.Handler == nil {
		panic("api route handler must not be nil")
	}

	handler := s.accessControlledHandler(spec.Auth, spec.Handler)
	s.mu.RLock()
	rl := s.rateLimiter
	s.mu.RUnlock()
	if rl != nil && spec.RateLimit.RefillRate > 0 {
		handler = s.rateLimitMiddleware(rl, spec.RateLimit, handler)
	}

	mux.Handle(spec.Pattern, handler)
}

func (s *APIServer) rateLimitMiddleware(rl ratelimit.RateLimiter, rule ratelimit.Rule, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := s.rateLimitKey(r, rule)
		allowed, retryAfter, err := rl.Allow(r.Context(), key, rule)
		if err != nil {
			s.logger.Error("Rate limiter error: %v", err)
			writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
			return
		}
		if !allowed {
			retrySeconds := int(math.Ceil(retryAfter.Seconds()))
			w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
			writeAPIErrorCode(w, http.StatusTooManyRequests, apiErrRateLimitExceeded)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *APIServer) rateLimitKey(r *http.Request, rule ratelimit.Rule) string {
	// Use Authorization header for authenticated requests, client IP for public
	var baseKey string
	if token, ok := bearerToken(r.Header.Get("Authorization")); ok {
		baseKey = hashAPIToken(token)
	}
	if baseKey == "" {
		// Fall back to client IP
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		if ip == "" {
			ip = r.RemoteAddr
		}
		baseKey = ip
	}

	// Include route pattern and rule parameters so different endpoints have isolated buckets
	return fmt.Sprintf("%s:%s:%d:%d", baseKey, r.Pattern, rule.RefillRate, rule.BurstSize)
}

func (s *APIServer) registerRouteFunc(mux *http.ServeMux, spec routeSpec, handler http.HandlerFunc) {
	s.registerRoute(mux, routeSpec{
		Pattern:   spec.Pattern,
		Handler:   handler,
		Auth:      spec.Auth,
		RateLimit: spec.RateLimit,
	})
}

func (s *APIServer) runHTTPServer(ctx context.Context, srv *http.Server, serve func() error) error {
	return cli.ServeHTTP(ctx, srv, serve, defaultShutdownTimeout, "API server", s.logger)
}

func (s *APIServer) Run(ctx context.Context, addr string) error {
	srvCtx, srvCancel := context.WithCancel(ctx)
	defer srvCancel()
	s.srvCtx.Store(&ctxHolder{ctx: srvCtx})

	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		IdleTimeout:       defaultIdleTimeout,
	}

	s.logger.Info("API server listening on %s", addr)
	return s.runHTTPServer(ctx, srv, srv.ListenAndServe)
}

func (s *APIServer) Serve(ctx context.Context, l net.Listener) error {
	srvCtx, srvCancel := context.WithCancel(ctx)
	defer srvCancel()
	s.srvCtx.Store(&ctxHolder{ctx: srvCtx})

	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		IdleTimeout:       defaultIdleTimeout,
	}

	s.logger.Info("API server serving on %s", l.Addr().String())
	return s.runHTTPServer(ctx, srv, func() error { return srv.Serve(l) })
}
