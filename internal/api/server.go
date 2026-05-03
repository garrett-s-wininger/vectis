package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/api/audit"
	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/api/ratelimit"
	"vectis/internal/cli"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/queueclient"
	"vectis/internal/resolver"

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
	logger         interfaces.Logger
	queueClient    atomic.Pointer[queueClientHolder]
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

	// ResolveWorkerAddress, when set, resolves a worker_id to a control address via the registry.
	ResolveWorkerAddress func(workerID string) (string, error)

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

func (s *APIServer) handleDBUnavailableError(w http.ResponseWriter, err error) bool {
	if database.IsUnavailableError(err) {
		s.markDBUnavailable(err)
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
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
		http.Error(w, "namespaces not configured", http.StatusServiceUnavailable)
		return false
	}
	return true
}

func (s *APIServer) requireRoleBindings(w http.ResponseWriter) bool {
	if s.roleBindings == nil {
		http.Error(w, "role bindings not configured", http.StatusServiceUnavailable)
		return false
	}
	return true
}

func (s *APIServer) requireAuthRepo(w http.ResponseWriter) bool {
	if s.authRepo == nil {
		http.Error(w, "auth not configured", http.StatusServiceUnavailable)
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

func (s *APIServer) HealthLive(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) HealthReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), healthDBPingTimeout)
	defer cancel()

	if s.healthDB != nil {
		if err := s.healthDB.PingContext(ctx); err != nil {
			http.Error(w, "database not ready", http.StatusServiceUnavailable)
			return
		}
	}

	holder := s.queueClient.Load()
	var qc interfaces.QueueService
	if holder != nil {
		qc = holder.client
	}

	if !queueRPCReady(qc) {
		http.Error(w, "queue not ready", http.StatusServiceUnavailable)
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
		return resolver.DialQueue(ctx, s.logger, config.PinnedQueueAddress(), config.APIRegistryDialAddress())
	})

	if err != nil {
		return fmt.Errorf("failed to connect to queue: %w", err)
	}

	s.queueClient.Store(&queueClientHolder{client: mq, close: func() { _ = mq.Close() }})
	return nil
}

func (s *APIServer) ForceFailRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
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
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunOperator, nsPath) {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	_, found, err := s.runs.GetRunStatus(ctx, runID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	if !found {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	reason := defaultForceFailReason
	body, err := io.ReadAll(io.LimitReader(r.Body, 64*1024))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	if len(body) > 0 {
		var req struct {
			Reason string `json:"reason"`
		}

		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
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
		http.Error(w, "internal server error", http.StatusInternalServerError)
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
		http.Error(w, "id is required", http.StatusBadRequest)
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
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunOperator, nsPath) {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	status, found, err := s.runs.GetRunStatus(ctx, runID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	if !found {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	if status == "succeeded" {
		http.Error(w, "cannot requeue succeeded run", http.StatusConflict)
		return
	}

	if err := s.runs.RequeueRunForRetry(ctx, runID); err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsConflict(err) {
			http.Error(w, "run cannot be requeued from current status", http.StatusConflict)
			return
		}

		if dal.IsNotFound(err) {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		s.logger.Error("Force-requeue run %s failed: %v", runID, err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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
		http.Error(w, "id is required", http.StatusBadRequest)
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
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunOperator, nsPath) {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	rec, err := s.runs.GetRunForCancel(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	if rec.Status != "running" {
		http.Error(w, "run is not executing", http.StatusConflict)
		return
	}

	if rec.LeaseOwner == "" {
		http.Error(w, "run has no assigned worker", http.StatusConflict)
		return
	}

	if s.ResolveWorkerAddress == nil {
		http.Error(w, "worker resolution not configured", http.StatusServiceUnavailable)
		return
	}

	workerAddr, err := s.ResolveWorkerAddress(rec.LeaseOwner)
	if err != nil {
		s.logger.Error("Failed to resolve worker %s for run %s: %v", rec.LeaseOwner, runID, err)
		http.Error(w, "worker not reachable", http.StatusBadGateway)
		return
	}

	if err := s.sendCancelToWorker(ctx, workerAddr, runID, rec.CancelToken); err != nil {
		s.logger.Error("Failed to send cancel to worker %s for run %s: %v", rec.LeaseOwner, runID, err)
		http.Error(w, "failed to send cancel to worker", http.StatusBadGateway)
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
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
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
		http.Error(w, "invalid job definition", http.StatusBadRequest)
		return
	}

	if job.Id == nil || *job.Id == "" {
		http.Error(w, "job id is required", http.StatusBadRequest)
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
			http.Error(w, "namespace not found", http.StatusNotFound)
			return
		}
		if s.handleDBUnavailableError(w, err) {
			return
		}
		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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
			http.Error(w, "job already exists", http.StatusConflict)
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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
		http.Error(w, "id is required", http.StatusBadRequest)
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
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionJobWrite, nsPath) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	err = s.jobs.Delete(ctx, jobID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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
		http.Error(w, "internal server error", http.StatusInternalServerError)
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
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(buf.Bytes())
}

func (s *APIServer) GetJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
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
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionJobRead, nsPath) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	var definitionJSON string
	var version int

	if versionParam := r.URL.Query().Get("version"); versionParam != "" {
		v, err := strconv.Atoi(versionParam)
		if err != nil {
			http.Error(w, "invalid version parameter", http.StatusBadRequest)
			return
		}

		definitionJSON, err = s.jobs.GetDefinitionVersion(ctx, jobID, v)
		if err != nil {
			if dal.IsNotFound(err) {
				http.Error(w, "job version not found", http.StatusNotFound)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		version = v
	} else {
		definitionJSON, version, err = s.jobs.GetDefinition(ctx, jobID)
		if err != nil {
			if dal.IsNotFound(err) {
				http.Error(w, "job not found", http.StatusNotFound)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
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
		http.Error(w, "id is required", http.StatusBadRequest)
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
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunTrigger, nsPath) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	definitionJSON, _, err := s.jobs.GetDefinition(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	var job api.Job
	if err := protojson.Unmarshal([]byte(definitionJSON), &job); err != nil {
		http.Error(w, "invalid job definition stored", http.StatusInternalServerError)
		return
	}

	job.Id = &jobID

	runID, runIndex, err := s.runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error creating job run: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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

	if err := enqueueWithRetry(ctx, qc, req, s.logger); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue")
		span.End()
		s.logger.Error("Failed to enqueue job (run %s): %v", runID, err)
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
		return
	}

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
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
		return
	}

	var job api.Job
	if err := json.Unmarshal(body, &job); err != nil {
		http.Error(w, "invalid job definition", http.StatusBadRequest)
		return
	}

	if job.Id == nil || *job.Id != jobID {
		http.Error(w, "job id mismatch", http.StatusBadRequest)
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
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionJobWrite, nsPath) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	newVersion, err := s.jobs.UpdateDefinition(ctx, jobID, string(body))
	if err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10*1024*1024))
	if err != nil {
		http.Error(w, "failed to read request body", http.StatusInternalServerError)
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
		http.Error(w, "invalid job definition", http.StatusBadRequest)
		return
	}

	if job.Id == nil || *job.Id == "" {
		generatedID := uuid.New().String()
		job.Id = &generatedID
	}

	if job.GetRoot() == nil {
		http.Error(w, "job must have a root node", http.StatusBadRequest)
		return
	}

	generatedID := uuid.New().String()
	job.Id = &generatedID

	definitionJSON, err := json.Marshal(&job)
	if err != nil {
		s.logger.Error("Failed to marshal job definition: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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
			http.Error(w, "namespace not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionRunTrigger, ns.Path) {
		return
	}

	runID, _, err := s.ephemeralRuns.CreateDefinitionAndRun(ctx, generatedID, string(definitionJSON), &runIndexOne)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error creating ephemeral job run: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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

	if err := enqueueWithRetry(ctx, qc, req, s.logger); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue")
		span.End()
		s.logger.Error("Failed to enqueue job (run %s): %v", runID, err)
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
		return
	}

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

	return trace.ContextWithSpanContext(context.Background(), sc)
}

func (s *APIServer) GetJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	sinceStr := r.URL.Query().Get("since")
	var since *int
	if sinceStr != "" {
		parsedSince, err := strconv.Atoi(sinceStr)
		if err != nil || parsedSince < 0 {
			http.Error(w, "since must be a non-negative integer", http.StatusBadRequest)
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
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	runRows, nextCursor, err := s.runs.ListByJob(ctx, jobID, since, params.Cursor, params.Limit)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
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
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(buf.Bytes())
}

func (s *APIServer) GetRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
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
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}
		if s.handleDBUnavailableError(w, err) {
			return
		}
		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	jobID, err := s.runs.GetRunJobID(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

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

	resp := runRow{
		RunID:         rec.RunID,
		RunIndex:      rec.RunIndex,
		Status:        rec.Status,
		OrphanReason:  rec.OrphanReason,
		FailureCode:   rec.FailureCode,
		StartedAt:     rec.StartedAt,
		FinishedAt:    rec.FinishedAt,
		FailureReason: rec.FailureReason,
	}

	w.Header().Set("Content-Type", "application/json")
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(resp); err != nil {
		s.logger.Error("Failed to encode run: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write(buf.Bytes())
}

func (s *APIServer) HandleSSEJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
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
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// NOTE(garrett): Prevent buffering behind various proxies for lower latency.
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
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
		http.Error(w, "id is required", http.StatusBadRequest)
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
			http.Error(w, "run not found", http.StatusNotFound)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		http.Error(w, "run not found", http.StatusNotFound)
		return
	}

	logAddr, err := resolver.ResolveLogSSEAddressWithTimeout(s.logger, config.APIRegistryDialAddress())
	if err != nil {
		s.logger.Warn("Failed to resolve log SSE address: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(map[string]string{
			"error": "log_service_unavailable",
		})

		_, _ = w.Write(buf.Bytes())
		return
	}

	logURL := fmt.Sprintf("http://%s/sse/logs/%s", logAddr, url.PathEscape(runID))
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, logURL, nil)
	if err != nil {
		s.logger.Error("Failed to create log proxy request: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	req.Header.Set("Accept", "text/event-stream")

	// SSE streams are long-lived; rely on request context cancellation
	// instead of a fixed client timeout that can terminate healthy streams.
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		s.logger.Warn("Log service unreachable: %v", err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		var buf bytes.Buffer
		_ = json.NewEncoder(&buf).Encode(map[string]string{
			"error": "log_service_unavailable",
		})

		_, _ = w.Write(buf.Bytes())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		s.logger.Warn("Log service returned status %d", resp.StatusCode)
		_, _ = io.Copy(io.Discard, resp.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "log_service_unavailable",
		})

		return
	}

	if resp.StatusCode >= 400 {
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			_, writeErr := w.Write(buf[:n])
			if writeErr != nil {
				return
			}
			flusher.Flush()
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			s.logger.Debug("Log proxy read error: %v", err)
			return
		}
	}
}

func (s *APIServer) Handler() http.Handler {
	mux := http.NewServeMux()

	if s.MetricsHandler != nil {
		s.registerRoute(mux, routeSpec{
			Pattern: "GET /metrics",
			Handler: s.MetricsHandler,
			Auth:    routeAuthPolicy{Public: true},
		})
	}

	defaultLimits := ratelimit.DefaultCategory()

	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /health/live", Auth: routeAuthPolicy{Public: true}}, s.HealthLive)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /health/ready", Auth: routeAuthPolicy{Public: true}}, s.HealthReady)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/jobs", Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General}, s.GetJobs)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/jobs/{id}", Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General}, s.GetJob)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/jobs", Auth: routeAuthPolicy{Action: authz.ActionJobWrite}, RateLimit: defaultLimits.General}, s.CreateJob)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/jobs/run", Auth: routeAuthPolicy{Action: authz.ActionRunTrigger}, RateLimit: defaultLimits.General}, s.RunJob)
	s.registerRouteFunc(mux, routeSpec{Pattern: "DELETE /api/v1/jobs/{id}", Auth: routeAuthPolicy{Action: authz.ActionJobWrite}, RateLimit: defaultLimits.General}, s.DeleteJob)
	s.registerRouteFunc(mux, routeSpec{Pattern: "PUT /api/v1/jobs/{id}", Auth: routeAuthPolicy{Action: authz.ActionJobWrite}, RateLimit: defaultLimits.General}, s.UpdateJobDefinition)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/jobs/trigger/{id}", Auth: routeAuthPolicy{Action: authz.ActionRunTrigger}, RateLimit: defaultLimits.General}, s.TriggerJob)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/jobs/{id}/runs", Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General}, s.GetJobRuns)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/sse/jobs/{id}/runs", Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General}, s.HandleSSEJobRuns)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/runs/{id}", Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General}, s.GetRun)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/runs/{id}/cancel", Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General}, s.CancelRun)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/runs/{id}/force-fail", Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General}, s.ForceFailRun)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/runs/{id}/force-requeue", Auth: routeAuthPolicy{Action: authz.ActionRunOperator}, RateLimit: defaultLimits.General}, s.ForceRequeueRun)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/runs/{id}/logs", Auth: routeAuthPolicy{Action: authz.ActionRunRead}, RateLimit: defaultLimits.General}, s.GetRunLogs)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/setup/status", Auth: routeAuthPolicy{Action: authz.ActionSetupStatus}, RateLimit: defaultLimits.Auth}, s.GetSetupStatus)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/setup/complete", Auth: routeAuthPolicy{Action: authz.ActionSetupComplete}, RateLimit: defaultLimits.Auth}, s.PostSetupComplete)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/login", Auth: routeAuthPolicy{Public: true}, RateLimit: defaultLimits.Auth}, s.Login)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/tokens", Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Token}, s.ListTokens)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/tokens", Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Token}, s.CreateToken)
	s.registerRouteFunc(mux, routeSpec{Pattern: "DELETE /api/v1/tokens/{id}", Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Token}, s.DeleteToken)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/users/change-password", Auth: routeAuthPolicy{Action: authz.ActionAPI}, RateLimit: defaultLimits.Auth}, s.ChangePassword)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/users", Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General}, s.CreateUser)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/users", Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General}, s.ListUsers)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/users/{id}", Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General}, s.GetUser)
	s.registerRouteFunc(mux, routeSpec{Pattern: "PUT /api/v1/users/{id}", Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General}, s.UpdateUser)
	s.registerRouteFunc(mux, routeSpec{Pattern: "DELETE /api/v1/users/{id}", Auth: routeAuthPolicy{Action: authz.ActionUserAdmin}, RateLimit: defaultLimits.General}, s.DeleteUser)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/namespaces", Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General}, s.ListNamespaces)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/namespaces", Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General}, s.CreateNamespace)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/namespaces/{id}", Auth: routeAuthPolicy{Action: authz.ActionJobRead}, RateLimit: defaultLimits.General}, s.GetNamespace)
	s.registerRouteFunc(mux, routeSpec{Pattern: "DELETE /api/v1/namespaces/{id}", Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General}, s.DeleteNamespace)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/namespaces/{id}/bindings", Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General}, s.ListBindings)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/namespaces/{id}/bindings", Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General}, s.CreateBinding)
	s.registerRouteFunc(mux, routeSpec{Pattern: "DELETE /api/v1/namespaces/{id}/bindings/{user_id}", Auth: routeAuthPolicy{Action: authz.ActionAdmin}, RateLimit: defaultLimits.General}, s.DeleteBinding)

	h := http.Handler(mux)
	h = accessLogMiddleware(s.AccessLogger, apiHTTPExcludedFromAuxLogging, h)
	h = observability.CorrelationMiddleware(h)
	h = panicRecoveryMiddleware(s.logger, h)
	return instrumentHTTPServer(h)
}

func panicRecoveryMiddleware(log interfaces.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("panic in HTTP handler: %v", rec)

				// Only write an error response if headers have not yet been sent.
				// If they have, the client will see a truncated response; we log it here.
				if sw, ok := w.(*statusResponseWriter); !ok || !sw.wroteHeader {
					http.Error(w, "internal server error", http.StatusInternalServerError)
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
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if !allowed {
			retrySeconds := int(math.Ceil(retryAfter.Seconds()))
			w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
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
