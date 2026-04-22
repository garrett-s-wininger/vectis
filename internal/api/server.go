package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/queueclient"
	"vectis/internal/resolver"

	"github.com/google/uuid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	defaultForceFailReason = "manually failed via API"

	defaultShutdownTimeout   = 30 * time.Second
	healthDBPingTimeout      = 2 * time.Second
	defaultReadHeaderTimeout = 10 * time.Second
	defaultIdleTimeout       = 120 * time.Second
	defaultHandlerDBTimeout  = 60 * time.Second
)

type APIServer struct {
	jobs           dal.JobsRepository
	runs           dal.RunsRepository
	ephemeralRuns  dal.EphemeralRunStarter
	authRepo       dal.AuthRepository
	namespaces     dal.NamespacesRepository
	roleBindings   dal.RoleBindingsRepository
	logger         interfaces.Logger
	queueClient    interfaces.QueueService
	queueClose     func()
	runBroadcaster *RunBroadcaster
	dbUnavailable  atomic.Bool
	healthDB       *sql.DB
	MetricsHandler http.Handler
	// AccessLogger, when set, writes one structured slog record per HTTP request
	// (typically JSON on stderr). Health and /metrics are excluded.
	AccessLogger *slog.Logger

	// authzOverride, if non-nil, replaces SelectAuthorizer(complete) in middleware (tests).
	authzOverride authz.Authorizer
}

type routeSpec struct {
	Pattern string
	Handler http.Handler
	Auth    routeAuthPolicy
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
		if dal.IsNotFound(err) {
			return "/", nil
		}

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

	return s.getJobNamespacePath(ctx, jobID)
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

	if !queueRPCReady(s.queueClient) {
		http.Error(w, "queue not ready", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *APIServer) SetQueueClient(client interfaces.QueueService) {
	if s.queueClose != nil {
		s.queueClose()
		s.queueClose = nil
	}
	s.queueClient = client
}

func (s *APIServer) ConnectToQueue(ctx context.Context) error {
	if s.queueClose != nil {
		s.queueClose()
		s.queueClose = nil
	}
	s.queueClient = nil

	mq, err := queueclient.NewManagingQueueService(ctx, s.logger, func(ctx context.Context) (*grpc.ClientConn, func(), error) {
		return resolver.DialQueue(ctx, s.logger, config.PinnedQueueAddress(), config.APIRegistryDialAddress())
	})

	if err != nil {
		return fmt.Errorf("failed to connect to queue: %w", err)
	}

	s.queueClient = mq
	s.queueClose = func() { _ = mq.Close() }
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
	body, err := io.ReadAll(r.Body)
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

	s.logger.Warn("Run force-requeued via API: %s", runID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *APIServer) CreateJob(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(r.Body)
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

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

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

	if !s.authorizeNamespace(ctx, w, p, authz.ActionJobWrite, nsPath) {
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

	records, err := s.jobs.List(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	// TODO(garrett): Cursor-based pagination.
	// TODO(garrett): Option to avoid returning the definition.
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

	if err := json.NewEncoder(w).Encode(jobs); err != nil {
		s.logger.Error("Failed to encode jobs as JSON: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
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

	definitionJSON, err := s.jobs.GetDefinition(ctx, jobID)
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

	w.Header().Set("Content-Type", "application/json")
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

	definitionJSON, err := s.jobs.GetDefinition(ctx, jobID)
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
	if err := json.Unmarshal([]byte(definitionJSON), &job); err != nil {
		s.logger.Error("Failed to parse job definition JSON: %v", err)
		http.Error(w, "invalid job definition", http.StatusInternalServerError)
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"job_id":    jobID,
		"run_id":    runID,
		"run_index": runIndex,
	}); err != nil {
		s.logger.Error("Failed to encode trigger response: %v", err)
		return
	}

	// NOTE(garrett): We finish the enqueue asynchronously so that we can response immediately to the client,
	// rather than them waiting for the enqueue to complete (dual enqueue is idempotent by worker claim).
	jobCopy := job
	go s.finishTriggerEnqueue(context.Background(), jobID, runID, runIndex, &jobCopy)
}

func (s *APIServer) finishTriggerEnqueue(ctx context.Context, jobID, runID string, runIndex int, job *api.Job) {
	if err := enqueueWithRetry(ctx, s.queueClient, job, s.logger); err != nil {
		s.logger.Error("Failed to enqueue job (run %s): %v", runID, err)
		return
	}

	if err := s.runs.TouchDispatched(ctx, runID); err != nil {
		s.logger.Error("TouchDispatched after enqueue (run %s): %v", runID, err)
	}

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

	body, err := io.ReadAll(r.Body)
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

	if !s.authorizeNamespace(ctx, w, p, authz.ActionJobWrite, nsPath) {
		return
	}

	err = s.jobs.UpdateDefinition(ctx, jobID, string(body))
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	s.markDBRecovered()

	s.logger.Info("Updated job definition: %s", jobID)
	w.WriteHeader(http.StatusNoContent)
}

// Ephemeral runs persist definition version 1 in job_definitions so the reconciler can re-enqueue if the queue drops work.
func (s *APIServer) RunJob(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/json" {
		http.Error(w, "content type must be application/json", http.StatusUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(r.Body)
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

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"id":     generatedID,
		"run_id": runID,
	}); err != nil {
		s.logger.Error("Failed to encode response: %v", err)
		return
	}

	jobCopy := job
	go s.finishRunJobEnqueue(context.Background(), generatedID, runID, &jobCopy)
}

func (s *APIServer) finishRunJobEnqueue(ctx context.Context, generatedID, runID string, job *api.Job) {
	if err := enqueueWithRetry(ctx, s.queueClient, job, s.logger); err != nil {
		s.logger.Error("Failed to enqueue job (run %s): %v", runID, err)
		return
	}

	if err := s.runs.TouchDispatched(ctx, runID); err != nil {
		s.logger.Error("TouchDispatched after enqueue (run %s): %v", runID, err)
	}

	s.logger.Info("Enqueued ephemeral job: %s (run %s)", generatedID, runID)
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

	if !s.authorizeNamespace(ctx, w, p, authz.ActionRunRead, nsPath) {
		return
	}

	runRows, err := s.runs.ListByJob(ctx, jobID, since)
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
	if err := json.NewEncoder(w).Encode(runs); err != nil {
		s.logger.Error("Failed to encode runs: %v", err)
	}
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

	if !s.authorizeNamespace(ctx, w, p, authz.ActionRunRead, nsPath) {
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

func (s *APIServer) Handler() http.Handler {
	mux := http.NewServeMux()

	if s.MetricsHandler != nil {
		s.registerRoute(mux, routeSpec{
			Pattern: "GET /metrics",
			Handler: s.MetricsHandler,
			Auth:    routeAuthPolicy{Public: true},
		})
	}

	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /health/live", Auth: routeAuthPolicy{Public: true}}, s.HealthLive)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /health/ready", Auth: routeAuthPolicy{Public: true}}, s.HealthReady)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/jobs", Auth: routeAuthPolicy{Action: authz.ActionJobRead}}, s.GetJobs)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/jobs/{id}", Auth: routeAuthPolicy{Action: authz.ActionJobRead}}, s.GetJob)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/jobs", Auth: routeAuthPolicy{Action: authz.ActionJobWrite}}, s.CreateJob)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/jobs/run", Auth: routeAuthPolicy{Action: authz.ActionRunTrigger}}, s.RunJob)
	s.registerRouteFunc(mux, routeSpec{Pattern: "DELETE /api/v1/jobs/{id}", Auth: routeAuthPolicy{Action: authz.ActionJobWrite}}, s.DeleteJob)
	s.registerRouteFunc(mux, routeSpec{Pattern: "PUT /api/v1/jobs/{id}", Auth: routeAuthPolicy{Action: authz.ActionJobWrite}}, s.UpdateJobDefinition)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/jobs/trigger/{id}", Auth: routeAuthPolicy{Action: authz.ActionRunTrigger}}, s.TriggerJob)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/jobs/{id}/runs", Auth: routeAuthPolicy{Action: authz.ActionRunRead}}, s.GetJobRuns)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/sse/jobs/{id}/runs", Auth: routeAuthPolicy{Action: authz.ActionRunRead}}, s.HandleSSEJobRuns)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/runs/{id}/force-fail", Auth: routeAuthPolicy{Action: authz.ActionRunOperator}}, s.ForceFailRun)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/runs/{id}/force-requeue", Auth: routeAuthPolicy{Action: authz.ActionRunOperator}}, s.ForceRequeueRun)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/setup/status", Auth: routeAuthPolicy{Action: authz.ActionSetupStatus}}, s.GetSetupStatus)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/setup/complete", Auth: routeAuthPolicy{Action: authz.ActionSetupComplete}}, s.PostSetupComplete)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/namespaces", Auth: routeAuthPolicy{Action: authz.ActionJobRead}}, s.ListNamespaces)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/namespaces", Auth: routeAuthPolicy{Action: authz.ActionAdmin}}, s.CreateNamespace)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/namespaces/{id}", Auth: routeAuthPolicy{Action: authz.ActionJobRead}}, s.GetNamespace)
	s.registerRouteFunc(mux, routeSpec{Pattern: "DELETE /api/v1/namespaces/{id}", Auth: routeAuthPolicy{Action: authz.ActionAdmin}}, s.DeleteNamespace)
	s.registerRouteFunc(mux, routeSpec{Pattern: "GET /api/v1/namespaces/{id}/bindings", Auth: routeAuthPolicy{Action: authz.ActionJobRead}}, s.ListBindings)
	s.registerRouteFunc(mux, routeSpec{Pattern: "POST /api/v1/namespaces/{id}/bindings", Auth: routeAuthPolicy{Action: authz.ActionAdmin}}, s.CreateBinding)
	s.registerRouteFunc(mux, routeSpec{Pattern: "DELETE /api/v1/namespaces/{id}/bindings/{user_id}", Auth: routeAuthPolicy{Action: authz.ActionAdmin}}, s.DeleteBinding)

	h := http.Handler(mux)
	h = accessLogMiddleware(s.AccessLogger, apiHTTPExcludedFromAuxLogging, h)
	h = observability.CorrelationMiddleware(h)
	return instrumentHTTPServer(h)
}

func (s *APIServer) registerRoute(mux *http.ServeMux, spec routeSpec) {
	if spec.Pattern == "" {
		panic("api route pattern must not be empty")
	}
	if spec.Handler == nil {
		panic("api route handler must not be nil")
	}

	mux.Handle(spec.Pattern, s.accessControlledHandler(spec.Auth, spec.Handler))
}

func (s *APIServer) registerRouteFunc(mux *http.ServeMux, spec routeSpec, handler http.HandlerFunc) {
	s.registerRoute(mux, routeSpec{
		Pattern: spec.Pattern,
		Handler: handler,
		Auth:    spec.Auth,
	})
}

func (s *APIServer) runHTTPServer(ctx context.Context, srv *http.Server, serve func() error) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- serve()
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
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
	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		IdleTimeout:       defaultIdleTimeout,
	}

	s.logger.Info("API server serving on %s", l.Addr().String())
	return s.runHTTPServer(ctx, srv, func() error { return srv.Serve(l) })
}
