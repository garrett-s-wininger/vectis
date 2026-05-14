package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/api/audit"
	"vectis/internal/api/authz"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	jobvalidation "vectis/internal/job/validation"
	"vectis/internal/observability"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
)

const defaultForceFailReason = "manually failed via API"

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
	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONDocumentBodyBytes))
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

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJobDefinitionBodyBytes))
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

	definitionJSON, definitionVersion, err := s.jobs.GetDefinition(ctx, jobID)
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

	runID, runIndex, err := s.runs.CreateRun(ctx, jobID, nil, definitionVersion)
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

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJobDefinitionBodyBytes))
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
// The API always assigns a fresh job id server-side; any id in the request body is ignored (idempotency hashes the raw body).
func (s *APIServer) RunJob(w http.ResponseWriter, r *http.Request) {
	if !requestContentTypeIsJSON(r) {
		writeAPIError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "content type must be application/json", nil)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJobDefinitionBodyBytes))
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

	if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{}); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", jobvalidation.ErrorDetails(err))
		return
	}

	ephemeralJobID := uuid.New().String()
	job.Id = &ephemeralJobID

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

	runID, _, err := s.ephemeralRuns.CreateDefinitionAndRun(ctx, ephemeralJobID, string(definitionJSON), &runIndexOne)
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
		"job_id":    ephemeralJobID,
		"run_id":    runID,
		"namespace": ns.Path,
		"ephemeral": true,
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(map[string]string{
		"id":     ephemeralJobID,
		"run_id": runID,
	}); err != nil {
		s.logger.Error("Failed to encode response: %v", err)
		return
	}

	_, _ = w.Write(buf.Bytes())
	s.completeIdempotency(ctx, idempotencyScope, idempotencyKey, buf.Bytes())

	bgCtx := detachedTraceContextFromRequest(r)

	go s.finishRunJobEnqueue(bgCtx, ephemeralJobID, runID, &job)
}

func (s *APIServer) finishRunJobEnqueue(ctx context.Context, jobID, runID string, job *api.Job) {
	ctx, span := observability.Tracer("vectis/api").Start(ctx, "run.enqueue.ephemeral.async", trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
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
	tdSpan.End()

	s.logger.Info("Enqueued ephemeral job: %s (run %s)", jobID, runID)
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
		RunID             string             `json:"run_id"`
		RunIndex          int                `json:"run_index"`
		Status            string             `json:"status"`
		OrphanReason      *string            `json:"orphan_reason,omitempty"`
		FailureCode       *string            `json:"failure_code,omitempty"`
		StartedAt         *string            `json:"started_at,omitempty"`
		FinishedAt        *string            `json:"finished_at,omitempty"`
		FailureReason     *string            `json:"failure_reason,omitempty"`
		DefinitionVersion int                `json:"definition_version"`
		DefinitionHash    string             `json:"definition_hash,omitempty"`
		OwningCell        string             `json:"owning_cell"`
		DispatchEvents    []dispatchEventRow `json:"dispatch_events"`
	}

	resp := runRow{
		RunID:             rec.RunID,
		RunIndex:          rec.RunIndex,
		Status:            rec.Status,
		OrphanReason:      rec.OrphanReason,
		FailureCode:       rec.FailureCode,
		StartedAt:         rec.StartedAt,
		FinishedAt:        rec.FinishedAt,
		FailureReason:     rec.FailureReason,
		DefinitionVersion: rec.DefinitionVersion,
		DefinitionHash:    rec.DefinitionHash,
		OwningCell:        rec.OwningCell,
		DispatchEvents:    []dispatchEventRow{},
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
