package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/api/audit"
	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/cell"
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
	"google.golang.org/protobuf/proto"
)

const defaultForceFailReason = "manually failed via API"

type runTargetOptions struct {
	CellID        string   `json:"cell_id"`
	TargetCellID  string   `json:"target_cell_id"`
	CellIDs       []string `json:"cell_ids"`
	TargetCellIDs []string `json:"target_cell_ids"`
}

func (o runTargetOptions) targetCellID() string {
	targetCellIDs := o.targetCellIDs()

	if len(targetCellIDs) == 0 {
		return ""
	}

	return targetCellIDs[0]
}

func (o runTargetOptions) targetCellIDs() []string {
	var out []string
	seen := map[string]struct{}{}
	appendCell := func(cellID string) {
		cellID = strings.TrimSpace(cellID)
		if cellID == "" {
			return
		}

		if _, ok := seen[cellID]; ok {
			return
		}

		seen[cellID] = struct{}{}
		out = append(out, cellID)
	}

	appendCell(o.TargetCellID)
	appendCell(o.CellID)

	for _, cellID := range o.TargetCellIDs {
		appendCell(cellID)
	}

	for _, cellID := range o.CellIDs {
		appendCell(cellID)
	}

	return out
}

func parseRunTargetOptions(body []byte) ([]string, error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return nil, nil
	}

	var opts runTargetOptions
	if err := json.Unmarshal(body, &opts); err != nil {
		return nil, err
	}

	return opts.targetCellIDs(), nil
}

type triggerJobRunResponse struct {
	RunID    string `json:"run_id"`
	RunIndex int    `json:"run_index"`
	CellID   string `json:"cell_id,omitempty"`
}

type triggerJobResponseBody struct {
	JobID    string                  `json:"job_id"`
	RunID    string                  `json:"run_id,omitempty"`
	RunIndex int                     `json:"run_index,omitempty"`
	Runs     []triggerJobRunResponse `json:"runs,omitempty"`
}

func triggerJobResponse(jobID string, createdRuns []dal.CreatedRun) triggerJobResponseBody {
	resp := triggerJobResponseBody{JobID: jobID}
	if len(createdRuns) == 1 {
		resp.RunID = createdRuns[0].RunID
		resp.RunIndex = createdRuns[0].RunIndex
		return resp
	}

	resp.Runs = make([]triggerJobRunResponse, 0, len(createdRuns))
	for _, createdRun := range createdRuns {
		resp.Runs = append(resp.Runs, triggerJobRunResponse{
			RunID:    createdRun.RunID,
			RunIndex: createdRun.RunIndex,
			CellID:   createdRun.TargetCellID,
		})
	}

	return resp
}

func cloneJobForRun(job *api.Job, runID string) *api.Job {
	if job == nil {
		return &api.Job{RunId: &runID}
	}

	cloned, ok := proto.Clone(job).(*api.Job)
	if !ok {
		cloned = &api.Job{}
	}

	cloned.RunId = &runID
	return cloned
}

type repairMarkKind string

const (
	repairMarkSucceeded repairMarkKind = "succeeded"
	repairMarkFailed    repairMarkKind = "failed"
	repairMarkCancelled repairMarkKind = "cancelled"
	repairMarkAbandoned repairMarkKind = "abandoned"
	repairMarkQueued    repairMarkKind = "queued"
)

func (s *APIServer) RepairMarkRunSucceeded(w http.ResponseWriter, r *http.Request) {
	s.repairMarkRun(w, r, repairMarkSucceeded)
}

func (s *APIServer) RepairMarkRunFailed(w http.ResponseWriter, r *http.Request) {
	s.repairMarkRun(w, r, repairMarkFailed)
}

func (s *APIServer) RepairMarkRunCancelled(w http.ResponseWriter, r *http.Request) {
	s.repairMarkRun(w, r, repairMarkCancelled)
}

func (s *APIServer) RepairMarkRunAbandoned(w http.ResponseWriter, r *http.Request) {
	s.repairMarkRun(w, r, repairMarkAbandoned)
}

func (s *APIServer) RepairMarkRunQueued(w http.ResponseWriter, r *http.Request) {
	s.repairMarkRun(w, r, repairMarkQueued)
}

func (s *APIServer) repairMarkRun(w http.ResponseWriter, r *http.Request, mark repairMarkKind) {
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

	nsPath, ok := s.authorizeRunOperator(ctx, w, p, runID)
	if !ok {
		return
	}

	reason, ok := readRepairReason(w, r)
	if !ok {
		return
	}

	var err error
	switch mark {
	case repairMarkSucceeded:
		err = s.runs.RepairMarkRunSucceeded(ctx, runID, reason)
	case repairMarkFailed:
		err = s.runs.RepairMarkRunFailed(ctx, runID, reason)
	case repairMarkCancelled:
		err = s.runs.RepairMarkRunCancelled(ctx, runID, reason)
	case repairMarkAbandoned:
		err = s.runs.RepairMarkRunAbandoned(ctx, runID, reason)
	case repairMarkQueued:
		err = s.runs.RequeueRunForRetry(ctx, runID)
	default:
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusConflict, "run_repair_conflict", "run cannot be repair-marked from current status", nil)
			return
		}

		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		s.logger.Error("Repair mark run %s as %s failed: %v", runID, mark, err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	fields := map[string]any{
		"run_id":    runID,
		"namespace": nsPath,
		"status":    string(mark),
	}
	if reason != "" {
		fields["reason"] = reason
	}

	s.auditLog(ctx, audit.EventRunRepairMarked, actorID, 0, fields)
	s.logger.Warn("Run repair-marked via API: %s -> %s", runID, mark)
	w.WriteHeader(http.StatusNoContent)
}

func readRepairReason(w http.ResponseWriter, r *http.Request) (string, bool) {
	if r.Body == nil {
		return "", true
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONDocumentBodyBytes))
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "request_read_failed", "failed to read request body", nil)
		return "", false
	}

	if len(bytes.TrimSpace(body)) == 0 {
		return "", true
	}

	var req struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request_body", "invalid request body", nil)
		return "", false
	}

	return req.Reason, true
}

func (s *APIServer) authorizeRunOperator(ctx context.Context, w http.ResponseWriter, p *authn.Principal, runID string) (string, bool) {
	nsPath, err := s.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return "", false
		}

		if s.handleDBUnavailableError(w, err) {
			return "", false
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return "", false
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunOperator, nsPath) {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return "", false
	}

	return nsPath, true
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

	s.logger.Info("Cancelation request sent to worker: %s", runID)
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

	triggerBody, err := io.ReadAll(io.LimitReader(r.Body, maxJobDefinitionBodyBytes))
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "request_read_failed", "failed to read request body", nil)
		return
	}

	if len(bytes.TrimSpace(triggerBody)) > 0 && !requestContentTypeIsJSON(r) {
		writeAPIError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "content type must be application/json", nil)
		return
	}

	targetCellIDs, err := parseRunTargetOptions(triggerBody)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_trigger_options", "invalid trigger options", nil)
		return
	}

	idempotencyKey := idempotencyKeyFromRequest(r)
	idempotencyScope := principalIdempotencyScope("trigger:"+jobID, p)
	idempotencyHashParts := []string{http.MethodPost, "/api/v1/jobs/trigger/" + jobID}
	if trimmed := bytes.TrimSpace(triggerBody); len(trimmed) > 0 {
		idempotencyHashParts = append(idempotencyHashParts, string(trimmed))
	}

	idempotencyHash := hashIdempotencyRequest(idempotencyHashParts...)
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

	createdRuns, err := s.runs.CreateRunsInCells(ctx, jobID, nil, definitionVersion, targetCellIDs)
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

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	for _, createdRun := range createdRuns {
		s.runBroadcaster.Broadcast(jobID, createdRun.RunID, createdRun.RunIndex)
		s.auditLog(ctx, audit.EventRunTriggered, actorID, 0, map[string]any{
			"job_id":      jobID,
			"run_id":      createdRun.RunID,
			"run_index":   createdRun.RunIndex,
			"namespace":   nsPath,
			"target_cell": createdRun.TargetCellID,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	response := triggerJobResponse(jobID, createdRuns)
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(response); err != nil {
		s.logger.Error("Failed to encode trigger response: %v", err)
		return
	}

	_, _ = w.Write(buf.Bytes())
	s.completeIdempotency(ctx, idempotencyScope, idempotencyKey, buf.Bytes())

	// NOTE(garrett): We finish the enqueue asynchronously so that we can response immediately to the client,
	// rather than them waiting for the enqueue to complete (dual enqueue is idempotent by worker claim).
	bgCtx := detachedTraceContextFromRequest(r)
	for _, createdRun := range createdRuns {
		runID := createdRun.RunID
		jobForRun := cloneJobForRun(&job, runID)
		go s.finishTriggerEnqueue(bgCtx, jobID, runID, createdRun.RunIndex, jobForRun)
	}
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

	enqueuedAt := time.Now().UnixNano()
	req.Metadata[observability.JobEnqueuedAtUnixNanoKey] = strconv.FormatInt(enqueuedAt, 10)
	observability.InjectJobTraceContext(ctx, req)
	if err := s.attachExecutionEnvelope(ctx, req, runID, enqueuedAt); err != nil {
		span.RecordError(err)
		s.logger.Error("Failed to attach execution envelope (run %s): %v", runID, err)
	}

	s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventAttempt, nil)
	if err := s.submitExecution(ctx, qc, req); err != nil {
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
		Namespace    string          `json:"namespace"`
		Job          json.RawMessage `json:"job"`
		CellID       string          `json:"cell_id"`
		TargetCellID string          `json:"target_cell_id"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		req.Job = body
	}

	if req.Job == nil {
		req.Job = body
	}
	targetCellID := runTargetOptions{CellID: req.CellID, TargetCellID: req.TargetCellID}.targetCellID()

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

	runID, _, err := s.ephemeralRuns.CreateDefinitionAndRunInCell(ctx, ephemeralJobID, string(definitionJSON), &runIndexOne, targetCellID)
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

	enqueuedAt := time.Now().UnixNano()
	req.Metadata[observability.JobEnqueuedAtUnixNanoKey] = strconv.FormatInt(enqueuedAt, 10)
	observability.InjectJobTraceContext(ctx, req)
	if err := s.attachExecutionEnvelope(ctx, req, runID, enqueuedAt); err != nil {
		span.RecordError(err)
		s.logger.Error("Failed to attach execution envelope (run %s): %v", runID, err)
	}

	s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventAttempt, nil)
	if err := s.submitExecution(ctx, qc, req); err != nil {
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

func (s *APIServer) attachExecutionEnvelope(ctx context.Context, req *api.JobRequest, runID string, createdAtUnixNano int64) error {
	_, err := cell.AttachPendingExecutionEnvelope(ctx, s.runs, req, runID, createdAtUnixNano)
	return err
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
	var since *time.Time
	if sinceStr != "" {
		parsedSince, err := parseRunSince(sinceStr)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_since", "since must be an RFC3339 timestamp or YYYY-MM-DD date", nil)
			return
		}

		since = &parsedSince
	}

	afterIndexStr := r.URL.Query().Get("after_index")
	var afterIndex *int
	if afterIndexStr != "" {
		parsedAfterIndex, err := strconv.Atoi(afterIndexStr)
		if err != nil || parsedAfterIndex < 0 {
			writeAPIError(w, http.StatusBadRequest, "invalid_after_index", "after_index must be a non-negative integer", nil)
			return
		}

		afterIndex = &parsedAfterIndex
	}

	owningCell, ok := parseRunOwningCellFilter(w, r)
	if !ok {
		return
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

	runRows, nextCursor, err := s.runs.ListByJob(ctx, jobID, afterIndex, since, owningCell, params.Cursor, params.Limit)
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
		CreatedAt     *string `json:"created_at,omitempty"`
		StartedAt     *string `json:"started_at,omitempty"`
		FinishedAt    *string `json:"finished_at,omitempty"`
		FailureReason *string `json:"failure_reason,omitempty"`
		OwningCell    string  `json:"owning_cell,omitempty"`
	}

	var runs []runRow
	for _, rec := range runRows {
		runs = append(runs, runRow{
			RunID:         rec.RunID,
			RunIndex:      rec.RunIndex,
			Status:        rec.Status,
			OrphanReason:  rec.OrphanReason,
			FailureCode:   rec.FailureCode,
			CreatedAt:     rec.CreatedAt,
			StartedAt:     rec.StartedAt,
			FinishedAt:    rec.FinishedAt,
			FailureReason: rec.FailureReason,
			OwningCell:    rec.OwningCell,
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

func parseRunOwningCellFilter(w http.ResponseWriter, r *http.Request) (string, bool) {
	cellID := strings.TrimSpace(r.URL.Query().Get("cell_id"))
	owningCell := strings.TrimSpace(r.URL.Query().Get("owning_cell"))
	if cellID != "" && owningCell != "" && cellID != owningCell {
		writeAPIError(w, http.StatusBadRequest, "invalid_cell_id", "cell_id and owning_cell must match when both are provided", nil)
		return "", false
	}

	if owningCell != "" {
		return owningCell, true
	}

	return cellID, true
}

func parseRunSince(raw string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC(), nil
	}

	t, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, err
	}

	return t.UTC(), nil
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
		CreatedAt         *string            `json:"created_at,omitempty"`
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
		CreatedAt:         rec.CreatedAt,
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
