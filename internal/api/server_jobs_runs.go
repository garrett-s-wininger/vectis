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
	"vectis/internal/dispatchmeta"
	"vectis/internal/interfaces"
	jobpkg "vectis/internal/job"
	jobvalidation "vectis/internal/job/validation"
	"vectis/internal/observability"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

const (
	defaultForceFailReason               = "manually failed via API"
	idempotencyResourceTriggerInvocation = "trigger_invocation"
)

func newRunAuditMetadata(triggerInvocationID, namespacePath string) dal.RunAuditMetadata {
	return dal.RunAuditMetadata{
		TriggerInvocationID:   triggerInvocationID,
		NamespacePath:         namespacePath,
		StartDeadlineUnixNano: dispatchmeta.DeadlineUnixNano(time.Now(), config.DispatchStartTTL()),
	}
}

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

type replayRunResponseBody struct {
	JobID         string `json:"job_id"`
	RunID         string `json:"run_id"`
	RunIndex      int    `json:"run_index"`
	CellID        string `json:"cell_id,omitempty"`
	ReplayOfRunID string `json:"replay_of_run_id"`
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

func (s *APIServer) recoverRunCreationIdempotency(
	w http.ResponseWriter,
	ctx context.Context,
	scope string,
	key string,
	record dal.IdempotencyRecord,
	buildResponse func([]dal.CreatedRun) (any, bool),
) bool {
	if record.ResourceType != idempotencyResourceTriggerInvocation || record.ResourceID == "" {
		return false
	}

	createdRuns, err := s.runs.ListCreatedByTriggerInvocation(ctx, record.ResourceID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return true
		}

		s.logger.Error("Database error recovering idempotency response: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return true
	}

	if len(createdRuns) == 0 {
		return false
	}

	response, ok := buildResponse(createdRuns)
	if !ok {
		return false
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(response); err != nil {
		s.logger.Error("Failed to encode recovered idempotency response: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return true
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write(buf.Bytes())
	s.completeIdempotency(ctx, scope, key, buf.Bytes())

	return true
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

	body, ok := readRequestBody(w, r, maxJSONDocumentBodyBytes)
	if !ok {
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
	body, ok := readRequestBody(w, r, maxJSONDocumentBodyBytes)
	if !ok {
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

	if err := s.runs.MarkRunFailed(ctx, runID, dal.FailureCodeForceFailed, reason); err != nil {
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

	rec, err := s.runs.RequestRunCancel(ctx, runID, dal.CancelReasonAPI)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusConflict, "run_not_executing", "run is not executing", map[string]any{"status": rec.Status})
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

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	if rec.LeaseOwner != "" && rec.CancelToken != "" && s.ResolveWorkerAddress != nil {
		workerAddr, err := s.ResolveWorkerAddress(ctx, rec.LeaseOwner)
		if err != nil {
			s.logger.Warn("Cancel request stored for run %s but worker %s could not be resolved: %v", runID, rec.LeaseOwner, err)
		} else if err := s.sendCancelToWorker(ctx, workerAddr, runID, rec.CancelToken); err != nil {
			s.logger.Warn("Cancel request stored for run %s but worker %s did not accept fast-path cancel: %v", runID, rec.LeaseOwner, err)
		} else {
			s.auditLog(ctx, audit.EventRunCancelled, actorID, 0, map[string]any{
				"run_id":    runID,
				"namespace": nsPath,
				"delivery":  "worker_control",
			})

			s.logger.Info("Cancellation request sent to worker: %s", runID)
			w.WriteHeader(http.StatusNoContent)
			return
		}
	}

	s.auditLog(ctx, audit.EventRunCancelled, actorID, 0, map[string]any{
		"run_id":    runID,
		"namespace": nsPath,
		"delivery":  "pending",
	})

	s.logger.Info("Cancellation request stored for run: %s", runID)
	writeJSON(w, http.StatusAccepted, map[string]string{
		"status":   "cancel_requested",
		"run_id":   runID,
		"delivery": "pending",
	})
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
	body, ok := readRequestBody(w, r, maxJobDefinitionBodyBytes)
	if !ok {
		return
	}

	if s.writeSourceJobDefinitionFromJobsFacade(w, r, body, "", true) {
		return
	}

	writeAPIError(w, http.StatusBadRequest, "missing_repository_id", "repository_id is required for reusable jobs", nil)
}

func (s *APIServer) DeleteJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	if repositoryID := sourceJobRepositoryIDFromQuery(r); repositoryID != "" {
		s.deleteSourceJobDefinitionFromJobsFacade(w, r, repositoryID, jobID)
		return
	}

	writeAPIError(w, http.StatusBadRequest, "missing_repository_id", "repository_id is required for reusable jobs", nil)
}

func (s *APIServer) GetJobs(w http.ResponseWriter, r *http.Request) {
	repositoryID, ok := requireSourceJobRepositoryIDFromQuery(w, r)
	if !ok {
		return
	}

	r.SetPathValue("id", repositoryID)
	s.ListSourceRepositoryJobs(w, r)
}

func (s *APIServer) GetJob(w http.ResponseWriter, r *http.Request) {
	repositoryID, ok := requireSourceJobRepositoryIDFromQuery(w, r)
	if !ok {
		return
	}

	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	setSourceJobPathValues(r, repositoryID, jobID)
	s.GetSourceRepositoryJobDefinition(w, r)
}

func (s *APIServer) TriggerJob(w http.ResponseWriter, r *http.Request) {
	if repositoryID, ok := sourceJobRepositoryIDFromTriggerBody(w, r); !ok {
		return
	} else if repositoryID != "" {
		jobID := r.PathValue("id")
		setSourceJobPathValues(r, repositoryID, jobID)
		s.TriggerSourceRepositoryJob(w, r)
		return
	}

	writeAPIError(w, http.StatusBadRequest, "missing_repository_id", "repository_id is required for reusable jobs", nil)
}

func (s *APIServer) finishTriggerEnqueue(ctx context.Context, jobID string, createdRun dal.CreatedRun, job *api.Job, definitionHash string) {
	runID := createdRun.RunID
	runIndex := createdRun.RunIndex
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
	env, err := s.attachCreatedRunExecutionEnvelope(ctx, req, createdRun, enqueuedAt)
	targetCellID := ""
	if env != nil {
		targetCellID = env.CellID
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "attach execution envelope")
		span.End()

		s.logger.Error("Failed to attach execution envelope (run %s): %v", runID, err)
		msg := err.Error()

		s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, targetCellID, &msg)
		s.recordAPIEnqueueMetric(ctx, observability.APIEnqueueRunKindReplay, observability.APIEnqueueOutcomeFailedEnqueue)
		return
	}

	s.recordAPIEnqueueMetric(ctx, observability.APIEnqueueRunKindReplay, observability.APIEnqueueOutcomeAttempt)
	dispatchReq, err := s.recordExecutionPayload(ctx, runID, req, definitionHash)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "record execution payload")
		span.End()
		s.logger.Error("Failed to record execution payload (run %s): %v", runID, err)

		msg := err.Error()
		if dispatchErr := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, targetCellID, &msg); dispatchErr != nil {
			s.logger.Error("Failed to record dispatch failure for run %s: %v", runID, dispatchErr)
		}

		return
	}

	req = dispatchReq
	if env, ok, envErr := cell.ExecutionEnvelopeFromRequest(req); envErr == nil && ok {
		targetCellID = env.CellID
	}

	if err := s.submitExecution(ctx, qc, req); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue")
		span.End()
		s.logger.Error("Failed to enqueue job (run %s): %v", runID, err)
		msg := err.Error()
		s.recordAPIEnqueueMetric(ctx, observability.APIEnqueueRunKindReplay, observability.APIEnqueueOutcomeFailedEnqueue)
		if dispatchErr := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, targetCellID, &msg); dispatchErr != nil {
			s.logger.Error("Failed to record dispatch failure for run %s: %v", runID, dispatchErr)
		}
		return
	}
	span.SetAttributes(attribute.String("vectis.enqueue.outcome", "success"))
	span.End()

	if err := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventSuccess, targetCellID, nil); err != nil {
		_, tdSpan := observability.Tracer("vectis/api").Start(ctx, "run.touch_dispatched", trace.WithSpanKind(trace.SpanKindInternal))
		tdSpan.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
		tdSpan.RecordError(err)
		tdSpan.SetStatus(codes.Error, "touch dispatched")
		tdSpan.End()
		s.logger.Error("TouchDispatched after enqueue (run %s): %v", runID, err)
		msg := "touch dispatched: " + err.Error()
		s.recordAPIEnqueueMetric(ctx, observability.APIEnqueueRunKindReplay, observability.APIEnqueueOutcomeFailedTouchDispatch)
		s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, targetCellID, &msg)
		return
	}

	s.recordAPIEnqueueMetric(ctx, observability.APIEnqueueRunKindReplay, observability.APIEnqueueOutcomeSuccess)

	_, tdSpan := observability.Tracer("vectis/api").Start(ctx, "run.touch_dispatched", trace.WithSpanKind(trace.SpanKindInternal))
	tdSpan.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
	if runIndex > 0 {
		tdSpan.SetAttributes(observability.RunIndexAttrs(runIndex)...)
	}

	tdSpan.End()
	s.logger.Info("Triggered job: %s (run %s, index %d)", jobID, runID, runIndex)
}

func (s *APIServer) ReplayRun(w http.ResponseWriter, r *http.Request) {
	sourceRunID := r.PathValue("id")
	if sourceRunID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	nsPath, ok := s.authorizeRunOperator(ctx, w, p, sourceRunID)
	if !ok {
		return
	}

	sourceRun, err := s.runs.GetRun(ctx, sourceRunID)
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

	if sourceRun.Status == dal.RunStatusQueued || sourceRun.Status == dal.RunStatusRunning {
		writeAPIError(w, http.StatusConflict, "source_run_not_replayable", "source run cannot be replayed from its current status", nil)
		return
	}

	jobID, err := s.runs.GetRunJobID(ctx, sourceRunID)
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

	body, ok := readRequestBody(w, r, maxJobDefinitionBodyBytes)
	if !ok {
		return
	}

	targetCellIDs, err := parseRunTargetOptions(body)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_replay_options", "invalid replay options", nil)
		return
	}

	if len(targetCellIDs) > 1 {
		writeAPIError(w, http.StatusBadRequest, "invalid_replay_options", "replay accepts at most one target cell", nil)
		return
	}

	targetCellID := sourceRun.OwningCell
	if len(targetCellIDs) == 1 {
		targetCellID = targetCellIDs[0]
	}

	definitionJSON, err := s.jobs.GetDefinitionVersion(ctx, jobID, sourceRun.DefinitionVersion)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_definition_not_found", "source run definition not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	var job api.Job
	if err := jobpkg.DecodeDefinitionJSON([]byte(definitionJSON), &job); err != nil {
		writeAPIError(w, http.StatusInternalServerError, "invalid_run_definition", "invalid captured job definition", nil)
		return
	}

	definitionHash := sourceRun.DefinitionHash
	if strings.TrimSpace(definitionHash) == "" {
		definitionHash = dal.DefinitionHash(definitionJSON)
	}

	idempotencyKey := idempotencyKeyFromRequest(r)
	idempotencyScope := principalIdempotencyScope("replay:"+sourceRunID, p)
	idempotencyHash := hashIdempotencyRequest(http.MethodPost, "/api/v1/runs/"+sourceRunID+"/replay", string(bytes.TrimSpace(body)))
	idempotencyRecord, idempotencyReserved, idempotencyInProgress, ok := s.reserveRecoverableIdempotency(w, ctx, idempotencyScope, idempotencyKey, idempotencyHash)
	if !ok {
		return
	}

	if idempotencyRecord.ResponseJSON != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, *idempotencyRecord.ResponseJSON)
		return
	}

	if idempotencyInProgress {
		if s.recoverRunCreationIdempotency(w, ctx, idempotencyScope, idempotencyKey, idempotencyRecord, func(createdRuns []dal.CreatedRun) (any, bool) {
			if len(createdRuns) != 1 {
				return nil, false
			}

			return replayRunResponseBody{
				JobID:         jobID,
				RunID:         createdRuns[0].RunID,
				RunIndex:      createdRuns[0].RunIndex,
				CellID:        createdRuns[0].TargetCellID,
				ReplayOfRunID: sourceRunID,
			}, true
		}) {
			return
		}

		writeAPIError(w, http.StatusConflict, "idempotency_in_progress", "idempotent request is still in progress", nil)
		return
	}

	triggerPayload := map[string]string{
		"source_run_id":      sourceRunID,
		"definition_hash":    definitionHash,
		"definition_version": strconv.Itoa(sourceRun.DefinitionVersion),
		"target_cell_id":     targetCellID,
	}

	triggerPayloadJSON, err := json.Marshal(triggerPayload)
	if err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		s.logger.Error("Failed to encode replay trigger payload: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	invocationID, err := s.recordTriggerInvocation(ctx, jobID, dal.TriggerTypeReplay, string(triggerPayloadJSON), []string{targetCellID})
	if err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error recording replay trigger invocation: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if err := s.attachIdempotencyResource(ctx, idempotencyScope, idempotencyKey, idempotencyResourceTriggerInvocation, invocationID); err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error attaching replay idempotency resource: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	createdRun, err := s.runs.CreateReplayRun(ctx, sourceRunID, targetCellID, dal.RunAuditMetadata{
		TriggerInvocationID:   invocationID,
		ReplayOfRunID:         sourceRunID,
		NamespacePath:         nsPath,
		StartDeadlineUnixNano: dispatchmeta.DeadlineUnixNano(time.Now(), config.DispatchStartTTL()),
	})

	if err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusConflict, "source_run_not_replayable", "source run cannot be replayed from its current status", nil)
			return
		}

		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		s.logger.Error("Database error creating replay run: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	job.Id = &jobID
	job.RunId = &createdRun.RunID

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.runBroadcaster.Broadcast(jobID, createdRun.RunID, createdRun.RunIndex)
	s.broadcastSourceRepositoryRunEvent(ctx, jobID, sourceRun.DefinitionVersion, createdRun.RunID, createdRun.RunIndex)
	s.auditLog(ctx, audit.EventRunTriggered, actorID, 0, map[string]any{
		"job_id":           jobID,
		"run_id":           createdRun.RunID,
		"run_index":        createdRun.RunIndex,
		"namespace":        nsPath,
		"target_cell":      createdRun.TargetCellID,
		"invocation":       invocationID,
		"replay_of_run_id": sourceRunID,
	})

	s.recordDispatchEvent(ctx, createdRun.RunID, dal.DispatchSourceAPI, dal.DispatchEventAccepted, createdRun.TargetCellID, nil)
	s.recordAPIEnqueueMetric(ctx, observability.APIEnqueueRunKindReplay, observability.APIEnqueueOutcomeAccepted)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	response := replayRunResponseBody{
		JobID:         jobID,
		RunID:         createdRun.RunID,
		RunIndex:      createdRun.RunIndex,
		CellID:        createdRun.TargetCellID,
		ReplayOfRunID: sourceRunID,
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(response); err != nil {
		s.logger.Error("Failed to encode replay response: %v", err)
		return
	}

	_, _ = w.Write(buf.Bytes())
	s.completeIdempotency(ctx, idempotencyScope, idempotencyKey, buf.Bytes())

	bgCtx := detachedTraceContextFromRequest(r)
	jobForRun := cloneJobForRun(&job, createdRun.RunID)
	go s.finishTriggerEnqueue(bgCtx, jobID, createdRun, jobForRun, definitionHash)
}

func (s *APIServer) UpdateJobDefinition(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	body, ok := readRequestBody(w, r, maxJobDefinitionBodyBytes)
	if !ok {
		return
	}

	if s.writeSourceJobDefinitionFromJobsFacade(w, r, body, jobID, false) {
		return
	}

	writeAPIError(w, http.StatusBadRequest, "missing_repository_id", "repository_id is required for reusable jobs", nil)
}

// Ephemeral runs persist definition version 1 in job_definitions so the reconciler can re-enqueue if the queue drops work.
// The API always assigns a fresh job id server-side; any id in the request body is ignored (idempotency hashes the raw body).
func (s *APIServer) RunJob(w http.ResponseWriter, r *http.Request) {
	body, ok := readRequestBody(w, r, maxJobDefinitionBodyBytes)
	if !ok {
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
	if err := jobpkg.DecodeDefinitionJSON(req.Job, &job); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", nil)
		return
	}

	if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{Resolver: s.actionResolver}); err != nil {
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

	definitionHash := dal.DefinitionHash(string(definitionJSON))
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
	idempotencyRecord, idempotencyReserved, idempotencyInProgress, ok := s.reserveRecoverableIdempotency(w, ctx, idempotencyScope, idempotencyKey, idempotencyHash)
	if !ok {
		return
	}

	if idempotencyRecord.ResponseJSON != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, *idempotencyRecord.ResponseJSON)
		return
	}

	if idempotencyInProgress {
		if s.recoverRunCreationIdempotency(w, ctx, idempotencyScope, idempotencyKey, idempotencyRecord, func(createdRuns []dal.CreatedRun) (any, bool) {
			if len(createdRuns) != 1 || createdRuns[0].JobID == "" {
				return nil, false
			}

			return map[string]string{
				"id":     createdRuns[0].JobID,
				"run_id": createdRuns[0].RunID,
			}, true
		}) {
			return
		}

		writeAPIError(w, http.StatusConflict, "idempotency_in_progress", "idempotent request is still in progress", nil)
		return
	}

	invocationID, err := s.recordTriggerInvocation(ctx, ephemeralJobID, dal.TriggerTypeManual, string(bytes.TrimSpace(body)), []string{targetCellID})
	if err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error recording trigger invocation: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if err := s.attachIdempotencyResource(ctx, idempotencyScope, idempotencyKey, idempotencyResourceTriggerInvocation, invocationID); err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error attaching ephemeral idempotency resource: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	var runID string
	if starter, ok := s.ephemeralRuns.(dal.EphemeralRunStarterWithAudit); ok {
		runID, _, err = starter.CreateDefinitionAndRunInCellWithAudit(ctx, ephemeralJobID, string(definitionJSON), &runIndexOne, targetCellID, newRunAuditMetadata(invocationID, "/"))
	} else {
		runID, _, err = s.ephemeralRuns.CreateDefinitionAndRunInCell(ctx, ephemeralJobID, string(definitionJSON), &runIndexOne, targetCellID)
	}

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
		"job_id":     ephemeralJobID,
		"run_id":     runID,
		"namespace":  ns.Path,
		"ephemeral":  true,
		"invocation": invocationID,
	})

	s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventAccepted, targetCellID, nil)
	s.recordAPIEnqueueMetric(ctx, observability.APIEnqueueRunKindEphemeral, observability.APIEnqueueOutcomeAccepted)

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

	go s.finishRunJobEnqueue(bgCtx, ephemeralJobID, runID, &job, definitionHash)
}

func (s *APIServer) finishRunJobEnqueue(ctx context.Context, jobID, runID string, job *api.Job, definitionHash string) {
	s.finishRunJobEnqueueWithKind(ctx, observability.APIEnqueueRunKindEphemeral, jobID, runID, job, definitionHash)
}

func (s *APIServer) finishRunJobEnqueueWithKind(ctx context.Context, runKind, jobID, runID string, job *api.Job, definitionHash string) {
	if runKind == "" {
		runKind = observability.APIEnqueueRunKindEphemeral
	}

	ctx, span := observability.Tracer("vectis/api").Start(ctx, "run.enqueue."+runKind+".async", trace.WithSpanKind(trace.SpanKindInternal))
	span.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
	span.SetAttributes(attribute.String("vectis.run.kind", runKind))
	span.SetAttributes(attribute.Bool("vectis.run.ephemeral", runKind == observability.APIEnqueueRunKindEphemeral))
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
	env, err := s.attachExecutionEnvelope(ctx, req, runID, enqueuedAt)
	targetCellID := ""
	if env != nil {
		targetCellID = env.CellID
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "attach execution envelope")
		span.End()

		s.logger.Error("Failed to attach execution envelope (run %s): %v", runID, err)
		msg := err.Error()

		s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, targetCellID, &msg)
		s.recordAPIEnqueueMetric(ctx, observability.APIEnqueueRunKindEphemeral, observability.APIEnqueueOutcomeFailedEnqueue)
		return
	}

	s.recordAPIEnqueueMetric(ctx, runKind, observability.APIEnqueueOutcomeAttempt)
	dispatchReq, err := s.recordExecutionPayload(ctx, runID, req, definitionHash)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "record execution payload")
		span.End()
		s.logger.Error("Failed to record execution payload (run %s): %v", runID, err)
		msg := err.Error()
		if dispatchErr := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, targetCellID, &msg); dispatchErr != nil {
			s.logger.Error("Failed to record dispatch failure for run %s: %v", runID, dispatchErr)
		}
		return
	}

	req = dispatchReq
	if env, ok, envErr := cell.ExecutionEnvelopeFromRequest(req); envErr == nil && ok {
		targetCellID = env.CellID
	}

	if err := s.submitExecution(ctx, qc, req); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "enqueue")
		span.End()
		s.logger.Error("Failed to enqueue job (run %s): %v", runID, err)
		msg := err.Error()
		s.recordAPIEnqueueMetric(ctx, runKind, observability.APIEnqueueOutcomeFailedEnqueue)
		if dispatchErr := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, targetCellID, &msg); dispatchErr != nil {
			s.logger.Error("Failed to record dispatch failure for run %s: %v", runID, dispatchErr)
		}
		return
	}
	span.SetAttributes(attribute.String("vectis.enqueue.outcome", "success"))
	span.End()

	if err := s.recordDispatchAttemptOutcome(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventSuccess, targetCellID, nil); err != nil {
		_, tdSpan := observability.Tracer("vectis/api").Start(ctx, "run.touch_dispatched", trace.WithSpanKind(trace.SpanKindInternal))
		tdSpan.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
		tdSpan.RecordError(err)
		tdSpan.SetStatus(codes.Error, "touch dispatched")
		tdSpan.End()
		s.logger.Error("TouchDispatched after enqueue (run %s): %v", runID, err)
		msg := "touch dispatched: " + err.Error()
		s.recordAPIEnqueueMetric(ctx, runKind, observability.APIEnqueueOutcomeFailedTouchDispatch)
		s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, targetCellID, &msg)
		return
	}
	s.recordAPIEnqueueMetric(ctx, runKind, observability.APIEnqueueOutcomeSuccess)

	_, tdSpan := observability.Tracer("vectis/api").Start(ctx, "run.touch_dispatched", trace.WithSpanKind(trace.SpanKindInternal))
	tdSpan.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
	tdSpan.End()

	s.logger.Info("Enqueued %s job: %s (run %s)", runKind, jobID, runID)
}

func (s *APIServer) recordTriggerInvocation(ctx context.Context, jobID, triggerType, triggerPayload string, targetCellIDs []string) (string, error) {
	if s.triggerEvents == nil {
		return "", nil
	}

	requestedCells := requestedCellsForInvocation(targetCellIDs)
	rec, err := s.triggerEvents.Record(ctx, dal.TriggerInvocation{
		JobID:              jobID,
		TriggerType:        triggerType,
		TriggerPayloadHash: dal.PayloadHash(triggerPayload),
		RequestedCells:     requestedCells,
	})

	if err != nil {
		return "", err
	}

	return rec.InvocationID, nil
}

func requestedCellsForInvocation(targetCellIDs []string) []string {
	if len(targetCellIDs) == 0 {
		return []string{config.CellID()}
	}

	out := make([]string, 0, len(targetCellIDs))
	for _, cellID := range targetCellIDs {
		cellID = strings.TrimSpace(cellID)
		if cellID == "" {
			continue
		}

		out = append(out, cellID)
	}

	if len(out) == 0 {
		return []string{config.CellID()}
	}

	return out
}

func (s *APIServer) recordExecutionPayload(ctx context.Context, runID string, req *api.JobRequest, definitionHash string) (*api.JobRequest, error) {
	return cell.RecordExecutionHandoffPayload(ctx, s.runs, runID, req, definitionHash)
}

func (s *APIServer) attachExecutionEnvelope(ctx context.Context, req *api.JobRequest, runID string, createdAtUnixNano int64) (*cell.ExecutionEnvelope, error) {
	dispatch, err := s.runs.GetPendingExecution(ctx, runID)
	if err != nil {
		return nil, err
	}

	deadline, err := s.runs.EnsureExecutionStartDeadline(ctx, dispatch.ExecutionID, dispatchmeta.DeadlineUnixNano(time.Now(), config.DispatchStartTTL()))
	if err != nil {
		return nil, err
	}

	dispatch.StartDeadlineUnixNano = deadline
	return cell.AttachExecutionEnvelopeWithActions(req, dispatch, createdAtUnixNano, s.actionDescriptorResolver)
}

func (s *APIServer) attachCreatedRunExecutionEnvelope(ctx context.Context, req *api.JobRequest, createdRun dal.CreatedRun, createdAtUnixNano int64) (*cell.ExecutionEnvelope, error) {
	dispatch := createdRun.RootDispatch
	if strings.TrimSpace(dispatch.ExecutionID) == "" || strings.TrimSpace(dispatch.SegmentID) == "" {
		return s.attachExecutionEnvelope(ctx, req, createdRun.RunID, createdAtUnixNano)
	}

	dispatch.RunID = strings.TrimSpace(dispatch.RunID)
	if dispatch.RunID == "" {
		dispatch.RunID = createdRun.RunID
	} else if dispatch.RunID != createdRun.RunID {
		return nil, fmt.Errorf("created run dispatch run_id %q does not match created run %q", dispatch.RunID, createdRun.RunID)
	}

	if dispatch.RunIndex <= 0 {
		dispatch.RunIndex = createdRun.RunIndex
	} else if createdRun.RunIndex > 0 && dispatch.RunIndex != createdRun.RunIndex {
		return nil, fmt.Errorf("created run dispatch run_index %d does not match created run index %d", dispatch.RunIndex, createdRun.RunIndex)
	}

	targetCellID := strings.TrimSpace(createdRun.TargetCellID)
	dispatch.CellID = strings.TrimSpace(dispatch.CellID)
	if dispatch.CellID == "" {
		dispatch.CellID = targetCellID
	} else if targetCellID != "" && dispatch.CellID != targetCellID {
		return nil, fmt.Errorf("created run dispatch cell_id %q does not match target cell %q", dispatch.CellID, targetCellID)
	}

	dispatch.OwningCell = strings.TrimSpace(dispatch.OwningCell)
	if dispatch.OwningCell == "" {
		dispatch.OwningCell = targetCellID
	} else if targetCellID != "" && dispatch.OwningCell != targetCellID {
		return nil, fmt.Errorf("created run dispatch owning_cell %q does not match target cell %q", dispatch.OwningCell, targetCellID)
	}

	if strings.TrimSpace(dispatch.NamespacePath) == "" {
		dispatch.NamespacePath = "/"
	}

	if dispatch.StartDeadlineUnixNano <= 0 {
		deadline, err := s.runs.EnsureExecutionStartDeadline(ctx, dispatch.ExecutionID, dispatchmeta.DeadlineUnixNano(time.Now(), config.DispatchStartTTL()))
		if err != nil {
			return nil, err
		}

		dispatch.StartDeadlineUnixNano = deadline
	}

	return cell.AttachExecutionEnvelopeWithActions(req, dispatch, createdAtUnixNano, s.actionDescriptorResolver)
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
	repositoryID, ok := requireSourceJobRepositoryIDFromQuery(w, r)
	if !ok {
		return
	}

	jobID := r.PathValue("id")
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	setSourceJobPathValues(r, repositoryID, jobID)
	s.GetSourceRepositoryJobRuns(w, r)
}

type runListRequestOptions struct {
	since      *time.Time
	afterIndex *int
	owningCell string
	cursor     int64
	limit      int
}

func parseRunListRequestOptions(w http.ResponseWriter, r *http.Request) (runListRequestOptions, bool) {
	var opts runListRequestOptions

	sinceStr := r.URL.Query().Get("since")
	if sinceStr != "" {
		parsedSince, err := parseRunSince(sinceStr)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_since", "since must be an RFC3339 timestamp or YYYY-MM-DD date", nil)
			return opts, false
		}

		opts.since = &parsedSince
	}

	afterIndexStr := r.URL.Query().Get("after_index")
	if afterIndexStr != "" {
		parsedAfterIndex, err := strconv.Atoi(afterIndexStr)
		if err != nil || parsedAfterIndex < 0 {
			writeAPIError(w, http.StatusBadRequest, "invalid_after_index", "after_index must be a non-negative integer", nil)
			return opts, false
		}

		opts.afterIndex = &parsedAfterIndex
	}

	owningCell, ok := parseRunOwningCellFilter(w, r)
	if !ok {
		return opts, false
	}

	opts.owningCell = owningCell
	params := parsePageParams(r)
	opts.cursor = params.Cursor
	opts.limit = params.Limit

	return opts, true
}

func (s *APIServer) writeJobRunsResponse(w http.ResponseWriter, ctx context.Context, jobID string, runRows []dal.RunRecord, nextCursor int64) {
	versions := make([]int, 0, len(runRows))
	for _, rec := range runRows {
		versions = append(versions, rec.DefinitionVersion)
	}

	sourceByVersion, err := s.definitionSourceProvenanceByVersion(ctx, jobID, versions)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error getting run source provenance: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	type runRow struct {
		RunID                string                    `json:"run_id"`
		RunIndex             int                       `json:"run_index"`
		Status               string                    `json:"status"`
		OrphanReason         *string                   `json:"orphan_reason,omitempty"`
		FailureCode          *string                   `json:"failure_code,omitempty"`
		CreatedAt            *string                   `json:"created_at,omitempty"`
		StartedAt            *string                   `json:"started_at,omitempty"`
		FinishedAt           *string                   `json:"finished_at,omitempty"`
		FailureReason        *string                   `json:"failure_reason,omitempty"`
		DefinitionVersion    int                       `json:"definition_version"`
		DefinitionHash       string                    `json:"definition_hash,omitempty"`
		Source               *sourceProvenanceResponse `json:"source,omitempty"`
		OwningCell           string                    `json:"owning_cell,omitempty"`
		ReplayOfRunID        *string                   `json:"replay_of_run_id,omitempty"`
		TriggerInvocationID  *string                   `json:"trigger_invocation_id,omitempty"`
		TriggerID            *int64                    `json:"trigger_id,omitempty"`
		TriggerType          *string                   `json:"trigger_type,omitempty"`
		TriggerPayloadHash   *string                   `json:"trigger_payload_hash,omitempty"`
		RequestedCells       []string                  `json:"requested_cells,omitempty"`
		ExecutionPayloadHash string                    `json:"execution_payload_hash,omitempty"`
	}

	var runs []runRow
	for _, rec := range runRows {
		var source *sourceProvenanceResponse
		if sourceValue, ok := sourceByVersion[rec.DefinitionVersion]; ok {
			source = &sourceValue
		}

		runs = append(runs, runRow{
			RunID:                rec.RunID,
			RunIndex:             rec.RunIndex,
			Status:               rec.Status,
			OrphanReason:         rec.OrphanReason,
			FailureCode:          rec.FailureCode,
			CreatedAt:            rec.CreatedAt,
			StartedAt:            rec.StartedAt,
			FinishedAt:           rec.FinishedAt,
			FailureReason:        rec.FailureReason,
			DefinitionVersion:    rec.DefinitionVersion,
			DefinitionHash:       rec.DefinitionHash,
			Source:               source,
			OwningCell:           rec.OwningCell,
			ReplayOfRunID:        rec.ReplayOfRunID,
			TriggerInvocationID:  rec.TriggerInvocationID,
			TriggerID:            rec.TriggerID,
			TriggerType:          rec.TriggerType,
			TriggerPayloadHash:   rec.TriggerPayloadHash,
			RequestedCells:       rec.RequestedCells,
			ExecutionPayloadHash: rec.ExecutionPayloadHash,
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

	source, err := s.definitionSourceProvenance(ctx, rec.JobID, rec.DefinitionVersion)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error getting run source provenance: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

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

	taskCompletionSummary, err := s.runs.GetRunTaskCompletion(ctx, runID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	pendingTaskContinuation := false
	if rec.Status == dal.RunStatusQueued && taskCompletionSummary.Total > 0 && taskCompletionSummary.Incomplete > 0 {
		pendingTaskContinuation, err = s.runHasPendingTaskContinuation(ctx, runID)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
			return
		}
	}

	latestFailedSecurityEvent, err := s.runs.LatestRunSecurityEvent(ctx, runID, true)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	type dispatchEventRow struct {
		ID        int64   `json:"id"`
		Source    string  `json:"source"`
		EventType string  `json:"event_type"`
		Message   *string `json:"message,omitempty"`
		CreatedAt int64   `json:"created_at"`
	}

	type taskCompletionRow struct {
		Total          int `json:"total"`
		Succeeded      int `json:"succeeded"`
		TerminalFailed int `json:"terminal_failed"`
		Incomplete     int `json:"incomplete"`
	}

	type executionSecurityEventRow struct {
		ID            int64   `json:"id"`
		RunID         string  `json:"run_id"`
		TaskID        string  `json:"task_id,omitempty"`
		TaskAttemptID string  `json:"task_attempt_id,omitempty"`
		ExecutionID   string  `json:"execution_id,omitempty"`
		EventType     string  `json:"event_type"`
		Outcome       string  `json:"outcome"`
		Reason        string  `json:"reason,omitempty"`
		Provider      *string `json:"provider,omitempty"`
		SecretCount   *int    `json:"secret_count,omitempty"`
		FileCount     *int    `json:"file_count,omitempty"`
		CreatedAt     int64   `json:"created_at"`
	}

	type runRow struct {
		RunID                     string                     `json:"run_id"`
		RunIndex                  int                        `json:"run_index"`
		Status                    string                     `json:"status"`
		OrphanReason              *string                    `json:"orphan_reason,omitempty"`
		FailureCode               *string                    `json:"failure_code,omitempty"`
		CreatedAt                 *string                    `json:"created_at,omitempty"`
		StartedAt                 *string                    `json:"started_at,omitempty"`
		FinishedAt                *string                    `json:"finished_at,omitempty"`
		FailureReason             *string                    `json:"failure_reason,omitempty"`
		DefinitionVersion         int                        `json:"definition_version"`
		DefinitionHash            string                     `json:"definition_hash,omitempty"`
		Source                    *sourceProvenanceResponse  `json:"source,omitempty"`
		OwningCell                string                     `json:"owning_cell"`
		ReplayOfRunID             *string                    `json:"replay_of_run_id,omitempty"`
		TriggerInvocationID       *string                    `json:"trigger_invocation_id,omitempty"`
		TriggerID                 *int64                     `json:"trigger_id,omitempty"`
		TriggerType               *string                    `json:"trigger_type,omitempty"`
		TriggerPayloadHash        *string                    `json:"trigger_payload_hash,omitempty"`
		RequestedCells            []string                   `json:"requested_cells,omitempty"`
		ExecutionPayloadHash      string                     `json:"execution_payload_hash,omitempty"`
		NextAction                *string                    `json:"next_action,omitempty"`
		DispatchSummary           []dispatchSummary          `json:"dispatch_summary,omitempty"`
		DispatchEvents            []dispatchEventRow         `json:"dispatch_events"`
		TaskCompletion            *taskCompletionRow         `json:"task_completion,omitempty"`
		LatestFailedSecurityEvent *executionSecurityEventRow `json:"latest_failed_security_event,omitempty"`
	}

	resp := runRow{
		RunID:                rec.RunID,
		RunIndex:             rec.RunIndex,
		Status:               rec.Status,
		OrphanReason:         rec.OrphanReason,
		FailureCode:          rec.FailureCode,
		CreatedAt:            rec.CreatedAt,
		StartedAt:            rec.StartedAt,
		FinishedAt:           rec.FinishedAt,
		FailureReason:        rec.FailureReason,
		DefinitionVersion:    rec.DefinitionVersion,
		DefinitionHash:       rec.DefinitionHash,
		Source:               source,
		OwningCell:           rec.OwningCell,
		ReplayOfRunID:        rec.ReplayOfRunID,
		TriggerInvocationID:  rec.TriggerInvocationID,
		TriggerID:            rec.TriggerID,
		TriggerType:          rec.TriggerType,
		TriggerPayloadHash:   rec.TriggerPayloadHash,
		RequestedCells:       rec.RequestedCells,
		ExecutionPayloadHash: rec.ExecutionPayloadHash,
		NextAction:           runNextAction(rec.Status, taskCompletionSummary, pendingTaskContinuation, latestFailedSecurityEvent != nil),
		DispatchSummary:      buildDispatchSummary(dispatchEvents),
		DispatchEvents:       []dispatchEventRow{},
	}

	if latestFailedSecurityEvent != nil {
		resp.LatestFailedSecurityEvent = &executionSecurityEventRow{
			ID:            latestFailedSecurityEvent.ID,
			RunID:         latestFailedSecurityEvent.RunID,
			TaskID:        latestFailedSecurityEvent.TaskID,
			TaskAttemptID: latestFailedSecurityEvent.TaskAttemptID,
			ExecutionID:   latestFailedSecurityEvent.ExecutionID,
			EventType:     latestFailedSecurityEvent.EventType,
			Outcome:       latestFailedSecurityEvent.Outcome,
			Reason:        latestFailedSecurityEvent.Reason,
			Provider:      latestFailedSecurityEvent.Provider,
			SecretCount:   latestFailedSecurityEvent.SecretCount,
			FileCount:     latestFailedSecurityEvent.FileCount,
			CreatedAt:     latestFailedSecurityEvent.CreatedAt,
		}
	}

	if taskCompletionSummary.Total > 0 {
		resp.TaskCompletion = &taskCompletionRow{
			Total:          taskCompletionSummary.Total,
			Succeeded:      taskCompletionSummary.Succeeded,
			TerminalFailed: taskCompletionSummary.TerminalFailed,
			Incomplete:     taskCompletionSummary.Incomplete,
		}
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

func (s *APIServer) GetRunDefinition(w http.ResponseWriter, r *http.Request) {
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

	definitionJSON, err := s.jobs.GetDefinitionVersion(ctx, rec.JobID, rec.DefinitionVersion)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_definition_not_found", "run definition not found", nil)
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

	if !json.Valid([]byte(definitionJSON)) {
		s.logger.Error("Captured definition for run %s is not valid JSON", runID)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	source, err := s.definitionSourceProvenance(ctx, rec.JobID, rec.DefinitionVersion)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error getting run source provenance: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	definitionHash := rec.DefinitionHash
	if strings.TrimSpace(definitionHash) == "" {
		definitionHash = dal.DefinitionHash(definitionJSON)
	}

	resp := struct {
		RunID             string                    `json:"run_id"`
		JobID             string                    `json:"job_id"`
		DefinitionVersion int                       `json:"definition_version"`
		DefinitionHash    string                    `json:"definition_hash"`
		Source            *sourceProvenanceResponse `json:"source,omitempty"`
		Definition        json.RawMessage           `json:"definition"`
	}{
		RunID:             rec.RunID,
		JobID:             rec.JobID,
		DefinitionVersion: rec.DefinitionVersion,
		DefinitionHash:    definitionHash,
		Source:            source,
		Definition:        json.RawMessage(definitionJSON),
	}

	w.Header().Set("Content-Type", "application/json")
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(resp); err != nil {
		s.logger.Error("Failed to encode run definition: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	_, _ = w.Write(buf.Bytes())
}

const (
	runNextActionTaskCompletionPending         = "task_completion_pending"
	runNextActionTaskContinuationPending       = "task_continuation_pending"
	runNextActionTaskFinalizationRepairPending = "task_finalization_repair_pending"
	runNextActionSecurityGateFailed            = "security_gate_failed"
)

type dispatchSummary struct {
	Source        string  `json:"source"`
	Accepted      int     `json:"accepted"`
	Attempts      int     `json:"attempts"`
	Successes     int     `json:"successes"`
	Failures      int     `json:"failures"`
	FirstEventAt  int64   `json:"first_event_at"`
	LastEventAt   int64   `json:"last_event_at"`
	LastEventType string  `json:"last_event_type"`
	LastMessage   *string `json:"last_message,omitempty"`
}

func buildDispatchSummary(events []dal.DispatchEvent) []dispatchSummary {
	if len(events) == 0 {
		return nil
	}

	bySource := map[string]int{}
	summary := make([]dispatchSummary, 0)
	for _, event := range events {
		source := strings.TrimSpace(event.Source)
		if source == "" {
			source = "unknown"
		}

		idx, ok := bySource[source]
		if !ok {
			idx = len(summary)
			bySource[source] = idx
			summary = append(summary, dispatchSummary{
				Source:       source,
				FirstEventAt: event.CreatedAt,
			})
		}

		row := &summary[idx]
		switch event.EventType {
		case dal.DispatchEventAccepted:
			row.Accepted++
		case dal.DispatchEventAttempt:
			row.Attempts++
		case dal.DispatchEventSuccess:
			row.Successes++
		case dal.DispatchEventFailure:
			row.Failures++
		}

		row.LastEventAt = event.CreatedAt
		row.LastEventType = event.EventType
		row.LastMessage = event.Message
	}

	return summary
}

func runNextAction(status string, taskCompletion dal.RunTaskCompletion, pendingTaskContinuation bool, securityGateFailed bool) *string {
	if status == dal.RunStatusFailed && securityGateFailed {
		action := runNextActionSecurityGateFailed
		return &action
	}

	if status == dal.RunStatusOrphaned && taskCompletion.Total > 0 && (taskCompletion.TerminalFailed > 0 || taskCompletion.AllSucceeded()) {
		action := runNextActionTaskFinalizationRepairPending
		return &action
	}

	if status != dal.RunStatusQueued {
		return nil
	}

	if pendingTaskContinuation {
		action := runNextActionTaskContinuationPending
		return &action
	}

	if taskCompletion.Total > 0 && taskCompletion.Incomplete > 0 {
		action := runNextActionTaskCompletionPending
		return &action
	}

	return nil
}

func (s *APIServer) runHasPendingTaskContinuation(ctx context.Context, runID string) (bool, error) {
	executions, err := s.runs.ListPendingExecutions(ctx, runID)
	if err != nil {
		return false, err
	}

	for _, execution := range executions {
		if strings.TrimSpace(execution.TaskKey) != "" && execution.TaskKey != dal.RootTaskKey {
			return true, nil
		}
	}

	return false, nil
}

func (s *APIServer) GetRunTasks(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	params := parsePageParams(r)

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

	taskRecords, nextCursor, err := s.runs.ListRunTasks(ctx, runID, params.Cursor, params.Limit)
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

	type executionSecurityEventRow struct {
		ID            int64   `json:"id"`
		RunID         string  `json:"run_id"`
		TaskID        string  `json:"task_id,omitempty"`
		TaskAttemptID string  `json:"task_attempt_id,omitempty"`
		ExecutionID   string  `json:"execution_id,omitempty"`
		EventType     string  `json:"event_type"`
		Outcome       string  `json:"outcome"`
		Reason        string  `json:"reason,omitempty"`
		Provider      *string `json:"provider,omitempty"`
		SecretCount   *int    `json:"secret_count,omitempty"`
		FileCount     *int    `json:"file_count,omitempty"`
		CreatedAt     int64   `json:"created_at"`
	}

	type taskAttemptRow struct {
		AttemptID       string                      `json:"attempt_id"`
		TaskID          string                      `json:"task_id"`
		RunID           string                      `json:"run_id"`
		ExecutionID     string                      `json:"execution_id,omitempty"`
		ExecutionStatus string                      `json:"execution_status,omitempty"`
		CellID          string                      `json:"cell_id"`
		LeaseOwner      *string                     `json:"lease_owner,omitempty"`
		LeaseUntil      *int64                      `json:"lease_until,omitempty"`
		Attempt         int                         `json:"attempt"`
		Status          string                      `json:"status"`
		AcceptedAt      *string                     `json:"accepted_at,omitempty"`
		StartedAt       *string                     `json:"started_at,omitempty"`
		FinishedAt      *string                     `json:"finished_at,omitempty"`
		LastObservedAt  *int64                      `json:"last_observed_at,omitempty"`
		EventSequence   int64                       `json:"event_sequence"`
		CreatedAt       *string                     `json:"created_at,omitempty"`
		UpdatedAt       *string                     `json:"updated_at,omitempty"`
		SecurityEvents  []executionSecurityEventRow `json:"security_events,omitempty"`
	}

	type taskRow struct {
		TaskID       string           `json:"task_id"`
		RunID        string           `json:"run_id"`
		ParentTaskID *string          `json:"parent_task_id,omitempty"`
		TaskKey      string           `json:"task_key"`
		Name         string           `json:"name"`
		Status       string           `json:"status"`
		SpecHash     string           `json:"spec_hash,omitempty"`
		CreatedAt    *string          `json:"created_at,omitempty"`
		UpdatedAt    *string          `json:"updated_at,omitempty"`
		Attempts     []taskAttemptRow `json:"attempts"`
	}

	tasks := make([]taskRow, 0, len(taskRecords))
	for _, rec := range taskRecords {
		task := taskRow{
			TaskID:       rec.TaskID,
			RunID:        rec.RunID,
			ParentTaskID: rec.ParentTaskID,
			TaskKey:      rec.TaskKey,
			Name:         rec.Name,
			Status:       rec.Status,
			SpecHash:     rec.SpecHash,
			CreatedAt:    rec.CreatedAt,
			UpdatedAt:    rec.UpdatedAt,
			Attempts:     []taskAttemptRow{},
		}

		for _, attempt := range rec.Attempts {
			securityEvents := make([]executionSecurityEventRow, 0, len(attempt.SecurityEvents))
			for _, event := range attempt.SecurityEvents {
				securityEvents = append(securityEvents, executionSecurityEventRow{
					ID:            event.ID,
					RunID:         event.RunID,
					TaskID:        event.TaskID,
					TaskAttemptID: event.TaskAttemptID,
					ExecutionID:   event.ExecutionID,
					EventType:     event.EventType,
					Outcome:       event.Outcome,
					Reason:        event.Reason,
					Provider:      event.Provider,
					SecretCount:   event.SecretCount,
					FileCount:     event.FileCount,
					CreatedAt:     event.CreatedAt,
				})
			}

			task.Attempts = append(task.Attempts, taskAttemptRow{
				AttemptID:       attempt.AttemptID,
				TaskID:          attempt.TaskID,
				RunID:           attempt.RunID,
				ExecutionID:     attempt.ExecutionID,
				ExecutionStatus: attempt.ExecutionStatus,
				CellID:          attempt.CellID,
				LeaseOwner:      attempt.LeaseOwner,
				LeaseUntil:      attempt.LeaseUntil,
				Attempt:         attempt.Attempt,
				Status:          attempt.Status,
				AcceptedAt:      attempt.AcceptedAt,
				StartedAt:       attempt.StartedAt,
				FinishedAt:      attempt.FinishedAt,
				LastObservedAt:  attempt.LastObservedAt,
				EventSequence:   attempt.EventSequence,
				CreatedAt:       attempt.CreatedAt,
				UpdatedAt:       attempt.UpdatedAt,
				SecurityEvents:  securityEvents,
			})
		}

		tasks = append(tasks, task)
	}

	w.Header().Set("Content-Type", "application/json")
	resp := buildPaginatedResponse(tasks, nextCursor)
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(resp); err != nil {
		s.logger.Error("Failed to encode run tasks: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	_, _ = w.Write(buf.Bytes())
}

func (s *APIServer) GetRunExecutionPayload(w http.ResponseWriter, r *http.Request) {
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

	payload, err := s.runs.GetExecutionPayloadForRun(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "execution_payload_not_found", "execution payload not found", nil)
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

	if !json.Valid([]byte(payload.PayloadJSON)) {
		s.logger.Error("Stored execution payload for run %s is not valid JSON", runID)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	resp := struct {
		RunID          string          `json:"run_id"`
		PayloadHash    string          `json:"payload_hash"`
		DefinitionHash string          `json:"definition_hash,omitempty"`
		Payload        json.RawMessage `json:"payload"`
	}{
		RunID:          payload.RunID,
		PayloadHash:    payload.PayloadHash,
		DefinitionHash: payload.DefinitionHash,
		Payload:        json.RawMessage(payload.PayloadJSON),
	}

	w.Header().Set("Content-Type", "application/json")
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(resp); err != nil {
		s.logger.Error("Failed to encode execution payload: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	_, _ = w.Write(buf.Bytes())
}
