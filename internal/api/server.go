package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
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
	defaultHandlerDBTimeout = 60 * time.Second
)

type APIServer struct {
	jobs           dal.JobsRepository
	runs           dal.RunsRepository
	ephemeralRuns  dal.EphemeralRunStarter
	logger         interfaces.Logger
	queueClient    interfaces.QueueService
	queueClose     func()
	runBroadcaster *RunBroadcaster
	dbUnavailable  atomic.Bool
	healthDB       *sql.DB
}

func NewAPIServer(logger interfaces.Logger, db *sql.DB) *APIServer {
	repos := dal.NewSQLRepositories(db)
	s := NewAPIServerWithRepositories(logger, repos.Jobs(), repos.Runs(), repos)
	s.healthDB = db
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

	var job api.Job
	if err := json.Unmarshal(body, &job); err != nil {
		http.Error(w, "invalid job definition", http.StatusBadRequest)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	err = s.jobs.Create(ctx, *job.Id, string(body))
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

	err := s.jobs.Delete(ctx, jobID)
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
	for _, rec := range records {
		var definition any
		if err := json.Unmarshal([]byte(rec.DefinitionJSON), &definition); err != nil {
			s.logger.Error("Failed to parse job definition for job %s: %v", rec.JobID, err)
			continue
		}

		jobs = append(jobs, map[string]any{
			"name":       rec.JobID,
			"definition": definition,
		})
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

	var job api.Job
	if err := json.Unmarshal(body, &job); err != nil {
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

	ctx := r.Context()
	ch := s.runBroadcaster.Subscribe(jobID)
	defer s.runBroadcaster.Unsubscribe(jobID, ch)

	s.logger.Info("SSE client subscribed to runs for job: %s", jobID)

	_, _ = w.Write([]byte(": connected\n\n"))
	flusher.Flush()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
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
	mux.HandleFunc("GET /health/live", s.HealthLive)
	mux.HandleFunc("GET /health/ready", s.HealthReady)
	mux.HandleFunc("GET /api/v1/jobs", s.GetJobs)
	mux.HandleFunc("GET /api/v1/jobs/{id}", s.GetJob)
	mux.HandleFunc("POST /api/v1/jobs", s.CreateJob)
	mux.HandleFunc("POST /api/v1/jobs/run", s.RunJob)
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", s.DeleteJob)
	mux.HandleFunc("PUT /api/v1/jobs/{id}", s.UpdateJobDefinition)
	mux.HandleFunc("POST /api/v1/jobs/trigger/{id}", s.TriggerJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}/runs", s.GetJobRuns)
	mux.HandleFunc("GET /api/v1/sse/jobs/{id}/runs", s.HandleSSEJobRuns)
	mux.HandleFunc("POST /api/v1/runs/{id}/force-fail", s.ForceFailRun)
	mux.HandleFunc("POST /api/v1/runs/{id}/force-requeue", s.ForceRequeueRun)
	return mux
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
