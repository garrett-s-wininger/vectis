package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/registry"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var defaultUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

type APIServer struct {
	db             *sql.DB
	logger         interfaces.Logger
	queueClient    interfaces.QueueService
	registryClient interfaces.RegistryClient
	runBroadcaster *RunBroadcaster
}

func NewAPIServer(logger interfaces.Logger, db *sql.DB) *APIServer {
	return &APIServer{
		db:             db,
		logger:         logger,
		runBroadcaster: NewRunBroadcaster(logger),
	}
}

func (s *APIServer) SetQueueClient(client interfaces.QueueService) {
	s.queueClient = client
}

func (s *APIServer) ConnectToRegistry(ctx context.Context) error {
	regClient, err := registry.New(ctx, s.logger, interfaces.SystemClock{})
	if err != nil {
		return fmt.Errorf("failed to connect to registry: %w", err)
	}

	s.registryClient = regClient

	// TODO(garrett): Re-generate queue address when calling Enqueue.
	queueAddr, err := regClient.Address(ctx, api.Component_COMPONENT_QUEUE)
	if err != nil {
		return fmt.Errorf("failed to get queue address: %w", err)
	}

	conn, err := grpc.NewClient(queueAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to queue: %w", err)
	}

	s.queueClient = interfaces.NewQueueService(api.NewQueueServiceClient(conn))
	s.logger.Info("Connected to queue at %s", queueAddr)

	return nil
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

	_, err = s.db.Exec("INSERT INTO stored_jobs (job_id, definition_json) VALUES (?, ?)", job.Id, body)
	if err != nil {
		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	s.logger.Info("Stored job: %s", *job.Id)
	w.WriteHeader(http.StatusCreated)
}

func (s *APIServer) DeleteJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	_, err := s.db.Exec("DELETE FROM stored_jobs WHERE job_id = ?", jobID)
	if err != nil {
		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	s.logger.Info("Deleted job: %s", jobID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *APIServer) GetJobs(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query("SELECT job_id, definition_json FROM stored_jobs")
	if err != nil {
		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// TODO(garrett): Cursor-based pagination.
	// TODO(garrett): Option to avoid returning the definition.
	var jobs []map[string]any
	for rows.Next() {
		var jobID string
		var definitionJSON string
		if err := rows.Scan(&jobID, &definitionJSON); err != nil {
			s.logger.Error("Failed to scan row: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		var definition any
		if err := json.Unmarshal([]byte(definitionJSON), &definition); err != nil {
			s.logger.Error("Failed to parse job definition for job %s: %v", jobID, err)
			continue
		}

		jobs = append(jobs, map[string]any{
			"name":       jobID,
			"definition": definition,
		})
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Row iteration error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
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

func (s *APIServer) TriggerJob(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	var definitionJSON string
	err := s.db.QueryRow("SELECT definition_json FROM stored_jobs WHERE job_id = ?", jobID).Scan(&definitionJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "job not found", http.StatusNotFound)
			return
		}

		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	var job api.Job
	if err := json.Unmarshal([]byte(definitionJSON), &job); err != nil {
		s.logger.Error("Failed to parse job definition JSON: %v", err)
		http.Error(w, "invalid job definition", http.StatusInternalServerError)
		return
	}

	job.Id = &jobID

	var runID string
	var runIndex int
	tx, txErr := s.db.BeginTx(r.Context(), nil)
	if txErr != nil {
		s.logger.Error("Database error starting transaction: %v", txErr)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if err := tx.QueryRowContext(r.Context(), "SELECT COALESCE(MAX(run_index), 0) + 1 FROM job_runs WHERE job_id = ?", jobID).Scan(&runIndex); err != nil {
		_ = tx.Rollback()
		s.logger.Error("Database error computing run index: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	runID = uuid.New().String()
	if _, err := tx.ExecContext(r.Context(), "INSERT INTO job_runs (run_id, job_id, run_index, status) VALUES (?, ?, ?, ?)", runID, jobID, runIndex, "queued"); err != nil {
		_ = tx.Rollback()
		s.logger.Error("Database error inserting job run: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		s.logger.Error("Database error committing job run: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	s.runBroadcaster.Broadcast(jobID, runID, runIndex)
	job.RunId = &runID

	_, err = s.queueClient.Enqueue(r.Context(), &job)
	if err != nil {
		s.logger.Error("Failed to enqueue job: %v", err)
		http.Error(w, "failed to enqueue job", http.StatusServiceUnavailable)
		return
	}

	s.logger.Info("Triggered job: %s (run %s, index %d)", jobID, runID, runIndex)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"job_id":    jobID,
		"run_id":    runID,
		"run_index": runIndex,
	}); err != nil {
		s.logger.Error("Failed to encode trigger response: %v", err)
	}
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

	_, err = s.db.Exec("UPDATE stored_jobs SET definition_json = ? WHERE job_id = ?", body, jobID)
	if err != nil {
		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	s.logger.Info("Updated job definition: %s", jobID)
	w.WriteHeader(http.StatusNoContent)
}

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
	runID := uuid.New().String()
	job.Id = &generatedID
	job.RunId = &runID

	_, err = s.queueClient.Enqueue(r.Context(), &job)
	if err != nil {
		s.logger.Error("Failed to enqueue job: %v", err)
		http.Error(w, "failed to enqueue job", http.StatusServiceUnavailable)
		return
	}

	s.logger.Info("Enqueued ephemeral job: %s (run %s)", generatedID, runID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"id":     generatedID,
		"run_id": runID,
	}); err != nil {
		s.logger.Error("Failed to encode response: %v", err)
	}
}

func (s *APIServer) GetJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	sinceStr := r.URL.Query().Get("since")
	query := "SELECT run_id, run_index, status, started_at, finished_at FROM job_runs WHERE job_id = ?"
	args := []any{jobID}
	if sinceStr != "" {
		since, err := strconv.Atoi(sinceStr)
		if err != nil || since < 0 {
			http.Error(w, "since must be a non-negative integer", http.StatusBadRequest)
			return
		}
		query += " AND run_index > ?"
		args = append(args, since)
	}

	query += " ORDER BY run_index ASC"
	rows, err := s.db.Query(query, args...)
	if err != nil {
		s.logger.Error("Database error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type runRow struct {
		RunID      string  `json:"run_id"`
		RunIndex   int     `json:"run_index"`
		Status     string  `json:"status"`
		StartedAt  *string `json:"started_at,omitempty"`
		FinishedAt *string `json:"finished_at,omitempty"`
	}

	var runs []runRow
	for rows.Next() {
		var row runRow
		var startedAt, finishedAt sql.NullString
		if err := rows.Scan(&row.RunID, &row.RunIndex, &row.Status, &startedAt, &finishedAt); err != nil {
			s.logger.Error("Failed to scan run row: %v", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		if startedAt.Valid {
			row.StartedAt = &startedAt.String
		}

		if finishedAt.Valid {
			row.FinishedAt = &finishedAt.String
		}

		runs = append(runs, row)
	}

	if err := rows.Err(); err != nil {
		s.logger.Error("Row iteration error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if runs == nil {
		runs = []runRow{}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(runs); err != nil {
		s.logger.Error("Failed to encode runs: %v", err)
	}
}

func (s *APIServer) HandleWebSocketJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("id")
	if jobID == "" {
		http.Error(w, "id is required", http.StatusBadRequest)
		return
	}

	conn, err := defaultUpgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	ch := s.runBroadcaster.Subscribe(jobID, conn)
	defer func() {
		if c, ok := s.runBroadcaster.Unsubscribe(jobID, conn); ok {
			close(c)
		}
	}()

	s.logger.Info("WebSocket client subscribed to runs for job: %s", jobID)

	go func() {
		for payload := range ch {
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				if !websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					return
				}
				s.logger.Error("WebSocket write error: %v", err)
				return
			}
		}
	}()

	for {
		// NOTE(garrett): We only use read to detect client disconnect; ignore message content
		if _, _, err := conn.ReadMessage(); err != nil {
			if !websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				return
			}

			s.logger.Error("WebSocket read error: %v", err)
			return
		}
	}
}

func (s *APIServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/jobs", s.GetJobs)
	mux.HandleFunc("POST /api/v1/jobs", s.CreateJob)
	mux.HandleFunc("POST /api/v1/jobs/run", s.RunJob)
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", s.DeleteJob)
	mux.HandleFunc("PUT /api/v1/jobs/{id}", s.UpdateJobDefinition)
	mux.HandleFunc("POST /api/v1/jobs/trigger/{id}", s.TriggerJob)
	mux.HandleFunc("GET /api/v1/jobs/{id}/runs", s.GetJobRuns)
	mux.HandleFunc("GET /api/v1/ws/jobs/{id}/runs", s.HandleWebSocketJobRuns)
	return mux
}

func (s *APIServer) Run(addr string) error {
	s.logger.Info("API server listening on %s", addr)
	return http.ListenAndServe(addr, s.Handler())
}
