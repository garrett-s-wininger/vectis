package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/registry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type APIServer struct {
	db             *sql.DB
	logger         interfaces.Logger
	queueClient    interfaces.QueueService
	registryClient interfaces.RegistryClient
}

func NewAPIServer(logger interfaces.Logger, db *sql.DB) *APIServer {
	return &APIServer{
		db:     db,
		logger: logger,
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

	_, err = s.queueClient.Enqueue(r.Context(), &job)
	if err != nil {
		s.logger.Error("Failed to enqueue job: %v", err)
		http.Error(w, "failed to enqueue job", http.StatusServiceUnavailable)
		return
	}

	s.logger.Info("Triggered job: %s", jobID)
	w.WriteHeader(http.StatusNoContent)
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

func (s *APIServer) Run(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/jobs", s.GetJobs)
	mux.HandleFunc("POST /api/v1/jobs", s.CreateJob)
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", s.DeleteJob)
	mux.HandleFunc("PUT /api/v1/jobs/{id}", s.UpdateJobDefinition)
	mux.HandleFunc("POST /api/v1/jobs/trigger/{id}", s.TriggerJob)

	s.logger.Info("API server listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
