package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	api "vectis/api/gen/go"
	"vectis/internal/log"
	"vectis/internal/registry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type APIServer struct {
	db             *sql.DB
	logger         *log.Logger
	queueClient    api.QueueServiceClient
	registryClient *registry.Registry
}

func NewAPIServer(logger *log.Logger, db *sql.DB) *APIServer {
	return &APIServer{
		db:     db,
		logger: logger,
	}
}

func (s *APIServer) ConnectToRegistry(ctx context.Context) error {
	regClient, err := registry.New(ctx, s.logger)
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

	s.queueClient = api.NewQueueServiceClient(conn)
	s.logger.Info("Connected to queue at %s", queueAddr)

	return nil
}

func (s *APIServer) triggerJob(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) Run(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/jobs/trigger/{id}", s.triggerJob)

	s.logger.Info("API server listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
