package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	api "vectis/api/gen/go"
	"vectis/internal/log"
	"vectis/internal/migrations"
	"vectis/internal/registry"

	_ "github.com/mattn/go-sqlite3"
)

func getDBPath() string {
	dataHome := os.Getenv("XDG_DATA_HOME")
	if dataHome == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			panic(fmt.Sprintf("cannot determine home directory: %v", err))
		}

		dataHome = filepath.Join(home, ".local", "share")
	}

	return filepath.Join(dataHome, "vectis", "db.sqlite3")
}

func initDB(dbPath string) (*sql.DB, error) {
	// TODO(garrett): Skip if network-based RDBMS.
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := migrations.Run(db); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return db, nil
}

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

func runVectisAPI(cmd *cobra.Command, args []string) {
	logger := log.New("api")
	logger.Info("Starting API server...")

	// TODO(garrett): Make configurable.
	dbPath := getDBPath()
	logger.Info("Using database: %s", dbPath)

	// TODO(garrett): Skip if production.
	db, err := initDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to initialize database: %v", err)
	}
	defer db.Close()

	server := NewAPIServer(logger, db)

	if err := server.ConnectToRegistry(cmd.Context()); err != nil {
		logger.Fatal("Failed to connect to services: %v", err)
	}

	port := viper.GetInt("port")
	if port <= 0 {
		port = 8080
	}
	addr := fmt.Sprintf(":%d", port)
	if err := server.Run(addr); err != nil {
		logger.Fatal("Server failed: %v", err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "vectis-api-server",
	Short: "Vectis API Server",
	Long:  `The Vectis API Server provides REST endpoints for triggering stored jobs.`,
	Run:   runVectisAPI,
}

func init() {
	viper.SetDefault("port", 8080)
	rootCmd.PersistentFlags().Int("port", 8080, "Port for the API server")
	_ = viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	viper.SetEnvPrefix("VECTIS_API_SERVER")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
