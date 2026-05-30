package cellingress

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"

	"google.golang.org/protobuf/encoding/protojson"
)

const maxExecutionRequestBytes = 2 << 20

type Server struct {
	localCellID string
	router      cell.ExecutionIngress
	acceptances dal.CellExecutionAcceptancesRepository
	logger      interfaces.Logger
	mux         *http.ServeMux
}

type submitExecutionRequest struct {
	JobRequest json.RawMessage `json:"job_request"`
}

type submitExecutionResponse struct {
	Status      string `json:"status"`
	CellID      string `json:"cell_id"`
	RunID       string `json:"run_id"`
	ExecutionID string `json:"execution_id"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewQueueServer(localCellID string, queue interfaces.QueueService, logger interfaces.Logger) *Server {
	return NewServer(localCellID, cell.NewQueueExecutionIngress(queue, logger), logger)
}

func NewServer(localCellID string, ingress cell.ExecutionIngress, logger interfaces.Logger) *Server {
	localCellID = normalizeLocalCellID(localCellID)
	s := &Server{
		localCellID: localCellID,
		router: cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{
			localCellID: ingress,
		}),
		logger: logger,
		mux:    http.NewServeMux(),
	}

	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) SetAcceptanceStore(acceptances dal.CellExecutionAcceptancesRepository) {
	s.acceptances = acceptances
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health/live", s.handleLive)
	s.mux.HandleFunc("GET /health/ready", s.handleReady)
	s.mux.HandleFunc("POST /cell/v1/executions", s.submitExecution)
}

func (s *Server) handleLive(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) submitExecution(w http.ResponseWriter, r *http.Request) {
	var body submitExecutionRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxExecutionRequestBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("invalid execution request: %v", err))
		return
	}

	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid execution request: multiple JSON values")
		return
	}

	if len(body.JobRequest) == 0 {
		writeError(w, http.StatusBadRequest, "missing_job_request", "job_request is required")
		return
	}

	var jobReq api.JobRequest
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(body.JobRequest, &jobReq); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_job_request", fmt.Sprintf("invalid job_request: %v", err))
		return
	}

	submission, err := cell.NewExecutionSubmission(&jobReq)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_execution_envelope", err.Error())
		return
	}

	if submission.Envelope == nil {
		writeError(w, http.StatusBadRequest, "missing_execution_envelope", "execution envelope metadata is required")
		return
	}

	durable := s.acceptances != nil
	if durable {
		acceptance, err := executionAcceptance(submission)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_execution_envelope", err.Error())
			return
		}

		if _, err := s.acceptances.AcceptExecution(r.Context(), acceptance); err != nil {
			if dal.IsConflict(err) {
				writeError(w, http.StatusConflict, "execution_conflict", err.Error())
				return
			}

			if s.logger != nil {
				s.logger.Warn("Cell ingress durable accept failed: %v", err)
			}

			writeError(w, http.StatusServiceUnavailable, "acceptance_unavailable", "local cell did not durably accept execution")
			return
		}
	}

	if err := s.router.SubmitExecution(r.Context(), submission); err != nil {
		if cell.IsCellNotRoutable(err) {
			writeError(w, http.StatusConflict, "wrong_cell", fmt.Sprintf("execution targets cell %q, local cell is %q", submission.TargetCellID(), s.localCellID))
			return
		}

		if s.logger != nil {
			s.logger.Warn("Cell ingress enqueue failed: %v", err)
		}

		writeError(w, http.StatusServiceUnavailable, "queue_unavailable", "local queue did not accept execution")
		return
	}

	writeJSON(w, http.StatusAccepted, acceptedExecutionResponse(submission))
}

func acceptedExecutionResponse(submission cell.ExecutionSubmission) submitExecutionResponse {
	return submitExecutionResponse{
		Status:      "accepted",
		CellID:      submission.Envelope.CellID,
		RunID:       submission.Envelope.RunID,
		ExecutionID: submission.Envelope.ExecutionID,
	}
}

func executionAcceptance(submission cell.ExecutionSubmission) (dal.CellExecutionAcceptance, error) {
	if submission.Request == nil || submission.Envelope == nil {
		return dal.CellExecutionAcceptance{}, errors.New("execution submission is incomplete")
	}

	definitionJSON, err := json.Marshal(submission.Envelope.Job)
	if err != nil {
		return dal.CellExecutionAcceptance{}, fmt.Errorf("marshal job definition: %w", err)
	}

	requestJSON, err := protojson.Marshal(submission.Request)
	if err != nil {
		return dal.CellExecutionAcceptance{}, fmt.Errorf("marshal job request: %w", err)
	}

	env := submission.Envelope
	return dal.CellExecutionAcceptance{
		ExecutionID:        env.ExecutionID,
		RunID:              env.RunID,
		JobID:              env.Job.GetId(),
		RunIndex:           env.RunIndex,
		SegmentID:          env.SegmentID,
		SegmentName:        "root",
		CellID:             env.CellID,
		Attempt:            1,
		DefinitionVersion:  env.DefinitionVersion,
		DefinitionHash:     env.DefinitionHash,
		DefinitionJSON:     string(definitionJSON),
		RequestJSON:        string(requestJSON),
		AcceptedAtUnixNano: env.CreatedAtUnixNano,
	}, nil
}

func normalizeLocalCellID(cellID string) string {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return dal.DefaultCellID
	}

	return cellID
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Code: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	_ = enc.Encode(payload)
}

func HTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
}
