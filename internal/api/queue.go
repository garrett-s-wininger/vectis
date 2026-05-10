package api

import (
	"net/http"
)

type queueBacklogResponse struct {
	Queued int64 `json:"queued"`
}

func (s *APIServer) GetQueueBacklog(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	n, err := s.runs.CountByStatus(ctx, "queued")
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}
		s.logger.Error("queue backlog query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	writeJSON(w, http.StatusOK, queueBacklogResponse{Queued: n})
}
