package api

import (
	"net/http"
)

type queueBacklogResponse struct {
	Queued int64                      `json:"queued"`
	Cells  []queueBacklogCellResponse `json:"cells,omitempty"`
}

type queueBacklogCellResponse struct {
	CellID string `json:"cell_id"`
	Queued int64  `json:"queued"`
}

func (s *APIServer) GetQueueBacklog(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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

	counts, err := s.runs.CountByStatusByCell(ctx, "queued")
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("queue backlog by cell query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	cells := make([]queueBacklogCellResponse, 0, len(counts))
	for _, count := range counts {
		cells = append(cells, queueBacklogCellResponse{
			CellID: count.CellID,
			Queued: count.Count,
		})
	}

	writeJSON(w, http.StatusOK, queueBacklogResponse{
		Queued: n,
		Cells:  cells,
	})
}
