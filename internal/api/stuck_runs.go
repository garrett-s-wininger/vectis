package api

import (
	"net/http"
	"time"

	"vectis/internal/reconciler"
)

type stuckRunsResponse struct {
	Stuck           int64                   `json:"stuck"`
	DispatchGapSecs int64                   `json:"dispatch_gap_secs"`
	Cells           []stuckRunsCellResponse `json:"cells,omitempty"`
}

type stuckRunsCellResponse struct {
	CellID string `json:"cell_id"`
	Stuck  int64  `json:"stuck"`
}

func (s *APIServer) GetStuckRuns(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	cutoff := time.Now().UTC().Add(-reconciler.MinDispatchGap).Unix()
	n, err := s.runs.CountStuckBeforeDispatchCutoff(ctx, cutoff)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("stuck runs query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	counts, err := s.runs.CountStuckBeforeDispatchCutoffByCell(ctx, cutoff)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("stuck runs by cell query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	cells := make([]stuckRunsCellResponse, 0, len(counts))
	for _, count := range counts {
		cells = append(cells, stuckRunsCellResponse{
			CellID: count.CellID,
			Stuck:  count.Count,
		})
	}

	writeJSON(w, http.StatusOK, stuckRunsResponse{
		Stuck:           n,
		DispatchGapSecs: int64(reconciler.MinDispatchGap.Seconds()),
		Cells:           cells,
	})
}
