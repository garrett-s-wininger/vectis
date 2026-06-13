package api

import (
	"net/http"
	"time"

	"vectis/internal/reconciler"
)

type stuckRunsResponse struct {
	Stuck                   int64                      `json:"stuck"`
	DispatchGapSecs         int64                      `json:"dispatch_gap_secs"`
	Cells                   []stuckRunsCellResponse    `json:"cells,omitempty"`
	TaskFinalizationPending int64                      `json:"task_finalization_pending"`
	TaskFinalizationCells   []pendingCellCountResponse `json:"task_finalization_cells,omitempty"`
	TaskContinuationPending int64                      `json:"task_continuation_pending"`
	TaskContinuationCells   []pendingCellCountResponse `json:"task_continuation_cells,omitempty"`
}

type stuckRunsCellResponse struct {
	CellID string `json:"cell_id"`
	Stuck  int64  `json:"stuck"`
}

type pendingCellCountResponse struct {
	CellID  string `json:"cell_id"`
	Pending int64  `json:"pending"`
}

func (s *APIServer) GetStuckRuns(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	now := time.Now().UTC()
	cutoff := now.Add(-reconciler.MinDispatchGap).Unix()
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

	taskFinalizationPending, err := s.runs.CountOrphanedTaskFinalizationCandidates(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("pending task finalization query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	taskFinalizationCounts, err := s.runs.CountOrphanedTaskFinalizationCandidatesByCell(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("pending task finalization by cell query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	taskFinalizationCells := make([]pendingCellCountResponse, 0, len(taskFinalizationCounts))
	for _, count := range taskFinalizationCounts {
		taskFinalizationCells = append(taskFinalizationCells, pendingCellCountResponse{
			CellID:  count.CellID,
			Pending: count.Count,
		})
	}

	taskContinuationPending, err := s.runs.CountPendingTaskContinuations(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("pending task continuation query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	taskContinuationCounts, err := s.runs.CountPendingTaskContinuationsByCell(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("pending task continuation by cell query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	taskContinuationCells := make([]pendingCellCountResponse, 0, len(taskContinuationCounts))
	for _, count := range taskContinuationCounts {
		taskContinuationCells = append(taskContinuationCells, pendingCellCountResponse{
			CellID:  count.CellID,
			Pending: count.Count,
		})
	}

	writeJSON(w, http.StatusOK, stuckRunsResponse{
		Stuck:                   n,
		DispatchGapSecs:         int64(reconciler.MinDispatchGap.Seconds()),
		Cells:                   cells,
		TaskFinalizationPending: taskFinalizationPending,
		TaskFinalizationCells:   taskFinalizationCells,
		TaskContinuationPending: taskContinuationPending,
		TaskContinuationCells:   taskContinuationCells,
	})
}
