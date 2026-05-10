package api

import (
	"net/http"
)

type cronStatusResponse struct {
	ScheduleCount int64 `json:"schedule_count"`
	Active        bool  `json:"active"`
}

func (s *APIServer) GetCronStatus(w http.ResponseWriter, r *http.Request) {
	if s.schedules == nil {
		writeJSON(w, http.StatusOK, cronStatusResponse{})
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	n, err := s.schedules.CountCronSchedules(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("cron schedule count failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	writeJSON(w, http.StatusOK, cronStatusResponse{
		ScheduleCount: n,
		Active:        n > 0,
	})
}
