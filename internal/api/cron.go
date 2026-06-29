package api

import (
	"net/http"
	"time"
)

type cronStatusResponse struct {
	ScheduleCount int64  `json:"schedule_count"`
	DueCount      int64  `json:"due_count"`
	ClaimedCount  int64  `json:"claimed_count"`
	OldestDueUnix *int64 `json:"oldest_due_unix,omitempty"`
	Active        bool   `json:"active"`
}

func (s *APIServer) GetCronStatus(w http.ResponseWriter, r *http.Request) {
	if s.schedules == nil {
		writeJSON(w, http.StatusOK, cronStatusResponse{})
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	summary, err := s.schedules.CronScheduleSummary(ctx, time.Now().UTC())
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("cron schedule status failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	var oldestDueUnix *int64
	if summary.OldestDueAt != nil {
		v := summary.OldestDueAt.UTC().Unix()
		oldestDueUnix = &v
	}

	writeJSON(w, http.StatusOK, cronStatusResponse{
		ScheduleCount: summary.ScheduleCount,
		DueCount:      summary.DueCount,
		ClaimedCount:  summary.ClaimedCount,
		OldestDueUnix: oldestDueUnix,
		Active:        summary.ScheduleCount > 0,
	})
}
