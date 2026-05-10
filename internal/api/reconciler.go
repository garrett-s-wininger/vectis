package api

import (
	"net/http"
)

type reconcilerHeartbeatResponse struct {
	LastActivityUnix *int64 `json:"last_activity_unix,omitempty"`
	Active           bool   `json:"active"`
}

func (s *APIServer) GetReconcilerHeartbeat(w http.ResponseWriter, r *http.Request) {
	if s.dispatchEvents == nil {
		writeJSON(w, http.StatusOK, reconcilerHeartbeatResponse{Active: false})
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	ts, err := s.dispatchEvents.LastReconcilerActivity(ctx)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}
		s.logger.Error("reconciler heartbeat query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	writeJSON(w, http.StatusOK, reconcilerHeartbeatResponse{
		LastActivityUnix: ts,
		Active:           ts != nil,
	})
}
