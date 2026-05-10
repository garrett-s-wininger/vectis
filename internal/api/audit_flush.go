package api

import (
	"net/http"
)

type auditFlushFailuresResponse struct {
	FlushFailures int64 `json:"flush_failures"`
}

type flushedCounter interface {
	FlushFailureCount() int64
}

func (s *APIServer) GetAuditFlushFailures(w http.ResponseWriter, _ *http.Request) {
	var count int64

	if fc, ok := s.auditor.(flushedCounter); ok {
		count = fc.FlushFailureCount()
	}

	writeJSON(w, http.StatusOK, auditFlushFailuresResponse{FlushFailures: count})
}
