package api

import (
	"net/http"
)

type auditDropsResponse struct {
	Dropped int64 `json:"dropped"`
}

type droppedCounter interface {
	DroppedCount() int64
}

func (s *APIServer) GetAuditDrops(w http.ResponseWriter, _ *http.Request) {
	var count int64

	if dc, ok := s.auditor.(droppedCounter); ok {
		count = dc.DroppedCount()
	}

	writeJSON(w, http.StatusOK, auditDropsResponse{Dropped: count})
}
