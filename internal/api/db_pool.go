package api

import (
	"net/http"
	"time"
)

type dbPoolStatsResponse struct {
	MaxOpenConnections int           `json:"max_open_connections"`
	OpenConnections    int           `json:"open_connections"`
	InUse              int           `json:"in_use"`
	Idle               int           `json:"idle"`
	WaitCount          int64         `json:"wait_count"`
	WaitDuration       time.Duration `json:"wait_duration"`
}

func (s *APIServer) GetDBPoolStats(w http.ResponseWriter, _ *http.Request) {
	if s.healthDB == nil {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrDatabaseNotReady)
		return
	}

	stats := s.healthDB.Stats()
	writeJSON(w, http.StatusOK, dbPoolStatsResponse{
		MaxOpenConnections: stats.MaxOpenConnections,
		OpenConnections:    stats.OpenConnections,
		InUse:              stats.InUse,
		Idle:               stats.Idle,
		WaitCount:          stats.WaitCount,
		WaitDuration:       stats.WaitDuration,
	})
}
