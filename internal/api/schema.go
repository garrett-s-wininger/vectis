package api

import (
	"database/sql"
	"net/http"
)

type schemaStatusResponse struct {
	CurrentVersion int  `json:"current_version"`
	HasSchema      bool `json:"has_schema"`
}

func (s *APIServer) GetSchemaStatus(w http.ResponseWriter, r *http.Request) {
	if s.healthDB == nil {
		writeAPIErrorCode(w, http.StatusServiceUnavailable, apiErrDatabaseNotReady)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	var version sql.NullInt64
	err := s.healthDB.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&version)
	if err != nil {
		if err == sql.ErrNoRows {
			writeJSON(w, http.StatusOK, schemaStatusResponse{HasSchema: false})
			return
		}
		if s.handleDBUnavailableError(w, err) {
			return
		}
		s.logger.Error("schema status query failed: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	if !version.Valid {
		writeJSON(w, http.StatusOK, schemaStatusResponse{HasSchema: false})
		return
	}

	writeJSON(w, http.StatusOK, schemaStatusResponse{
		CurrentVersion: int(version.Int64),
		HasSchema:      true,
	})
}
