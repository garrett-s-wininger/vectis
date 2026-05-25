package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"vectis/internal/cell"
	"vectis/internal/dal"
)

type postCellCatalogEventRequest struct {
	EventKey  string          `json:"event_key"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

type postCellCatalogEventResponse struct {
	ID         int64  `json:"id"`
	SourceCell string `json:"source_cell"`
	EventKey   string `json:"event_key"`
	EventType  string `json:"event_type"`
	Status     string `json:"status"`
	Created    bool   `json:"created"`
}

func (s *APIServer) PostCellCatalogEvent(w http.ResponseWriter, r *http.Request) {
	if s.catalogEvents == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "catalog_events_unavailable", "catalog event inbox not configured", nil)
		return
	}

	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	sourceCell := strings.TrimSpace(r.PathValue("cell_id"))
	if sourceCell == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_cell_id", "cell_id is required", nil)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONDocumentBodyBytes+1))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	if len(body) > maxJSONDocumentBodyBytes {
		writeAPIErrorCode(w, http.StatusRequestEntityTooLarge, apiErrRequestBodyTooLarge)
		return
	}

	var req postCellCatalogEventRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_catalog_event", "invalid catalog event", nil)
		return
	}

	eventKey := strings.TrimSpace(req.EventKey)
	eventType := strings.TrimSpace(req.EventType)
	payload := bytes.TrimSpace(req.Payload)
	if eventKey == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_event_key", "event_key is required", nil)
		return
	}

	if eventType == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_event_type", "event_type is required", nil)
		return
	}

	if len(payload) == 0 {
		writeAPIError(w, http.StatusBadRequest, "missing_payload", "payload is required", nil)
		return
	}

	if _, err := cell.CatalogEventFromRecord(dal.CatalogEventRecord{
		SourceCell: sourceCell,
		EventType:  eventType,
		Payload:    payload,
	}); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_catalog_event", "invalid catalog event", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	rec, created, err := s.catalogEvents.Record(ctx, sourceCell, eventKey, eventType, payload)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusBadRequest, "invalid_catalog_event", "invalid catalog event", nil)
			return
		}

		s.logger.Error("Record cell catalog event failed: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(postCellCatalogEventResponse{
		ID:         rec.ID,
		SourceCell: rec.SourceCell,
		EventKey:   rec.EventKey,
		EventType:  rec.EventType,
		Status:     rec.Status,
		Created:    created,
	})
}
