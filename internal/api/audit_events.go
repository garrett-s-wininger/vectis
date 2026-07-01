package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"vectis/internal/dal"
)

const (
	defaultAuditEventListLimit = 100
	maxAuditEventListLimit     = 1000
)

type auditEventListResponse struct {
	Events []auditEventResponse `json:"events"`
	Limit  int                  `json:"limit"`
}

type auditEventResponse struct {
	ID            int64           `json:"id"`
	EventType     string          `json:"event_type"`
	ActorID       *int64          `json:"actor_id,omitempty"`
	TargetID      *int64          `json:"target_id,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
	IPAddress     string          `json:"ip_address,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	CreatedAt     string          `json:"created_at,omitempty"`
}

func (s *APIServer) ListAuditEvents(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuthRepo(w) {
		return
	}

	filter, ok := parseAuditEventListFilter(w, r)
	if !ok {
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	events, err := s.authRepo.ListAuditEvents(ctx, filter)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Failed to list audit events: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}

	resp := auditEventListResponse{
		Events: make([]auditEventResponse, 0, len(events)),
		Limit:  filter.Limit,
	}

	for _, event := range events {
		resp.Events = append(resp.Events, auditEventRecordResponse(event))
	}

	writeJSON(w, http.StatusOK, resp)
}

func parseAuditEventListFilter(w http.ResponseWriter, r *http.Request) (dal.AuditEventListFilter, bool) {
	filter := dal.AuditEventListFilter{
		EventType:     strings.TrimSpace(r.URL.Query().Get("event_type")),
		CorrelationID: strings.TrimSpace(r.URL.Query().Get("correlation_id")),
		Limit:         defaultAuditEventListLimit,
	}

	if limitRaw := strings.TrimSpace(r.URL.Query().Get("limit")); limitRaw != "" {
		limit, err := strconv.Atoi(limitRaw)
		if err != nil || limit < 1 || limit > maxAuditEventListLimit {
			writeAPIError(w, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 1000", nil)
			return filter, false
		}

		filter.Limit = limit
	}

	actorID, ok := parseAuditEventInt64Filter(w, r.URL.Query().Get("actor_id"), "actor_id", "invalid_actor_id")
	if !ok {
		return filter, false
	}

	if actorID.Valid {
		filter.ActorID = actorID
	}

	targetID, ok := parseAuditEventInt64Filter(w, r.URL.Query().Get("target_id"), "target_id", "invalid_target_id")
	if !ok {
		return filter, false
	}

	if targetID.Valid {
		filter.TargetID = targetID
	}

	since, ok := parseAuditEventTimeFilter(w, r.URL.Query().Get("since"), "since", "invalid_since")
	if !ok {
		return filter, false
	}

	filter.Since = since
	until, ok := parseAuditEventTimeFilter(w, r.URL.Query().Get("until"), "until", "invalid_until")
	if !ok {
		return filter, false
	}

	filter.Until = until
	if filter.Since != nil && filter.Until != nil && filter.Since.After(*filter.Until) {
		writeAPIError(w, http.StatusBadRequest, "invalid_time_range", "since must be before or equal to until", nil)
		return filter, false
	}

	return filter, true
}

func parseAuditEventInt64Filter(w http.ResponseWriter, raw, name, code string) (sql.NullInt64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return sql.NullInt64{}, true
	}

	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n <= 0 {
		writeAPIError(w, http.StatusBadRequest, code, name+" must be a positive integer", nil)
		return sql.NullInt64{}, false
	}

	return sql.NullInt64{Int64: n, Valid: true}, true
}

func parseAuditEventTimeFilter(w http.ResponseWriter, raw, name, code string) (*time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, true
	}

	t, err := parseRunSince(raw)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, code, name+" must be an RFC3339 timestamp or YYYY-MM-DD date", nil)
		return nil, false
	}

	return &t, true
}

func auditEventRecordResponse(event *dal.AuditEventRecord) auditEventResponse {
	if event == nil {
		return auditEventResponse{}
	}

	resp := auditEventResponse{
		ID:            event.ID,
		EventType:     event.Type,
		IPAddress:     event.IPAddress,
		CorrelationID: event.CorrelationID,
	}

	if event.ActorID.Valid {
		resp.ActorID = &event.ActorID.Int64
	}

	if event.TargetID.Valid {
		resp.TargetID = &event.TargetID.Int64
	}

	if len(event.Metadata) > 0 && json.Valid(event.Metadata) {
		resp.Metadata = json.RawMessage(event.Metadata)
	}

	if event.CreatedAt.Valid {
		resp.CreatedAt = event.CreatedAt.Time.UTC().Format(time.RFC3339Nano)
	}

	return resp
}
