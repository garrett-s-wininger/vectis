package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestListAuditEventsFiltersAndResponds(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	base := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	if err := s.authRepo.InsertAuditEvents(context.Background(), []*dal.AuditEventRecord{
		{
			Type:          "auth.success",
			ActorID:       sql.NullInt64{Int64: 1, Valid: true},
			TargetID:      sql.NullInt64{Int64: 2, Valid: true},
			Metadata:      []byte(`{"username":"root"}`),
			IPAddress:     "127.0.0.1",
			CorrelationID: "corr-1",
			CreatedAt:     sql.NullTime{Time: base, Valid: true},
		},
		{
			Type:          "auth.failure",
			ActorID:       sql.NullInt64{Int64: 1, Valid: true},
			CorrelationID: "corr-2",
			CreatedAt:     sql.NullTime{Time: base.Add(time.Minute), Valid: true},
		},
	}); err != nil {
		t.Fatalf("InsertAuditEvents: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/audit/events?event_type=auth.success&actor_id=1&target_id=2&correlation_id=corr-1&since=2026-06-29T11:00:00Z&until=2026-06-29T13:00:00Z&limit=5", nil)
	s.ListAuditEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}

	var out auditEventListResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if out.Limit != 5 {
		t.Fatalf("limit=%d", out.Limit)
	}

	if len(out.Events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(out.Events))
	}

	event := out.Events[0]
	if event.ID <= 0 {
		t.Fatalf("id=%d", event.ID)
	}

	if event.EventType != "auth.success" {
		t.Fatalf("event_type=%s", event.EventType)
	}

	if event.ActorID == nil || *event.ActorID != 1 {
		t.Fatalf("actor_id=%v", event.ActorID)
	}

	if event.TargetID == nil || *event.TargetID != 2 {
		t.Fatalf("target_id=%v", event.TargetID)
	}

	if string(event.Metadata) != `{"username":"root"}` {
		t.Fatalf("metadata=%s", string(event.Metadata))
	}

	if event.IPAddress != "127.0.0.1" {
		t.Fatalf("ip=%s", event.IPAddress)
	}

	if event.CorrelationID != "corr-1" {
		t.Fatalf("correlation_id=%s", event.CorrelationID)
	}

	if event.CreatedAt != "2026-06-29T12:00:00Z" {
		t.Fatalf("created_at=%s", event.CreatedAt)
	}
}

func TestListAuditEventsRejectsInvalidLimit(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/audit/events?limit=1001", nil)
	s.ListAuditEvents(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}
