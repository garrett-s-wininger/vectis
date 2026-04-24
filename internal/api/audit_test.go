package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/api/audit"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestAuditLogging_endToEnd(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	// Create a test auditor that captures events synchronously
	var capturedEvents []audit.Event
	testAuditor := &testAuditCapturer{events: &capturedEvents}
	s.SetAuditor(testAuditor)

	h := s.Handler()

	var adminToken string
	var adminUserID int64
	t.Run("setup", func(t *testing.T) {
		body := map[string]string{
			"bootstrap_token": "sixteenchars----",
			"admin_username":  "root",
			"admin_password":  "longenough",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("setup failed: code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out setupCompleteResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		adminToken = out.APIToken
		row := db.QueryRow("SELECT id FROM local_users WHERE username = 'root'")
		if err := row.Scan(&adminUserID); err != nil {
			t.Fatalf("failed to get admin user id: %v", err)
		}
	})

	t.Run("token_created_event", func(t *testing.T) {
		capturedEvents = nil
		body := map[string]string{"label": "test-token", "expires_in": "1y"}
		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		if len(capturedEvents) == 0 {
			t.Fatal("expected audit events, got none")
		}

		found := false
		for _, event := range capturedEvents {
			if event.Type == audit.EventTokenCreated {
				found = true
				if event.ActorID != adminUserID {
					t.Fatalf("expected actor_id %d, got %d", adminUserID, event.ActorID)
				}

				break
			}
		}

		if !found {
			t.Fatalf("expected token.created event, got: %v", capturedEvents)
		}
	})

	t.Run("auth_success_event", func(t *testing.T) {
		capturedEvents = nil
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		found := false
		for _, event := range capturedEvents {
			if event.Type == audit.EventAuthSuccess {
				found = true
				if event.ActorID != adminUserID {
					t.Fatalf("expected actor_id %d, got %d", adminUserID, event.ActorID)
				}

				break
			}
		}

		if !found {
			t.Fatalf("expected auth.success event, got: %v", capturedEvents)
		}
	})
}

type testAuditCapturer struct {
	events *[]audit.Event
}

func (t *testAuditCapturer) Log(ctx context.Context, event audit.Event) error {
	*t.events = append(*t.events, event)
	return nil
}
