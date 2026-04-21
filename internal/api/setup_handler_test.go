package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestSetupHandlers_endToEndThroughHandler(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	t.Run("status_before", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/setup/status", nil)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var st setupStatusResponse
		if err := json.NewDecoder(rec.Body).Decode(&st); err != nil {
			t.Fatal(err)
		}

		if st.SetupComplete {
			t.Fatal("expected incomplete")
		}
	})

	var apiToken string
	t.Run("complete", func(t *testing.T) {
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
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out setupCompleteResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if out.APIToken == "" || out.Username != "root" {
			t.Fatalf("bad response: %+v", out)
		}

		apiToken = out.APIToken
	})

	t.Run("complete_idempotent_conflict", func(t *testing.T) {
		body := map[string]string{
			"bootstrap_token": "sixteenchars----",
			"admin_username":  "other",
			"admin_password":  "longenough2",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusConflict {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("jobs_with_token", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
		req.Header.Set("Authorization", "Bearer "+apiToken)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("jobs_without_token_rejected", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d", rec.Code)
		}
	})
}

func TestPostSetupComplete_invalidBootstrap(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	body := map[string]string{
		"bootstrap_token": "wrong-------------",
		"admin_username":  "root",
		"admin_password":  "longenough",
	}

	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostSetupComplete_bodyTooLarge(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	large := bytes.Repeat([]byte("a"), maxSetupCompleteBodyBytes+1)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(large))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code=%d want 413", rec.Code)
	}
}

func TestPostSetupComplete_invalidUsername(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	body := map[string]string{
		"bootstrap_token": "sixteenchars----",
		"admin_username":  "bad\nname",
		"admin_password":  "longenough",
	}

	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostSetupComplete_shortPassword(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	body := map[string]string{
		"bootstrap_token": "sixteenchars----",
		"admin_username":  "root",
		"admin_password":  "short",
	}

	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestPostSetupComplete_wrongContentType(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "text/plain")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestPostSetupComplete_jsonCharsetContentTypeAccepted(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	body := map[string]string{
		"bootstrap_token": "sixteenchars----",
		"admin_username":  "root",
		"admin_password":  "longenough",
	}

	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}
