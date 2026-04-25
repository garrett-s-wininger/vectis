package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestLogin_endToEnd(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	// Complete setup first
	setupBody := map[string]string{
		"bootstrap_token": "sixteenchars----",
		"admin_username":  "root",
		"admin_password":  "longenough",
	}

	b, _ := json.Marshal(setupBody)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("setup failed code=%d body=%s", rec.Code, rec.Body.String())
	}

	t.Run("success", func(t *testing.T) {
		body := map[string]string{
			"username": "root",
			"password": "longenough",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out loginResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if out.Token == "" || out.UserID == 0 || out.ExpiresAt == nil {
			t.Fatalf("bad response: %+v", out)
		}

		// Token should work on a protected route
		rec2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
		req2.Header.Set("Authorization", "Bearer "+out.Token)
		h.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusOK {
			t.Fatalf("token rejected code=%d", rec2.Code)
		}
	})

	t.Run("wrong_password", func(t *testing.T) {
		body := map[string]string{
			"username": "root",
			"password": "wrongpass----",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("unknown_user", func(t *testing.T) {
		body := map[string]string{
			"username": "nobody",
			"password": "longenough",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("disabled_user", func(t *testing.T) {
		ctx := context.Background()
		if err := s.authRepo.UpdateLocalUserEnabled(ctx, 1, false); err != nil {
			t.Fatalf("failed to disable user: %v", err)
		}
		t.Cleanup(func() {
			_ = s.authRepo.UpdateLocalUserEnabled(ctx, 1, true)
		})

		body := map[string]string{
			"username": "root",
			"password": "longenough",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("missing_fields", func(t *testing.T) {
		body := map[string]string{
			"username": "root",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("wrong_content_type", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", strings.NewReader("{}"))
		req.Header.Set("Content-Type", "text/plain")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnsupportedMediaType {
			t.Fatalf("code=%d", rec.Code)
		}
	})

	t.Run("body_too_large", func(t *testing.T) {
		large := bytes.Repeat([]byte("a"), maxLoginBodyBytes+1)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(large))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("code=%d want 413", rec.Code)
		}
	})
}

func TestLogin_setupNotComplete(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	body := map[string]string{
		"username": "root",
		"password": "longenough",
	}

	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}

	var errResp authAPIError
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatal(err)
	}

	if errResp.Error != AuthJSONSetupRequired {
		t.Fatalf("expected setup_required, got %q", errResp.Error)
	}
}

func TestLogin_authDisabled(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	body := map[string]string{
		"username": "root",
		"password": "longenough",
	}

	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}
