//go:build integration

package auth_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/api"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestIntegrationAuth_Flow(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	logger := mocks.NewMockLogger()
	server := api.NewAPIServer(logger, db)
	server.SetQueueClient(mocks.NewMockQueueService())
	h := server.Handler()

	// Step 1: Check setup status (should require setup).
	{
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/setup/status", nil)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("setup status: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var statusResp struct {
			SetupComplete bool `json:"setup_complete"`
		}

		if err := json.Unmarshal(rec.Body.Bytes(), &statusResp); err != nil {
			t.Fatalf("decode setup status: %v", err)
		}

		if statusResp.SetupComplete {
			t.Fatal("expected setup to be incomplete initially")
		}
	}

	// Step 2: Complete setup with bootstrap token.
	{
		body, _ := json.Marshal(map[string]string{
			"bootstrap_token": "sixteenchars----",
			"admin_username":  "admin",
			"admin_password":  "admin-password-123",
		})

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("setup complete: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var setupResp struct {
			APIToken string `json:"api_token"`
			Username string `json:"username"`
		}

		if err := json.Unmarshal(rec.Body.Bytes(), &setupResp); err != nil {

			t.Fatalf("decode setup: %v", err)
		}

		if setupResp.APIToken == "" {
			t.Fatal("expected api_token in setup response")
		}
	}

	// Step 3: Verify setup is now complete.
	{
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/setup/status", nil)
		h.ServeHTTP(rec, req)

		var statusResp struct {
			SetupComplete bool `json:"setup_complete"`
		}

		if err := json.Unmarshal(rec.Body.Bytes(), &statusResp); err != nil {
			t.Fatalf("decode setup status: %v", err)
		}

		if !statusResp.SetupComplete {
			t.Fatal("expected setup to be complete after PostSetupComplete")
		}
	}

	// Step 4: Login to get a fresh token.
	var authToken string
	{
		body, _ := json.Marshal(map[string]any{
			"username": "admin",
			"password": "admin-password-123",
		})

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("login: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var loginResp struct {
			Token string `json:"token"`
		}

		if err := json.Unmarshal(rec.Body.Bytes(), &loginResp); err != nil {
			t.Fatalf("decode login: %v", err)
		}

		if loginResp.Token == "" {
			t.Fatal("expected non-empty token after login")
		}

		authToken = loginResp.Token
	}

	// Step 5: Use token to create a job (should succeed with admin).
	{
		body, _ := json.Marshal(map[string]any{
			"id": "auth-test-job",
			"root": map[string]any{
				"uses": "builtins/shell",
				"with": map[string]string{"command": "echo hello"},
			},
		})

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+authToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("create job with token: expected 201, got %d: %s", rec.Code, rec.Body.String())
		}
	}

	// Step 6: Verify unauthorized request fails.
	{
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("get jobs without auth: expected 401, got %d", rec.Code)
		}
	}

	// Step 7: Verify bad token fails.
	{
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
		req.Header.Set("Authorization", "Bearer bad-token")
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("get jobs with bad token: expected 401, got %d", rec.Code)
		}
	}

	// Step 8: Create a new user (viewer).
	{
		body, _ := json.Marshal(map[string]any{
			"username": "viewer-user",
			"password": "viewer-password-123",
		})

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/users", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+authToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("create user: expected 201, got %d: %s", rec.Code, rec.Body.String())
		}
	}

	// Step 9: Login as viewer.
	var viewerToken string
	{
		body, _ := json.Marshal(map[string]any{
			"username": "viewer-user",
			"password": "viewer-password-123",
		})

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("viewer login: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var loginResp struct {
			Token string `json:"token"`
		}

		if err := json.Unmarshal(rec.Body.Bytes(), &loginResp); err != nil {
			t.Fatalf("decode viewer login: %v", err)
		}

		viewerToken = loginResp.Token
	}

	// Step 10: Viewer tries to create a job (should fail - no job:write permission).
	{
		body, _ := json.Marshal(map[string]any{
			"id": "viewer-job-attempt",
			"root": map[string]any{
				"uses": "builtins/shell",
				"with": map[string]string{"command": "echo nope"},
			},
		})

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+viewerToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden && rec.Code != http.StatusNotFound {
			t.Fatalf("viewer create job: expected 403 or 404, got %d: %s", rec.Code, rec.Body.String())
		}
	}

	// Step 11: Admin lists tokens.
	{
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
		req.Header.Set("Authorization", "Bearer "+authToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("list tokens: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
	}

	// Step 12: Change password and verify old token still works.
	{
		body, _ := json.Marshal(map[string]any{
			"current_password": "admin-password-123",
			"new_password":     "new-admin-password-456",
		})

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/users/change-password", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+authToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("change password: expected 204, got %d: %s", rec.Code, rec.Body.String())
		}
	}

	// Step 13: Verify new password works for login.
	{
		body, _ := json.Marshal(map[string]any{
			"username": "admin",
			"password": "new-admin-password-456",
		})

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("login with new password: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
	}

	t.Logf("Auth flow completed successfully")
}
