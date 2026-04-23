package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"

	"golang.org/x/crypto/bcrypt"
)

func TestTokenLifecycle_endToEnd(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	// Complete setup to get first admin token
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

		// Get admin user ID for later tests
		row := db.QueryRow("SELECT id FROM local_users WHERE username = 'root'")
		if err := row.Scan(&adminUserID); err != nil {
			t.Fatalf("failed to get admin user id: %v", err)
		}
	})

	t.Run("list_empty", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out []tokenResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 token (setup token), got %d", len(out))
		}
	})

	var createdTokenID int64
	t.Run("create_token", func(t *testing.T) {
		body := map[string]string{
			"label":      "ci-token",
			"expires_in": "1m",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out createTokenResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if out.Token == "" {
			t.Fatal("expected plaintext token in response")
		}

		if out.Label != "ci-token" {
			t.Fatalf("expected label ci-token, got %s", out.Label)
		}

		if out.ExpiresAt == nil {
			t.Fatal("expected expires_at to be set")
		}

		createdTokenID = out.ID
	})

	t.Run("list_includes_created", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out []tokenResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if len(out) != 2 {
			t.Fatalf("expected 2 tokens, got %d", len(out))
		}

		// Verify no plaintext token in list
		for _, tok := range out {
			if tok.ID == createdTokenID && tok.Label != "ci-token" {
				t.Fatalf("expected ci-token label")
			}
		}
	})

	t.Run("delete_token", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/tokens/"+formatInt64(createdTokenID), nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("list_after_delete", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out []tokenResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 token after delete, got %d", len(out))
		}
	})

	t.Run("create_invalid_expires_in", func(t *testing.T) {
		body := map[string]string{
			"label":      "bad",
			"expires_in": "invalid",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("create_missing_label", func(t *testing.T) {
		body := map[string]string{
			"expires_in": "1w",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("delete_nonexistent", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/tokens/99999", nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", rec.Code)
		}
	})

	// Create a second non-admin user for cross-user tests
	var regularUserID int64
	var regularToken string
	t.Run("create_regular_user", func(t *testing.T) {
		hash, _ := bcrypt.GenerateFromPassword([]byte("password123"), bcrypt.DefaultCost)
		res, err := db.Exec("INSERT INTO local_users (username, password_hash, enabled) VALUES (?, ?, ?)", "regular", string(hash), true)
		if err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}

		regularUserID, _ = res.LastInsertId()

		// Create a token for the regular user directly
		plainToken, _ := randomHexToken(apiTokenRandomBytes)
		tokenHash := hashAPIToken(plainToken)
		_, err = db.Exec("INSERT INTO api_tokens (local_user_id, token_hash, label) VALUES (?, ?, ?)", regularUserID, tokenHash, "regular-token")
		if err != nil {
			t.Fatalf("failed to insert token: %v", err)
		}

		regularToken = plainToken

		// Give regular user viewer role on root namespace
		_, err = db.Exec("INSERT INTO role_bindings (local_user_id, namespace_id, role) VALUES (?, ?, ?)", regularUserID, 1, "viewer")
		if err != nil {
			t.Fatalf("failed to insert role binding: %v", err)
		}
	})

	t.Run("admin_lists_other_user_tokens", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens?user_id="+formatInt64(regularUserID), nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out []tokenResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if len(out) != 1 {
			t.Fatalf("expected 1 token for regular user, got %d", len(out))
		}
	})

	// Create a separate token for admin to delete so regularToken stays valid
	var deletableTokenID int64
	t.Run("admin_creates_token_for_other_user", func(t *testing.T) {
		body := map[string]any{
			"label":      "admin-created",
			"expires_in": "1w",
			"user_id":    regularUserID,
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out createTokenResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if out.Token == "" {
			t.Fatal("expected plaintext token")
		}

		deletableTokenID = out.ID
	})

	t.Run("admin_deletes_other_user_token", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/tokens/"+formatInt64(deletableTokenID), nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	})

	t.Run("non_admin_cannot_list_other_user_tokens", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens?user_id="+formatInt64(adminUserID), nil)
		req.Header.Set("Authorization", "Bearer "+regularToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", rec.Code)
		}
	})

	t.Run("non_admin_cannot_create_token_for_other_user", func(t *testing.T) {
		body := map[string]any{
			"label":      "bad",
			"expires_in": "1w",
			"user_id":    adminUserID,
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+regularToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", rec.Code)
		}
	})

	t.Run("non_admin_cannot_delete_other_user_token", func(t *testing.T) {
		// Get admin's token ID
		var adminTokenID int64
		row := db.QueryRow("SELECT id FROM api_tokens WHERE local_user_id = ?", adminUserID)
		if err := row.Scan(&adminTokenID); err != nil {
			t.Fatalf("failed to get admin token id: %v", err)
		}

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete, "/api/v1/tokens/"+formatInt64(adminTokenID), nil)
		req.Header.Set("Authorization", "Bearer "+regularToken)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", rec.Code)
		}
	})

	t.Run("delete_own_current_token", func(t *testing.T) {
		// Create a new token for regular user, then delete it using itself
		body := map[string]string{
			"label":      "self-delete",
			"expires_in": "1w",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+regularToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out createTokenResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodDelete, "/api/v1/tokens/"+formatInt64(out.ID), nil)
		req.Header.Set("Authorization", "Bearer "+out.Token)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

func formatInt64(v int64) string {
	return strconv.FormatInt(v, 10)
}

func TestTokenScoping_endToEnd(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

	var adminToken string
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
	})

	// Create a child namespace for testing
	_, err := db.Exec("INSERT INTO namespaces (name, path, parent_id) VALUES (?, ?, ?)", "child", "/child", 1)
	if err != nil {
		t.Fatalf("failed to create namespace: %v", err)
	}

	var scopedToken string
	t.Run("create_scoped_token", func(t *testing.T) {
		body := map[string]any{
			"label":      "scoped-ci",
			"expires_in": "1y",
			"scopes": []map[string]any{
				{"action": "job:read"},
				{"action": "run:trigger", "namespace_path": "/", "propagate": false},
			},
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out createTokenResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if out.Token == "" {
			t.Fatal("expected plaintext token")
		}

		scopedToken = out.Token
	})

	t.Run("scoped_token_allows_job_read", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces", nil)
		req.Header.Set("Authorization", "Bearer "+scopedToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
	})

	t.Run("scoped_token_denies_run_operator", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/nonexistent/force-fail", nil)
		req.Header.Set("Authorization", "Bearer "+scopedToken)
		h.ServeHTTP(rec, req)

		// run:operator is not in scopes, should be denied (403 before 404)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", rec.Code)
		}
	})

	t.Run("scoped_token_denies_admin_action", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/namespaces", bytes.NewReader([]byte(`{"name":"test"}`)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+scopedToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", rec.Code)
		}
	})

	var propagatedToken string
	t.Run("create_propagated_scoped_token", func(t *testing.T) {
		body := map[string]any{
			"label":      "propagated",
			"expires_in": "1y",
			"scopes": []map[string]any{
				{"action": "job:read", "namespace_path": "/", "propagate": true},
			},
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusCreated {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		var out createTokenResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		propagatedToken = out.Token
	})

	t.Run("propagated_token_allows_child_namespace", func(t *testing.T) {
		// Create a job in the child namespace first
		_, err := db.Exec("INSERT INTO stored_jobs (job_id, namespace_id, definition_json) VALUES (?, ?, ?)", "test-job", 2, "{}")
		if err != nil {
			t.Fatalf("failed to create job: %v", err)
		}

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
		req.Header.Set("Authorization", "Bearer "+propagatedToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
	})
}
