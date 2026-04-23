package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"

	"golang.org/x/crypto/bcrypt"
)

func TestChangePassword_endToEnd(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
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

	// Create a second non-admin user
	var regularUserID int64
	var regularToken string

	t.Run("create_regular_user", func(t *testing.T) {
		hash, _ := bcrypt.GenerateFromPassword([]byte("password123"), bcrypt.DefaultCost)
		res, err := db.Exec("INSERT INTO local_users (username, password_hash, enabled) VALUES (?, ?, ?)", "regular", string(hash), true)

		if err != nil {
			t.Fatalf("failed to insert user: %v", err)
		}

		regularUserID, _ = res.LastInsertId()
		plainToken, _ := randomHexToken(apiTokenRandomBytes)
		tokenHash := hashAPIToken(plainToken)
		_, err = db.Exec("INSERT INTO api_tokens (local_user_id, token_hash, label) VALUES (?, ?, ?)", regularUserID, tokenHash, "regular-token")
		if err != nil {
			t.Fatalf("failed to insert token: %v", err)
		}

		regularToken = plainToken
		_, err = db.Exec("INSERT INTO role_bindings (local_user_id, namespace_id, role) VALUES (?, ?, ?)", regularUserID, 1, "viewer")
		if err != nil {
			t.Fatalf("failed to insert role binding: %v", err)
		}
	})

	t.Run("self_service_change_password", func(t *testing.T) {
		body := map[string]string{
			"current_password": "longenough",
			"new_password":     "newpassword123",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/users/change-password", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		// Verify password was actually changed
		var hash string
		row := db.QueryRow("SELECT password_hash FROM local_users WHERE id = ?", adminUserID)
		if err := row.Scan(&hash); err != nil {
			t.Fatalf("failed to get password hash: %v", err)
		}

		if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte("newpassword123")); err != nil {
			t.Fatal("password was not updated")
		}
	})

	t.Run("tokens_revoked_after_password_change", func(t *testing.T) {
		// Try to use the old admin token - should fail since all tokens were revoked
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 after token revocation, got %d", rec.Code)
		}
	})

	t.Run("wrong_current_password", func(t *testing.T) {
		body := map[string]string{
			"current_password": "wrongpassword",
			"new_password":     "anotherpassword",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/users/change-password", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+regularToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", rec.Code)
		}
	})

	t.Run("missing_current_password", func(t *testing.T) {
		body := map[string]string{
			"new_password": "anotherpassword",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/users/change-password", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+regularToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("invalid_new_password", func(t *testing.T) {
		body := map[string]string{
			"current_password": "password123",
			"new_password":     "short",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/users/change-password", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+regularToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("admin_reset_other_user_password", func(t *testing.T) {
		// First create a new admin token since the old one was revoked
		var newAdminToken string

		{
			hash, _ := bcrypt.GenerateFromPassword([]byte("newpassword123"), bcrypt.DefaultCost)
			_, err := db.Exec("UPDATE local_users SET password_hash = ? WHERE id = ?", string(hash), adminUserID)
			if err != nil {
				t.Fatalf("failed to update admin password: %v", err)
			}

			plainToken, _ := randomHexToken(apiTokenRandomBytes)
			tokenHash := hashAPIToken(plainToken)
			_, err = db.Exec("INSERT INTO api_tokens (local_user_id, token_hash, label) VALUES (?, ?, ?)", adminUserID, tokenHash, "admin-token-2")
			if err != nil {
				t.Fatalf("failed to insert admin token: %v", err)
			}

			newAdminToken = plainToken
		}

		body := map[string]any{
			"new_password": "resetpassword456",
			"user_id":      regularUserID,
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/users/change-password", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+newAdminToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}

		// Verify regular user's password was changed
		var hash string
		row := db.QueryRow("SELECT password_hash FROM local_users WHERE id = ?", regularUserID)
		if err := row.Scan(&hash); err != nil {
			t.Fatalf("failed to get password hash: %v", err)
		}

		if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte("resetpassword456")); err != nil {
			t.Fatal("password was not updated by admin")
		}

		// Verify regular user's tokens were revoked
		var count int
		row = db.QueryRow("SELECT COUNT(*) FROM api_tokens WHERE local_user_id = ?", regularUserID)
		if err := row.Scan(&count); err != nil {
			t.Fatalf("failed to count tokens: %v", err)
		}

		if count != 0 {
			t.Fatalf("expected 0 tokens after admin reset, got %d", count)
		}
	})

	t.Run("non_admin_cannot_reset_other_user", func(t *testing.T) {
		// Create a new regular token since the old one was revoked
		var newRegularToken string
		{
			hash, _ := bcrypt.GenerateFromPassword([]byte("resetpassword456"), bcrypt.DefaultCost)
			_, err := db.Exec("UPDATE local_users SET password_hash = ? WHERE id = ?", string(hash), regularUserID)
			if err != nil {
				t.Fatalf("failed to update regular password: %v", err)
			}

			plainToken, _ := randomHexToken(apiTokenRandomBytes)
			tokenHash := hashAPIToken(plainToken)
			_, err = db.Exec("INSERT INTO api_tokens (local_user_id, token_hash, label) VALUES (?, ?, ?)", regularUserID, tokenHash, "regular-token-2")
			if err != nil {
				t.Fatalf("failed to insert regular token: %v", err)
			}

			newRegularToken = plainToken
		}

		body := map[string]any{
			"new_password": "hackerpassword",
			"user_id":      adminUserID,
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/users/change-password", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+newRegularToken)
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403, got %d", rec.Code)
		}
	})
}
