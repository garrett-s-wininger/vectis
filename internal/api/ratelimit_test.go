package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/api/ratelimit"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestRateLimiting_endToEnd(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	lim := ratelimit.NewMemoryRateLimiter()
	defer lim.Stop()
	s.SetRateLimiter(lim)
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

	t.Run("different_token_has_own_bucket", func(t *testing.T) {
		// Create a second token first, before we exhaust any buckets
		body := map[string]string{"label": "second", "expires_in": "1y"}
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
		secondToken := out.Token

		// Exhaust admin token's token bucket
		for i := 0; i < 19; i++ {
			body := map[string]string{"label": "test", "expires_in": "1y"}
			b, _ := json.Marshal(body)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer "+adminToken)
			h.ServeHTTP(rec, req)
			if rec.Code == http.StatusTooManyRequests {
				t.Fatalf("request %d should not be rate limited yet, got 429", i+1)
			}
		}

		// 20th request for adminToken should be rate limited
		body = map[string]string{"label": "test", "expires_in": "1y"}
		b, _ = json.Marshal(body)
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+adminToken)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("expected 429, got %d", rec.Code)
		}

		// Second token should not be rate limited - it has its own bucket
		body = map[string]string{"label": "third", "expires_in": "1y"}
		b, _ = json.Marshal(body)
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+secondToken)
		h.ServeHTTP(rec, req)
		if rec.Code == http.StatusTooManyRequests {
			t.Fatal("second token should not be rate limited")
		}
	})

	t.Run("auth_endpoint_rate_limited", func(t *testing.T) {
		// Auth endpoints have burst=5. The setup request consumed 1 token,
		// so 4 more should succeed, then the 5th should be rate limited.
		for i := 0; i < 4; i++ {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/setup/status", nil)
			h.ServeHTTP(rec, req)
			if rec.Code == http.StatusTooManyRequests {
				t.Fatalf("request %d should not be rate limited yet, got 429", i+1)
			}
		}

		// 5th request should be rate limited (setup + 4 = 5 consumed, burst=5)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/setup/status", nil)
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusTooManyRequests {
			t.Fatalf("expected 429, got %d", rec.Code)
		}
		if rec.Header().Get("Retry-After") == "" {
			t.Fatal("expected Retry-After header")
		}
	})

	t.Run("general_endpoint_higher_limit", func(t *testing.T) {
		// General endpoints have higher limits (burst=150)
		// Make sure we can make many requests
		for i := 0; i < 20; i++ {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces", nil)
			req.Header.Set("Authorization", "Bearer "+adminToken)
			h.ServeHTTP(rec, req)
			if rec.Code == http.StatusTooManyRequests {
				t.Fatalf("general endpoint request %d should not be rate limited, got 429", i+1)
			}
		}
	})
}
