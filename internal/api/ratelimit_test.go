package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"vectis/internal/api/authz"
	"vectis/internal/api/ratelimit"
	"vectis/internal/cache"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestRateLimiting_endToEnd(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	cacheService := cache.NewMemoryService()
	s.SetCacheService(cacheService)
	s.SetRateLimiter(ratelimit.NewCacheRateLimiter(cacheService))
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
		for i := range 19 {
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
		// Auth endpoints have burst=5 and each route has its own bucket.
		// Exhaust the bucket for /api/v1/setup/status with 5 requests.
		for i := range 5 {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/v1/setup/status", nil)
			h.ServeHTTP(rec, req)
			if rec.Code == http.StatusTooManyRequests {
				t.Fatalf("request %d should not be rate limited yet, got 429", i+1)
			}
		}

		// 6th request to the same route should be rate limited.
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
		for i := range 20 {
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

func TestRateLimitKey_ignoresSessionCookie(t *testing.T) {
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	rule := ratelimit.Rule{RefillRate: time.Second, BurstSize: 1}
	spec := routeSpec{
		Pattern:   "POST /api/v1/login",
		Auth:      routeAuthPolicy{Public: true},
		RateLimit: rule,
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	req.Pattern = "POST /api/v1/login"
	baseKey := s.rateLimitKey(req, spec)

	req = httptest.NewRequest(http.MethodPost, "/api/v1/login", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	req.Pattern = "POST /api/v1/login"
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "attacker-controlled-cookie"})
	cookieKey := s.rateLimitKey(req, spec)

	if cookieKey != baseKey {
		t.Fatalf("session cookie should not change rate-limit key: got %q want %q", cookieKey, baseKey)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/login", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	req.Pattern = "POST /api/v1/login"
	req.Header.Set("Authorization", "Bearer explicit-token")
	bearerKey := s.rateLimitKey(req, spec)

	if bearerKey != baseKey {
		t.Fatalf("public auth routes should ignore bearer token for rate-limit key: got %q want %q", bearerKey, baseKey)
	}

	protectedSpec := routeSpec{
		Pattern:   "GET /api/v1/tokens",
		Auth:      routeAuthPolicy{Action: authz.ActionAPI},
		RateLimit: rule,
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	req.Pattern = "GET /api/v1/tokens"
	protectedIPKey := s.rateLimitKey(req, protectedSpec)

	req = httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
	req.RemoteAddr = "203.0.113.10:1234"
	req.Pattern = "GET /api/v1/tokens"
	req.Header.Set("Authorization", "Bearer explicit-token")
	protectedBearerKey := s.rateLimitKey(req, protectedSpec)

	if protectedBearerKey == protectedIPKey {
		t.Fatal("bearer token should still scope rate-limit key")
	}
}

func TestRateLimiting_CacheBackendSharedAcrossAPIServers(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s1 := NewAPIServer(mocks.NewMockLogger(), db)
	s1.SetQueueClient(mocks.NewMockQueueService())
	cache1 := cache.NewSQLService(db, "sqlite3")
	s1.SetCacheService(cache1)
	s1.SetRateLimiter(ratelimit.NewCacheRateLimiter(cache1))
	h1 := s1.Handler()

	s2 := NewAPIServer(mocks.NewMockLogger(), db)
	s2.SetQueueClient(mocks.NewMockQueueService())
	cache2 := cache.NewSQLService(db, "sqlite3")
	s2.SetCacheService(cache2)
	s2.SetRateLimiter(ratelimit.NewCacheRateLimiter(cache2))
	h2 := s2.Handler()

	body := map[string]string{
		"bootstrap_token": "sixteenchars----",
		"admin_username":  "root",
		"admin_password":  "longenough",
	}

	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h1.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("setup failed: code=%d body=%s", rec.Code, rec.Body.String())
	}

	var setupOut setupCompleteResponse
	if err := json.NewDecoder(rec.Body).Decode(&setupOut); err != nil {
		t.Fatal(err)
	}

	for i := range 20 {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
		req.Header.Set("Authorization", "Bearer "+setupOut.APIToken)

		if i%2 == 0 {
			h1.ServeHTTP(rec, req)
		} else {
			h2.ServeHTTP(rec, req)
		}

		if rec.Code == http.StatusTooManyRequests {
			t.Fatalf("request %d should not be rate limited yet, got 429", i+1)
		}
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/tokens", nil)
	req.Header.Set("Authorization", "Bearer "+setupOut.APIToken)
	h2.ServeHTTP(rec, req)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected shared SQL bucket to return 429, got %d body=%s", rec.Code, rec.Body.String())
	}
}
