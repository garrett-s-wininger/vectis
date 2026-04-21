package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"

	"golang.org/x/crypto/bcrypt"
)

// denyExceptSetup authorizes only setup routes. Used to verify middleware calls
// Authorizer.Allow after Bearer validation; if Allow were skipped, requests would incorrectly succeed.
type denyExceptSetup struct{}

func (denyExceptSetup) Allow(_ context.Context, _ *authn.Principal, a authz.Action, _ authz.Resource) bool {
	return a == authz.ActionSetupStatus || a == authz.ActionSetupComplete
}

func wrapTestAccessHandler(s *APIServer, policy routeAuthPolicy, fn http.HandlerFunc) http.Handler {
	return s.accessControlledHandler(policy, http.HandlerFunc(fn))
}

func TestAccessControlMiddleware_authDisabled(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	var hit bool
	h := wrapTestAccessHandler(s, routeAuthPolicy{}, func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusNoContent)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent || !hit {
		t.Fatalf("code=%d hit=%v", rec.Code, hit)
	}
}

func TestAccessControlMiddleware_setupRequiredBlocksJobs(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	var hit bool
	h := wrapTestAccessHandler(s, routeAuthPolicy{Action: authz.ActionJobRead}, func(w http.ResponseWriter, r *http.Request) {
		hit = true
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	h.ServeHTTP(rec, req)

	if hit {
		t.Fatal("handler should not run")
	}

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d", rec.Code)
	}

	var body authAPIError
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}

	if body.Error != AuthJSONSetupRequired {
		t.Fatalf("body=%+v", body)
	}
}

func TestAccessControlMiddleware_setupAllowsSetupStatus(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	var hit bool
	h := wrapTestAccessHandler(s, routeAuthPolicy{Action: authz.ActionSetupStatus}, func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusNoContent)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/setup/status", nil)
	h.ServeHTTP(rec, req)

	if !hit || rec.Code != http.StatusNoContent {
		t.Fatalf("hit=%v code=%d", hit, rec.Code)
	}
}

func TestAccessControlMiddleware_requiresBearerAfterSetup(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	ctx := context.Background()
	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	plain := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd"
	th := hashAPIToken(plain)
	if _, err := s.authRepo.CompleteInitialSetup(ctx, "op", string(passHash), th, "t"); err != nil {
		t.Fatal(err)
	}

	runHandler := func(policy routeAuthPolicy) http.Handler {
		return wrapTestAccessHandler(s, policy, func(w http.ResponseWriter, r *http.Request) {
			p, ok := authn.PrincipalFromContext(r.Context())
			if !ok || p.Username != "op" {
				t.Fatalf("principal missing or wrong: %+v", p)
			}

			w.WriteHeader(http.StatusTeapot)
		})
	}

	t.Run("no_auth", func(t *testing.T) {
		cases := []struct {
			name   string
			policy routeAuthPolicy
			method string
			path   string
		}{
			{name: "read", policy: routeAuthPolicy{Action: authz.ActionJobRead}, method: http.MethodGet, path: "/api/v1/jobs"},
			{name: "write", policy: routeAuthPolicy{Action: authz.ActionJobWrite}, method: http.MethodPost, path: "/api/v1/jobs"},
			{name: "operator", policy: routeAuthPolicy{Action: authz.ActionRunOperator}, method: http.MethodPost, path: "/api/v1/runs/run-1/force-fail"},
			{name: "default_secure", policy: routeAuthPolicy{}, method: http.MethodGet, path: "/api/v1/forgotten"},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(tc.method, tc.path, nil)
				runHandler(tc.policy).ServeHTTP(rec, req)
				if rec.Code != http.StatusUnauthorized {
					t.Fatalf("code=%d", rec.Code)
				}
			})
		}
	})

	t.Run("bad_token", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
		req.Header.Set("Authorization", "Bearer wrong")
		runHandler(routeAuthPolicy{Action: authz.ActionJobRead}).ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d", rec.Code)
		}
	})

	t.Run("oversized_bearer_rejected", func(t *testing.T) {
		longTok := strings.Repeat("a", maxBearerTokenBytes+1)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
		req.Header.Set("Authorization", "Bearer "+longTok)
		runHandler(routeAuthPolicy{Action: authz.ActionJobRead}).ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d", rec.Code)
		}
	})

	t.Run("ok", func(t *testing.T) {
		cases := []struct {
			name   string
			policy routeAuthPolicy
			method string
			path   string
		}{
			{name: "read", policy: routeAuthPolicy{Action: authz.ActionJobRead}, method: http.MethodGet, path: "/api/v1/jobs"},
			{name: "write", policy: routeAuthPolicy{Action: authz.ActionJobWrite}, method: http.MethodPost, path: "/api/v1/jobs"},
			{name: "operator", policy: routeAuthPolicy{Action: authz.ActionRunOperator}, method: http.MethodPost, path: "/api/v1/runs/run-1/force-fail"},
			{name: "default_secure", policy: routeAuthPolicy{}, method: http.MethodGet, path: "/api/v1/forgotten"},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(tc.method, tc.path, nil)
				req.Header.Set("Authorization", "Bearer "+plain)
				runHandler(tc.policy).ServeHTTP(rec, req)
				if rec.Code != http.StatusTeapot {
					t.Fatalf("code=%d", rec.Code)
				}
			})
		}
	})
}

func TestAccessControlMiddleware_authorizerDeniesAfterBearerAuth(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	s.authzOverride = denyExceptSetup{}

	ctx := context.Background()
	passHash, err := bcrypt.GenerateFromPassword([]byte("longenough"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	plain := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd"
	th := hashAPIToken(plain)
	if _, err := s.authRepo.CompleteInitialSetup(ctx, "op", string(passHash), th, "t"); err != nil {
		t.Fatal(err)
	}

	h := wrapTestAccessHandler(s, routeAuthPolicy{Action: authz.ActionJobRead}, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler must not run when Authorizer denies")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+plain)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("code=%d", rec.Code)
	}

	var body authAPIError
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error != AuthJSONAuthorizationDenied {
		t.Fatalf("error=%q", body.Error)
	}
}

func TestAccessControlledHandler_publicRouteBypassesAuth(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	var hit bool
	h := wrapTestAccessHandler(s, routeAuthPolicy{Public: true}, func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusNoContent)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.ServeHTTP(rec, req)

	if !hit || rec.Code != http.StatusNoContent {
		t.Fatalf("hit=%v code=%d", hit, rec.Code)
	}
}

func TestAccessControlMiddleware_nilAuthRepo(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	s := NewAPIServerWithRepositories(mocks.NewMockLogger(),
		mocks.NewMockJobsRepository(), mocks.NewMockRunsRepository(), mocks.StubEphemeralRunStarter{})
	s.authRepo = nil

	h := wrapTestAccessHandler(s, routeAuthPolicy{Action: authz.ActionSetupStatus}, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("should not reach handler")
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/setup/status", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d", rec.Code)
	}

	var body authAPIError
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error != AuthJSONUnavailable {
		t.Fatalf("error=%q", body.Error)
	}
}
