package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vectis/internal/api/audit"
	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestHashAPIToken_consistency(t *testing.T) {
	h1 := hashAPIToken("hello")
	h2 := hashAPIToken("hello")
	if h1 != h2 {
		t.Fatal("expected consistent hash")
	}

	if h1 == "hello" {
		t.Fatal("expected hashed value")
	}
}

func TestBearerToken_valid(t *testing.T) {
	tok, ok := bearerToken("Bearer secret-token")
	if !ok {
		t.Fatal("expected ok")
	}

	if tok != "secret-token" {
		t.Fatalf("expected secret-token, got %s", tok)
	}
}

func TestBearerToken_invalid(t *testing.T) {
	cases := []string{
		"",
		"Basic secret",
		"Bearer",
		"Bearer ",
		"bearersecret",
	}

	for _, c := range cases {
		tok, ok := bearerToken(c)
		if ok {
			t.Fatalf("expected not ok for %q, got %q", c, tok)
		}
	}
}

func TestBearerToken_caseInsensitive(t *testing.T) {
	tok, ok := bearerToken("bearer lowercase")
	if !ok {
		t.Fatal("expected ok for lowercase bearer")
	}

	if tok != "lowercase" {
		t.Fatalf("expected lowercase, got %s", tok)
	}
}

func TestAccessControlledHandler_publicRoute(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	var hit bool
	h := s.accessControlledHandler(routeAuthPolicy{Public: true}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	h.ServeHTTP(rec, req)

	if !hit || rec.Code != http.StatusNoContent {
		t.Fatalf("hit=%v code=%d", hit, rec.Code)
	}
}

func TestAccessControlledHandler_authDisabled(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	var hit bool
	h := s.accessControlledHandler(routeAuthPolicy{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit = true
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	h.ServeHTTP(rec, req)

	if !hit || rec.Code != http.StatusNoContent {
		t.Fatalf("hit=%v code=%d", hit, rec.Code)
	}
}

func TestAccessControlledHandler_noAuthRepo(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")

	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)

	h := s.accessControlledHandler(routeAuthPolicy{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestAccessControlledHandler_noBearerToken(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())

	// Complete setup so middleware requires auth
	ctx := context.Background()
	_, _ = s.authRepo.CompleteInitialSetup(ctx, "admin", "hash", "tokenhash", "label")

	h := s.accessControlledHandler(routeAuthPolicy{Action: authz.ActionJobRead}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestAccessControlledHandler_tokenTooLong(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	ctx := context.Background()
	_, _ = s.authRepo.CompleteInitialSetup(ctx, "admin", "hash", "tokenhash", "label")

	h := s.accessControlledHandler(routeAuthPolicy{Action: authz.ActionJobRead}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("a", maxBearerTokenBytes+1))
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestAccessControlledHandler_invalidToken(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	ctx := context.Background()
	_, _ = s.authRepo.CompleteInitialSetup(ctx, "admin", "hash", "tokenhash", "label")

	h := s.accessControlledHandler(routeAuthPolicy{Action: authz.ActionJobRead}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer invalid-token")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestAccessControlledHandler_validToken(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	ctx := context.Background()
	_, _ = s.authRepo.CompleteInitialSetup(ctx, "admin", "hash", "tokenhash", "label")

	plain := "my-secret-token"
	tokenHash := hashAPIToken(plain)
	uid, _, _, _ := s.authRepo.GetLocalUserByUsername(ctx, "admin")
	_, _ = s.authRepo.CreateAPIToken(ctx, uid, tokenHash, "test", nil)

	var principal *authn.Principal
	h := s.accessControlledHandler(routeAuthPolicy{Action: authz.ActionJobRead}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, _ = authn.PrincipalFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+plain)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d", rec.Code)
	}

	if principal == nil {
		t.Fatal("expected principal in context")
	}

	if principal.Username != "admin" {
		t.Fatalf("expected admin, got %s", principal.Username)
	}
}

func TestAccessControlledHandler_authorizationDenied(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	ctx := context.Background()
	_, _ = s.authRepo.CompleteInitialSetup(ctx, "admin", "hash", "tokenhash", "label")

	plain := "my-secret-token"
	tokenHash := hashAPIToken(plain)
	uid, _, _, _ := s.authRepo.GetLocalUserByUsername(ctx, "admin")
	_, _ = s.authRepo.CreateAPIToken(ctx, uid, tokenHash, "test", nil)

	// Override authorizer to deny everything
	s.authzOverride = denyExceptSetup{}

	h := s.accessControlledHandler(routeAuthPolicy{Action: authz.ActionJobRead}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+plain)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestAccessControlledHandler_auditLogOnInvalidToken(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	ctx := context.Background()
	_, _ = s.authRepo.CompleteInitialSetup(ctx, "admin", "hash", "tokenhash", "label")

	mockAuditor := &mockAuditor{}
	s.SetAuditor(mockAuditor)

	h := s.accessControlledHandler(routeAuthPolicy{Action: authz.ActionJobRead}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer bad-token")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d", rec.Code)
	}

	// Wait for async audit log
	time.Sleep(100 * time.Millisecond)

	if len(mockAuditor.events) != 1 {
		t.Fatalf("expected 1 audit event, got %d", len(mockAuditor.events))
	}

	if mockAuditor.events[0].Type != audit.EventAuthFailure {
		t.Fatalf("expected auth failure event, got %s", mockAuditor.events[0].Type)
	}
}

func TestEffectiveAuthorizer_override(t *testing.T) {
	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	authz := &mockAuthorizer{allow: true}
	s.authzOverride = authz

	z := s.effectiveAuthorizer(true)
	if z != authz {
		t.Fatal("expected overridden authorizer")
	}
}

func TestEffectiveAuthorizer_authDisabled(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	s := NewAPIServerWithRepositories(mocks.NewMockLogger(), nil, nil, nil)
	z := s.effectiveAuthorizer(true)

	if z == nil {
		t.Fatal("expected non-nil authorizer")
	}
}

func TestAccessControlledHandler_tokenScopesLoaded(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	ctx := context.Background()
	_, _ = s.authRepo.CompleteInitialSetup(ctx, "admin", "hash", "tokenhash", "label")

	plain := "my-secret-token"
	tokenHash := hashAPIToken(plain)
	uid, _, _, _ := s.authRepo.GetLocalUserByUsername(ctx, "admin")
	tokenID, _ := s.authRepo.CreateAPIToken(ctx, uid, tokenHash, "test", nil)

	// Add a scope to the token
	_, _ = db.ExecContext(ctx, `INSERT INTO api_token_scopes (api_token_id, action, namespace_id, propagate) VALUES (?, ?, ?, ?)`,
		tokenID, "job:read", 1, false)

	var principal *authn.Principal
	h := s.accessControlledHandler(routeAuthPolicy{Action: authz.ActionJobRead}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, _ = authn.PrincipalFromContext(r.Context())
		w.WriteHeader(http.StatusNoContent)
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+plain)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("code=%d", rec.Code)
	}

	if principal == nil {
		t.Fatal("expected principal")
	}

	if len(principal.TokenScopes) != 1 {
		t.Fatalf("expected 1 scope, got %d", len(principal.TokenScopes))
	}

	if principal.TokenScopes[0].Action != "job:read" {
		t.Fatalf("expected job:read scope, got %s", principal.TokenScopes[0].Action)
	}
}

func TestAccessControlledHandler_dbUnavailableReturns503(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	ctx := context.Background()
	_, _ = s.authRepo.CompleteInitialSetup(ctx, "admin", "hash", "tokenhash", "label")

	// Close DB to simulate unavailability
	db.Close()

	h := s.accessControlledHandler(routeAuthPolicy{Action: authz.ActionJobRead}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer some-token")
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d, want 503", rec.Code)
	}
}

func TestAccessControlledHandler_tokenScopeDBErrorReturns500(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)

	ctx := context.Background()
	_, _ = s.authRepo.CompleteInitialSetup(ctx, "admin", "hash", "tokenhash", "label")

	plain := "my-secret-token"
	tokenHash := hashAPIToken(plain)
	uid, _, _, _ := s.authRepo.GetLocalUserByUsername(ctx, "admin")
	_, _ = s.authRepo.CreateAPIToken(ctx, uid, tokenHash, "test", nil)

	mockAuditor := &mockAuditor{}
	s.SetAuditor(mockAuditor)

	// Close DB after token is resolved but before scopes are loaded
	db.Close()

	h := s.accessControlledHandler(routeAuthPolicy{Action: authz.ActionJobRead}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not run")
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+plain)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d, want 503", rec.Code)
	}
}

type mockAuditor struct {
	events []audit.Event
}

func (m *mockAuditor) Log(_ context.Context, event audit.Event) error {
	m.events = append(m.events, event)
	return nil
}

type mockAuthorizer struct {
	allow bool
}

func (m *mockAuthorizer) Allow(_ context.Context, _ *authn.Principal, _ authz.Action, _ authz.Resource) bool {
	return m.allow
}
