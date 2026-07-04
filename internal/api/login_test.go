package api

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"vectis/internal/api/audit"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
	sdkauth "vectis/sdk/auth"

	"golang.org/x/crypto/bcrypt"
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
		assertNoStore(t, rec)

		var out loginResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if out.Token != "" || out.UserID == 0 || out.ExpiresAt == nil {
			t.Fatalf("bad response: %+v", out)
		}

		if out.CSRFToken == "" {
			t.Fatalf("missing csrf token: %+v", out)
		}

		cookies := rec.Result().Cookies()
		var sessionCookie, csrfCookie http.Cookie
		var hasSessionCookie, hasCSRFCookie bool
		for _, c := range cookies {
			if c.Name == sessionCookieName {
				sessionCookie = *c
				hasSessionCookie = true
			}

			if c.Name == csrfCookieName {
				csrfCookie = *c
				hasCSRFCookie = true
			}
		}

		if !hasSessionCookie || !hasCSRFCookie {
			t.Fatalf("expected session and csrf cookies, got %+v", cookies)
		}

		if !sessionCookie.HttpOnly {
			t.Fatal("session cookie must be HttpOnly")
		}

		if csrfCookie.HttpOnly {
			t.Fatal("csrf cookie must be readable by browser clients")
		}

		if csrfCookie.Value != out.CSRFToken {
			t.Fatal("csrf cookie must match csrf response token")
		}

		var loginTokenRows int
		if err := db.QueryRow(`SELECT COUNT(*) FROM api_tokens WHERE label = 'login'`).Scan(&loginTokenRows); err != nil {
			t.Fatalf("count login api tokens: %v", err)
		}

		if loginTokenRows != 0 {
			t.Fatalf("login should create cache sessions, got %d login api token rows", loginTokenRows)
		}

		rec2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces", nil)
		req2.AddCookie(&sessionCookie)
		h.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusOK {
			t.Fatalf("session cookie rejected code=%d", rec2.Code)
		}
	})

	t.Run("return_token_allows_bearer_session", func(t *testing.T) {
		body := map[string]any{
			"username":     "root",
			"password":     "longenough",
			"return_token": true,
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
		assertNoStore(t, rec)

		var out loginResponse
		if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}

		if out.Token == "" || out.UserID == 0 || out.ExpiresAt == nil || out.CSRFToken == "" {
			t.Fatalf("bad response: %+v", out)
		}

		rec2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces", nil)
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

	var errResp struct {
		Code string `json:"code"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatal(err)
	}

	if errResp.Code != string(apiErrSetupRequired) {
		t.Fatalf("expected setup_required, got %q", errResp.Code)
	}
}

func TestLogin_externalProvider(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	t.Run("auto_provisions_local_user", func(t *testing.T) {
		db := dbtest.NewTestDB(t)
		s := NewAPIServer(mocks.NewMockLogger(), db)
		s.SetQueueClient(mocks.NewMockQueueService())
		h := s.Handler()
		completeLoginTestSetup(t, h)

		provider := &fakeLoginProvider{identity: sdkauth.Identity{
			Provider: "ldap",
			Subject:  "uid=alice,ou=people,dc=example,dc=org",
			Username: "alice",
		}}

		setFakeExternalLoginProvider(s, "ldap", provider)
		s.SetExternalLoginAutoProvision(true)

		out := postLoginForTest(t, h, map[string]any{
			"username":     "alice",
			"password":     "ldap-secret",
			"return_token": true,
		}, http.StatusOK)

		if out.UserID == 0 || out.Token == "" {
			t.Fatalf("bad login response: %+v", out)
		}

		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM local_users WHERE username = 'alice'`).Scan(&count); err != nil {
			t.Fatalf("count alice user: %v", err)
		}

		if count != 1 {
			t.Fatalf("alice user count = %d, want 1", count)
		}

		var passwordAuthEnabled bool
		if err := db.QueryRow(`SELECT password_auth_enabled FROM local_users WHERE username = 'alice'`).Scan(&passwordAuthEnabled); err != nil {
			t.Fatalf("query alice password_auth_enabled: %v", err)
		}

		if passwordAuthEnabled {
			t.Fatal("auto-provisioned external user should have password auth disabled")
		}

		var linkCount int
		if err := db.QueryRow(`SELECT COUNT(*)
FROM external_identities ei
JOIN auth_providers ap ON ap.id = ei.auth_provider_id
WHERE ap.provider_id = 'ldap' AND ei.subject = 'uid=alice,ou=people,dc=example,dc=org'`).Scan(&linkCount); err != nil {
			t.Fatalf("count alice external identity: %v", err)
		}

		if linkCount != 1 {
			t.Fatalf("alice external identity count = %d, want 1", linkCount)
		}
	})

	t.Run("reuses_existing_local_user", func(t *testing.T) {
		db := dbtest.NewTestDB(t)
		s := NewAPIServer(mocks.NewMockLogger(), db)
		s.SetQueueClient(mocks.NewMockQueueService())
		h := s.Handler()
		completeLoginTestSetup(t, h)

		passHash, err := bcrypt.GenerateFromPassword([]byte("local-password"), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}

		localUserID, err := s.authRepo.CreateLocalUser(context.Background(), "dana", string(passHash))
		if err != nil {
			t.Fatalf("CreateLocalUser: %v", err)
		}

		provider := &fakeLoginProvider{identity: sdkauth.Identity{
			Provider: "ldap",
			Subject:  "uid=dana,ou=people,dc=example,dc=org",
			Username: "dana",
		}}

		setFakeExternalLoginProvider(s, "ldap", provider)
		out := postLoginForTest(t, h, map[string]any{
			"username": "dana",
			"password": "ldap-secret",
		}, http.StatusOK)

		if out.UserID != localUserID {
			t.Fatalf("UserID = %d, want %d", out.UserID, localUserID)
		}

		var linkedUserID int64
		if err := db.QueryRow(`SELECT ei.local_user_id
FROM external_identities ei
JOIN auth_providers ap ON ap.id = ei.auth_provider_id
WHERE ap.provider_id = 'ldap' AND ei.subject = 'uid=dana,ou=people,dc=example,dc=org'`).Scan(&linkedUserID); err != nil {
			t.Fatalf("lookup dana external identity: %v", err)
		}

		if linkedUserID != localUserID {
			t.Fatalf("external identity local_user_id = %d, want %d", linkedUserID, localUserID)
		}
	})

	t.Run("requires_existing_link_when_auto_link_disabled", func(t *testing.T) {
		db := dbtest.NewTestDB(t)
		s := NewAPIServer(mocks.NewMockLogger(), db)
		s.SetQueueClient(mocks.NewMockQueueService())
		h := s.Handler()
		completeLoginTestSetup(t, h)

		passHash, err := bcrypt.GenerateFromPassword([]byte("local-password"), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}

		localUserID, err := s.authRepo.CreateLocalUser(context.Background(), "dana", string(passHash))
		if err != nil {
			t.Fatalf("CreateLocalUser: %v", err)
		}

		provider := &fakeLoginProvider{identity: sdkauth.Identity{
			Provider: "ldap",
			Subject:  "uid=dana,ou=people,dc=example,dc=org",
			Username: "dana",
		}}

		setFakeExternalLoginProvider(s, "ldap", provider)
		s.SetExternalLoginAutoLinkUsers(false)

		postLoginForTest(t, h, map[string]any{
			"username": "dana",
			"password": "ldap-secret",
		}, http.StatusUnauthorized)

		if _, err := s.authRepo.LinkExternalIdentity(context.Background(), localUserID, "ldap", "uid=dana,ou=people,dc=example,dc=org", "dana", ""); err != nil {
			t.Fatalf("LinkExternalIdentity: %v", err)
		}

		out := postLoginForTest(t, h, map[string]any{
			"username": "dana",
			"password": "ldap-secret",
		}, http.StatusOK)

		if out.UserID != localUserID {
			t.Fatalf("UserID = %d, want %d", out.UserID, localUserID)
		}
	})

	t.Run("password_auth_disabled_user_requires_external_identity", func(t *testing.T) {
		db := dbtest.NewTestDB(t)
		s := NewAPIServer(mocks.NewMockLogger(), db)
		s.SetQueueClient(mocks.NewMockQueueService())
		h := s.Handler()
		completeLoginTestSetup(t, h)

		passHash, err := bcrypt.GenerateFromPassword([]byte("local-password"), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}

		localUserID, err := s.authRepo.CreateLocalUserWithPasswordAuth(context.Background(), "dana", string(passHash), false)
		if err != nil {
			t.Fatalf("CreateLocalUserWithPasswordAuth: %v", err)
		}

		if _, err := s.authRepo.EnsureAuthProvider(context.Background(), "ldap", "test"); err != nil {
			t.Fatalf("EnsureAuthProvider: %v", err)
		}

		if _, err := s.authRepo.LinkExternalIdentity(context.Background(), localUserID, "ldap", "uid=dana,ou=people,dc=example,dc=org", "dana", ""); err != nil {
			t.Fatalf("LinkExternalIdentity: %v", err)
		}

		postLoginForTest(t, h, map[string]any{
			"username": "dana",
			"password": "local-password",
		}, http.StatusUnauthorized)

		provider := &fakeLoginProvider{identity: sdkauth.Identity{
			Provider: "ldap",
			Subject:  "uid=dana,ou=people,dc=example,dc=org",
			Username: "dana",
		}}

		setFakeExternalLoginProvider(s, "ldap", provider)
		out := postLoginForTest(t, h, map[string]any{
			"username": "dana",
			"password": "ldap-secret",
		}, http.StatusOK)

		if out.UserID != localUserID {
			t.Fatalf("UserID = %d, want %d", out.UserID, localUserID)
		}
	})

	t.Run("requires_local_user_without_auto_provision", func(t *testing.T) {
		db := dbtest.NewTestDB(t)
		s := NewAPIServer(mocks.NewMockLogger(), db)
		s.SetQueueClient(mocks.NewMockQueueService())
		h := s.Handler()
		completeLoginTestSetup(t, h)

		provider := &fakeLoginProvider{identity: sdkauth.Identity{
			Provider: "ldap",
			Subject:  "uid=bob,ou=people,dc=example,dc=org",
			Username: "bob",
		}}

		setFakeExternalLoginProvider(s, "ldap", provider)
		postLoginForTest(t, h, map[string]any{
			"username": "bob",
			"password": "ldap-secret",
		}, http.StatusUnauthorized)
	})

	t.Run("reuses_subject_link_when_claimed_username_changes", func(t *testing.T) {
		db := dbtest.NewTestDB(t)
		s := NewAPIServer(mocks.NewMockLogger(), db)
		s.SetQueueClient(mocks.NewMockQueueService())
		h := s.Handler()
		completeLoginTestSetup(t, h)

		provider := &fakeLoginProvider{identity: sdkauth.Identity{
			Provider: "ldap",
			Subject:  "uid=alice,ou=people,dc=example,dc=org",
			Username: "alice",
		}}

		setFakeExternalLoginProvider(s, "ldap", provider)
		s.SetExternalLoginAutoProvision(true)

		first := postLoginForTest(t, h, map[string]any{
			"username": "alice",
			"password": "ldap-secret",
		}, http.StatusOK)

		provider.identity.Username = "alice-renamed"
		second := postLoginForTest(t, h, map[string]any{
			"username": "alice-renamed",
			"password": "ldap-secret",
		}, http.StatusOK)

		if second.UserID != first.UserID {
			t.Fatalf("renamed subject UserID = %d, want %d", second.UserID, first.UserID)
		}

		var renamedUsers int
		if err := db.QueryRow(`SELECT COUNT(*) FROM local_users WHERE username = 'alice-renamed'`).Scan(&renamedUsers); err != nil {
			t.Fatalf("count renamed users: %v", err)
		}

		if renamedUsers != 0 {
			t.Fatalf("renamed local user count = %d, want 0", renamedUsers)
		}
	})

	t.Run("rejects_second_subject_for_same_provider_and_local_user", func(t *testing.T) {
		db := dbtest.NewTestDB(t)
		s := NewAPIServer(mocks.NewMockLogger(), db)
		s.SetQueueClient(mocks.NewMockQueueService())
		h := s.Handler()
		completeLoginTestSetup(t, h)

		passHash, err := bcrypt.GenerateFromPassword([]byte("local-password"), bcrypt.MinCost)
		if err != nil {
			t.Fatal(err)
		}

		localUserID, err := s.authRepo.CreateLocalUser(context.Background(), "alice", string(passHash))
		if err != nil {
			t.Fatalf("CreateLocalUser: %v", err)
		}

		provider := &fakeLoginProvider{identity: sdkauth.Identity{
			Provider: "ldap",
			Subject:  "uid=alice,ou=people,dc=example,dc=org",
			Username: "alice",
		}}

		setFakeExternalLoginProvider(s, "ldap", provider)
		postLoginForTest(t, h, map[string]any{
			"username": "alice",
			"password": "ldap-secret",
		}, http.StatusOK)

		provider.identity.Subject = "uid=alice2,ou=people,dc=example,dc=org"
		postLoginForTest(t, h, map[string]any{
			"username": "alice",
			"password": "ldap-secret",
		}, http.StatusUnauthorized)

		var linkCount int
		if err := db.QueryRow(`SELECT COUNT(*)
FROM external_identities ei
JOIN auth_providers ap ON ap.id = ei.auth_provider_id
WHERE ap.provider_id = 'ldap' AND ei.local_user_id = ?`, localUserID).Scan(&linkCount); err != nil {
			t.Fatalf("count local user links: %v", err)
		}

		if linkCount != 1 {
			t.Fatalf("local user provider link count = %d, want 1", linkCount)
		}
	})

	t.Run("rejects_provider_id_mismatch", func(t *testing.T) {
		db := dbtest.NewTestDB(t)
		s := NewAPIServer(mocks.NewMockLogger(), db)
		s.SetQueueClient(mocks.NewMockQueueService())
		h := s.Handler()
		completeLoginTestSetup(t, h)

		provider := &fakeLoginProvider{identity: sdkauth.Identity{
			Provider: "other-ldap",
			Subject:  "uid=alice,ou=people,dc=example,dc=org",
			Username: "alice",
		}}

		setFakeExternalLoginProvider(s, "corp-ldap", provider)
		s.SetExternalLoginAutoProvision(true)

		postLoginForTest(t, h, map[string]any{
			"username": "alice",
			"password": "ldap-secret",
		}, http.StatusUnauthorized)
	})

	t.Run("provider_unavailable", func(t *testing.T) {
		db := dbtest.NewTestDB(t)
		s := NewAPIServer(mocks.NewMockLogger(), db)
		s.SetQueueClient(mocks.NewMockQueueService())
		h := s.Handler()
		completeLoginTestSetup(t, h)

		setFakeExternalLoginProvider(s, "ldap", &fakeLoginProvider{err: sdkauth.ErrUnavailable})

		postLoginForTest(t, h, map[string]any{
			"username": "erin",
			"password": "ldap-secret",
		}, http.StatusServiceUnavailable)
	})

	t.Run("auto_provision_conflict_with_disabled_user_is_rejected", func(t *testing.T) {
		db := dbtest.NewTestDB(t)
		s := NewAPIServer(mocks.NewMockLogger(), db)
		s.SetQueueClient(mocks.NewMockQueueService())
		h := s.Handler()
		completeLoginTestSetup(t, h)

		s.authRepo = &conflictDisabledAuthRepo{AuthRepository: s.authRepo}
		setFakeExternalLoginProvider(s, "ldap", &fakeLoginProvider{identity: sdkauth.Identity{
			Provider: "ldap",
			Subject:  "uid=casey,ou=people,dc=example,dc=org",
			Username: "casey",
		}})

		s.SetExternalLoginAutoProvision(true)

		postLoginForTest(t, h, map[string]any{
			"username": "casey",
			"password": "ldap-secret",
		}, http.StatusUnauthorized)
	})
}

func TestLogin_sessionSharedAcrossAPIServers(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s1 := NewAPIServer(mocks.NewMockLogger(), db)
	s1.SetQueueClient(mocks.NewMockQueueService())
	h1 := s1.Handler()

	s2 := NewAPIServer(mocks.NewMockLogger(), db)
	s2.SetQueueClient(mocks.NewMockQueueService())
	h2 := s2.Handler()

	setupBody := map[string]string{
		"bootstrap_token": "sixteenchars----",
		"admin_username":  "root",
		"admin_password":  "longenough",
	}

	b, _ := json.Marshal(setupBody)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/setup/complete", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h1.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("setup failed code=%d body=%s", rec.Code, rec.Body.String())
	}

	var setupOut setupCompleteResponse
	if err := json.NewDecoder(rec.Body).Decode(&setupOut); err != nil {
		t.Fatal(err)
	}

	loginBody := map[string]string{
		"username": "root",
		"password": "longenough",
	}

	b, _ = json.Marshal(loginBody)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h1.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login failed code=%d body=%s", rec.Code, rec.Body.String())
	}
	assertNoStore(t, rec)

	var out loginResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	if out.Token != "" || out.CSRFToken == "" {
		t.Fatalf("bad browser session response: %+v", out)
	}

	var sessionCookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			sessionCookie = c
			break
		}
	}

	if sessionCookie == nil {
		t.Fatal("expected session cookie")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/namespaces", nil)
	req.AddCookie(sessionCookie)
	h2.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("session rejected by second API server: code=%d body=%s", rec.Code, rec.Body.String())
	}

	changePasswordBody := map[string]string{
		"current_password": "longenough",
		"new_password":     "newpassword123",
	}

	b, _ = json.Marshal(changePasswordBody)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/users/change-password", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+setupOut.APIToken)
	h1.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("change password failed code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.AddCookie(sessionCookie)
	h2.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("session should be revoked after password change, got code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestLogout_deletesSession(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	events := []audit.Event{}
	s.SetAuditor(&testAuditCapturer{events: &events})
	h := s.Handler()

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

	loginBody := map[string]any{
		"username":     "root",
		"password":     "longenough",
		"return_token": true,
	}

	b, _ = json.Marshal(loginBody)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login failed code=%d body=%s", rec.Code, rec.Body.String())
	}

	var out loginResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	if out.Token == "" {
		t.Fatal("expected bearer session token")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil)
	req.Header.Set("Authorization", "Bearer "+out.Token)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("logout failed code=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Clear-Site-Data"); got != logoutClearSiteData {
		t.Fatalf("Clear-Site-Data=%q, want %q", got, logoutClearSiteData)
	}

	var logoutEvent *audit.Event
	for i := range events {
		if events[i].Type == audit.EventAuthLogout {
			logoutEvent = &events[i]
			break
		}
	}

	if logoutEvent == nil {
		t.Fatalf("missing %s audit event in %+v", audit.EventAuthLogout, events)
	}
	if logoutEvent.ActorID != out.UserID || logoutEvent.Metadata["credential_source"] != "bearer" {
		t.Fatalf("logout audit event mismatch: %+v", logoutEvent)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.Header.Set("Authorization", "Bearer "+out.Token)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("session should be invalid after logout, got code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestCookieSessionAuth_requiresCSRFForUnsafeMethods(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	metrics := &fakeSecurityRejectionMetrics{}
	s.SetAPISecurityMetrics(metrics)
	h := s.Handler()

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

	loginBody := map[string]string{
		"username": "root",
		"password": "longenough",
	}

	b, _ = json.Marshal(loginBody)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login failed code=%d body=%s", rec.Code, rec.Body.String())
	}

	var out loginResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	if out.Token != "" || out.CSRFToken == "" {
		t.Fatalf("bad browser session response: %+v", out)
	}

	var sessionCookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			sessionCookie = c
			break
		}
	}

	if sessionCookie == nil {
		t.Fatal("expected session cookie")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.AddCookie(sessionCookie)
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("cross-site cookie read should be forbidden, got code=%d body=%s", rec.Code, rec.Body.String())
	}

	var readErr apiError
	if err := json.NewDecoder(rec.Body).Decode(&readErr); err != nil {
		t.Fatalf("decode cross-site cookie read error: %v; body=%s", err, rec.Body.String())
	}

	if readErr.Code != string(apiErrFetchMetadataForbidden) {
		t.Fatalf("cross-site cookie read code=%q, want %q", readErr.Code, apiErrFetchMetadataForbidden)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil)
	req.AddCookie(sessionCookie)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("logout without csrf should be forbidden, got code=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Clear-Site-Data"); got != "" {
		t.Fatalf("Clear-Site-Data on forbidden logout = %q, want empty", got)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil)
	req.AddCookie(sessionCookie)
	req.Header.Set(csrfHeaderName, "wrong-csrf-token")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("logout with wrong csrf should be forbidden, got code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil)
	req.AddCookie(sessionCookie)
	req.Header.Set(csrfHeaderName, out.CSRFToken)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("logout without origin should be forbidden, got code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil)
	req.AddCookie(sessionCookie)
	req.Header.Set(csrfHeaderName, out.CSRFToken)
	req.Host = "vectis.example"
	req.Header.Set("Origin", "https://evil.example")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("logout with wrong origin should be forbidden, got code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil)
	req.AddCookie(sessionCookie)
	req.Header.Set(csrfHeaderName, out.CSRFToken)
	req.Host = "vectis.example"
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("Origin", "https://vectis.example")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("logout with cross-site fetch metadata should be forbidden, got code=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/logout", nil)
	req.AddCookie(sessionCookie)
	req.Header.Set(csrfHeaderName, out.CSRFToken)
	req.Host = "vectis.example"
	req.TLS = &tls.ConnectionState{}
	req.Header.Set("Origin", "https://vectis.example")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("logout with csrf failed code=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get("Clear-Site-Data"); got != logoutClearSiteData {
		t.Fatalf("Clear-Site-Data=%q, want %q", got, logoutClearSiteData)
	}

	cleared := map[string]bool{}
	for _, c := range rec.Result().Cookies() {
		if (c.Name == sessionCookieName || c.Name == csrfCookieName) && c.MaxAge < 0 {
			cleared[c.Name] = true
		}
	}

	if !cleared[sessionCookieName] || !cleared[csrfCookieName] {
		t.Fatalf("logout should clear session cookies, got %+v", rec.Result().Cookies())
	}

	if got := countSecurityRejections(metrics, securityReasonCSRFTokenRequired, "POST /api/v1/logout", http.StatusForbidden); got != 2 {
		t.Fatalf("csrf token rejection count=%d, want 2 in %+v", got, metrics.Records())
	}

	if got := countSecurityRejections(metrics, securityReasonCSRFOriginForbidden, "POST /api/v1/logout", http.StatusForbidden); got != 1 {
		t.Fatalf("csrf origin rejection count=%d, want 1 in %+v", got, metrics.Records())
	}

	requireSecurityRejection(t, metrics, securityReasonCORSOriginForbidden, securityRejectionUnknownRoute, http.StatusForbidden)
	requireSecurityRejection(t, metrics, securityReasonCSRFFetchMetadataBlocked, "POST /api/v1/logout", http.StatusForbidden)
	requireSecurityRejection(t, metrics, securityReasonFetchMetadataForbidden, securityRejectionUnknownRoute, http.StatusForbidden)
}

func TestCookieSessionAuth_idleExpiry(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")
	t.Setenv("VECTIS_API_SESSION_IDLE_TTL", "1h")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

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

	loginBody := map[string]string{
		"username": "root",
		"password": "longenough",
	}

	b, _ = json.Marshal(loginBody)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("login failed code=%d body=%s", rec.Code, rec.Body.String())
	}

	var sessionCookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			sessionCookie = c
			break
		}
	}

	if sessionCookie == nil {
		t.Fatal("expected session cookie")
	}

	oldLastUsed := time.Now().UTC().Add(-2 * time.Hour).UnixNano()
	if _, err := db.Exec(`UPDATE api_sessions SET last_used_unix_nano = ?`, oldLastUsed); err != nil {
		t.Fatalf("make session idle: %v", err)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.AddCookie(sessionCookie)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("idle session should be rejected, got code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestLogin_usernameThrottle(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	metrics := &fakeSecurityRejectionMetrics{}
	s.SetAPISecurityMetrics(metrics)
	h := s.Handler()

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

	for i := range 5 {
		body := map[string]string{
			"username": "root",
			"password": "wrongpassword",
		}

		b, _ := json.Marshal(body)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
		req.RemoteAddr = fmt.Sprintf("203.0.113.%d:1234", i+1)
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("request %d code=%d body=%s", i+1, rec.Code, rec.Body.String())
		}
	}

	body := map[string]string{
		"username": "root",
		"password": "wrongpassword",
	}

	b, _ = json.Marshal(body)
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
	req.RemoteAddr = "203.0.113.99:1234"
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("expected username throttle 429, got code=%d body=%s", rec.Code, rec.Body.String())
	}

	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header")
	}

	requireSecurityRejection(t, metrics, securityReasonRateLimitExceeded, "POST /api/v1/login", http.StatusTooManyRequests)
}

func TestCookieSessionAuth_doesNotAcceptAPITokenCookie(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")

	db := dbtest.NewTestDB(t)
	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()

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

	var setupOut setupCompleteResponse
	if err := json.NewDecoder(rec.Body).Decode(&setupOut); err != nil {
		t.Fatal(err)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/v1/jobs", nil)
	req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: setupOut.APIToken})
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("api token presented as a session cookie should be rejected, got code=%d body=%s", rec.Code, rec.Body.String())
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

func completeLoginTestSetup(t *testing.T, h http.Handler) {
	t.Helper()

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
}

func postLoginForTest(t *testing.T, h http.Handler, body map[string]any, wantStatus int) loginResponse {
	t.Helper()

	b, _ := json.Marshal(body)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/login", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec, req)
	if rec.Code != wantStatus {
		t.Fatalf("login status=%d want %d body=%s", rec.Code, wantStatus, rec.Body.String())
	}

	if wantStatus != http.StatusOK {
		return loginResponse{}
	}

	var out loginResponse
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	return out
}

type fakeLoginProvider struct {
	identity sdkauth.Identity
	err      error
}

func setFakeExternalLoginProvider(s *APIServer, providerID string, provider *fakeLoginProvider) {
	s.SetLoginProviderRegistrations([]LoginProviderRegistration{{
		ID:       providerID,
		Kind:     "test",
		Provider: provider,
	}})
}

func (p *fakeLoginProvider) Authenticate(context.Context, string, string) (sdkauth.Identity, error) {
	if p.err != nil {
		return sdkauth.Identity{}, p.err
	}

	return p.identity, nil
}

type conflictDisabledAuthRepo struct {
	dal.AuthRepository
}

func (r *conflictDisabledAuthRepo) CreateLocalUser(ctx context.Context, username, passwordHash string) (int64, error) {
	return r.CreateLocalUserWithPasswordAuth(ctx, username, passwordHash, true)
}

func (r *conflictDisabledAuthRepo) CreateLocalUserWithPasswordAuth(ctx context.Context, username, passwordHash string, passwordAuthEnabled bool) (int64, error) {
	id, err := r.AuthRepository.CreateLocalUserWithPasswordAuth(ctx, username, passwordHash, passwordAuthEnabled)
	if err != nil {
		return 0, err
	}

	if err := r.AuthRepository.UpdateLocalUserEnabled(ctx, id, false); err != nil {
		return 0, err
	}

	return 0, fmt.Errorf("%w: injected disabled user conflict", dal.ErrConflict)
}
