package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"vectis/extensions/secrets/encryptedfs"
	"vectis/internal/api/authz"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	sourcepkg "vectis/internal/source"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/gittest"
)

func securityBoundaryServer(t *testing.T) (*APIServer, string, int64) {
	t.Helper()
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTHZ_ENGINE", "hierarchical_rbac")
	s := NewAPIServer(mocks.NewMockLogger(), dbtest.NewTestDB(t))
	token := "audit-initial-admin-token"
	uid, err := s.authRepo.CompleteInitialSetup(t.Context(), "audit-root", dummyBcryptHash, hashAPIToken(token), "audit")

	if err != nil {
		t.Fatal(err)
	}

	return s, token, uid
}

func securityBoundaryRequest(t *testing.T, h http.Handler, method, target, token string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	var body []byte
	var err error
	if payload != nil {
		body, err = json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
	}

	req := httptest.NewRequest(method, target, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}
func TestTokenDelegationCannotExpandPropagation(t *testing.T) {
	s, _, uid := securityBoundaryServer(t)
	ctx := t.Context()
	root, err := s.namespaces.GetByPath(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}

	own, err := s.namespaces.Create(ctx, "audit-team", &root.ID)
	if err != nil {
		t.Fatal(err)
	}

	child, err := s.namespaces.Create(ctx, "private", &own.ID)
	if err != nil {
		t.Fatal(err)
	}

	token := "audit-nonpropagating-token"
	if _, err := s.authRepo.CreateAPITokenWithScopes(ctx, uid, hashAPIToken(token), "restricted", nil, []*dal.TokenScopeRecord{
		{Action: string(authz.ActionAPI)},
		{Action: string(authz.ActionJobRead), NamespaceID: sql.NullInt64{Int64: own.ID, Valid: true}, Propagate: false},
	}); err != nil {
		t.Fatal(err)
	}

	h := s.Handler()
	target := "/api/v1/namespaces/" + boundaryID(child.ID)
	denied := securityBoundaryRequest(t, h, http.MethodGet, target, token, nil)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("control = %d %s", denied.Code, denied.Body.String())
	}

	minted := securityBoundaryRequest(t, h, http.MethodPost, "/api/v1/tokens", token, map[string]any{
		"label": "expanded", "expires_in": "1m", "scopes": []map[string]any{
			{"action": "job:read", "namespace_path": own.Path, "propagate": true},
		},
	})

	if minted.Code != http.StatusForbidden {
		t.Fatalf("expanded grant accepted: %d %s", minted.Code, minted.Body.String())
	}

	minted = securityBoundaryRequest(t, h, http.MethodPost, "/api/v1/tokens", token, map[string]any{
		"label": "narrowed", "expires_in": "1m", "scopes": []map[string]any{
			{"action": "job:read", "namespace_path": own.Path, "propagate": false},
		},
	})

	if minted.Code != http.StatusCreated {
		t.Fatalf("narrow grant rejected: %d %s", minted.Code, minted.Body.String())
	}

	var response createTokenResponse
	if err := json.Unmarshal(minted.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}

	ownRead := securityBoundaryRequest(t, h, http.MethodGet, "/api/v1/namespaces/"+boundaryID(own.ID), response.Token, nil)
	if ownRead.Code != http.StatusOK {
		t.Fatalf("narrow grant unusable: %d %s", ownRead.Code, ownRead.Body.String())
	}

	childRead := securityBoundaryRequest(t, h, http.MethodGet, target, response.Token, nil)
	if childRead.Code != http.StatusForbidden {
		t.Fatalf("narrow grant escaped: %d %s", childRead.Code, childRead.Body.String())
	}
}

func boundaryID(id int64) string { return strconv.FormatInt(id, 10) }

func TestNamespaceAdminCannotRegisterCheckoutAliases(t *testing.T) {
	s, rootToken, _ := securityBoundaryServer(t)
	ctx := t.Context()
	root, err := s.namespaces.GetByPath(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}

	own, err := s.namespaces.Create(ctx, "attacker", &root.ID)
	if err != nil {
		t.Fatal(err)
	}

	victim, err := s.namespaces.Create(ctx, "victim", &root.ID)
	if err != nil {
		t.Fatal(err)
	}

	uid, err := s.authRepo.CreateLocalUser(ctx, "tenant-admin", dummyBcryptHash)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.roleBindings.Create(ctx, uid, own.ID, authz.RoleAdmin); err != nil {
		t.Fatal(err)
	}

	token := "audit-tenant-admin-token"
	if _, err := s.authRepo.CreateAPIToken(ctx, uid, hashAPIToken(token), "tenant", nil); err != nil {
		t.Fatal(err)
	}

	repo := gittest.InitRepository(t)
	if err := os.MkdirAll(filepath.Join(repo, ".vectis", "jobs"), 0700); err != nil {
		t.Fatal(err)
	}

	definition := `{"id":"build","root":{"id":"root","uses":"builtins/script","with":{"script":"echo VICTIM_PRIVATE_DEFINITION"}}}`
	if err := os.WriteFile(filepath.Join(repo, ".vectis", "jobs", "build.json"), []byte(definition), 0600); err != nil {
		t.Fatal(err)
	}

	gittest.CommitAll(t, repo, "private definition")
	s.SetSourceCheckoutRoot(filepath.Dir(repo))
	if _, err := s.sources.CreateRepository(ctx, dal.SourceRepositoryRecord{RepositoryID: "victim-private", NamespaceID: victim.ID, SourceKind: dal.SourceKindLocalCheckout, CheckoutPath: repo, CheckoutMode: dal.SourceCheckoutModeExternal, DefaultRef: "HEAD", Enabled: true}); err != nil {
		t.Fatal(err)
	}

	h := s.Handler()
	denied := securityBoundaryRequest(t, h, http.MethodGet, "/api/v1/source-repositories/victim-private/jobs/build/definition", token, nil)
	if denied.Code != http.StatusForbidden {
		t.Fatalf("victim control=%d %s", denied.Code, denied.Body.String())
	}

	alias := securityBoundaryRequest(t, h, http.MethodPost, "/api/v1/source-repositories", token, map[string]any{
		"repository_id": "attacker-alias", "namespace": own.Path, "source_kind": "local_checkout", "checkout_mode": "external", "checkout_path": filepath.Join(repo, ".git"), "default_ref": "HEAD",
	})

	if alias.Code != http.StatusForbidden {
		t.Fatalf("namespace admin registered alias: %d %s", alias.Code, alias.Body.String())
	}

	if _, err := s.sources.GetRepository(ctx, "attacker-alias"); !dal.IsNotFound(err) {
		t.Fatalf("rejected alias persisted: %v", err)
	}

	approved := securityBoundaryRequest(t, h, http.MethodPost, "/api/v1/source-repositories", rootToken, map[string]any{
		"repository_id": "operator-approved", "namespace": own.Path, "checkout_path": filepath.Join(repo, ".git"), "default_ref": "HEAD",
	})

	if approved.Code != http.StatusCreated {
		t.Fatalf("operator registration: %d %s", approved.Code, approved.Body.String())
	}

	allowed := securityBoundaryRequest(t, h, http.MethodPost, "/api/v1/source-repositories/operator-approved/definitions/resolve", token, map[string]any{"ref": "HEAD", "path": ".vectis/jobs/build.json"})
	if allowed.Code != http.StatusOK || !strings.Contains(allowed.Body.String(), "VICTIM_PRIVATE_DEFINITION") {
		t.Fatalf("approved namespace access rejected: %d %s", allowed.Code, allowed.Body.String())
	}

	changed := securityBoundaryRequest(t, h, http.MethodPut, "/api/v1/source-repositories/operator-approved", token, map[string]any{"canonical_url": "https://untrusted.invalid/repo.git"})
	if changed.Code != http.StatusForbidden {
		t.Fatalf("namespace admin changed server binding: %d %s", changed.Code, changed.Body.String())
	}

}

func TestNamespaceAdminCannotBindSourceCredentials(t *testing.T) {
	s, rootToken, _ := securityBoundaryServer(t)
	ctx := t.Context()

	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("NO_PROXY", "localhost,127.0.0.1")
	t.Setenv("no_proxy", "localhost,127.0.0.1")

	root, err := s.namespaces.GetByPath(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}

	own, err := s.namespaces.Create(ctx, "attacker", &root.ID)
	if err != nil {
		t.Fatal(err)
	}

	uid, err := s.authRepo.CreateLocalUser(ctx, "tenant-admin", dummyBcryptHash)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.roleBindings.Create(ctx, uid, own.ID, authz.RoleAdmin); err != nil {
		t.Fatal(err)
	}

	token := "audit-tenant-source-token"
	if _, err := s.authRepo.CreateAPIToken(ctx, uid, hashAPIToken(token), "tenant", nil); err != nil {
		t.Fatal(err)
	}

	secretRoot := t.TempDir()
	key := []byte("0123456789abcdef0123456789abcdef")
	secretRef := "encryptedfs://victim/deploy-token"
	synthetic := "audit-synthetic-secret-only"
	if err := encryptedfs.WriteEncryptedFSSecretFile(secretRoot, secretRef, []byte(synthetic), key); err != nil {
		t.Fatal(err)
	}

	provider, err := encryptedfs.NewEncryptedFSProvider(secretRoot, encryptedfs.WithEncryptedFSKey(key))
	if err != nil {
		t.Fatal(err)
	}

	resolver := sourcepkg.NewRepositoryCredentialResolverFromSecrets(provider)
	received := make(chan bool, 4)
	localRemote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, password, ok := r.BasicAuth()
		if !ok {
			w.Header().Set("WWW-Authenticate", `Basic realm="audit fixture"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		received <- password == synthetic
		w.WriteHeader(http.StatusNotFound)
	}))
	defer localRemote.Close()

	s.SetSourceCheckoutRoot(t.TempDir())
	s.SetSourceSyncCheckoutStatus(func(ctx context.Context, rec dal.SourceRepositoryRecord, ref string) sourcepkg.GitCheckoutStatus {
		credentials, err := sourcepkg.RepositoryGitCredentials(ctx, rec, resolver)
		if err != nil {
			t.Errorf("fixture credential resolution: %v", err)
			return sourcepkg.GitCheckoutStatus{}
		}

		bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		return sourcepkg.SyncManagedGitCheckout(bounded, sourcepkg.ManagedGitCheckoutRequest{CheckoutPath: rec.CheckoutPath, RemoteURL: rec.CanonicalURL, DefaultRef: ref, Credentials: credentials})
	})

	h := s.Handler()
	created := securityBoundaryRequest(t, h, http.MethodPost, "/api/v1/source-repositories", token, map[string]any{
		"repository_id": "credential-probe", "namespace": own.Path, "checkout_mode": "managed", "canonical_url": localRemote.URL + "/fixture.git", "credential_ref": secretRef, "default_ref": "HEAD",
	})

	if created.Code != http.StatusForbidden {
		t.Fatalf("namespace admin bound credential: %d %s", created.Code, created.Body.String())
	}

	updated := securityBoundaryRequest(t, h, http.MethodPost, "/api/v1/source-repositories", rootToken, map[string]any{
		"repository_id": "credential-probe", "namespace": own.Path, "checkout_mode": "managed", "canonical_url": localRemote.URL + "/fixture.git", "credential_ref": secretRef, "default_ref": "HEAD",
	})

	if updated.Code != http.StatusCreated {
		t.Fatalf("operator credential binding: %d %s", updated.Code, updated.Body.String())
	}

	synced := securityBoundaryRequest(t, h, http.MethodPost, "/api/v1/source-repositories/credential-probe/sync", token, nil)
	select {
	case match := <-received:
		if !match {
			t.Fatal("unexpected approved credential")
		}
	default:
		t.Fatalf("approved credential not sent; sync=%d %s", synced.Code, synced.Body.String())
	}

	for _, payload := range []map[string]any{
		{"credential_ref": "encryptedfs://another/private-token"},
		{"canonical_url": "https://untrusted.invalid/repo.git"},
		{"fallback_remote_urls": []string{"https://untrusted.invalid/repo.git"}},
		{"checkout_path": filepath.Join(t.TempDir(), "alias")},
	} {
		changed := securityBoundaryRequest(t, h, http.MethodPut, "/api/v1/source-repositories/credential-probe", token, payload)
		if changed.Code != http.StatusForbidden {
			t.Fatalf("namespace admin changed binding: %d %s", changed.Code, changed.Body.String())
		}
	}
}
