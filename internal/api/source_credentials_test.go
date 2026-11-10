package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"vectis/internal/api/authz"
)

func TestSourceCredentialInventoryRequiresRootAdmin(t *testing.T) {
	s, rootToken, _ := securityBoundaryServer(t)
	ctx := t.Context()
	root, err := s.namespaces.GetByPath(ctx, "/")
	if err != nil {
		t.Fatal(err)
	}

	team, err := s.namespaces.Create(ctx, "team", &root.ID)
	if err != nil {
		t.Fatal(err)
	}

	calls := 0
	s.SetSourceCredentialReferences(func(context.Context) ([]string, error) {
		calls++
		return []string{"encryptedfs://git/z", "encryptedfs://git/a"}, nil
	})

	h := s.Handler()
	for _, role := range []string{authz.RoleViewer, authz.RoleOperator, authz.RoleAdmin} {
		name := strings.ToLower(role)
		uid, err := s.authRepo.CreateLocalUser(ctx, name, dummyBcryptHash)
		if err != nil {
			t.Fatal(err)
		}

		if _, err := s.roleBindings.Create(ctx, uid, team.ID, role); err != nil {
			t.Fatal(err)
		}

		if _, err := s.authRepo.CreateAPIToken(ctx, uid, hashAPIToken(name), "test", nil); err != nil {
			t.Fatal(err)
		}

		rec := securityBoundaryRequest(t, h, http.MethodGet, "/api/v1/source-credentials", name, nil)
		if rec.Code != http.StatusForbidden || strings.Contains(rec.Body.String(), "encryptedfs") {
			t.Fatalf("%s read credential names: %d %s", role, rec.Code, rec.Body.String())
		}
	}

	if calls != 0 {
		t.Fatal("unauthorized request enumerated credentials")
	}

	rec := securityBoundaryRequest(t, h, http.MethodGet, "/api/v1/source-credentials", rootToken, nil)
	if rec.Code != http.StatusOK || rec.Body.String() != "[\"encryptedfs://git/a\",\"encryptedfs://git/z\"]\n" {
		t.Fatalf("root inventory: %d %s", rec.Code, rec.Body.String())
	}

	s.SetSourceCredentialReferences(func(context.Context) ([]string, error) { return nil, errors.New("sensitive provider error") })
	rec = securityBoundaryRequest(t, h, http.MethodGet, "/api/v1/source-credentials", rootToken, nil)
	if rec.Code != http.StatusServiceUnavailable || strings.Contains(rec.Body.String(), "sensitive") {
		t.Fatalf("provider failure leaked details: %d %s", rec.Code, rec.Body.String())
	}
}
