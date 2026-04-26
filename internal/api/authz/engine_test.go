package authz

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"vectis/internal/api/authn"
	"vectis/internal/dal"
)

// mockNamespacesRepo implements dal.NamespacesRepository for testing
type mockNamespacesRepo struct {
	records map[string]*dal.NamespaceRecord
}

func newMockNamespacesRepo() *mockNamespacesRepo {
	return &mockNamespacesRepo{records: map[string]*dal.NamespaceRecord{}}
}

func (m *mockNamespacesRepo) add(id int64, name, path string, parentID *int64, breakInheritance bool) {
	m.records[path] = &dal.NamespaceRecord{
		ID:               id,
		Name:             name,
		Path:             path,
		ParentID:         parentID,
		BreakInheritance: breakInheritance,
	}
}

func (m *mockNamespacesRepo) Create(ctx context.Context, name string, parentID *int64) (*dal.NamespaceRecord, error) {
	return nil, errors.New("not implemented")
}

func (m *mockNamespacesRepo) GetByID(ctx context.Context, id int64) (*dal.NamespaceRecord, error) {
	for _, rec := range m.records {
		if rec.ID == id {
			return rec, nil
		}
	}
	return nil, fmt.Errorf("%w: namespace %d", dal.ErrNotFound, id)
}

func (m *mockNamespacesRepo) GetByPath(ctx context.Context, path string) (*dal.NamespaceRecord, error) {
	rec, ok := m.records[path]
	if !ok {
		return nil, fmt.Errorf("%w: namespace %s", dal.ErrNotFound, path)
	}
	return rec, nil
}

func (m *mockNamespacesRepo) List(ctx context.Context) ([]dal.NamespaceRecord, error) {
	return nil, nil
}

func (m *mockNamespacesRepo) ListChildren(ctx context.Context, parentID int64) ([]dal.NamespaceRecord, error) {
	return nil, nil
}

func (m *mockNamespacesRepo) Delete(ctx context.Context, id int64) error { return nil }
func (m *mockNamespacesRepo) HasChildren(ctx context.Context, id int64) (bool, error) {
	return false, nil
}

func (m *mockNamespacesRepo) HasJobs(ctx context.Context, id int64) (bool, error) { return false, nil }

// mockRoleBindingsRepo implements dal.RoleBindingsRepository for testing
type mockRoleBindingsRepo struct {
	bindings map[int64]map[int64][]string // userID -> namespaceID -> roles
}

func newMockRoleBindingsRepo() *mockRoleBindingsRepo {
	return &mockRoleBindingsRepo{bindings: map[int64]map[int64][]string{}}
}

func (m *mockRoleBindingsRepo) add(userID, namespaceID int64, roles ...string) {
	if m.bindings[userID] == nil {
		m.bindings[userID] = map[int64][]string{}
	}

	m.bindings[userID][namespaceID] = append(m.bindings[userID][namespaceID], roles...)
}

func (m *mockRoleBindingsRepo) Create(ctx context.Context, localUserID, namespaceID int64, role string) (*dal.RoleBindingRecord, error) {
	return nil, errors.New("not implemented")
}

func (m *mockRoleBindingsRepo) Delete(ctx context.Context, localUserID, namespaceID int64, role string) error {
	return errors.New("not implemented")
}

func (m *mockRoleBindingsRepo) ListByNamespace(ctx context.Context, namespaceID int64) ([]dal.RoleBindingRecord, error) {
	return nil, nil
}

func (m *mockRoleBindingsRepo) ListByUser(ctx context.Context, localUserID int64) ([]dal.RoleBindingRecord, error) {
	var out []dal.RoleBindingRecord
	for nsID, roles := range m.bindings[localUserID] {
		for _, role := range roles {
			out = append(out, dal.RoleBindingRecord{LocalUserID: localUserID, NamespaceID: nsID, Role: role})
		}
	}

	return out, nil
}
func (m *mockRoleBindingsRepo) GetUserRolesInNamespace(ctx context.Context, localUserID, namespaceID int64) ([]string, error) {
	if roles, ok := m.bindings[localUserID][namespaceID]; ok {
		return roles, nil
	}

	return nil, nil
}

func TestHierarchicalRBAC_NilPrincipal(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	rb := newMockRoleBindingsRepo()
	z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}

	if z.Allow(ctx, nil, ActionJobRead, Resource{NamespacePath: "/"}) {
		t.Fatal("nil principal should be denied")
	}
}

func TestHierarchicalRBAC_SetupActionsAlwaysAllowed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	rb := newMockRoleBindingsRepo()
	z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}

	if !z.Allow(ctx, nil, ActionSetupStatus, Resource{}) {
		t.Fatal("setup status must allow nil principal")
	}

	if !z.Allow(ctx, nil, ActionSetupComplete, Resource{}) {
		t.Fatal("setup complete must allow nil principal")
	}
}

func TestHierarchicalRBAC_RolePermissions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	ns.add(1, "root", "/", nil, false)
	ns.add(2, "team-a", "/team-a", &[]int64{1}[0], false)

	tests := []struct {
		role     string
		action   Action
		want     bool
		resource Resource
	}{
		{RoleViewer, ActionJobRead, true, Resource{NamespacePath: "/team-a"}},
		{RoleViewer, ActionJobWrite, false, Resource{NamespacePath: "/team-a"}},
		{RoleViewer, ActionRunTrigger, false, Resource{NamespacePath: "/team-a"}},
		{RoleTrigger, ActionRunTrigger, true, Resource{NamespacePath: "/team-a"}},
		{RoleOperator, ActionRunOperator, true, Resource{NamespacePath: "/team-a"}},
		{RoleAdmin, ActionJobWrite, true, Resource{NamespacePath: "/team-a"}},
		{RoleAdmin, ActionAdmin, true, Resource{NamespacePath: "/team-a"}},
	}

	for _, tt := range tests {
		t.Run(string(tt.role)+"_"+string(tt.action), func(t *testing.T) {
			t.Parallel()
			rb := newMockRoleBindingsRepo()
			rb.add(42, 2, tt.role)
			z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
			p := &authn.Principal{LocalUserID: 42, Username: "u"}

			got := z.Allow(ctx, p, tt.action, tt.resource)
			if got != tt.want {
				t.Fatalf("role=%s action=%s: got=%v want=%v", tt.role, tt.action, got, tt.want)
			}
		})
	}
}

func TestHierarchicalRBAC_Inheritance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	ns.add(1, "root", "/", nil, false)
	ns.add(2, "team-a", "/team-a", &[]int64{1}[0], false)
	ns.add(3, "project-1", "/team-a/project-1", &[]int64{2}[0], false)

	// Viewer on root should inherit to all descendants
	rb := newMockRoleBindingsRepo()
	rb.add(42, 1, RoleViewer)
	z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
	p := &authn.Principal{LocalUserID: 42, Username: "u"}

	if !z.Allow(ctx, p, ActionJobRead, Resource{NamespacePath: "/team-a/project-1"}) {
		t.Fatal("viewer on root should inherit to /team-a/project-1")
	}

	if z.Allow(ctx, p, ActionJobWrite, Resource{NamespacePath: "/team-a/project-1"}) {
		t.Fatal("viewer on root should not allow write")
	}
}

func TestHierarchicalRBAC_BreakInheritance(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	ns.add(1, "root", "/", nil, false)
	ns.add(2, "team-a", "/team-a", &[]int64{1}[0], true) // break inheritance
	ns.add(3, "project-1", "/team-a/project-1", &[]int64{2}[0], false)

	// Admin on root, but break_inheritance at team-a resets it
	rb := newMockRoleBindingsRepo()
	rb.add(42, 1, RoleAdmin)
	z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
	p := &authn.Principal{LocalUserID: 42, Username: "u"}

	if z.Allow(ctx, p, ActionJobRead, Resource{NamespacePath: "/team-a"}) {
		t.Fatal("break_inheritance should reset root permissions")
	}

	if z.Allow(ctx, p, ActionJobRead, Resource{NamespacePath: "/team-a/project-1"}) {
		t.Fatal("break_inheritance should block inherited access to descendants")
	}
}

func TestHierarchicalRBAC_BreakInheritance_ExplicitOnChild(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	ns.add(1, "root", "/", nil, false)
	ns.add(2, "team-a", "/team-a", &[]int64{1}[0], true) // break inheritance
	ns.add(3, "project-1", "/team-a/project-1", &[]int64{2}[0], false)

	// Admin on root + explicit viewer on project-1
	rb := newMockRoleBindingsRepo()
	rb.add(42, 1, RoleAdmin)
	rb.add(42, 3, RoleViewer)
	z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
	p := &authn.Principal{LocalUserID: 42, Username: "u"}

	if !z.Allow(ctx, p, ActionJobRead, Resource{NamespacePath: "/team-a/project-1"}) {
		t.Fatal("explicit role on child should work after inheritance break")
	}
}

func TestHierarchicalRBAC_DBErrorReturnsFalse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	rb := newMockRoleBindingsRepo()
	z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
	p := &authn.Principal{LocalUserID: 42, Username: "u"}

	// namespace doesn't exist
	if z.Allow(ctx, p, ActionJobRead, Resource{NamespacePath: "/nonexistent"}) {
		t.Fatal("missing namespace should deny")
	}
}

func TestHierarchicalRBAC_HasActionAnywhere(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	ns.add(1, "root", "/", nil, false)

	rb := newMockRoleBindingsRepo()
	rb.add(42, 1, RoleViewer)
	z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
	p := &authn.Principal{LocalUserID: 42, Username: "u"}

	if !z.Allow(ctx, p, ActionJobRead, Resource{}) {
		t.Fatal("empty namespace path should check any binding")
	}

	if z.Allow(ctx, p, ActionJobWrite, Resource{}) {
		t.Fatal("viewer should not allow write anywhere")
	}
}

func TestHierarchicalRBAC_AdminRoleGrantsAllActions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	ns.add(1, "root", "/", nil, false)

	rb := newMockRoleBindingsRepo()
	rb.add(42, 1, RoleAdmin)
	z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
	p := &authn.Principal{LocalUserID: 42, Username: "u"}

	allActions := allDistinctActions()
	for _, action := range allActions {
		if action == ActionSetupStatus || action == ActionSetupComplete || action == ActionUserAdmin {
			continue
		}

		if !z.Allow(ctx, p, action, Resource{NamespacePath: "/"}) {
			t.Fatalf("admin should allow action %s", action)
		}
	}
}

func TestHierarchicalRBAC_UserAdminRequiresRootAdmin(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	ns.add(1, "root", "/", nil, false)
	ns.add(2, "team-a", "/team-a", &[]int64{1}[0], false)

	t.Run("root admin allowed", func(t *testing.T) {
		rb := newMockRoleBindingsRepo()
		rb.add(42, 1, RoleAdmin)
		z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
		p := &authn.Principal{LocalUserID: 42, Username: "root-admin"}

		if !z.Allow(ctx, p, ActionUserAdmin, Resource{}) {
			t.Fatal("root admin should be allowed to manage users")
		}
	})

	t.Run("namespace admin denied", func(t *testing.T) {
		rb := newMockRoleBindingsRepo()
		rb.add(42, 2, RoleAdmin)
		z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
		p := &authn.Principal{LocalUserID: 42, Username: "team-admin"}

		if z.Allow(ctx, p, ActionUserAdmin, Resource{}) {
			t.Fatal("namespace admin should not become global user admin")
		}
	})
}

func TestHierarchicalRBAC_TokenScopesCannotExpandPermissions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	ns.add(1, "root", "/", nil, false)

	rb := newMockRoleBindingsRepo()
	rb.add(42, 1, RoleViewer)
	z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
	p := &authn.Principal{
		LocalUserID: 42,
		Username:    "viewer",
		TokenScopes: []authn.TokenScope{
			{Action: string(ActionAdmin), NamespaceID: 1},
			{Action: string(ActionUserAdmin)},
		},
	}

	if z.Allow(ctx, p, ActionAdmin, Resource{NamespacePath: "/"}) {
		t.Fatal("token scopes must not grant namespace admin beyond the user role")
	}

	if z.Allow(ctx, p, ActionUserAdmin, Resource{}) {
		t.Fatal("token scopes must not grant global user admin")
	}
}

func TestHierarchicalRBAC_TokenScopesBreakInheritanceOnAncestorAboveScope(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ns := newMockNamespacesRepo()
	ns.add(1, "root", "/", nil, true) // break inheritance at root
	ns.add(2, "team-a", "/team-a", &[]int64{1}[0], false)
	ns.add(3, "project-1", "/team-a/project-1", &[]int64{2}[0], false)

	// Give the user a base role that allows the action.
	rb := newMockRoleBindingsRepo()
	rb.add(42, 1, RoleViewer)
	z := &HierarchicalRBAC{Namespaces: ns, RoleBindings: rb}
	p := &authn.Principal{
		LocalUserID: 42,
		Username:    "viewer",
		TokenScopes: []authn.TokenScope{
			{Action: string(ActionJobRead), NamespaceID: 2, Propagate: true},
		},
	}

	// Scope granted at /team-a should propagate to /team-a/project-1
	// even though root has break_inheritance, because the scope is
	// explicitly granted below root.
	if !z.Allow(ctx, p, ActionJobRead, Resource{NamespacePath: "/team-a/project-1"}) {
		t.Fatal("propagating scope on team-a should allow access to project-1 despite break_inheritance on root")
	}
}
