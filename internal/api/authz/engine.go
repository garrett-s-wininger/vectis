package authz

import (
	"context"

	"vectis/internal/api/authn"
	"vectis/internal/dal"
)

// HierarchicalRBAC implements namespace-scoped role-based access control with inheritance.
// Permissions flow down the namespace tree unless break_inheritance is set on a namespace.
type HierarchicalRBAC struct {
	Namespaces   dal.NamespacesRepository
	RoleBindings dal.RoleBindingsRepository
}

func (r *HierarchicalRBAC) Allow(ctx context.Context, p *authn.Principal, action Action, res Resource) bool {
	switch action {
	case ActionSetupStatus, ActionSetupComplete:
		return true
	}

	if p == nil {
		return false
	}

	namespacePath := res.NamespacePath
	if namespacePath == "" {
		return r.hasActionAnywhere(ctx, p.LocalUserID, action)
	}

	paths := dal.ParseNamespacePath(namespacePath)

	var allowed bool
	for _, path := range paths {
		ns, err := r.Namespaces.GetByPath(ctx, path)
		if err != nil {
			return false
		}

		if ns.BreakInheritance {
			allowed = false
		}

		roles, err := r.RoleBindings.GetUserRolesInNamespace(ctx, p.LocalUserID, ns.ID)
		if err != nil {
			return false
		}

		for _, role := range roles {
			if roleAllows(role, action) {
				allowed = true
			}
		}
	}

	return allowed
}

func (r *HierarchicalRBAC) hasActionAnywhere(ctx context.Context, localUserID int64, action Action) bool {
	bindings, err := r.RoleBindings.ListByUser(ctx, localUserID)
	if err != nil {
		return false
	}

	for _, b := range bindings {
		if roleAllows(b.Role, action) {
			return true
		}
	}

	return false
}

// SelectAuthorizer returns the authorizer for the current instance state.
// Before initial setup completes, only [SetupPending] applies. After setup, it uses
// [HierarchicalRBAC] when namespace and role binding repositories are available;
// otherwise it falls back to [AuthenticatedFull].
func SelectAuthorizer(setupComplete bool, namespaces dal.NamespacesRepository, roleBindings dal.RoleBindingsRepository) Authorizer {
	if !setupComplete {
		return SetupPending{}
	}

	if namespaces != nil && roleBindings != nil {
		return &HierarchicalRBAC{
			Namespaces:   namespaces,
			RoleBindings: roleBindings,
		}
	}

	return AuthenticatedFull{}
}
