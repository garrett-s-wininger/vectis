package authz

import (
	"context"
	"slices"

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

	// Scoped tokens can only reduce a principal's permissions, never expand them.
	if len(p.TokenScopes) > 0 {
		return r.baseAllow(ctx, p, action, res) && r.tokenScopesAllow(ctx, p, action, res)
	}

	return r.baseAllow(ctx, p, action, res)
}

func (r *HierarchicalRBAC) baseAllow(ctx context.Context, p *authn.Principal, action Action, res Resource) bool {
	namespacePath := res.NamespacePath
	if namespacePath == "" {
		return r.allowNonNamespacedAction(ctx, p.LocalUserID, action)
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

func (r *HierarchicalRBAC) allowNonNamespacedAction(ctx context.Context, localUserID int64, action Action) bool {
	switch action {
	case ActionAPI:
		return true
	case ActionUserAdmin:
		return r.hasRootRole(ctx, localUserID, RoleAdmin)
	default:
		return r.hasActionAnywhere(ctx, localUserID, action)
	}
}

func (r *HierarchicalRBAC) tokenScopesAllow(ctx context.Context, p *authn.Principal, action Action, res Resource) bool {
	if res.NamespacePath == "" {
		for _, scope := range p.TokenScopes {
			if scope.Action != string(action) {
				continue
			}

			if action == ActionUserAdmin {
				if scope.NamespaceID == 0 {
					return true
				}

				continue
			}

			return true
		}

		return false
	}

	// For namespaced resources, check if the scope applies to this namespace
	paths := dal.ParseNamespacePath(res.NamespacePath)

	// Get the current (target) namespace
	currentPath := paths[len(paths)-1]
	currentNs, err := r.Namespaces.GetByPath(ctx, currentPath)
	if err != nil {
		return false
	}

	for _, scope := range p.TokenScopes {
		if scope.Action != string(action) {
			continue
		}

		// Scope with no namespace ID applies globally
		if scope.NamespaceID == 0 {
			return true
		}

		// Exact match on current namespace (always allowed)
		if scope.NamespaceID == currentNs.ID {
			return true
		}

		// Non-propagating scope: only exact match allowed, skip
		if !scope.Propagate {
			continue
		}

		// Propagating scope: check if scope's namespace is an ancestor of current
		for _, ancestorPath := range paths {
			ancestorNs, err := r.Namespaces.GetByPath(ctx, ancestorPath)
			if err != nil {
				return false
			}

			if ancestorNs.ID != scope.NamespaceID {
				continue
			}

			// Found the scope's namespace as an ancestor
			// Check if inheritance is broken strictly between ancestor and current
			broken := false
			started := false
			for _, intermediatePath := range paths {
				if intermediatePath == ancestorPath {
					started = true
					continue
				}

				if !started {
					continue
				}

				intermediateNs, err := r.Namespaces.GetByPath(ctx, intermediatePath)
				if err != nil {
					return false
				}

				if intermediateNs.BreakInheritance {
					broken = true
					break
				}

				if intermediatePath == currentPath {
					break
				}
			}

			if !broken {
				return true
			}
		}
	}

	return false
}

func (r *HierarchicalRBAC) hasRootRole(ctx context.Context, localUserID int64, role string) bool {
	root, err := r.Namespaces.GetByPath(ctx, "/")
	if err != nil {
		return false
	}

	roles, err := r.RoleBindings.GetUserRolesInNamespace(ctx, localUserID, root.ID)
	if err != nil {
		return false
	}

	return slices.Contains(roles, role)
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
// DenyAll blocks every request. Used when authorization is misconfigured.
type DenyAll struct{}

func (DenyAll) Allow(_ context.Context, _ *authn.Principal, _ Action, _ Resource) bool {
	return false
}

// Before initial setup completes, only [SetupPending] applies. After setup:
//   - "hierarchical_rbac" uses [HierarchicalRBAC] when repositories are available.
//   - "authenticated_full" uses [AuthenticatedFull] (any authenticated principal).
//   - Any other configuration falls back to [DenyAll] to fail closed.
func SelectAuthorizer(setupComplete bool, engine string, namespaces dal.NamespacesRepository, roleBindings dal.RoleBindingsRepository) Authorizer {
	if !setupComplete {
		return SetupPending{}
	}

	if engine == "hierarchical_rbac" {
		if namespaces != nil && roleBindings != nil {
			return &HierarchicalRBAC{
				Namespaces:   namespaces,
				RoleBindings: roleBindings,
			}
		}

		return DenyAll{}
	}

	if engine == "authenticated_full" {
		return AuthenticatedFull{}
	}

	return DenyAll{}
}
