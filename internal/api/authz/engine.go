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

	// If token has scopes, check them first (scopes are a ceiling)
	if len(p.TokenScopes) > 0 {
		return r.tokenScopesAllow(ctx, p, action, res)
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

func (r *HierarchicalRBAC) tokenScopesAllow(ctx context.Context, p *authn.Principal, action Action, res Resource) bool {
	// Check if the requested action is in any token scope
	var actionAllowed bool
	for _, scope := range p.TokenScopes {
		if scope.Action == string(action) {
			actionAllowed = true
			break
		}
	}

	if !actionAllowed {
		return false
	}

	// For non-namespaced resources, any matching scope is sufficient
	if res.NamespacePath == "" {
		return true
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
			// Check if inheritance is broken between ancestor and current
			broken := false
			for _, intermediatePath := range paths {
				if intermediatePath == ancestorPath {
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
