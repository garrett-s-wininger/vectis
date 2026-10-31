package authz

import (
	"context"

	"vectis/internal/api/authn"
	"vectis/internal/dal"
)

func CanDelegateTokenScope(ctx context.Context, p *authn.Principal, requested authn.TokenScope, namespaces dal.NamespacesRepository) bool {
	if p == nil {
		return false
	}

	if len(p.TokenScopes) == 0 {
		return true
	}

	eligible := make([]authn.TokenScope, 0, len(p.TokenScopes))
	for _, scope := range p.TokenScopes {
		if scope.Action != requested.Action {
			continue
		}

		if requested.NamespaceID == 0 {
			if scope.NamespaceID == 0 {
				return true
			}

			continue
		}

		if requested.Propagate && scope.NamespaceID != 0 && !scope.Propagate {
			continue
		}

		eligible = append(eligible, scope)
	}

	if requested.NamespaceID == 0 || len(eligible) == 0 || namespaces == nil {
		return false
	}

	ns, err := namespaces.GetByID(ctx, requested.NamespaceID)
	if err != nil {
		return false
	}

	narrowed := *p
	narrowed.TokenScopes = eligible
	engine := &HierarchicalRBAC{Namespaces: namespaces}

	return engine.tokenScopesAllow(ctx, &narrowed, Action(requested.Action), Resource{NamespacePath: ns.Path})
}
