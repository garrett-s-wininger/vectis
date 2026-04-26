// Package authz maps HTTP requests to coarse actions and implements Authorizer strategies.
//
// Authentication (api.auth.enabled) is separate from authorization policy: when auth is enabled,
// middleware validates Bearer tokens before consulting [Authorizer.Allow] on API routes.
// Shipped post-setup policies are [AuthenticatedFull] and [HierarchicalRBAC];
// [SelectAuthorizer] picks the active engine based on configuration. See internal/api/auth_http.go.
package authz

import (
	"context"

	"vectis/internal/api/authn"
)

type Action string

const (
	ActionJobRead       Action = "job:read"
	ActionJobWrite      Action = "job:write"
	ActionRunTrigger    Action = "run:trigger"
	ActionRunRead       Action = "run:read"
	ActionRunOperator   Action = "run:operator"
	ActionAdmin         Action = "admin:*"
	ActionUserAdmin     Action = "user:admin"
	ActionSetupStatus   Action = "setup:status"
	ActionSetupComplete Action = "setup:complete"
	ActionAPI           Action = "api:any"
)

const (
	RoleViewer   = "viewer"
	RoleTrigger  = "trigger"
	RoleOperator = "operator"
	RoleAdmin    = "admin"
)

var rolePermissions = map[string][]Action{
	RoleViewer:   {ActionJobRead, ActionRunRead},
	RoleTrigger:  {ActionJobRead, ActionRunRead, ActionRunTrigger},
	RoleOperator: {ActionJobRead, ActionRunRead, ActionRunTrigger, ActionRunOperator},
	RoleAdmin:    {ActionJobRead, ActionJobWrite, ActionRunRead, ActionRunTrigger, ActionRunOperator, ActionAdmin, ActionAPI},
}

func roleAllows(role string, action Action) bool {
	perms, ok := rolePermissions[role]
	if !ok {
		return false
	}

	for _, a := range perms {
		if a == action {
			return true
		}
	}

	return false
}

func ParseAction(raw string) (Action, bool) {
	action := Action(raw)
	switch action {
	case ActionJobRead,
		ActionJobWrite,
		ActionRunTrigger,
		ActionRunRead,
		ActionRunOperator,
		ActionAdmin,
		ActionUserAdmin,
		ActionSetupStatus,
		ActionSetupComplete,
		ActionAPI:
		return action, true
	default:
		return "", false
	}
}

func ActionSupportsNamespace(action Action) bool {
	switch action {
	case ActionJobRead, ActionJobWrite, ActionRunTrigger, ActionRunRead, ActionRunOperator, ActionAdmin:
		return true
	default:
		return false
	}
}

type Resource struct {
	JobID         string
	NamespacePath string
}

type Authorizer interface {
	Allow(ctx context.Context, p *authn.Principal, action Action, res Resource) bool
}

// AuthenticatedFull allows any authenticated principal for non-setup actions.
// When the principal carries token scopes, the action must be present in the scopes.
type AuthenticatedFull struct{}

func (AuthenticatedFull) Allow(_ context.Context, p *authn.Principal, action Action, _ Resource) bool {
	switch action {
	case ActionSetupStatus, ActionSetupComplete:
		return true
	}

	if p == nil {
		return false
	}

	// Scoped tokens can only reduce permissions, never expand them.
	if len(p.TokenScopes) > 0 {
		for _, scope := range p.TokenScopes {
			if scope.Action == string(action) {
				return true
			}
		}

		return false
	}

	return true
}

type SetupPending struct{}

func (SetupPending) Allow(_ context.Context, p *authn.Principal, action Action, _ Resource) bool {
	switch action {
	case ActionSetupStatus, ActionSetupComplete:
		return true
	default:
		return false
	}
}

// AllowAll authorizes every request. Used when API authentication is disabled.
type AllowAll struct{}

func (AllowAll) Allow(_ context.Context, _ *authn.Principal, _ Action, _ Resource) bool {
	return true
}
