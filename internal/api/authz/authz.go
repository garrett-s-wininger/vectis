// Package authz maps HTTP requests to coarse actions and implements Authorizer strategies.
//
// Authentication (api.auth.enabled) is separate from authorization policy: when auth is enabled,
// middleware validates Bearer tokens before consulting [Authorizer.Allow] on API routes.
// The only shipped post-setup policy is [AuthenticatedFull]; [SelectAuthorizer] is the hook
// for future RBAC/DAC/MAC engines. See internal/api/auth_http.go.
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
	ActionSetupStatus   Action = "setup:status"
	ActionSetupComplete Action = "setup:complete"
	ActionAPI           Action = "api:any"
	ActionTokenRead     Action = "token:read"
	ActionTokenWrite    Action = "token:write"
)

const (
	RoleViewer   = "viewer"
	RoleTrigger  = "trigger"
	RoleOperator = "operator"
	RoleAdmin    = "admin"
)

var rolePermissions = map[string][]Action{
	RoleViewer:   {ActionJobRead, ActionRunRead, ActionTokenRead, ActionTokenWrite},
	RoleTrigger:  {ActionJobRead, ActionRunRead, ActionRunTrigger, ActionTokenRead, ActionTokenWrite},
	RoleOperator: {ActionJobRead, ActionRunRead, ActionRunTrigger, ActionRunOperator, ActionTokenRead, ActionTokenWrite},
	RoleAdmin:    {ActionJobRead, ActionJobWrite, ActionRunRead, ActionRunTrigger, ActionRunOperator, ActionAdmin, ActionAPI, ActionTokenRead, ActionTokenWrite},
}

func roleAllows(role string, action Action) bool {
	if role == RoleAdmin {
		return true
	}

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

type Resource struct {
	JobID         string
	NamespacePath string
}

type Authorizer interface {
	Allow(ctx context.Context, p *authn.Principal, action Action, res Resource) bool
}

// AuthenticatedFull allows any authenticated principal for non-setup actions until finer
// policy (RBAC, DAC, etc.) is implemented.
type AuthenticatedFull struct{}

func (AuthenticatedFull) Allow(_ context.Context, p *authn.Principal, action Action, _ Resource) bool {
	switch action {
	case ActionSetupStatus, ActionSetupComplete:
		return true
	case ActionAPI:
		return p != nil
	default:
		return p != nil
	}
}

type SetupPending struct{}

func (SetupPending) Allow(_ context.Context, p *authn.Principal, action Action, _ Resource) bool {
	switch action {
	case ActionSetupStatus, ActionSetupComplete:
		return true
	default:
		return p != nil
	}
}

// AllowAll authorizes every request. Used when API authentication is disabled.
type AllowAll struct{}

func (AllowAll) Allow(_ context.Context, _ *authn.Principal, _ Action, _ Resource) bool {
	return true
}
