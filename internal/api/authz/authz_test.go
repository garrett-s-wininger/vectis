package authz

import (
	"context"
	"testing"

	"vectis/internal/api/authn"
)

func TestSelectAuthorizer(t *testing.T) {
	t.Parallel()

	if _, ok := SelectAuthorizer(false, "", nil, nil).(SetupPending); !ok {
		t.Fatal("before setup -> SetupPending")
	}

	if _, ok := SelectAuthorizer(true, "", nil, nil).(AuthenticatedFull); !ok {
		t.Fatal("after setup with no engine -> AuthenticatedFull")
	}

	if _, ok := SelectAuthorizer(true, "authenticated_full", nil, nil).(AuthenticatedFull); !ok {
		t.Fatal("authenticated_full -> AuthenticatedFull")
	}

	if _, ok := SelectAuthorizer(true, "hierarchical_rbac", nil, nil).(AuthenticatedFull); !ok {
		t.Fatal("hierarchical_rbac without repos -> AuthenticatedFull")
	}
}

func allDistinctActions() []Action {
	return []Action{
		ActionJobRead,
		ActionJobWrite,
		ActionRunTrigger,
		ActionRunRead,
		ActionRunOperator,
		ActionAdmin,
		ActionUserAdmin,
		ActionSetupStatus,
		ActionSetupComplete,
		ActionAPI,
	}
}

func TestAuthorizers_Allow_matrix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	user := &authn.Principal{Username: "u", Kind: authn.KindLocalUser}

	for _, action := range allDistinctActions() {
		isSetup := action == ActionSetupStatus || action == ActionSetupComplete

		t.Run("SetupPending/"+string(action), func(t *testing.T) {
			t.Parallel()

			var z SetupPending
			gotNil := z.Allow(ctx, nil, action, Resource{})
			gotUser := z.Allow(ctx, user, action, Resource{})

			switch {
			case isSetup:
				if !gotNil || !gotUser {
					t.Fatalf("setup actions must allow with or without principal: nil=%v user=%v", gotNil, gotUser)
				}
			default:
				if gotNil {
					t.Fatal("must deny nil principal for non-setup actions")
				}
				if !gotUser {
					t.Fatal("must allow authenticated principal")
				}
			}
		})

		t.Run("AuthenticatedFull/"+string(action), func(t *testing.T) {
			t.Parallel()

			var z AuthenticatedFull
			gotNil := z.Allow(ctx, nil, action, Resource{})
			gotUser := z.Allow(ctx, user, action, Resource{})

			switch {
			case isSetup:
				if !gotNil || !gotUser {
					t.Fatalf("setup: nil=%v user=%v", gotNil, gotUser)
				}
			default:
				if gotNil {
					t.Fatal("must deny nil principal")
				}
				if !gotUser {
					t.Fatal("must allow authenticated principal")
				}
			}
		})
	}
}

func TestAuthenticatedFull_TokenScopes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	z := AuthenticatedFull{}

	pScoped := &authn.Principal{
		LocalUserID: 1,
		Username:    "scoped",
		TokenScopes: []authn.TokenScope{
			{Action: string(ActionJobRead)},
		},
	}

	if !z.Allow(ctx, pScoped, ActionJobRead, Resource{}) {
		t.Fatal("scoped token should allow matching action")
	}

	if z.Allow(ctx, pScoped, ActionJobWrite, Resource{}) {
		t.Fatal("scoped token should deny non-matching action")
	}
}

func TestActionSupportsNamespace(t *testing.T) {
	t.Parallel()

	if !ActionSupportsNamespace(ActionJobRead) {
		t.Fatal("job:read should support namespace scoping")
	}

	if ActionSupportsNamespace(ActionUserAdmin) {
		t.Fatal("user:admin should not support namespace scoping")
	}
}
