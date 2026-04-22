package authz

import (
	"context"
	"testing"

	"vectis/internal/api/authn"
)

func TestSelectAuthorizer(t *testing.T) {
	t.Parallel()

	if _, ok := SelectAuthorizer(false, nil, nil).(SetupPending); !ok {
		t.Fatal("before setup -> SetupPending")
	}

	if _, ok := SelectAuthorizer(true, nil, nil).(AuthenticatedFull); !ok {
		t.Fatal("after setup -> AuthenticatedFull")
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
