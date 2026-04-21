package authz

import (
	"context"
	"net/http"
	"testing"

	"vectis/internal/api/authn"
)

func TestActionForRequest(t *testing.T) {
	t.Parallel()

	cases := []struct {
		method string
		path   string
		want   Action
	}{
		{http.MethodGet, "/api/v1/setup/status", ActionSetupStatus},
		{http.MethodPost, "/api/v1/setup/complete", ActionSetupComplete},
		{http.MethodGet, "/api/v1/jobs", ActionAPI},
		{http.MethodPost, "/api/v1/jobs/run", ActionAPI},
	}

	for _, tc := range cases {
		req, err := http.NewRequest(tc.method, tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}

		if got := ActionForRequest(req); got != tc.want {
			t.Fatalf("%s %s: got %q want %q", tc.method, tc.path, got, tc.want)
		}
	}
}

func TestSelectAuthorizer(t *testing.T) {
	t.Parallel()

	if _, ok := SelectAuthorizer(false).(SetupPending); !ok {
		t.Fatal("before setup -> SetupPending")
	}

	if _, ok := SelectAuthorizer(true).(AuthenticatedFull); !ok {
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
