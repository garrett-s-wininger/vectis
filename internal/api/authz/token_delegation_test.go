package authz

import (
	"testing"
	"vectis/internal/api/authn"
)

func TestTokenScopeDelegationBounds(t *testing.T) {
	t.Parallel()
	rootID, teamID := int64(1), int64(2)

	ns := newMockNamespacesRepo()
	ns.add(rootID, "root", "/", nil, false)
	ns.add(teamID, "team", "/team", &rootID, false)
	ns.add(3, "child", "/team/child", &teamID, false)
	ns.add(4, "isolated", "/team/isolated", &teamID, true)
	ns.add(5, "other", "/other", &rootID, false)
	scope := func(id int64, propagate bool) authn.TokenScope {
		return authn.TokenScope{Action: string(ActionJobRead), NamespaceID: id, Propagate: propagate}
	}

	for _, tc := range []struct {
		name          string
		parent, child authn.TokenScope
		want          bool
	}{
		{"exact grant", scope(2, false), scope(2, false), true},
		{"cannot add propagation", scope(2, false), scope(2, true), false},
		{"cannot enter child", scope(2, false), scope(3, false), false},
		{"may narrow propagation", scope(2, true), scope(2, false), true},
		{"may delegate subtree", scope(2, true), scope(3, true), true},
		{"may narrow to child", scope(2, true), scope(3, false), true},
		{"inheritance break", scope(2, true), scope(4, true), false},
		{"exact grant at break", scope(4, true), scope(4, true), true},
		{"cannot enter sibling", scope(2, true), scope(5, false), false},
		{"cannot grant ancestor", scope(3, true), scope(2, true), false},
		{"cannot grant global", scope(2, true), scope(0, true), false},
		{"legacy global covers subtree", scope(0, false), scope(2, true), true},
		{"missing namespace fails closed", scope(2, true), scope(999, false), false},
		{"cannot add action", scope(2, true), authn.TokenScope{Action: string(ActionJobWrite), NamespaceID: 2}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &authn.Principal{TokenScopes: []authn.TokenScope{tc.parent}}
			if got := CanDelegateTokenScope(t.Context(), p, tc.child, ns); got != tc.want {
				t.Fatalf("delegation allowed=%v want=%v", got, tc.want)
			}
		})
	}

	if CanDelegateTokenScope(t.Context(), nil, scope(2, false), ns) {
		t.Fatal("anonymous delegation allowed")
	}
}
