package authn

import (
	"context"
	"testing"
)

func TestPrincipalFromContext_roundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	p := &Principal{LocalUserID: 42, Username: "u", Kind: KindLocalUser}

	ctx = WithPrincipal(ctx, p)
	got, ok := PrincipalFromContext(ctx)
	if !ok || got == nil {
		t.Fatalf("expected principal in context")
	}

	if got.LocalUserID != 42 || got.Username != "u" || got.Kind != KindLocalUser {
		t.Fatalf("principal mismatch: %+v", got)
	}
}

func TestPrincipalFromContext_absent(t *testing.T) {
	t.Parallel()

	if _, ok := PrincipalFromContext(context.Background()); ok {
		t.Fatal("expected no principal")
	}
}

func TestPrincipalFromContext_nilValue(t *testing.T) {
	t.Parallel()

	ctx := context.WithValue(context.Background(), principalCtxKey, (*Principal)(nil))
	if _, ok := PrincipalFromContext(ctx); ok {
		t.Fatal("expected nil principal to count as absent")
	}
}
