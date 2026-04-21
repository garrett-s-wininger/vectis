package authn

import "context"

type ctxKey int

const principalCtxKey ctxKey = 1

func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, principalCtxKey, p)
}

func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	v := ctx.Value(principalCtxKey)
	if v == nil {
		return nil, false
	}

	p, ok := v.(*Principal)
	if !ok || p == nil {
		return nil, false
	}

	return p, true
}
