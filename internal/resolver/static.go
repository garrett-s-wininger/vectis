package resolver

import (
	"vectis/internal/interfaces"

	"google.golang.org/grpc/resolver"
)

type staticBuilder struct {
	addr   string
	logger interfaces.Logger
}

func (b *staticBuilder) Scheme() string {
	return "static"
}

func (b *staticBuilder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	b.logger.Debug("resolver: using static address %s", b.addr)
	_ = cc.UpdateState(resolver.State{
		Addresses: []resolver.Address{{Addr: b.addr}},
	})

	return &staticResolver{cc: cc}, nil
}

type staticResolver struct {
	cc resolver.ClientConn
}

func (r *staticResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *staticResolver) Close() {}
