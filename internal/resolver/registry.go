package resolver

import (
	"context"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/config"
	"vectis/internal/interfaces"

	"google.golang.org/grpc/resolver"
)

type registryAddressGetter interface {
	Address(ctx context.Context, component api.Component) (string, error)
}

type registryBuilder struct {
	reg    registryAddressGetter
	comp   api.Component
	logger interfaces.Logger
}

func (b *registryBuilder) Scheme() string {
	return grpcResolverScheme(b.comp)
}

func (b *registryBuilder) Build(_ resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	return newRegistryResolver(cc, b.reg, b.comp, b.logger, config.RegistryResolverPollInterval()), nil
}

type registryResolver struct {
	cc              resolver.ClientConn
	client          registryAddressGetter
	comp            api.Component
	logger          interfaces.Logger
	refreshInterval time.Duration
	mu              sync.Mutex
	closed          bool
	lastGood        string
	done            chan struct{}
	closeOnce       sync.Once
}

func newRegistryResolver(cc resolver.ClientConn, client registryAddressGetter, comp api.Component, logger interfaces.Logger, refreshInterval time.Duration) *registryResolver {
	r := &registryResolver{
		cc: cc, client: client, comp: comp, logger: logger,
		refreshInterval: refreshInterval,
		done:            make(chan struct{}),
	}
	logger.Debug("resolver: starting registry resolver for %s (interval=%v)", comp.String(), refreshInterval)
	go r.poll()
	return r
}

func (r *registryResolver) poll() {
	ticker := time.NewTicker(r.refreshInterval)
	defer ticker.Stop()
	r.resolveNow()
	for {
		select {
		case <-r.done:
			return
		case <-ticker.C:
			r.resolveNow()
		}
	}
}

func (r *registryResolver) resolveNow() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	addr, err := r.client.Address(context.Background(), r.comp)
	if err != nil {
		r.logger.Warn("resolver: failed to resolve %s: %v", r.comp.String(), err)
		if r.lastGood != "" {
			r.logger.Info("resolver: %s falling back to last known good: %s", r.comp.String(), r.lastGood)
			_ = r.cc.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: r.lastGood}}})
		}
		return
	}

	if addr != r.lastGood {
		r.logger.Debug("resolver: %s resolved to %s", r.comp.String(), addr)
	}
	r.lastGood = addr
	_ = r.cc.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: addr}}})
}

func (r *registryResolver) ResolveNow(resolver.ResolveNowOptions) {
	r.resolveNow()
}

func (r *registryResolver) Close() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.closeOnce.Do(func() { close(r.done) })
}
