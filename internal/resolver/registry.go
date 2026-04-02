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
	return newRegistryResolver(
		cc, b.reg, b.comp, b.logger,
		config.RegistryResolverPollInterval(),
		config.RegistryResolverPollTimeout(),
		config.RegistryResolverErrorRefresh(),
	), nil
}

type registryResolver struct {
	cc                      resolver.ClientConn
	client                  registryAddressGetter
	comp                    api.Component
	logger                  interfaces.Logger
	refreshInterval         time.Duration
	pollTimeout             time.Duration
	errorRefresh            time.Duration
	mu                      sync.Mutex
	closed                  bool
	lastGood                string
	resolveFailureAnnounced bool
	done                    chan struct{}
	closeOnce               sync.Once
}

func newRegistryResolver(cc resolver.ClientConn, client registryAddressGetter, comp api.Component, logger interfaces.Logger, refreshInterval, pollTimeout, errorRefresh time.Duration) *registryResolver {
	r := &registryResolver{
		cc: cc, client: client, comp: comp, logger: logger,
		refreshInterval: refreshInterval,
		pollTimeout:     pollTimeout,
		errorRefresh:    errorRefresh,
		done:            make(chan struct{}),
	}

	logger.Debug("resolver: starting registry resolver for %s (interval=%v poll_timeout=%v error_refresh=%v)", comp.String(), refreshInterval, pollTimeout, errorRefresh)
	firstWait := r.resolveNow()
	go r.pollLoop(firstWait)

	return r
}

func (r *registryResolver) pollLoop(initialWait time.Duration) {
	next := time.NewTimer(sanitizeResolverWait(r, initialWait))
	defer next.Stop()

	for {
		select {
		case <-r.done:
			return
		case <-next.C:
			wait := r.resolveNow()
			wait = sanitizeResolverWait(r, wait)

			if !next.Stop() {
				select {
				case <-next.C:
				default:
				}
			}

			next.Reset(wait)
		}
	}
}

func sanitizeResolverWait(r *registryResolver, wait time.Duration) time.Duration {
	if wait > 0 {
		return wait
	}

	if r.refreshInterval > 0 {
		return r.refreshInterval
	}

	return 10 * time.Second
}

func (r *registryResolver) resolveNow() time.Duration {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return r.refreshInterval
	}
	r.mu.Unlock()

	ctx := context.Background()
	if r.pollTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.pollTimeout)
		defer cancel()
	}

	addr, err := r.client.Address(ctx, r.comp)
	if err != nil {
		r.mu.Lock()
		if r.lastGood != "" {
			lg := r.lastGood
			r.mu.Unlock()
			r.logger.Debug("resolver: registry lookup failed for %s (keeping last known good %s): %v", r.comp.String(), lg, err)
			_ = r.cc.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: lg}}})
			return r.refreshInterval
		}

		if !r.resolveFailureAnnounced {
			r.resolveFailureAnnounced = true
			r.mu.Unlock()
			r.logger.Warn("resolver: %s not yet reachable via registry (retries at debug; ensure registry and service are up)", r.comp.String())
		} else {
			r.mu.Unlock()
			r.logger.Debug("resolver: failed to resolve %s: %v", r.comp.String(), err)
		}

		r.cc.ReportError(err)
		if r.errorRefresh > 0 {
			return r.errorRefresh
		}

		return r.refreshInterval
	}

	r.mu.Lock()
	if addr != r.lastGood {
		r.logger.Debug("resolver: %s resolved to %s", r.comp.String(), addr)
	}

	r.resolveFailureAnnounced = false
	r.lastGood = addr
	r.mu.Unlock()

	_ = r.cc.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: addr}}})
	return r.refreshInterval
}

func (r *registryResolver) ResolveNow(resolver.ResolveNowOptions) {
	_ = r.resolveNow()
}

func (r *registryResolver) Close() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	r.closeOnce.Do(func() { close(r.done) })
}
