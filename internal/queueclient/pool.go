package queueclient

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/queueid"
	"vectis/internal/registry"
	"vectis/internal/resolver"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const poolDequeuePollInterval = 250 * time.Millisecond

type QueuePoolOptions struct {
	PinnedAddress             string
	RegistryAddress           string
	RetryMetrics              backoff.RetryMetrics
	RefreshInterval           time.Duration
	DequeueSupportedIsolation []string
}

type ManagingQueuePoolService struct {
	pool *queuePool
}

func NewManagingQueuePoolService(ctx context.Context, logger interfaces.Logger, opts QueuePoolOptions) (*ManagingQueuePoolService, error) {
	pool, err := newQueuePool(ctx, logger, opts)
	if err != nil {
		return nil, err
	}

	return &ManagingQueuePoolService{pool: pool}, nil
}

func (m *ManagingQueuePoolService) Enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error) {
	return m.pool.enqueue(ctx, req)
}

func (m *ManagingQueuePoolService) GRPCConnectivityState() connectivity.State {
	return m.pool.connectivityState()
}

func (m *ManagingQueuePoolService) Close() error {
	return m.pool.close()
}

type ManagingQueuePoolClient struct {
	pool *queuePool
}

func NewManagingQueuePoolClient(ctx context.Context, logger interfaces.Logger, opts QueuePoolOptions) (*ManagingQueuePoolClient, error) {
	pool, err := newQueuePool(ctx, logger, opts)
	if err != nil {
		return nil, err
	}

	return &ManagingQueuePoolClient{pool: pool}, nil
}

func (m *ManagingQueuePoolClient) Enqueue(ctx context.Context, req *api.JobRequest) error {
	_, err := m.pool.enqueue(ctx, req)
	return err
}

func (m *ManagingQueuePoolClient) Dequeue(ctx context.Context) (*api.JobRequest, error) {
	return m.pool.dequeue(ctx)
}

func (m *ManagingQueuePoolClient) TryDequeue(ctx context.Context) (*api.JobRequest, error) {
	return m.pool.tryDequeue(ctx)
}

func (m *ManagingQueuePoolClient) Ack(ctx context.Context, deliveryID string) error {
	return m.pool.ack(ctx, deliveryID)
}

func (m *ManagingQueuePoolClient) Close() error {
	return m.pool.close()
}

type queuePool struct {
	logger   interfaces.Logger
	opts     QueuePoolOptions
	registry *registry.Registry
	dial     queueEndpointDialer

	mu        sync.RWMutex
	endpoints map[string]*queuePoolEndpoint
	activeIDs []string
	active    []*queuePoolEndpoint

	enqueueCounter atomic.Uint64
	dequeueCounter atomic.Uint64
	dequeueRequest *api.DequeueRequest
	cancelFn       context.CancelFunc
}

type queuePoolEndpoint struct {
	id      string
	address string
	conn    *grpc.ClientConn
	client  api.QueueServiceClient
	cleanup func()

	inflight atomic.Int64
}

type queueEndpointDialer func(context.Context, string, string) (*queuePoolEndpoint, error)

type desiredQueueEndpoint struct {
	id      string
	address string
}

func newQueuePool(ctx context.Context, logger interfaces.Logger, opts QueuePoolOptions) (*queuePool, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = config.RegistryResolverPollInterval()
	}

	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = 10 * time.Second
	}

	p := &queuePool{
		logger:    logger,
		opts:      opts,
		endpoints: make(map[string]*queuePoolEndpoint),
	}

	p.setDequeueSupportedIsolation(opts.DequeueSupportedIsolation)
	p.dial = p.dialEndpoint

	if opts.PinnedAddress == "" {
		reg, err := resolver.NewRegistryClient(ctx, opts.RegistryAddress, logger, interfaces.SystemClock{}, opts.RetryMetrics)
		if err != nil {
			return nil, fmt.Errorf("registry client: %w", err)
		}

		p.registry = reg
	}

	if err := p.refresh(ctx); err != nil {
		_ = p.close()
		return nil, err
	}

	watchCtx, cancel := context.WithCancel(ctx)
	p.cancelFn = cancel
	go p.refreshLoop(watchCtx)

	return p, nil
}

func (p *queuePool) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(p.opts.RefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCtx := context.Background()
			if timeout := config.RegistryResolverPollTimeout(); timeout > 0 {
				var cancel context.CancelFunc
				refreshCtx, cancel = context.WithTimeout(refreshCtx, timeout)
				err := p.refresh(refreshCtx)

				cancel()
				if err != nil {
					p.logger.Debug("queue pool refresh failed: %v", err)
				}

				continue
			}

			if err := p.refresh(refreshCtx); err != nil {
				p.logger.Debug("queue pool refresh failed: %v", err)
			}
		}
	}
}

func (p *queuePool) refresh(ctx context.Context) error {
	desired, err := p.resolveDesired(ctx)
	if err != nil {
		if p.hasActiveEndpoints() {
			return err
		}

		return fmt.Errorf("resolve queue pool: %w", err)
	}

	p.mu.RLock()
	existing := make(map[string]*queuePoolEndpoint, len(p.endpoints))
	maps.Copy(existing, p.endpoints)
	p.mu.RUnlock()

	replacements := make(map[string]*queuePoolEndpoint)
	var firstErr error
	for _, d := range desired {
		if ep := existing[d.id]; ep != nil && ep.address == d.address {
			continue
		}

		ep, err := p.connectEndpoint(ctx, d.id, d.address)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}

			p.logger.Warn("queue pool: failed to connect to %s at %s: %v", d.id, d.address, err)
			continue
		}

		replacements[d.id] = ep
	}

	activeIDs := make([]string, 0, len(desired))
	activeEndpoints := make([]*queuePoolEndpoint, 0, len(desired))
	for _, d := range desired {
		if replacements[d.id] != nil {
			activeIDs = append(activeIDs, d.id)
			activeEndpoints = append(activeEndpoints, replacements[d.id])
			continue
		}

		if ep := existing[d.id]; ep != nil && ep.address == d.address {
			activeIDs = append(activeIDs, d.id)
			activeEndpoints = append(activeEndpoints, ep)
		}
	}

	sort.Strings(activeIDs)
	sort.Slice(activeEndpoints, func(i, j int) bool {
		return activeEndpoints[i].id < activeEndpoints[j].id
	})

	p.mu.Lock()
	for id, ep := range replacements {
		if old := p.endpoints[id]; old != nil {
			old.close()
		}

		p.endpoints[id] = ep
	}

	p.activeIDs = activeIDs
	p.active = activeEndpoints
	activeCount := len(p.activeIDs)
	totalCount := len(p.endpoints)
	p.mu.Unlock()

	if activeCount == 0 {
		if firstErr != nil {
			return firstErr
		}

		return fmt.Errorf("no queue endpoints available")
	}

	p.logger.Debug("queue pool has %d active endpoint(s), %d known endpoint(s)", activeCount, totalCount)
	return nil
}

func (p *queuePool) resolveDesired(ctx context.Context) ([]desiredQueueEndpoint, error) {
	if p.opts.PinnedAddress != "" {
		return []desiredQueueEndpoint{{
			id:      "pinned",
			address: p.opts.PinnedAddress,
		}}, nil
	}

	if p.registry == nil {
		return nil, fmt.Errorf("registry client is required")
	}

	entries, err := p.registry.ListRegistrations(ctx, api.Component_COMPONENT_QUEUE, registry.QueueIngressMetadataForCell(config.CellID()))
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		entries, err = p.registry.ListRegistrations(ctx, api.Component_COMPONENT_QUEUE, nil)
		if err != nil {
			return nil, err
		}
	}

	seen := make(map[string]desiredQueueEndpoint, len(entries))
	for _, entry := range entries {
		address := entry.GetAddress()
		if address == "" {
			continue
		}

		id := entry.GetInstanceId()
		if id == "" {
			id = address
		}

		seen[id] = desiredQueueEndpoint{id: id, address: address}
	}

	out := make([]desiredQueueEndpoint, 0, len(seen))
	for _, endpoint := range seen {
		out = append(out, endpoint)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].id < out[j].id
	})

	if len(out) == 0 {
		return nil, fmt.Errorf("no queue registrations available")
	}

	return out, nil
}

func (p *queuePool) connectEndpoint(ctx context.Context, id, address string) (*queuePoolEndpoint, error) {
	if p.dial != nil {
		return p.dial(ctx, id, address)
	}

	return p.dialEndpoint(ctx, id, address)
}

func (p *queuePool) dialEndpoint(ctx context.Context, id, address string) (*queuePoolEndpoint, error) {
	conn, cleanup, err := resolver.NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_QUEUE, address, p.logger, nil, p.opts.RetryMetrics)
	if err != nil {
		return nil, err
	}

	return &queuePoolEndpoint{
		id:      id,
		address: address,
		conn:    conn,
		client:  api.NewQueueServiceClient(conn),
		cleanup: cleanup,
	}, nil
}

func (p *queuePool) hasActiveEndpoints() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.activeIDs) > 0
}

func (p *queuePool) snapshotActiveEndpoints() []*queuePoolEndpoint {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.active) > 0 {
		return p.active
	}

	out := make([]*queuePoolEndpoint, 0, len(p.activeIDs))
	for _, id := range p.activeIDs {
		if ep := p.endpoints[id]; ep != nil {
			out = append(out, ep)
		}
	}

	return out
}

func (p *queuePool) endpointByID(id string) *queuePoolEndpoint {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.endpoints[id]
}

func (p *queuePool) chooseEndpoint() (*queuePoolEndpoint, error) {
	return p.chooseEndpointFrom(p.snapshotActiveEndpoints())
}

func (p *queuePool) chooseEndpointFrom(endpoints []*queuePoolEndpoint) (*queuePoolEndpoint, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no queue endpoints available")
	}

	if len(endpoints) == 1 {
		return endpoints[0], nil
	}

	n := uint64(len(endpoints))
	next := p.enqueueCounter.Add(1)
	a := endpoints[int(next%n)]
	b := endpoints[int((next*1103515245+12345)%n)]
	if a == b {
		b = endpoints[(int(next)+1)%len(endpoints)]
	}

	if b.inflight.Load() < a.inflight.Load() {
		return b, nil
	}

	return a, nil
}

func (p *queuePool) rotatedActiveEndpointsFrom(endpoints []*queuePoolEndpoint) ([]*queuePoolEndpoint, int) {
	if len(endpoints) <= 1 {
		return endpoints, 0
	}

	start := int(p.dequeueCounter.Add(1) % uint64(len(endpoints)))
	return endpoints, start
}

func (p *queuePool) enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error) {
	endpoints := p.snapshotActiveEndpoints()
	if len(endpoints) == 0 {
		if err := p.refresh(ctx); err != nil {
			return nil, err
		}

		endpoints = p.snapshotActiveEndpoints()
	}

	var lastErr error
	attempts := max(len(endpoints), 1)

	if attempts > 2 {
		attempts = 2
	}

	for range attempts {
		ep, err := p.chooseEndpointFrom(endpoints)
		if err != nil {
			return nil, err
		}

		ep.inflight.Add(1)
		empty, err := ep.client.Enqueue(ctx, req)
		ep.inflight.Add(-1)
		if err == nil {
			return empty, nil
		}

		lastErr = err
		if !IsTransientRPCError(err) {
			return empty, err
		}

		if rerr := p.reconnectEndpoint(ctx, ep.id); rerr != nil {
			p.logger.Debug("queue pool reconnect to %s failed: %v", ep.id, rerr)
		}

		endpoints = p.snapshotActiveEndpoints()
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("enqueue failed without a queue endpoint error")
	}

	return nil, lastErr
}

func (p *queuePool) dequeue(ctx context.Context) (*api.JobRequest, error) {
	for {
		req, err := p.tryDequeue(ctx)
		if err != nil || req != nil {
			return req, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(poolDequeuePollInterval):
		}
	}
}

func (p *queuePool) tryDequeue(ctx context.Context) (*api.JobRequest, error) {
	endpoints := p.snapshotActiveEndpoints()
	if len(endpoints) == 0 {
		if err := p.refresh(ctx); err != nil {
			return nil, err
		}

		endpoints = p.snapshotActiveEndpoints()
	}

	endpoints, start := p.rotatedActiveEndpointsFrom(endpoints)
	var lastErr error
	sawReachable := false

	for offset := range endpoints {
		ep := endpoints[(start+offset)%len(endpoints)]
		req, err := p.tryDequeueEndpoint(ctx, ep)
		if err == nil {
			sawReachable = true
			if req != nil && req.GetJob() != nil {
				return req, nil
			}

			continue
		}

		lastErr = err
		if IsTransientRPCError(err) {
			if rerr := p.reconnectEndpoint(ctx, ep.id); rerr != nil {
				p.logger.Debug("queue pool reconnect to %s failed: %v", ep.id, rerr)
			}

			continue
		}
	}

	if !sawReachable && lastErr != nil {
		return nil, lastErr
	}

	return nil, nil
}

func (p *queuePool) tryDequeueEndpoint(ctx context.Context, ep *queuePoolEndpoint) (*api.JobRequest, error) {
	return ep.client.TryDequeue(ctx, p.dequeueRequest)
}

func (p *queuePool) ack(ctx context.Context, deliveryID string) error {
	instanceID, _, ok := queueid.Decode(deliveryID)
	if ok {
		ep := p.endpointByID(instanceID)
		if ep == nil {
			ep = p.singlePinnedEndpoint()
		}

		if ep == nil {
			_ = p.refresh(ctx)
			ep = p.endpointByID(instanceID)
		}

		if ep == nil {
			ep = p.singlePinnedEndpoint()
		}

		if ep == nil {
			return fmt.Errorf("queue endpoint %q not available for delivery %q", instanceID, deliveryID)
		}

		return p.ackEndpoint(ctx, ep, deliveryID)
	}

	endpoints := p.snapshotActiveEndpoints()
	if len(endpoints) == 0 {
		return fmt.Errorf("no queue endpoints available")
	}

	var lastErr error
	for _, ep := range endpoints {
		if err := p.ackEndpoint(ctx, ep, deliveryID); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

func (p *queuePool) singlePinnedEndpoint() *queuePoolEndpoint {
	if strings.TrimSpace(p.opts.PinnedAddress) == "" {
		return nil
	}

	endpoints := p.snapshotActiveEndpoints()
	if len(endpoints) != 1 {
		return nil
	}

	return endpoints[0]
}

func (p *queuePool) ackEndpoint(ctx context.Context, ep *queuePoolEndpoint, deliveryID string) error {
	_, err := ep.client.Ack(ctx, &api.AckRequest{DeliveryId: &deliveryID})
	if err == nil || !IsTransientRPCError(err) {
		return err
	}

	if rerr := p.reconnectEndpoint(ctx, ep.id); rerr != nil {
		p.logger.Debug("queue pool reconnect to %s failed: %v", ep.id, rerr)
		return err
	}

	ep = p.endpointByID(ep.id)
	if ep == nil {
		return err
	}

	_, err = ep.client.Ack(ctx, &api.AckRequest{DeliveryId: &deliveryID})
	return err
}

func (p *queuePool) reconnectEndpoint(ctx context.Context, id string) error {
	current := p.endpointByID(id)
	if current == nil {
		return fmt.Errorf("queue endpoint %q not found", id)
	}

	replacement, err := p.connectEndpoint(ctx, current.id, current.address)
	if err != nil {
		return err
	}

	p.mu.Lock()
	old := p.endpoints[id]
	p.endpoints[id] = replacement
	if len(p.active) > 0 {
		active := append([]*queuePoolEndpoint(nil), p.active...)
		for i, ep := range active {
			if ep != nil && ep.id == id {
				active[i] = replacement
				break
			}
		}

		p.active = active
	}
	p.mu.Unlock()

	if old != nil {
		old.close()
	}

	return nil
}

func (p *queuePool) connectivityState() connectivity.State {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.activeIDs) == 0 {
		return connectivity.Shutdown
	}

	best := connectivity.Shutdown
	for _, id := range p.activeIDs {
		ep := p.endpoints[id]
		if ep == nil || ep.conn == nil {
			continue
		}

		state := ep.conn.GetState()
		if state == connectivity.Ready {
			return connectivity.Ready
		}

		if best == connectivity.Shutdown {
			best = state
		}
	}

	return best
}

func (p *queuePool) close() error {
	if p.cancelFn != nil {
		p.cancelFn()
		p.cancelFn = nil
	}

	if p.registry != nil {
		_ = p.registry.Close()
		p.registry = nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for _, ep := range p.endpoints {
		ep.close()
	}

	p.endpoints = make(map[string]*queuePoolEndpoint)
	p.activeIDs = nil
	p.active = nil
	return nil
}

func (e *queuePoolEndpoint) close() {
	if e.cleanup != nil {
		e.cleanup()
		e.cleanup = nil
		return
	}

	if e.conn != nil {
		_ = e.conn.Close()
	}
}

func normalizeDequeueSupportedIsolation(levels []string) []string {
	if len(levels) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(levels))
	out := make([]string, 0, len(levels))
	for _, level := range levels {
		level = strings.ToLower(strings.TrimSpace(level))
		if level == "" {
			continue
		}

		if _, ok := seen[level]; ok {
			continue
		}

		seen[level] = struct{}{}
		out = append(out, level)
	}

	return out
}

func (p *queuePool) setDequeueSupportedIsolation(levels []string) {
	normalized := normalizeDequeueSupportedIsolation(levels)
	p.opts.DequeueSupportedIsolation = normalized
	if len(normalized) == 0 {
		p.dequeueRequest = &api.DequeueRequest{}
		return
	}

	p.dequeueRequest = &api.DequeueRequest{
		SupportedIsolation: append([]string(nil), normalized...),
	}
}

var _ interfaces.QueueService = (*ManagingQueuePoolService)(nil)
var _ interfaces.QueueClient = (*ManagingQueuePoolClient)(nil)
