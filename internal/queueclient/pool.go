package queueclient

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
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
const defaultDequeueStickySuccessBudget = 64

var queuePoolSequence atomic.Uint64

type QueuePoolOptions struct {
	PinnedAddress              string
	RegistryAddress            string
	RetryMetrics               backoff.RetryMetrics
	RefreshInterval            time.Duration
	DequeueSupportedIsolation  []string
	DequeueConnectionLimit     int
	DequeueStickySuccessBudget int
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
	if opts.DequeueConnectionLimit <= 0 {
		opts.DequeueConnectionLimit = 1
	}

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

	enqueueCounter         atomic.Uint64
	dequeueCounter         atomic.Uint64
	dequeueRequest         *api.DequeueRequest
	dequeueStickyID        string
	dequeueStickyRemaining int
	dequeueRotateAfterID   string
	cancelFn               context.CancelFunc
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

	if opts.DequeueStickySuccessBudget <= 0 {
		opts.DequeueStickySuccessBudget = defaultDequeueStickySuccessBudget
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

	seed := queuePoolSequence.Add(1)
	p.enqueueCounter.Store(seed * uint64(11400714819323198485))
	p.dequeueCounter.Store(seed)

	if err := p.setDequeueSupportedIsolation(opts.DequeueSupportedIsolation); err != nil {
		return nil, err
	}
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

	if err := p.connectInitialEndpoint(ctx); err != nil {
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

	desiredByID := make(map[string]desiredQueueEndpoint, len(desired))
	activeIDs := make([]string, 0, len(desired))
	activeEndpoints := make([]*queuePoolEndpoint, 0, len(desired))
	for _, d := range desired {
		desiredByID[d.id] = d
	}

	p.mu.Lock()
	for id, ep := range p.endpoints {
		d, ok := desiredByID[id]
		if !ok || d.address != ep.address {
			ep.close()
			delete(p.endpoints, id)
		}
	}

	for _, d := range desired {
		ep := p.endpoints[d.id]
		if ep == nil {
			ep = &queuePoolEndpoint{id: d.id, address: d.address}
			p.endpoints[d.id] = ep
		}

		activeIDs = append(activeIDs, d.id)
		activeEndpoints = append(activeEndpoints, ep)
	}

	p.activeIDs = activeIDs
	p.active = activeEndpoints
	activeCount := len(p.activeIDs)
	totalCount := len(p.endpoints)
	p.mu.Unlock()

	if activeCount == 0 {
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

func (p *queuePool) ensureEndpointConnected(ctx context.Context, ep *queuePoolEndpoint) (*queuePoolEndpoint, error) {
	if ep == nil {
		return nil, fmt.Errorf("queue endpoint is nil")
	}

	if ep.client != nil {
		return ep, nil
	}

	p.mu.RLock()
	current := p.endpoints[ep.id]
	if current != nil && current.address == ep.address && current.client != nil {
		p.mu.RUnlock()
		return current, nil
	}

	id := ep.id
	address := ep.address
	p.mu.RUnlock()

	replacement, err := p.connectEndpoint(ctx, id, address)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	current = p.endpoints[id]
	if current == nil || current.address != address {
		p.mu.Unlock()
		replacement.close()
		return nil, fmt.Errorf("queue endpoint %q is no longer active", id)
	}

	if current.client != nil {
		p.mu.Unlock()
		replacement.close()
		return current, nil
	}

	p.endpoints[id] = replacement
	if len(p.active) > 0 {
		active := append([]*queuePoolEndpoint(nil), p.active...)
		for i, activeEndpoint := range active {
			if activeEndpoint != nil && activeEndpoint.id == id {
				active[i] = replacement
				break
			}
		}

		p.active = active
	}
	p.mu.Unlock()

	current.close()
	return replacement, nil
}

func (p *queuePool) connectInitialEndpoint(ctx context.Context) error {
	endpoints := p.snapshotActiveEndpoints()
	if len(endpoints) == 0 {
		return fmt.Errorf("no queue endpoints available")
	}

	start := 0
	if len(endpoints) > 1 {
		start = int((p.dequeueCounter.Load() + 1) % uint64(len(endpoints)))
	}

	var lastErr error
	for offset := range endpoints {
		ep := endpoints[(start+offset)%len(endpoints)]
		if _, err := p.ensureEndpointConnected(ctx, ep); err != nil {
			lastErr = err
			continue
		}

		return nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no queue endpoints available")
	}

	return lastErr
}

func (p *queuePool) disconnectConnectedEndpointsExcept(keepID string) {
	var closing []*queuePoolEndpoint

	p.mu.Lock()
	for id, ep := range p.endpoints {
		if id == keepID || ep == nil || ep.client == nil {
			continue
		}

		placeholder := &queuePoolEndpoint{id: ep.id, address: ep.address}
		p.endpoints[id] = placeholder
		if len(p.active) > 0 {
			for i, activeEndpoint := range p.active {
				if activeEndpoint != nil && activeEndpoint.id == id {
					p.active[i] = placeholder
					break
				}
			}
		}

		closing = append(closing, ep)
	}
	p.mu.Unlock()

	for _, ep := range closing {
		ep.close()
	}
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

	if p.opts.DequeueConnectionLimit == 1 {
		if start, ok := p.preferredDequeueStartFrom(endpoints); ok {
			return endpoints, start
		}
	}

	start := int(p.dequeueCounter.Add(1) % uint64(len(endpoints)))
	return endpoints, start
}

func (p *queuePool) preferredDequeueStartFrom(endpoints []*queuePoolEndpoint) (int, bool) {
	p.mu.Lock()
	stickyID := p.dequeueStickyID
	rotateAfterID := ""
	if stickyID == "" && p.dequeueRotateAfterID != "" {
		rotateAfterID = p.dequeueRotateAfterID
		p.dequeueRotateAfterID = ""
	}
	p.mu.Unlock()

	if stickyID != "" {
		for i, ep := range endpoints {
			if ep != nil && ep.id == stickyID {
				return i, true
			}
		}
	}

	if rotateAfterID != "" {
		for i, ep := range endpoints {
			if ep != nil && ep.id == rotateAfterID {
				return (i + 1) % len(endpoints), true
			}
		}
	}

	return 0, false
}

func (p *queuePool) recordDequeueSuccessEndpoint(id string) {
	if id == "" {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.dequeueStickyID != id || p.dequeueStickyRemaining <= 0 {
		p.dequeueStickyID = id
		p.dequeueStickyRemaining = p.opts.DequeueStickySuccessBudget
	}

	p.dequeueStickyRemaining--
	if p.dequeueStickyRemaining > 0 {
		return
	}

	p.dequeueStickyRemaining = 0
	p.dequeueRotateAfterID = id
	p.dequeueStickyID = ""
}

func (p *queuePool) clearDequeueStickyEndpoint(id string) {
	p.mu.Lock()
	if id != "" {
		p.dequeueRotateAfterID = id
	}

	if id == "" || p.dequeueStickyID == id {
		p.dequeueStickyID = ""
		p.dequeueStickyRemaining = 0
	}
	p.mu.Unlock()
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

	tried := make(map[string]struct{}, attempts)
	for range attempts {
		candidates := make([]*queuePoolEndpoint, 0, len(endpoints))
		for _, ep := range endpoints {
			if _, ok := tried[ep.id]; ok {
				continue
			}

			candidates = append(candidates, ep)
		}

		if len(candidates) == 0 {
			break
		}

		ep, err := p.chooseEndpointFrom(candidates)
		if err != nil {
			return nil, err
		}

		tried[ep.id] = struct{}{}
		ep, err = p.ensureEndpointConnected(ctx, ep)
		if err != nil {
			lastErr = err
			continue
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
	attempts := len(endpoints)
	if p.opts.DequeueConnectionLimit > 0 && attempts > p.opts.DequeueConnectionLimit {
		attempts = p.opts.DequeueConnectionLimit
	}

	for offset := range attempts {
		ep := endpoints[(start+offset)%len(endpoints)]
		ep, err := p.ensureEndpointConnected(ctx, ep)
		if err != nil {
			lastErr = err
			continue
		}

		if p.opts.DequeueConnectionLimit == 1 {
			p.disconnectConnectedEndpointsExcept(ep.id)
		}

		req, err := p.tryDequeueEndpoint(ctx, ep)
		if err == nil {
			sawReachable = true
			if req != nil && req.GetJob() != nil {
				if p.opts.DequeueConnectionLimit == 1 && len(endpoints) > 1 {
					p.recordDequeueSuccessEndpoint(ep.id)
				}

				return req, nil
			}

			if p.opts.DequeueConnectionLimit == 1 && len(endpoints) > 1 {
				p.clearDequeueStickyEndpoint(ep.id)
			}

			continue
		}

		lastErr = err
		if IsTransientRPCError(err) {
			if p.opts.DequeueConnectionLimit == 1 && len(endpoints) > 1 {
				p.clearDequeueStickyEndpoint(ep.id)
			}

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

		var err error
		ep, err = p.ensureEndpointConnected(ctx, ep)
		if err != nil {
			return err
		}

		return p.ackEndpoint(ctx, ep, deliveryID)
	}

	endpoints := p.snapshotActiveEndpoints()
	if len(endpoints) == 0 {
		return fmt.Errorf("no queue endpoints available")
	}

	var lastErr error
	for _, ep := range endpoints {
		var err error
		ep, err = p.ensureEndpointConnected(ctx, ep)
		if err != nil {
			lastErr = err
			continue
		}

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

func normalizeDequeueSupportedIsolation(levels []string) ([]string, error) {
	normalized, err := action.NormalizeSupportedIsolationLevels(levels)
	if err != nil {
		return nil, fmt.Errorf("dequeue supported isolation: %w", err)
	}

	return normalized, nil
}

func (p *queuePool) setDequeueSupportedIsolation(levels []string) error {
	normalized, err := normalizeDequeueSupportedIsolation(levels)
	if err != nil {
		return err
	}

	p.opts.DequeueSupportedIsolation = normalized
	if len(normalized) == 0 {
		p.dequeueRequest = &api.DequeueRequest{}
		return nil
	}

	p.dequeueRequest = &api.DequeueRequest{
		SupportedIsolation: append([]string(nil), normalized...),
	}
	return nil
}

var _ interfaces.QueueService = (*ManagingQueuePoolService)(nil)
var _ interfaces.QueueClient = (*ManagingQueuePoolClient)(nil)
