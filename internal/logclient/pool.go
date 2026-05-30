package logclient

import (
	"context"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/config"
	"vectis/internal/interfaces"
	"vectis/internal/registry"
	"vectis/internal/resolver"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type PoolOptions struct {
	PinnedAddress   string
	RegistryAddress string
	RetryMetrics    backoff.RetryMetrics
	RefreshInterval time.Duration
}

type ManagingLogClient struct {
	pool *logPool
}

func NewManagingLogClient(ctx context.Context, logger interfaces.Logger, opts PoolOptions) (*ManagingLogClient, error) {
	pool, err := newLogPool(ctx, logger, opts)
	if err != nil {
		return nil, err
	}

	return &ManagingLogClient{pool: pool}, nil
}

func (m *ManagingLogClient) StreamLogs(ctx context.Context) (interfaces.LogStream, error) {
	return m.pool.streamLogs(ctx)
}

func (m *ManagingLogClient) StreamLogsForRun(ctx context.Context, runID string) (interfaces.LogStream, error) {
	return m.pool.streamLogsForRun(ctx, runID)
}

func (m *ManagingLogClient) GetLogs(ctx context.Context, req *api.GetLogsRequest, opts ...grpc.CallOption) (api.LogService_GetLogsClient, error) {
	return m.pool.getLogs(ctx, req, opts...)
}

func (m *ManagingLogClient) Close() error {
	return m.pool.close()
}

func (m *ManagingLogClient) GRPCConnectivityState() connectivity.State {
	return m.pool.connectivityState()
}

type logPool struct {
	logger   interfaces.Logger
	opts     PoolOptions
	registry *registry.Registry

	mu        sync.RWMutex
	endpoints map[string]*logEndpoint
	active    []*logEndpoint

	cancelFn context.CancelFunc
}

type logEndpoint struct {
	id      string
	address string
	conn    *grpc.ClientConn
	client  api.LogServiceClient
	writer  interfaces.LogClient
	cleanup func()
}

type desiredLogEndpoint struct {
	id      string
	address string
}

func newLogPool(ctx context.Context, logger interfaces.Logger, opts PoolOptions) (*logPool, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = config.RegistryResolverPollInterval()
	}

	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = 10 * time.Second
	}

	p := &logPool{
		logger:    logger,
		opts:      opts,
		endpoints: make(map[string]*logEndpoint),
	}

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

func (p *logPool) refreshLoop(ctx context.Context) {
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
					p.logger.Debug("log pool refresh failed: %v", err)
				}
				continue
			}

			if err := p.refresh(refreshCtx); err != nil {
				p.logger.Debug("log pool refresh failed: %v", err)
			}
		}
	}
}

func (p *logPool) refresh(ctx context.Context) error {
	desired, err := p.resolveDesired(ctx)
	if err != nil {
		if len(p.snapshotActiveEndpoints()) > 0 {
			return err
		}

		return fmt.Errorf("resolve log pool: %w", err)
	}

	p.mu.RLock()
	existing := make(map[string]*logEndpoint, len(p.endpoints))
	for id, ep := range p.endpoints {
		existing[id] = ep
	}
	p.mu.RUnlock()

	replacements := make(map[string]*logEndpoint)
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
			p.logger.Warn("log pool: failed to connect to %s at %s: %v", d.id, d.address, err)
			continue
		}

		replacements[d.id] = ep
	}

	active := make([]*logEndpoint, 0, len(desired))
	for _, d := range desired {
		if ep := replacements[d.id]; ep != nil {
			active = append(active, ep)
			continue
		}

		if ep := existing[d.id]; ep != nil && ep.address == d.address {
			active = append(active, ep)
		}
	}

	sort.Slice(active, func(i, j int) bool {
		return active[i].id < active[j].id
	})

	p.mu.Lock()
	for id, ep := range replacements {
		if old := p.endpoints[id]; old != nil {
			old.close()
		}

		p.endpoints[id] = ep
	}

	for id, ep := range p.endpoints {
		found := false
		for _, d := range desired {
			if d.id == id {
				found = true
				break
			}
		}

		if !found {
			ep.close()
			delete(p.endpoints, id)
		}
	}

	p.active = active
	activeCount := len(p.active)
	totalCount := len(p.endpoints)
	p.mu.Unlock()

	if activeCount == 0 {
		if firstErr != nil {
			return firstErr
		}

		return fmt.Errorf("no log endpoints available")
	}

	p.logger.Debug("log pool has %d active endpoint(s), %d known endpoint(s)", activeCount, totalCount)
	return nil
}

func (p *logPool) resolveDesired(ctx context.Context) ([]desiredLogEndpoint, error) {
	if p.opts.PinnedAddress != "" {
		return []desiredLogEndpoint{{
			id:      "pinned",
			address: p.opts.PinnedAddress,
		}}, nil
	}

	if p.registry == nil {
		return nil, fmt.Errorf("registry client is required")
	}

	entries, err := p.registry.ListRegistrations(ctx, api.Component_COMPONENT_LOG, registry.DefaultServiceMetadata())
	if err != nil {
		return nil, err
	}

	if len(entries) == 0 {
		entries, err = p.registry.ListRegistrations(ctx, api.Component_COMPONENT_LOG, nil)
		if err != nil {
			return nil, err
		}
	}

	seen := make(map[string]desiredLogEndpoint, len(entries))
	for _, entry := range entries {
		address := entry.GetAddress()
		if address == "" {
			continue
		}

		id := entry.GetInstanceId()
		if id == "" {
			id = address
		}

		seen[id] = desiredLogEndpoint{id: id, address: address}
	}

	out := make([]desiredLogEndpoint, 0, len(seen))
	for _, endpoint := range seen {
		out = append(out, endpoint)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].id < out[j].id
	})

	if len(out) == 0 {
		return nil, fmt.Errorf("no log registrations available")
	}

	return out, nil
}

func (p *logPool) connectEndpoint(ctx context.Context, id, address string) (*logEndpoint, error) {
	conn, cleanup, err := resolver.NewClientWithPinnedAddress(ctx, api.Component_COMPONENT_LOG, address, p.logger, nil, p.opts.RetryMetrics)
	if err != nil {
		return nil, err
	}

	return &logEndpoint{
		id:      id,
		address: address,
		conn:    conn,
		client:  api.NewLogServiceClient(conn),
		writer:  interfaces.NewGRPCLogClient(conn),
		cleanup: cleanup,
	}, nil
}

func (p *logPool) snapshotActiveEndpoints() []*logEndpoint {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]*logEndpoint(nil), p.active...)
}

func (p *logPool) chooseEndpoint(runID string) (*logEndpoint, error) {
	if runID == "" {
		return nil, fmt.Errorf("run id is required")
	}

	endpoints := p.snapshotActiveEndpoints()
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no log endpoints available")
	}

	var best *logEndpoint
	var bestScore uint64
	for _, ep := range endpoints {
		score := rendezvousScore(runID, ep.id)
		if best == nil || score > bestScore {
			best = ep
			bestScore = score
		}
	}

	return best, nil
}

func rendezvousScore(runID, endpointID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(runID))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(endpointID))
	return h.Sum64()
}

func (p *logPool) streamLogs(ctx context.Context) (interfaces.LogStream, error) {
	return &routingLogStream{ctx: ctx, pool: p}, nil
}

func (p *logPool) streamLogsForRun(ctx context.Context, runID string) (interfaces.LogStream, error) {
	ep, err := p.chooseEndpoint(runID)
	if err != nil {
		return nil, err
	}

	stream, err := ep.writer.StreamLogs(ctx)
	if err == nil {
		return stream, nil
	}

	if rerr := p.reconnectEndpoint(ctx, ep.id); rerr != nil {
		p.logger.Debug("log pool reconnect to %s failed: %v", ep.id, rerr)
		return nil, err
	}

	ep, err = p.chooseEndpoint(runID)
	if err != nil {
		return nil, err
	}

	return ep.writer.StreamLogs(ctx)
}

func (p *logPool) getLogs(ctx context.Context, req *api.GetLogsRequest, opts ...grpc.CallOption) (api.LogService_GetLogsClient, error) {
	ep, err := p.chooseEndpoint(req.GetRunId())
	if err != nil {
		return nil, err
	}

	stream, err := ep.client.GetLogs(ctx, req, opts...)
	if err == nil {
		return stream, nil
	}

	if rerr := p.reconnectEndpoint(ctx, ep.id); rerr != nil {
		p.logger.Debug("log pool reconnect to %s failed: %v", ep.id, rerr)
		return nil, err
	}

	ep, err = p.chooseEndpoint(req.GetRunId())
	if err != nil {
		return nil, err
	}

	return ep.client.GetLogs(ctx, req, opts...)
}

func (p *logPool) reconnectEndpoint(ctx context.Context, id string) error {
	p.mu.RLock()
	current := p.endpoints[id]
	p.mu.RUnlock()
	if current == nil {
		return fmt.Errorf("log endpoint %q not found", id)
	}

	replacement, err := p.connectEndpoint(ctx, current.id, current.address)
	if err != nil {
		return err
	}

	p.mu.Lock()
	old := p.endpoints[id]
	p.endpoints[id] = replacement
	if len(p.active) > 0 {
		active := append([]*logEndpoint(nil), p.active...)
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

func (p *logPool) close() error {
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

	p.endpoints = make(map[string]*logEndpoint)
	p.active = nil
	return nil
}

func (p *logPool) connectivityState() connectivity.State {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.active) == 0 {
		return connectivity.Shutdown
	}

	best := connectivity.Shutdown
	for _, ep := range p.active {
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

func (e *logEndpoint) close() {
	if e.cleanup != nil {
		e.cleanup()
		e.cleanup = nil
		return
	}

	if e.conn != nil {
		_ = e.conn.Close()
	}
}

type routingLogStream struct {
	ctx    context.Context
	pool   *logPool
	mu     sync.Mutex
	runID  string
	stream interfaces.LogStream
}

func (s *routingLogStream) Send(chunk *api.LogChunk) error {
	if chunk == nil {
		return nil
	}

	runID := chunk.GetRunId()
	if runID == "" {
		return fmt.Errorf("run id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stream != nil {
		if runID != s.runID {
			return fmt.Errorf("log stream already routed to run %q, got chunk for run %q", s.runID, runID)
		}

		return s.stream.Send(chunk)
	}

	stream, err := s.pool.streamLogsForRun(s.ctx, runID)
	if err != nil {
		return err
	}

	s.runID = runID
	s.stream = stream
	return s.stream.Send(chunk)
}

func (s *routingLogStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stream == nil {
		return nil
	}

	return s.stream.CloseSend()
}

func (s *routingLogStream) CloseAndRecv() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stream == nil {
		return nil
	}

	if closer, ok := s.stream.(interface{ CloseAndRecv() error }); ok {
		return closer.CloseAndRecv()
	}

	return s.stream.CloseSend()
}

var _ interfaces.RunLogClient = (*ManagingLogClient)(nil)
var _ interfaces.LogStream = (*routingLogStream)(nil)
