package queueclient_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/queue"
	"vectis/internal/queueclient"
	"vectis/internal/registry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

func BenchmarkQueuePool_GRPCWorkerFanout(b *testing.B) {
	cases := []struct {
		shards  int
		workers int
	}{
		{shards: 1, workers: 100},
		{shards: 1, workers: 1000},
		{shards: 2, workers: 100},
		{shards: 2, workers: 1000},
		{shards: 4, workers: 100},
	}

	for _, tc := range cases {
		b.Run(fmt.Sprintf("shards_%d_workers_%05d", tc.shards, tc.workers), func(b *testing.B) {
			runQueuePoolGRPCWorkerFanout(b, tc.shards, tc.workers)
		})
	}
}

func BenchmarkQueuePool_GRPCWorkerFanoutLarge(b *testing.B) {
	cases := []struct {
		shards  int
		workers int
	}{
		{shards: 4, workers: 1000},
		{shards: 1, workers: 5000},
		{shards: 2, workers: 5000},
		{shards: 4, workers: 5000},
	}

	for _, tc := range cases {
		b.Run(fmt.Sprintf("shards_%d_workers_%05d", tc.shards, tc.workers), func(b *testing.B) {
			runQueuePoolGRPCWorkerFanout(b, tc.shards, tc.workers)
		})
	}
}

func BenchmarkQueuePool_GRPCIdleDequeuePolling(b *testing.B) {
	cases := []struct {
		name       string
		shards     int
		workers    int
		base       time.Duration
		jitter     float64
		max        time.Duration
		idleWindow time.Duration
	}{
		{name: "fixed_250ms", shards: 1, workers: 100, base: 250 * time.Millisecond, max: 250 * time.Millisecond, idleWindow: 5 * time.Second},
		{name: "fixed_250ms", shards: 1, workers: 1000, base: 250 * time.Millisecond, max: 250 * time.Millisecond, idleWindow: 5 * time.Second},
		{name: "fixed_250ms", shards: 4, workers: 100, base: 250 * time.Millisecond, max: 250 * time.Millisecond, idleWindow: 5 * time.Second},
		{name: "fixed_250ms", shards: 4, workers: 1000, base: 250 * time.Millisecond, max: 250 * time.Millisecond, idleWindow: 5 * time.Second},
		{name: "exp_max_1s_jitter_20pct", shards: 1, workers: 1000, base: 250 * time.Millisecond, jitter: 0.2, max: time.Second, idleWindow: 5 * time.Second},
		{name: "exp_max_1s_jitter_20pct", shards: 4, workers: 1000, base: 250 * time.Millisecond, jitter: 0.2, max: time.Second, idleWindow: 5 * time.Second},
	}

	for _, tc := range cases {
		b.Run(fmt.Sprintf("%s/shards_%d_workers_%05d", tc.name, tc.shards, tc.workers), func(b *testing.B) {
			runQueuePoolGRPCIdleDequeuePolling(b, tc.shards, tc.workers, tc.base, tc.jitter, tc.max, tc.idleWindow)
		})
	}
}

func BenchmarkQueuePool_GRPCIdleWakeLatency(b *testing.B) {
	cases := []struct {
		name       string
		shards     int
		workers    int
		base       time.Duration
		jitter     float64
		max        time.Duration
		idleWarmup time.Duration
	}{
		{name: "fixed_250ms", shards: 1, workers: 100, base: 250 * time.Millisecond, max: 250 * time.Millisecond, idleWarmup: 2125 * time.Millisecond},
		{name: "exp_max_1s_jitter_20pct", shards: 1, workers: 100, base: 250 * time.Millisecond, jitter: 0.2, max: time.Second, idleWarmup: 2125 * time.Millisecond},
		{name: "exp_max_1s_jitter_20pct", shards: 4, workers: 100, base: 250 * time.Millisecond, jitter: 0.2, max: time.Second, idleWarmup: 2125 * time.Millisecond},
	}

	for _, tc := range cases {
		b.Run(fmt.Sprintf("%s/shards_%d_workers_%05d", tc.name, tc.shards, tc.workers), func(b *testing.B) {
			runQueuePoolGRPCIdleWakeLatency(b, tc.shards, tc.workers, tc.base, tc.jitter, tc.max, tc.idleWarmup)
		})
	}
}

func runQueuePoolGRPCWorkerFanout(b *testing.B, shardCount, workers int) {
	if shardCount <= 0 {
		b.Fatal("shard count must be positive")
	}

	if workers <= 0 {
		b.Fatal("workers must be positive")
	}

	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := startGRPCBenchmarkRegistry(b)
	defer reg.close()

	shards := startGRPCBenchmarkQueueShards(b, shardCount)
	defer closeGRPCBenchmarkQueueShards(shards)
	registerGRPCBenchmarkQueueShards(b, ctx, reg.address, shards)

	workerClients := make([]*queueclient.ManagingQueuePoolClient, 0, workers)
	for workerIndex := range workers {
		client, err := queueclient.NewManagingQueuePoolClient(ctx, mocks.NopLogger{}, queueclient.QueuePoolOptions{
			RegistryAddress: reg.address,
			RefreshInterval: time.Hour,
		})

		if err != nil {
			b.Fatalf("create worker queue pool %d: %v", workerIndex, err)
		}

		workerClients = append(workerClients, client)
		if workerIndex%64 == 63 {
			time.Sleep(time.Millisecond)
		}
	}

	defer func() {
		for _, client := range workerClients {
			if err := client.Close(); err != nil {
				b.Fatalf("close worker queue pool: %v", err)
			}
		}
	}()

	var measured time.Duration
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		prefillGRPCBenchmarkQueueShards(b, shards, iteration, workers)

		workCtx, cancelWork := context.WithTimeout(ctx, 30*time.Second)
		var ready sync.WaitGroup
		var done sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, workers)
		ready.Add(workers)
		done.Add(workers)

		for _, client := range workerClients {
			client := client
			go func() {
				defer done.Done()
				ready.Done()
				<-start

				job, err := client.Dequeue(workCtx)
				if err != nil {
					errs <- fmt.Errorf("dequeue: %w", err)
					return
				}

				if job == nil || job.GetJob() == nil {
					errs <- fmt.Errorf("expected dequeued job")
					return
				}

				deliveryID := job.GetJob().GetDeliveryId()
				if deliveryID == "" {
					errs <- fmt.Errorf("expected delivery ID")
					return
				}

				if err := client.Ack(workCtx, deliveryID); err != nil {
					errs <- fmt.Errorf("ack: %w", err)
					return
				}
			}()
		}

		ready.Wait()
		b.StartTimer()
		iterationStart := time.Now()
		close(start)
		done.Wait()
		iterationMeasured := time.Since(iterationStart)
		b.StopTimer()
		cancelWork()

		close(errs)
		for err := range errs {
			if err != nil {
				b.Fatal(err)
			}
		}

		measured += iterationMeasured
	}

	if measured > 0 {
		totalDeliveries := float64(workers * b.N)
		b.ReportMetric(totalDeliveries/measured.Seconds(), "deliveries/s")
		b.ReportMetric(totalDeliveries/measured.Seconds(), "acks/s")
	}

	b.ReportMetric(float64(shardCount), "shards")
	b.ReportMetric(float64(workers), "workers")

	queueConns, queueConnPeak := measureGRPCBenchmarkQueueConns(shards)
	workerQueueConns := queueConns - int64(shardCount)
	if workerQueueConns < 0 {
		workerQueueConns = 0
	}

	registryConns := reg.conns.current.Load()
	b.ReportMetric(float64(workerQueueConns), "worker_queue_conns")
	b.ReportMetric(float64(queueConns), "queue_conns")
	b.ReportMetric(float64(queueConnPeak), "queue_conn_peak")
	b.ReportMetric(float64(registryConns), "registry_conns")
	b.ReportMetric(float64(reg.conns.max.Load()), "registry_conn_peak")
	b.ReportMetric(float64(queueConns+registryConns), "total_grpc_conns")
}

func runQueuePoolGRPCIdleWakeLatency(b *testing.B, shardCount, workers int, pollBase time.Duration, pollJitter float64, pollMax, idleWarmup time.Duration) {
	if shardCount <= 0 {
		b.Fatal("shard count must be positive")
	}

	if workers <= 0 {
		b.Fatal("workers must be positive")
	}

	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := startGRPCBenchmarkRegistry(b)
	defer reg.close()

	shards := startGRPCBenchmarkQueueShards(b, shardCount)
	defer closeGRPCBenchmarkQueueShards(shards)
	registerGRPCBenchmarkQueueShards(b, ctx, reg.address, shards)

	workerClients := make([]*queueclient.ManagingQueuePoolClient, 0, workers)
	for workerIndex := range workers {
		client, err := queueclient.NewManagingQueuePoolClient(ctx, mocks.NopLogger{}, queueclient.QueuePoolOptions{
			RegistryAddress:         reg.address,
			RefreshInterval:         time.Hour,
			DequeuePollBaseInterval: pollBase,
			DequeuePollJitterRatio:  pollJitter,
			DequeuePollMaxInterval:  pollMax,
		})

		if err != nil {
			b.Fatalf("create idle wake worker queue pool %d: %v", workerIndex, err)
		}

		workerClients = append(workerClients, client)
		if workerIndex%64 == 63 {
			time.Sleep(time.Millisecond)
		}
	}

	defer func() {
		for _, client := range workerClients {
			if err := client.Close(); err != nil {
				b.Fatalf("close worker queue pool: %v", err)
			}
		}
	}()

	var allLatencies []time.Duration
	var measured time.Duration

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		workCtx, cancelWork := context.WithTimeout(ctx, idleWarmup+5*time.Second)

		var ready sync.WaitGroup
		var done sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, workers)
		latencies := make(chan time.Duration, workers)

		var wakeStartNanos atomic.Int64
		ready.Add(workers)
		done.Add(workers)

		for _, client := range workerClients {
			client := client
			go func() {
				defer done.Done()
				ready.Done()
				<-start

				job, err := client.Dequeue(workCtx)
				if err != nil {
					errs <- fmt.Errorf("dequeue after idle: %w", err)
					return
				}

				if job == nil || job.GetJob() == nil {
					errs <- fmt.Errorf("expected dequeued job after idle")
					return
				}

				startNanos := wakeStartNanos.Load()
				if startNanos == 0 {
					errs <- fmt.Errorf("wake start was not recorded")
					return
				}

				latencies <- time.Since(time.Unix(0, startNanos))

				deliveryID := job.GetJob().GetDeliveryId()
				if deliveryID == "" {
					errs <- fmt.Errorf("expected delivery ID after idle")
					return
				}

				if err := client.Ack(workCtx, deliveryID); err != nil {
					errs <- fmt.Errorf("ack after idle: %w", err)
					return
				}
			}()
		}

		ready.Wait()
		close(start)
		time.Sleep(idleWarmup)

		b.StartTimer()
		iterationStart := time.Now()
		wakeStartNanos.Store(iterationStart.UnixNano())
		prefillGRPCBenchmarkQueueShards(b, shards, iteration, workers)
		done.Wait()
		iterationMeasured := time.Since(iterationStart)
		b.StopTimer()
		cancelWork()

		close(errs)
		for err := range errs {
			if err != nil {
				b.Fatal(err)
			}
		}

		close(latencies)
		for latency := range latencies {
			allLatencies = append(allLatencies, latency)
		}

		measured += iterationMeasured
	}

	if measured > 0 {
		totalDeliveries := float64(workers * b.N)
		b.ReportMetric(totalDeliveries/measured.Seconds(), "wake_deliveries/s")
		b.ReportMetric(float64(measured)/float64(b.N)/float64(time.Millisecond), "wake_all_ms/op")
	}

	reportGRPCBenchmarkLatencyMetrics(b, allLatencies, "wake")

	b.ReportMetric(float64(shardCount), "shards")
	b.ReportMetric(float64(workers), "workers")
	b.ReportMetric(float64(pollBase)/float64(time.Millisecond), "dequeue_poll_base_interval_ms")
	b.ReportMetric(pollJitter, "dequeue_poll_jitter_ratio")
	b.ReportMetric(float64(pollMax)/float64(time.Millisecond), "dequeue_poll_max_interval_ms")
	b.ReportMetric(float64(idleWarmup)/float64(time.Millisecond), "idle_warmup_ms")

	queueConns, queueConnPeak := measureGRPCBenchmarkQueueConns(shards)
	workerQueueConns := queueConns - int64(shardCount)
	if workerQueueConns < 0 {
		workerQueueConns = 0
	}

	registryConns := reg.conns.current.Load()
	b.ReportMetric(float64(workerQueueConns), "worker_queue_conns")
	b.ReportMetric(float64(queueConns), "queue_conns")
	b.ReportMetric(float64(queueConnPeak), "queue_conn_peak")
	b.ReportMetric(float64(registryConns), "registry_conns")
	b.ReportMetric(float64(reg.conns.max.Load()), "registry_conn_peak")
	b.ReportMetric(float64(queueConns+registryConns), "total_grpc_conns")
}

func runQueuePoolGRPCIdleDequeuePolling(b *testing.B, shardCount, workers int, pollBase time.Duration, pollJitter float64, pollMax, idleWindow time.Duration) {
	if shardCount <= 0 {
		b.Fatal("shard count must be positive")
	}

	if workers <= 0 {
		b.Fatal("workers must be positive")
	}

	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := startGRPCBenchmarkRegistry(b)
	defer reg.close()

	shards := startGRPCBenchmarkQueueShards(b, shardCount)
	defer closeGRPCBenchmarkQueueShards(shards)
	registerGRPCBenchmarkQueueShards(b, ctx, reg.address, shards)

	workerClients := make([]*queueclient.ManagingQueuePoolClient, 0, workers)
	for workerIndex := range workers {
		client, err := queueclient.NewManagingQueuePoolClient(ctx, mocks.NopLogger{}, queueclient.QueuePoolOptions{
			RegistryAddress:         reg.address,
			RefreshInterval:         time.Hour,
			DequeuePollBaseInterval: pollBase,
			DequeuePollJitterRatio:  pollJitter,
			DequeuePollMaxInterval:  pollMax,
		})

		if err != nil {
			b.Fatalf("create idle worker queue pool %d: %v", workerIndex, err)
		}

		workerClients = append(workerClients, client)
		if workerIndex%64 == 63 {
			time.Sleep(time.Millisecond)
		}
	}

	defer func() {
		for _, client := range workerClients {
			if err := client.Close(); err != nil {
				b.Fatalf("close worker queue pool: %v", err)
			}
		}
	}()

	var measured time.Duration
	var emptyTryDequeueRPCs int64

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		beforeRPCs := measureGRPCBenchmarkQueueRPCs(shards)
		workCtx, cancelWork := context.WithTimeout(ctx, idleWindow)

		var ready sync.WaitGroup
		var done sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, workers)
		ready.Add(workers)
		done.Add(workers)

		for _, client := range workerClients {
			client := client
			go func() {
				defer done.Done()
				ready.Done()
				<-start

				job, err := client.Dequeue(workCtx)
				if job != nil {
					errs <- fmt.Errorf("expected no idle dequeue job")
					return
				}

				if err != nil && !isGRPCBenchmarkIdleDequeueTimeout(err) {
					errs <- fmt.Errorf("idle dequeue: %w", err)
					return
				}
			}()
		}

		ready.Wait()
		b.StartTimer()
		iterationStart := time.Now()
		close(start)
		done.Wait()
		iterationMeasured := time.Since(iterationStart)
		b.StopTimer()
		cancelWork()

		close(errs)
		for err := range errs {
			if err != nil {
				b.Fatal(err)
			}
		}

		afterRPCs := measureGRPCBenchmarkQueueRPCs(shards)
		emptyTryDequeueRPCs += afterRPCs - beforeRPCs
		measured += iterationMeasured
	}

	if measured > 0 {
		b.ReportMetric(float64(emptyTryDequeueRPCs)/measured.Seconds(), "empty_try_dequeue_rpcs/s")
		b.ReportMetric(float64(workers*int(b.N))/measured.Seconds(), "idle_workers/s")
	}

	if workers > 0 && b.N > 0 {
		b.ReportMetric(float64(emptyTryDequeueRPCs)/float64(workers*int(b.N)), "empty_try_dequeue_rpcs_per_worker")
	}

	b.ReportMetric(float64(shardCount), "shards")
	b.ReportMetric(float64(workers), "workers")
	b.ReportMetric(float64(pollBase)/float64(time.Millisecond), "dequeue_poll_base_interval_ms")
	b.ReportMetric(pollJitter, "dequeue_poll_jitter_ratio")
	b.ReportMetric(float64(pollMax)/float64(time.Millisecond), "dequeue_poll_max_interval_ms")
	b.ReportMetric(idleWindow.Seconds(), "idle_window_s")

	queueConns, queueConnPeak := measureGRPCBenchmarkQueueConns(shards)
	workerQueueConns := queueConns - int64(shardCount)
	if workerQueueConns < 0 {
		workerQueueConns = 0
	}

	registryConns := reg.conns.current.Load()
	b.ReportMetric(float64(workerQueueConns), "worker_queue_conns")
	b.ReportMetric(float64(queueConns), "queue_conns")
	b.ReportMetric(float64(queueConnPeak), "queue_conn_peak")
	b.ReportMetric(float64(registryConns), "registry_conns")
	b.ReportMetric(float64(reg.conns.max.Load()), "registry_conn_peak")
	b.ReportMetric(float64(queueConns+registryConns), "total_grpc_conns")
}

func reportGRPCBenchmarkLatencyMetrics(b *testing.B, latencies []time.Duration, prefix string) {
	b.Helper()
	if len(latencies) == 0 {
		return
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	b.ReportMetric(float64(latencies[0])/float64(time.Millisecond), prefix+"_min_ms")
	b.ReportMetric(float64(benchmarkDurationQuantile(latencies, 0.50))/float64(time.Millisecond), prefix+"_p50_ms")
	b.ReportMetric(float64(benchmarkDurationQuantile(latencies, 0.95))/float64(time.Millisecond), prefix+"_p95_ms")
	b.ReportMetric(float64(benchmarkDurationQuantile(latencies, 0.99))/float64(time.Millisecond), prefix+"_p99_ms")
	b.ReportMetric(float64(latencies[len(latencies)-1])/float64(time.Millisecond), prefix+"_max_ms")
}

func benchmarkDurationQuantile(values []time.Duration, q float64) time.Duration {
	if len(values) == 0 {
		return 0
	}

	if q <= 0 {
		return values[0]
	}

	if q >= 1 {
		return values[len(values)-1]
	}

	index := int(float64(len(values)-1) * q)
	return values[index]
}

func isGRPCBenchmarkIdleDequeueTimeout(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded
}

type grpcBenchmarkRegistry struct {
	address  string
	server   *grpc.Server
	listener net.Listener
	done     chan struct{}
	conns    *grpcBenchmarkConnCounter
}

func startGRPCBenchmarkRegistry(tb testing.TB) grpcBenchmarkRegistry {
	tb.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen registry: %v", err)
	}

	conns := &grpcBenchmarkConnCounter{}
	server := grpc.NewServer(grpc.StatsHandler(conns))
	api.RegisterRegistryServiceServer(server, registry.NewRegistryService(mocks.NopLogger{}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()

	return grpcBenchmarkRegistry{
		address:  listener.Addr().String(),
		server:   server,
		listener: listener,
		done:     done,
		conns:    conns,
	}
}

func (r grpcBenchmarkRegistry) close() {
	r.server.Stop()
	_ = r.listener.Close()
	<-r.done
}

type grpcBenchmarkQueueShard struct {
	id       string
	address  string
	server   *grpc.Server
	listener net.Listener
	done     chan struct{}
	conn     *grpc.ClientConn
	client   api.QueueServiceClient
	conns    *grpcBenchmarkConnCounter
}

func startGRPCBenchmarkQueueShards(tb testing.TB, shardCount int) []grpcBenchmarkQueueShard {
	tb.Helper()

	shards := make([]grpcBenchmarkQueueShard, 0, shardCount)
	for shardIndex := range shardCount {
		id := fmt.Sprintf("queue-%02d", shardIndex+1)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatalf("listen queue shard %s: %v", id, err)
		}

		conns := &grpcBenchmarkConnCounter{}
		server := grpc.NewServer(grpc.StatsHandler(conns))
		queue.RegisterQueueService(server, mocks.NopLogger{}, queue.QueueOptions{InstanceID: id}, nil)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = server.Serve(listener)
		}()

		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			server.Stop()
			_ = listener.Close()
			<-done
			tb.Fatalf("dial enqueue client for shard %s: %v", id, err)
		}

		conn.Connect()
		shards = append(shards, grpcBenchmarkQueueShard{
			id:       id,
			address:  listener.Addr().String(),
			server:   server,
			listener: listener,
			done:     done,
			conn:     conn,
			client:   api.NewQueueServiceClient(conn),
			conns:    conns,
		})
	}

	return shards
}

func closeGRPCBenchmarkQueueShards(shards []grpcBenchmarkQueueShard) {
	for _, shard := range shards {
		_ = shard.conn.Close()
		shard.server.Stop()
		_ = shard.listener.Close()
		<-shard.done
	}
}

func measureGRPCBenchmarkQueueConns(shards []grpcBenchmarkQueueShard) (int64, int64) {
	var current int64
	var peak int64
	for _, shard := range shards {
		current += shard.conns.current.Load()
		peak += shard.conns.max.Load()
	}

	return current, peak
}

func measureGRPCBenchmarkQueueRPCs(shards []grpcBenchmarkQueueShard) int64 {
	var total int64
	for _, shard := range shards {
		total += shard.conns.rpcBegins.Load()
	}

	return total
}

type grpcBenchmarkConnCounter struct {
	current   atomic.Int64
	total     atomic.Int64
	max       atomic.Int64
	rpcBegins atomic.Int64
}

func (c *grpcBenchmarkConnCounter) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}

func (c *grpcBenchmarkConnCounter) HandleRPC(_ context.Context, stat stats.RPCStats) {
	if _, ok := stat.(*stats.Begin); ok {
		c.rpcBegins.Add(1)
	}
}

func (c *grpcBenchmarkConnCounter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}

func (c *grpcBenchmarkConnCounter) HandleConn(_ context.Context, stat stats.ConnStats) {
	switch stat.(type) {
	case *stats.ConnBegin:
		current := c.current.Add(1)
		c.total.Add(1)
		for {
			maximum := c.max.Load()
			if current <= maximum || c.max.CompareAndSwap(maximum, current) {
				return
			}
		}
	case *stats.ConnEnd:
		c.current.Add(-1)
	}
}

func registerGRPCBenchmarkQueueShards(tb testing.TB, ctx context.Context, registryAddress string, shards []grpcBenchmarkQueueShard) {
	tb.Helper()

	reg, err := registry.New(ctx, registryAddress, mocks.NopLogger{}, interfaces.SystemClock{}, nil)
	if err != nil {
		tb.Fatalf("registry client: %v", err)
	}
	defer func() { _ = reg.Close() }()

	for _, shard := range shards {
		if err := reg.RegisterInstanceWithMetadata(ctx, api.Component_COMPONENT_QUEUE, shard.id, shard.address, registry.QueueIngressMetadata()); err != nil {
			tb.Fatalf("register queue shard %s: %v", shard.id, err)
		}
	}
}

func prefillGRPCBenchmarkQueueShards(tb testing.TB, shards []grpcBenchmarkQueueShard, iteration, jobs int) {
	tb.Helper()

	ctx := context.Background()
	for jobIndex := range jobs {
		shard := shards[jobIndex%len(shards)]
		jobID := fmt.Sprintf("grpc-pool-fanout-job-%d-%d", iteration, jobIndex)
		if _, err := shard.client.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
			tb.Fatalf("prefill queue shard %s: %v", shard.id, err)
		}
	}
}
