package queueclient

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/queueid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	queuePoolEnqueueBurstJobs = 100_000
	queuePoolDequeueBurstJobs = 100_000
)

type benchmarkQueueServiceClient struct {
	id          string
	state       *localBenchmarkQueueShardState
	pending     atomic.Int64
	deliverySeq atomic.Int64
}

type localBenchmarkQueueShard struct {
	id     string
	client *benchmarkQueueServiceClient
	state  *localBenchmarkQueueShardState
}

type localBenchmarkQueueShardState struct {
	stats                      localQueueClientStats
	unavailable                atomic.Bool
	unavailableAfterTryDequeue atomic.Int64
}

type localQueueClientStats struct {
	enqueue         atomic.Int64
	tryDequeue      atomic.Int64
	tryDequeueEmpty atomic.Int64
	tryDequeueError atomic.Int64
	ack             atomic.Int64
}

type queueClientStatsSnapshot struct {
	enqueue         int64
	tryDequeue      int64
	tryDequeueEmpty int64
	tryDequeueError int64
	ack             int64
}

func (b *benchmarkQueueServiceClient) Enqueue(context.Context, *api.JobRequest, ...grpc.CallOption) (*api.Empty, error) {
	if b.state != nil {
		b.state.stats.enqueue.Add(1)
	}
	b.pending.Add(1)
	return &api.Empty{}, nil
}

func (b *benchmarkQueueServiceClient) Dequeue(context.Context, *api.DequeueRequest, ...grpc.CallOption) (*api.JobRequest, error) {
	return b.tryDequeue()
}

func (b *benchmarkQueueServiceClient) TryDequeue(context.Context, *api.DequeueRequest, ...grpc.CallOption) (*api.JobRequest, error) {
	return b.tryDequeue()
}

func (b *benchmarkQueueServiceClient) tryDequeue() (*api.JobRequest, error) {
	if b.state != nil {
		calls := b.state.stats.tryDequeue.Add(1)
		if threshold := b.state.unavailableAfterTryDequeue.Load(); threshold > 0 && calls > threshold {
			b.state.unavailable.Store(true)
		}

		if b.state.unavailable.Load() {
			b.state.stats.tryDequeueError.Add(1)
			return nil, status.Error(codes.Unavailable, "queue shard unavailable")
		}
	}

	for {
		pending := b.pending.Load()
		if pending <= 0 {
			if b.state != nil {
				b.state.stats.tryDequeueEmpty.Add(1)
			}
			return nil, nil //nolint:nilnil // Benchmark fake models an empty queue as nil request and nil error.
		}
		if b.pending.CompareAndSwap(pending, pending-1) {
			deliveryID := queueid.Encode(b.id, fmt.Sprintf("delivery-%d", b.deliverySeq.Add(1)))
			return &api.JobRequest{Job: &api.Job{DeliveryId: &deliveryID}}, nil
		}
	}
}

func (b *benchmarkQueueServiceClient) Ack(context.Context, *api.AckRequest, ...grpc.CallOption) (*api.Empty, error) {
	if b.state != nil {
		b.state.stats.ack.Add(1)
	}
	return &api.Empty{}, nil
}

func (b *benchmarkQueueServiceClient) ListDeadLetter(context.Context, *api.Empty, ...grpc.CallOption) (*api.ListDeadLetterResponse, error) {
	return &api.ListDeadLetterResponse{}, nil
}

func (b *benchmarkQueueServiceClient) RequeueDeadLetter(context.Context, *api.RequeueDeadLetterRequest, ...grpc.CallOption) (*api.Empty, error) {
	return &api.Empty{}, nil
}

func BenchmarkQueuePool_ShardedEnqueueBurst(b *testing.B) {
	for _, shards := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("shards_%d", shards), func(b *testing.B) {
			runQueuePoolEnqueueBurst(b, shards)
		})
	}
}

func BenchmarkQueuePool_ShardedDequeueBurst(b *testing.B) {
	for _, shards := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("shards_%d", shards), func(b *testing.B) {
			runQueuePoolDequeueBurst(b, shards)
		})
	}
}

func BenchmarkQueuePool_ShardedDequeueFilteredBurst(b *testing.B) {
	for _, shards := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("shards_%d", shards), func(b *testing.B) {
			runQueuePoolDequeueScenario(b, queuePoolDequeueScenario{
				shards:             shards,
				workers:            benchmarkWorkerCount(),
				totalJobs:          queuePoolDequeueBurstJobs * b.N,
				prefill:            prefillBenchmarkQueuePoolEvenly,
				supportedIsolation: []string{"host"},
			})
		})
	}
}

func BenchmarkQueuePool_ShardedDequeueSkewed(b *testing.B) {
	b.Run("shards_4_all_on_one", func(b *testing.B) {
		runQueuePoolDequeueScenario(b, queuePoolDequeueScenario{
			shards:    4,
			workers:   benchmarkWorkerCount(),
			totalJobs: queuePoolDequeueBurstJobs * b.N,
			prefill:   prefillBenchmarkQueuePoolSingleShard,
		})
	})
}

func BenchmarkQueuePool_ShardedDequeueOverprovisionedPollers(b *testing.B) {
	baseWorkers := benchmarkWorkerCount()
	for _, multiplier := range []int{4, 16} {
		workers := baseWorkers * multiplier
		b.Run(fmt.Sprintf("shards_4_workers_%d", workers), func(b *testing.B) {
			runQueuePoolDequeueScenario(b, queuePoolDequeueScenario{
				shards:      4,
				workers:     workers,
				totalJobs:   queuePoolDequeueBurstJobs * b.N,
				prefill:     prefillBenchmarkQueuePoolEvenly,
				globalDrain: true,
			})
		})
	}
}

func BenchmarkQueuePool_ShardedDequeueUnavailableShard(b *testing.B) {
	b.Run("shards_4_one_unavailable", func(b *testing.B) {
		runQueuePoolDequeueScenario(b, queuePoolDequeueScenario{
			shards:    4,
			workers:   benchmarkWorkerCount(),
			totalJobs: queuePoolDequeueBurstJobs * b.N,
			prefill:   prefillBenchmarkQueuePoolSurvivors,
			configure: func(shards []localBenchmarkQueueShard) {
				shards[0].state.unavailableAfterTryDequeue.Store(128)
			},
			globalDrain: true,
		})
	})
}

func runQueuePoolEnqueueBurst(b *testing.B, shards int) {
	pool, services := newLocalBenchmarkQueuePool(b, shards)
	defer closeLocalBenchmarkQueuePool(b, pool, services)

	ctx := context.Background()
	jobID := "sharded-enqueue-job"
	job := &api.Job{Id: &jobID}
	workers := benchmarkWorkerCount()
	totalJobs := queuePoolEnqueueBurstJobs * b.N

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	var wg sync.WaitGroup
	for worker := range workers {
		count := totalJobs / workers
		if worker < totalJobs%workers {
			count++
		}

		wg.Go(func() {
			for range count {
				if _, err := pool.enqueue(ctx, &api.JobRequest{Job: job}); err != nil {
					b.Fatalf("enqueue failed: %v", err)
				}
			}
		})
	}
	wg.Wait()

	elapsed := time.Since(start)
	b.StopTimer()

	if elapsed <= 0 {
		b.Fatal("invalid elapsed duration")
	}

	b.ReportMetric(float64(totalJobs)/elapsed.Seconds(), "enqueue_ops/s")
	b.ReportMetric(float64(totalJobs), "jobs")
	b.ReportMetric(float64(shards), "shards")
	b.ReportMetric(float64(workers), "workers")
	reportQueueDepthBalance(b, "", measureQueueDepthBalance(services))
}

func runQueuePoolDequeueBurst(b *testing.B, shards int) {
	runQueuePoolDequeueScenario(b, queuePoolDequeueScenario{
		shards:    shards,
		workers:   benchmarkWorkerCount(),
		totalJobs: queuePoolDequeueBurstJobs * b.N,
		prefill:   prefillBenchmarkQueuePoolEvenly,
	})
}

type queuePoolDequeueScenario struct {
	shards             int
	workers            int
	totalJobs          int
	prefill            func(*testing.B, []localBenchmarkQueueShard, int)
	configure          func([]localBenchmarkQueueShard)
	supportedIsolation []string
	globalDrain        bool
}

func runQueuePoolDequeueScenario(b *testing.B, opts queuePoolDequeueScenario) {
	pool, services := newLocalBenchmarkQueuePool(b, opts.shards)
	defer closeLocalBenchmarkQueuePool(b, pool, services)
	if err := pool.setDequeueSupportedIsolation(opts.supportedIsolation); err != nil {
		b.Fatalf("set supported isolation: %v", err)
	}

	ctx := context.Background()
	if opts.workers < 1 {
		opts.workers = 1
	}

	if opts.totalJobs < 1 {
		opts.totalJobs = 1
	}

	if opts.prefill == nil {
		opts.prefill = prefillBenchmarkQueuePoolEvenly
	}

	opts.prefill(b, services, opts.totalJobs)
	initialBalance := measureQueueDepthBalance(services)
	if opts.configure != nil {
		opts.configure(services)
	}

	initialStats := measureQueueClientStats(services)

	countsCh := make(chan queuePoolDequeueCounts, opts.workers)
	workCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	var wg sync.WaitGroup
	if opts.globalDrain {
		var remaining atomic.Int64
		remaining.Store(int64(opts.totalJobs))

		for range opts.workers {
			wg.Go(func() {
				var counts queuePoolDequeueCounts
				defer func() { countsCh <- counts }()

				for remaining.Load() > 0 {
					select {
					case <-workCtx.Done():
						counts.err = workCtx.Err()
						return
					default:
					}

					got, err := dequeueAndAckOnce(workCtx, pool, &counts)
					if err != nil {
						counts.err = err
						return
					}

					if !got {
						continue
					}

					if remaining.Add(-1) <= 0 {
						return
					}
				}
			})
		}
	} else {
		for worker := 0; worker < opts.workers; worker++ {
			count := opts.totalJobs / opts.workers
			if worker < opts.totalJobs%opts.workers {
				count++
			}

			wg.Go(func() {
				var counts queuePoolDequeueCounts
				defer func() { countsCh <- counts }()

				for counts.dequeue < int64(count) {
					select {
					case <-workCtx.Done():
						counts.err = workCtx.Err()
						return
					default:
					}

					_, err := dequeueAndAckOnce(workCtx, pool, &counts)
					if err != nil {
						counts.err = err
						return
					}
				}
			})
		}
	}
	wg.Wait()

	elapsed := time.Since(start)
	b.StopTimer()
	close(countsCh)
	finalStats := measureQueueClientStats(services)

	if elapsed <= 0 {
		b.Fatal("invalid elapsed duration")
	}

	var counts queuePoolDequeueCounts
	for workerCounts := range countsCh {
		if workerCounts.err != nil {
			b.Fatal(workerCounts.err)
		}

		counts.add(workerCounts)
	}

	if counts.dequeue != int64(opts.totalJobs) {
		b.Fatalf("dequeued %d jobs, want %d", counts.dequeue, opts.totalJobs)
	}

	pollCount := counts.dequeue + counts.emptyPoll
	emptyPollPct := 0.0
	if pollCount > 0 {
		emptyPollPct = float64(counts.emptyPoll) / float64(pollCount) * 100
	}

	b.ReportMetric(float64(counts.dequeue)/elapsed.Seconds(), "dequeue_ops/s")
	b.ReportMetric(float64(counts.ack)/elapsed.Seconds(), "ack_ops/s")
	b.ReportMetric(float64(counts.emptyPoll)/elapsed.Seconds(), "empty_poll_ops/s")
	b.ReportMetric(emptyPollPct, "empty_poll_pct")
	reportQueueClientStats(b, "", finalStats.subtract(initialStats), counts.dequeue, elapsed)

	b.ReportMetric(float64(opts.totalJobs), "jobs")
	b.ReportMetric(float64(opts.shards), "shards")
	b.ReportMetric(float64(opts.workers), "workers")
	reportQueueDepthBalance(b, "initial_", initialBalance)
	reportQueueDepthBalance(b, "final_", measureQueueDepthBalance(services))
}

func benchmarkWorkerCount() int {
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		return 1
	}

	return workers
}

func dequeueAndAckOnce(ctx context.Context, pool *queuePool, counts *queuePoolDequeueCounts) (bool, error) {
	got, err := pool.tryDequeue(ctx)
	if err != nil {
		return false, fmt.Errorf("try dequeue: %w", err)
	}

	if got == nil {
		counts.emptyPoll++
		runtime.Gosched()
		return false, nil
	}

	deliveryID := got.GetJob().GetDeliveryId()
	if deliveryID == "" {
		return false, fmt.Errorf("dequeued job without delivery ID")
	}

	if err := pool.ack(ctx, deliveryID); err != nil {
		return false, fmt.Errorf("ack: %w", err)
	}

	counts.dequeue++
	counts.ack++
	return true, nil
}

func newLocalBenchmarkQueuePool(b *testing.B, shards int) (*queuePool, []localBenchmarkQueueShard) {
	b.Helper()

	p := &queuePool{
		logger:    mocks.NopLogger{},
		endpoints: make(map[string]*queuePoolEndpoint, shards),
		activeIDs: make([]string, 0, shards),
	}

	services := make([]localBenchmarkQueueShard, 0, shards)
	byID := make(map[string]localBenchmarkQueueShard, shards)

	for i := range shards {
		id := fmt.Sprintf("queue-%02d", i+1)
		state := &localBenchmarkQueueShardState{}
		client := &benchmarkQueueServiceClient{id: id, state: state}
		shard := localBenchmarkQueueShard{
			id:     id,
			client: client,
			state:  state,
		}

		p.endpoints[id] = &queuePoolEndpoint{
			id:      id,
			address: id,
			client:  client,
		}

		p.activeIDs = append(p.activeIDs, id)
		p.active = append(p.active, p.endpoints[id])
		services = append(services, shard)
		byID[id] = shard
	}

	p.dial = func(_ context.Context, id, address string) (*queuePoolEndpoint, error) {
		shard, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("queue shard %q not found", id)
		}

		if shard.state.unavailable.Load() {
			return nil, status.Error(codes.Unavailable, "queue shard unavailable")
		}

		return &queuePoolEndpoint{
			id:      id,
			address: address,
			client:  shard.client,
		}, nil
	}

	return p, services
}

func closeLocalBenchmarkQueuePool(b *testing.B, pool *queuePool, _ []localBenchmarkQueueShard) {
	b.Helper()

	if err := pool.close(); err != nil {
		b.Fatalf("close pool: %v", err)
	}
}

func prefillBenchmarkQueuePoolEvenly(b *testing.B, services []localBenchmarkQueueShard, totalJobs int) {
	b.Helper()
	ctx := context.Background()
	for i := range totalJobs {
		jobID := "dequeue-burst-job-" + strconv.Itoa(i)
		shard := services[i%len(services)]
		if _, err := shard.client.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
			b.Fatalf("prefill enqueue: %v", err)
		}
	}
}

func prefillBenchmarkQueuePoolSingleShard(b *testing.B, services []localBenchmarkQueueShard, totalJobs int) {
	b.Helper()
	ctx := context.Background()
	for i := range totalJobs {
		jobID := "dequeue-skewed-job-" + strconv.Itoa(i)
		if _, err := services[0].client.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
			b.Fatalf("prefill enqueue: %v", err)
		}
	}
}

func prefillBenchmarkQueuePoolSurvivors(b *testing.B, services []localBenchmarkQueueShard, totalJobs int) {
	b.Helper()
	if len(services) < 2 {
		b.Fatal("survivor prefill requires at least two shards")
	}

	ctx := context.Background()
	survivors := services[1:]
	for i := range totalJobs {
		jobID := "dequeue-survivor-job-" + strconv.Itoa(i)
		shard := survivors[i%len(survivors)]
		if _, err := shard.client.Enqueue(ctx, &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
			b.Fatalf("prefill enqueue: %v", err)
		}
	}
}

type queuePoolDequeueCounts struct {
	dequeue   int64
	ack       int64
	emptyPoll int64
	err       error
}

func (c *queuePoolDequeueCounts) add(other queuePoolDequeueCounts) {
	c.dequeue += other.dequeue
	c.ack += other.ack
	c.emptyPoll += other.emptyPoll
}

func measureQueueClientStats(services []localBenchmarkQueueShard) queueClientStatsSnapshot {
	var snapshot queueClientStatsSnapshot
	for _, shard := range services {
		snapshot.enqueue += shard.state.stats.enqueue.Load()
		snapshot.tryDequeue += shard.state.stats.tryDequeue.Load()
		snapshot.tryDequeueEmpty += shard.state.stats.tryDequeueEmpty.Load()
		snapshot.tryDequeueError += shard.state.stats.tryDequeueError.Load()
		snapshot.ack += shard.state.stats.ack.Load()
	}

	return snapshot
}

func (s queueClientStatsSnapshot) subtract(other queueClientStatsSnapshot) queueClientStatsSnapshot {
	return queueClientStatsSnapshot{
		enqueue:         s.enqueue - other.enqueue,
		tryDequeue:      s.tryDequeue - other.tryDequeue,
		tryDequeueEmpty: s.tryDequeueEmpty - other.tryDequeueEmpty,
		tryDequeueError: s.tryDequeueError - other.tryDequeueError,
		ack:             s.ack - other.ack,
	}
}

func reportQueueClientStats(b *testing.B, prefix string, stats queueClientStatsSnapshot, dequeued int64, elapsed time.Duration) {
	b.Helper()

	tryDequeueRPCsPerJob := 0.0
	if dequeued > 0 {
		tryDequeueRPCsPerJob = float64(stats.tryDequeue) / float64(dequeued)
	}

	tryDequeueEmptyPct := 0.0
	if stats.tryDequeue > 0 {
		tryDequeueEmptyPct = float64(stats.tryDequeueEmpty) / float64(stats.tryDequeue) * 100
	}

	tryDequeueErrorPct := 0.0
	if stats.tryDequeue > 0 {
		tryDequeueErrorPct = float64(stats.tryDequeueError) / float64(stats.tryDequeue) * 100
	}

	b.ReportMetric(float64(stats.tryDequeue)/elapsed.Seconds(), prefix+"try_dequeue_rpc_ops/s")
	b.ReportMetric(tryDequeueRPCsPerJob, prefix+"try_dequeue_rpcs_per_job")
	b.ReportMetric(float64(stats.tryDequeueEmpty), prefix+"try_dequeue_empty_rpcs")
	b.ReportMetric(tryDequeueEmptyPct, prefix+"try_dequeue_empty_rpc_pct")
	b.ReportMetric(float64(stats.tryDequeueError), prefix+"try_dequeue_error_rpcs")
	b.ReportMetric(tryDequeueErrorPct, prefix+"try_dequeue_error_rpc_pct")
	b.ReportMetric(float64(stats.ack), prefix+"ack_rpcs")
}

type queueDepthBalance struct {
	totalPending  int64
	totalInflight int64
	minPending    int64
	maxPending    int64
	avgPending    float64
	spread        int64
	imbalancePct  float64
}

func measureQueueDepthBalance(services []localBenchmarkQueueShard) queueDepthBalance {
	if len(services) == 0 {
		return queueDepthBalance{}
	}

	var totalPending int64
	minPending := int64(-1)
	maxPending := int64(0)

	for _, shard := range services {
		pending := shard.client.pending.Load()
		totalPending += pending

		if minPending == -1 || pending < minPending {
			minPending = pending
		}

		if pending > maxPending {
			maxPending = pending
		}
	}

	avgPending := float64(totalPending) / float64(len(services))
	spread := maxPending - minPending
	imbalancePct := 0.0
	if avgPending > 0 {
		imbalancePct = float64(spread) / avgPending * 100
	}

	return queueDepthBalance{
		totalPending: totalPending,
		minPending:   minPending,
		maxPending:   maxPending,
		avgPending:   avgPending,
		spread:       spread,
		imbalancePct: imbalancePct,
	}
}

func reportQueueDepthBalance(b *testing.B, prefix string, balance queueDepthBalance) {
	b.Helper()

	b.ReportMetric(float64(balance.totalPending), prefix+"depth_total")
	b.ReportMetric(balance.avgPending, prefix+"depth_avg")
	b.ReportMetric(float64(balance.minPending), prefix+"depth_min")
	b.ReportMetric(float64(balance.maxPending), prefix+"depth_max")
	b.ReportMetric(float64(balance.spread), prefix+"depth_spread")
	b.ReportMetric(balance.imbalancePct, prefix+"depth_imbalance_pct")
	b.ReportMetric(float64(balance.totalInflight), prefix+"inflight_depth")
}
