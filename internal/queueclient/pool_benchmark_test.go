package queueclient

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/grpc"
)

const queuePoolEnqueueBurstJobs = 100_000

type benchmarkQueueServiceClient struct {
	pending atomic.Int64
}

func (b *benchmarkQueueServiceClient) Enqueue(context.Context, *api.JobRequest, ...grpc.CallOption) (*api.Empty, error) {
	b.pending.Add(1)
	return &api.Empty{}, nil
}

func (b *benchmarkQueueServiceClient) Dequeue(context.Context, *api.Empty, ...grpc.CallOption) (*api.JobRequest, error) {
	return b.TryDequeue(context.Background(), &api.Empty{})
}

func (b *benchmarkQueueServiceClient) TryDequeue(context.Context, *api.Empty, ...grpc.CallOption) (*api.JobRequest, error) {
	for {
		pending := b.pending.Load()
		if pending <= 0 {
			return nil, nil
		}
		if b.pending.CompareAndSwap(pending, pending-1) {
			return &api.JobRequest{Job: &api.Job{}}, nil
		}
	}
}

func (b *benchmarkQueueServiceClient) Ack(context.Context, *api.AckRequest, ...grpc.CallOption) (*api.Empty, error) {
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

func runQueuePoolEnqueueBurst(b *testing.B, shards int) {
	pool, services := newLocalBenchmarkQueuePool(b, shards)
	defer closeLocalBenchmarkQueuePool(b, pool, services)

	ctx := context.Background()
	jobID := "sharded-enqueue-job"
	job := &api.Job{Id: &jobID}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	totalJobs := queuePoolEnqueueBurstJobs * b.N

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
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
	reportQueueDepthBalance(b, services)
}

func newLocalBenchmarkQueuePool(b *testing.B, shards int) (*queuePool, []*benchmarkQueueServiceClient) {
	b.Helper()

	p := &queuePool{
		logger:    mocks.NopLogger{},
		endpoints: make(map[string]*queuePoolEndpoint, shards),
		activeIDs: make([]string, 0, shards),
	}
	services := make([]*benchmarkQueueServiceClient, 0, shards)

	for i := 0; i < shards; i++ {
		id := fmt.Sprintf("queue-%02d", i+1)
		svc := &benchmarkQueueServiceClient{}

		p.endpoints[id] = &queuePoolEndpoint{
			id:      id,
			address: id,
			client:  svc,
		}
		p.activeIDs = append(p.activeIDs, id)
		p.active = append(p.active, p.endpoints[id])
		services = append(services, svc)
	}

	return p, services
}

func closeLocalBenchmarkQueuePool(b *testing.B, pool *queuePool, services []*benchmarkQueueServiceClient) {
	b.Helper()

	if err := pool.close(); err != nil {
		b.Fatalf("close pool: %v", err)
	}
}

func reportQueueDepthBalance(b *testing.B, services []*benchmarkQueueServiceClient) {
	b.Helper()

	if len(services) == 0 {
		return
	}

	var totalPending int64
	var totalInflight int64
	minPending := int64(-1)
	maxPending := int64(0)

	for _, svc := range services {
		pending := svc.pending.Load()
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

	b.ReportMetric(float64(totalPending), "depth_total")
	b.ReportMetric(avgPending, "depth_avg")
	b.ReportMetric(float64(minPending), "depth_min")
	b.ReportMetric(float64(maxPending), "depth_max")
	b.ReportMetric(float64(spread), "depth_spread")
	b.ReportMetric(imbalancePct, "depth_imbalance_pct")
	b.ReportMetric(float64(totalInflight), "inflight_depth")
}
