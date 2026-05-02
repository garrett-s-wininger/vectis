package queue

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
)

func benchmarkJob() *api.Job {
	id := "bench-job"
	return &api.Job{Id: &id}
}

func BenchmarkQueue_Enqueue(b *testing.B) {
	ctx := context.Background()
	svc := NewQueueService(mocks.NopLogger{})
	job := benchmarkJob()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: job}); err != nil {
			b.Fatalf("enqueue failed: %v", err)
		}
	}
}

func BenchmarkQueue_Enqueue_DuplicateRunID(b *testing.B) {
	ctx := context.Background()
	svc := NewQueueService(mocks.NopLogger{})
	runID := "same-run-id"
	id := "bench-job"
	job := &api.Job{Id: &id, RunId: &runID}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: job}); err != nil {
			b.Fatalf("enqueue failed: %v", err)
		}
	}
}

func BenchmarkQueue_EnqueueDequeue_RoundTrip(b *testing.B) {
	ctx := context.Background()
	svc := NewQueueService(mocks.NopLogger{})
	job := benchmarkJob()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: job}); err != nil {
			b.Fatalf("enqueue failed: %v", err)
		}

		got, err := svc.Dequeue(ctx, &api.Empty{})
		if err != nil {
			b.Fatalf("dequeue failed: %v", err)
		}

		if got == nil {
			b.Fatal("expected dequeued job, got nil")
		}
	}
}

func BenchmarkQueue_ConcurrentEnqueueDequeue(b *testing.B) {
	ctx := context.Background()
	svc := NewQueueService(mocks.NopLogger{})
	job := benchmarkJob()
	var misses atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: job}); err != nil {
				b.Fatalf("enqueue failed: %v", err)
			}

			got, err := svc.TryDequeue(ctx, &api.Empty{})
			if err != nil {
				b.Fatalf("trydequeue failed: %v", err)
			}

			if got == nil {
				misses.Add(1)
			}
		}
	})

	if misses.Load() > 0 {
		b.Fatalf("unexpected empty dequeue count in steady-state benchmark: %d", misses.Load())
	}
}

type sustainedLoadConfig struct {
	name          string
	producers     int
	consumers     int
	consumerDelay time.Duration
	stepDuration  time.Duration
}

func BenchmarkQueue_SustainedLoad(b *testing.B) {
	cases := []sustainedLoadConfig{
		{
			name:         "balanced",
			producers:    4,
			consumers:    4,
			stepDuration: 500 * time.Millisecond,
		},
		{
			name:          "overloaded",
			producers:     6,
			consumers:     2,
			consumerDelay: 10 * time.Microsecond,
			stepDuration:  500 * time.Millisecond,
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			runSustainedLoadScenario(b, tc)
		})
	}
}

func runSustainedLoadScenario(b *testing.B, cfg sustainedLoadConfig) {
	if b.N <= 0 {
		b.Skip("invalid benchmark iteration count")
	}

	svc := NewQueueService(mocks.NopLogger{})
	queue, ok := svc.(*queueServer)
	if !ok {
		b.Fatal("expected queue server implementation")
	}

	var enqueueCount atomic.Int64
	var dequeueCount atomic.Int64
	var currentDepth atomic.Int64
	var maxDepth atomic.Int64

	var firstErr atomic.Value
	setErr := func(err error) {
		if err == nil || firstErr.Load() != nil {
			return
		}

		firstErr.Store(err)
	}

	perIteration := cfg.stepDuration
	totalDuration := perIteration * time.Duration(b.N)
	ctx, cancel := context.WithTimeout(context.Background(), totalDuration)
	defer cancel()

	var wg sync.WaitGroup
	jobID := "load-job"
	job := &api.Job{Id: &jobID}

	for i := 0; i < cfg.producers; i++ {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if _, err := svc.Enqueue(ctx, &api.JobRequest{Job: job}); err != nil {
					setErr(fmt.Errorf("producer enqueue: %w", err))
					return
				}

				enqueueCount.Add(1)
				depth := currentDepth.Add(1)
				for {
					prev := maxDepth.Load()
					if depth <= prev || maxDepth.CompareAndSwap(prev, depth) {
						break
					}
				}
			}
		})
	}

	for i := 0; i < cfg.consumers; i++ {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				got, err := svc.TryDequeue(ctx, &api.Empty{})
				if err != nil {
					setErr(fmt.Errorf("consumer dequeue: %w", err))
					return
				}

				if got == nil {
					runtime.Gosched()
					continue
				}

				dequeueCount.Add(1)
				currentDepth.Add(-1)

				if cfg.consumerDelay > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(cfg.consumerDelay):
					}
				}
			}
		})
	}

	wg.Wait()

	if v := firstErr.Load(); v != nil {
		b.Fatal(v.(error))
	}

	queue.mu.Lock()
	finalQueueDepth := queue.size
	queue.mu.Unlock()

	elapsedSeconds := totalDuration.Seconds()
	if elapsedSeconds == 0 {
		b.Skip("benchmark duration was zero")
	}

	b.ReportMetric(float64(enqueueCount.Load())/elapsedSeconds, "enqueue_ops/s")
	b.ReportMetric(float64(dequeueCount.Load())/elapsedSeconds, "dequeue_ops/s")
	b.ReportMetric(float64(finalQueueDepth), "final_depth")
	b.ReportMetric(float64(maxDepth.Load()), "max_depth")
}
