package queue

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
)

type latencyScenario struct {
	name          string
	producers     int
	consumers     int
	duration      time.Duration
	sampleEvery   uint64
	consumerDelay time.Duration
}

func BenchmarkQueue_RingLatency(b *testing.B) {
	scenarios := []latencyScenario{
		{
			name:        "balanced_p4_c4",
			producers:   4,
			consumers:   4,
			duration:    300 * time.Millisecond,
			sampleEvery: 256,
		},
		{
			name:        "bursty_p8_c4",
			producers:   8,
			consumers:   4,
			duration:    300 * time.Millisecond,
			sampleEvery: 256,
		},
		{
			name:          "overloaded_p8_c2",
			producers:     8,
			consumers:     2,
			duration:      300 * time.Millisecond,
			sampleEvery:   256,
			consumerDelay: 5 * time.Microsecond,
		},
	}

	for _, sc := range scenarios {
		sc := sc
		b.Run(sc.name, func(b *testing.B) {
			runLatencyScenario(b, sc)
		})
	}
}

func runLatencyScenario(b *testing.B, sc latencyScenario) {
	if b.N <= 0 {
		b.Skip("invalid benchmark iteration count")
	}

	var enqueueCount atomic.Uint64
	var dequeueCount atomic.Uint64
	var totalDepth atomic.Int64
	var maxDepth atomic.Int64

	var enqueueSamples []int64
	var dequeueSamples []int64

	sampleMask := sc.sampleEvery - 1
	if sc.sampleEvery == 0 || sc.sampleEvery&(sc.sampleEvery-1) != 0 {
		b.Fatalf("sampleEvery must be a power of two, got %d", sc.sampleEvery)
	}

	var totalDuration time.Duration
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		svc := NewQueueService(noopLogger{})
		ctx, cancel := context.WithTimeout(context.Background(), sc.duration)

		var wg sync.WaitGroup
		enqueueCh := make(chan []int64, sc.producers)
		dequeueCh := make(chan []int64, sc.consumers)
		jobID := "latency-job"
		job := &api.Job{Id: &jobID}

		iterStart := time.Now()

		for i := 0; i < sc.producers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				samples := make([]int64, 0, 512)
				var localCount uint64

				for {
					select {
					case <-ctx.Done():
						enqueueCh <- samples
						return
					default:
					}

					start := time.Now()
					_, err := svc.Enqueue(ctx, job)
					lat := time.Since(start)
					if err != nil {
						enqueueCh <- samples
						return
					}

					n := enqueueCount.Add(1)
					depth := totalDepth.Add(1)
					for {
						prev := maxDepth.Load()
						if depth <= prev || maxDepth.CompareAndSwap(prev, depth) {
							break
						}
					}

					localCount++
					if localCount&sampleMask == 0 {
						samples = append(samples, lat.Nanoseconds())
					}
					_ = n
				}
			}()
		}

		for i := 0; i < sc.consumers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				samples := make([]int64, 0, 512)
				var localCount uint64

				for {
					select {
					case <-ctx.Done():
						dequeueCh <- samples
						return
					default:
					}

					start := time.Now()
					got, err := svc.TryDequeue(ctx, &api.Empty{})
					lat := time.Since(start)
					if err != nil {
						dequeueCh <- samples
						return
					}

					if got == nil {
						runtime.Gosched()
						continue
					}

					dequeueCount.Add(1)
					totalDepth.Add(-1)

					localCount++
					if localCount&sampleMask == 0 {
						samples = append(samples, lat.Nanoseconds())
					}

					if sc.consumerDelay > 0 {
						select {
						case <-ctx.Done():
							dequeueCh <- samples
							return
						case <-time.After(sc.consumerDelay):
						}
					}
				}
			}()
		}

		wg.Wait()
		cancel()

		close(enqueueCh)
		for s := range enqueueCh {
			enqueueSamples = append(enqueueSamples, s...)
		}

		close(dequeueCh)
		for s := range dequeueCh {
			dequeueSamples = append(dequeueSamples, s...)
		}

		totalDuration += time.Since(iterStart)
	}

	if len(enqueueSamples) == 0 || len(dequeueSamples) == 0 {
		b.Fatalf("insufficient samples collected (enqueue=%d dequeue=%d)", len(enqueueSamples), len(dequeueSamples))
	}

	sort.Slice(enqueueSamples, func(i, j int) bool { return enqueueSamples[i] < enqueueSamples[j] })
	sort.Slice(dequeueSamples, func(i, j int) bool { return dequeueSamples[i] < dequeueSamples[j] })

	b.ReportMetric(float64(quantile(enqueueSamples, 0.50)), "enqueue_p50_ns")
	b.ReportMetric(float64(quantile(enqueueSamples, 0.95)), "enqueue_p95_ns")
	b.ReportMetric(float64(quantile(enqueueSamples, 0.99)), "enqueue_p99_ns")
	b.ReportMetric(float64(enqueueSamples[len(enqueueSamples)-1]), "enqueue_max_ns")

	b.ReportMetric(float64(quantile(dequeueSamples, 0.50)), "dequeue_p50_ns")
	b.ReportMetric(float64(quantile(dequeueSamples, 0.95)), "dequeue_p95_ns")
	b.ReportMetric(float64(quantile(dequeueSamples, 0.99)), "dequeue_p99_ns")
	b.ReportMetric(float64(dequeueSamples[len(dequeueSamples)-1]), "dequeue_max_ns")

	seconds := totalDuration.Seconds()
	if seconds == 0 {
		b.Fatalf("invalid elapsed duration")
	}

	b.ReportMetric(float64(enqueueCount.Load())/seconds, "enqueue_ops/s")
	b.ReportMetric(float64(dequeueCount.Load())/seconds, "dequeue_ops/s")
	b.ReportMetric(float64(totalDepth.Load()), "final_depth")
	b.ReportMetric(float64(maxDepth.Load()), "max_depth")
}

func quantile(values []int64, q float64) int64 {
	if len(values) == 0 {
		return 0
	}

	if q <= 0 {
		return values[0]
	}

	if q >= 1 {
		return values[len(values)-1]
	}

	idx := int(float64(len(values)-1) * q)
	return values[idx]
}
