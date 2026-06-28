package queue

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/cell"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func benchmarkJob() *api.Job {
	id := "bench-job"
	return &api.Job{Id: &id}
}

func benchmarkIsolationJob(id, isolation string) *api.Job {
	job := &api.Job{
		Id:   queueTestString(id),
		Root: queueTestNode(id+"-root", "builtins/shell"),
	}
	if isolation != "" {
		job.DefaultIsolation = queueTestString(isolation)
	}

	return job
}

func benchmarkQueueWorkerCount() int {
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		return 1
	}

	return workers
}

func BenchmarkQueue_Enqueue(b *testing.B) {
	ctx := context.Background()
	svc := NewQueueService(mocks.NopLogger{})
	job := benchmarkJob()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, job)); err != nil {
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
		if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, job)); err != nil {
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
		if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, job)); err != nil {
			b.Fatalf("enqueue failed: %v", err)
		}

		got, err := svc.Dequeue(ctx, &api.DequeueRequest{})
		if err != nil {
			b.Fatalf("dequeue failed: %v", err)
		}

		if got == nil {
			b.Fatal("expected dequeued job, got nil")
		}
	}
}

func BenchmarkQueue_TryDequeue_RoundTrip(b *testing.B) {
	ctx := context.Background()
	hostID := "bench-host-job"
	vmID := "bench-vm-job"
	vmDefault := action.IsolationVM
	hostJob := &api.Job{
		Id:   &hostID,
		Root: queueTestNode("host-root", "builtins/shell"),
	}

	vmJob := &api.Job{
		Id:               &vmID,
		DefaultIsolation: &vmDefault,
		Root:             queueTestNode("vm-root", "builtins/shell"),
	}

	hostOnly := &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost}}
	anyIsolation := &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost, action.IsolationVM}}

	cases := []struct {
		name  string
		first *api.Job
		next  *api.Job
	}{
		{name: "eligible_head", first: hostJob, next: vmJob},
		{name: "skip_one", first: vmJob, next: hostJob},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			svc := NewQueueService(mocks.NopLogger{})
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				tc.first.DeliveryId = nil
				tc.next.DeliveryId = nil
				if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, tc.first)); err != nil {
					b.Fatalf("enqueue first: %v", err)
				}
				if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, tc.next)); err != nil {
					b.Fatalf("enqueue next: %v", err)
				}

				got, err := svc.TryDequeue(ctx, hostOnly)
				if err != nil {
					b.Fatalf("trydequeue filtered: %v", err)
				}
				if got == nil || got.GetJob().GetId() != hostID {
					b.Fatalf("expected host job, got %#v", got)
				}
				if _, err := svc.Ack(ctx, &api.AckRequest{DeliveryId: got.GetJob().DeliveryId}); err != nil {
					b.Fatalf("ack host: %v", err)
				}

				cleanup, err := svc.TryDequeue(ctx, anyIsolation)
				if err != nil {
					b.Fatalf("cleanup trydequeue: %v", err)
				}
				if cleanup == nil || cleanup.GetJob().GetId() != vmID {
					b.Fatalf("expected vm cleanup job, got %#v", cleanup)
				}
				if _, err := svc.Ack(ctx, &api.AckRequest{DeliveryId: cleanup.GetJob().DeliveryId}); err != nil {
					b.Fatalf("ack cleanup: %v", err)
				}
			}
		})
	}
}

func BenchmarkQueue_IsolationSkipDepth(b *testing.B) {
	ctx := context.Background()
	hostOnly := &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost}}

	for _, skipDepth := range []int{0, 1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("skip_%d", skipDepth), func(b *testing.B) {
			svc := NewQueueService(mocks.NopLogger{})
			for i := 0; i < skipDepth; i++ {
				job := benchmarkIsolationJob(fmt.Sprintf("skip-vm-%d", i), action.IsolationVM)
				if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, job)); err != nil {
					b.Fatalf("enqueue ineligible job: %v", err)
				}
			}

			hostJob := benchmarkIsolationJob("skip-host", action.IsolationHost)
			b.ReportAllocs()
			b.ReportMetric(float64(skipDepth), "skip_depth")
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				hostJob.DeliveryId = nil
				if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, hostJob)); err != nil {
					b.Fatalf("enqueue eligible job: %v", err)
				}

				got, err := svc.TryDequeue(ctx, hostOnly)
				if err != nil {
					b.Fatalf("trydequeue filtered: %v", err)
				}
				if got == nil || got.GetJob().GetId() != hostJob.GetId() {
					b.Fatalf("expected eligible host job, got %#v", got)
				}

				deliveryID := got.GetJob().GetDeliveryId()
				if _, err := svc.Ack(ctx, &api.AckRequest{DeliveryId: &deliveryID}); err != nil {
					b.Fatalf("ack eligible job: %v", err)
				}
			}
		})
	}
}

func BenchmarkQueue_IsolationMixedDistribution(b *testing.B) {
	workers := benchmarkQueueWorkerCount()
	hostWorkers := max(1, workers*3/4)
	vmWorkers := max(1, workers-hostWorkers)

	for _, hostPercent := range []int{90, 50, 10} {
		b.Run(fmt.Sprintf("host_%d_vm_%d_workers_%dh_%dvm", hostPercent, 100-hostPercent, hostWorkers, vmWorkers), func(b *testing.B) {
			runMixedIsolationDistributionBenchmark(b, mixedIsolationDistributionConfig{
				hostPercent: hostPercent,
				hostWorkers: hostWorkers,
				vmWorkers:   vmWorkers,
			})
		})
	}
}

type mixedIsolationDistributionConfig struct {
	hostPercent int
	hostWorkers int
	vmWorkers   int
}

func runMixedIsolationDistributionBenchmark(b *testing.B, cfg mixedIsolationDistributionConfig) {
	const jobsPerIteration = 4096

	if cfg.hostWorkers < 0 || cfg.vmWorkers < 1 {
		b.Fatal("mixed isolation benchmark requires at least one VM-capable worker")
	}
	if cfg.hostPercent < 0 || cfg.hostPercent > 100 {
		b.Fatalf("invalid host percent %d", cfg.hostPercent)
	}

	ctx := context.Background()
	svc := NewQueueService(mocks.NopLogger{})
	totalJobs := jobsPerIteration * b.N
	if totalJobs < 1 {
		totalJobs = 1
	}

	for i := 0; i < totalJobs; i++ {
		isolation := action.IsolationVM
		if i%100 < cfg.hostPercent {
			isolation = action.IsolationHost
		}

		job := benchmarkIsolationJob(fmt.Sprintf("mixed-%d", i), isolation)
		if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, job)); err != nil {
			b.Fatalf("prefill enqueue: %v", err)
		}
	}

	hostOnly := &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost}}
	anyIsolation := &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost, action.IsolationVM}}
	workCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	var remaining atomic.Int64
	var delivered atomic.Int64
	var acked atomic.Int64
	var hostEmptyPolls atomic.Int64
	var vmEmptyPolls atomic.Int64
	var firstErr atomic.Value
	remaining.Store(int64(totalJobs))

	setErr := func(err error) {
		if err == nil || firstErr.Load() != nil {
			return
		}

		firstErr.Store(err)
	}

	worker := func(req *api.DequeueRequest, emptyPolls *atomic.Int64) {
		for remaining.Load() > 0 {
			select {
			case <-workCtx.Done():
				setErr(workCtx.Err())
				return
			default:
			}

			got, err := svc.TryDequeue(workCtx, req)
			if err != nil {
				setErr(fmt.Errorf("trydequeue: %w", err))
				return
			}

			if got == nil {
				emptyPolls.Add(1)
				runtime.Gosched()
				continue
			}

			deliveryID := got.GetJob().GetDeliveryId()
			if _, err := svc.Ack(workCtx, &api.AckRequest{DeliveryId: &deliveryID}); err != nil {
				setErr(fmt.Errorf("ack: %w", err))
				return
			}

			acked.Add(1)
			delivered.Add(1)
			remaining.Add(-1)
		}
	}

	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ReportMetric(float64(cfg.hostPercent), "host_job_pct")
	b.ReportMetric(float64(cfg.hostWorkers), "host_workers")
	b.ReportMetric(float64(cfg.vmWorkers), "vm_workers")
	b.ReportMetric(float64(totalJobs), "jobs")
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < cfg.hostWorkers; i++ {
		wg.Go(func() { worker(hostOnly, &hostEmptyPolls) })
	}
	for i := 0; i < cfg.vmWorkers; i++ {
		wg.Go(func() { worker(anyIsolation, &vmEmptyPolls) })
	}

	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()

	if v := firstErr.Load(); v != nil {
		b.Fatal(v.(error))
	}
	if delivered.Load() != int64(totalJobs) {
		b.Fatalf("delivered %d jobs, want %d", delivered.Load(), totalJobs)
	}
	if elapsed <= 0 {
		b.Fatal("invalid elapsed duration")
	}

	b.ReportMetric(float64(delivered.Load())/elapsed.Seconds(), "dequeue_ops/s")
	b.ReportMetric(float64(acked.Load())/elapsed.Seconds(), "ack_ops/s")
	b.ReportMetric(float64(hostEmptyPolls.Load()), "host_empty_polls")
	b.ReportMetric(float64(vmEmptyPolls.Load()), "vm_empty_polls")
	b.ReportMetric(float64(hostEmptyPolls.Load())/float64(totalJobs), "host_empty_polls_per_job")
	b.ReportMetric(float64(vmEmptyPolls.Load())/float64(totalJobs), "vm_empty_polls_per_job")
}

func BenchmarkQueue_IsolationLargeActionTreeMatching(b *testing.B) {
	ctx := context.Background()
	hostOnly := &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost}}

	for _, nodes := range []int{32, 256, 1024} {
		b.Run(fmt.Sprintf("task_key_vm_leaf_miss_nodes_%d", nodes), func(b *testing.B) {
			root, targetID := benchmarkNestedIsolationTree(nodes, action.IsolationVM)
			jobID := fmt.Sprintf("large-tree-%d", nodes)
			svc := NewQueueService(mocks.NopLogger{})
			if _, err := svc.Enqueue(ctx, queueTestRequest(b, &api.JobRequest{
				Job: &api.Job{
					Id:               &jobID,
					DefaultIsolation: queueTestString(action.IsolationHost),
					Root:             root,
				},
				Metadata: map[string]string{
					cell.ExecutionTaskKeyMetadataKey: targetID,
				},
			})); err != nil {
				b.Fatalf("enqueue large tree job: %v", err)
			}

			b.ReportAllocs()
			b.ReportMetric(float64(nodes), "action_nodes")
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				got, err := svc.TryDequeue(ctx, hostOnly)
				if err != nil {
					b.Fatalf("trydequeue large tree: %v", err)
				}
				if got != nil {
					b.Fatalf("expected unsupported VM leaf to miss, got %#v", got)
				}
			}
		})

		b.Run(fmt.Sprintf("task_key_far_leaf_miss_nodes_%d", nodes), func(b *testing.B) {
			root, targetID := benchmarkNestedIsolationTree(nodes, action.IsolationVM)
			jobID := fmt.Sprintf("large-tree-task-%d", nodes)
			svc := NewQueueService(mocks.NopLogger{})
			if _, err := svc.Enqueue(ctx, queueTestRequest(b, &api.JobRequest{
				Job: &api.Job{
					Id:               &jobID,
					DefaultIsolation: queueTestString(action.IsolationHost),
					Root:             root,
				},
				Metadata: map[string]string{
					cell.ExecutionTaskKeyMetadataKey: targetID,
				},
			})); err != nil {
				b.Fatalf("enqueue large tree task job: %v", err)
			}

			b.ReportAllocs()
			b.ReportMetric(float64(nodes), "action_nodes")
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				got, err := svc.TryDequeue(ctx, hostOnly)
				if err != nil {
					b.Fatalf("trydequeue large tree task: %v", err)
				}
				if got != nil {
					b.Fatalf("expected unsupported VM task to miss, got %#v", got)
				}
			}
		})
	}
}

func benchmarkNestedIsolationTree(nodes int, leafIsolation string) (*api.Node, string) {
	if nodes < 1 {
		nodes = 1
	}

	root := queueTestNode("node-0", "builtins/sequence")
	current := root
	for i := 1; i < nodes; i++ {
		id := fmt.Sprintf("node-%d", i)
		child := queueTestNode(id, "builtins/sequence")
		current.Steps = []*api.Node{child}
		current = child
	}

	current.Uses = queueTestString("builtins/shell")
	if leafIsolation != "" {
		current.Isolation = queueTestString(leafIsolation)
	}

	return root, current.GetId()
}

func BenchmarkQueue_FilteredBlockingDequeueLatency(b *testing.B) {
	ctx := context.Background()
	hostOnly := &api.DequeueRequest{SupportedIsolation: []string{action.IsolationHost}}

	for _, skipDepth := range []int{1, 100} {
		b.Run(fmt.Sprintf("skip_%d", skipDepth), func(b *testing.B) {
			svc := NewQueueService(mocks.NopLogger{})
			for i := 0; i < skipDepth; i++ {
				job := benchmarkIsolationJob(fmt.Sprintf("blocking-vm-%d", i), action.IsolationVM)
				if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, job)); err != nil {
					b.Fatalf("enqueue blocking ineligible job: %v", err)
				}
			}

			type dequeueResult struct {
				req *api.JobRequest
				err error
			}

			results := make(chan dequeueResult)
			go func() {
				for i := 0; i < b.N; i++ {
					got, err := svc.Dequeue(ctx, hostOnly)
					results <- dequeueResult{req: got, err: err}
				}
			}()

			hostJob := benchmarkIsolationJob("blocking-host", action.IsolationHost)
			var totalLatency time.Duration

			b.ReportAllocs()
			b.ReportMetric(float64(skipDepth), "skip_depth")
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				hostJob.DeliveryId = nil
				start := time.Now()
				if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, hostJob)); err != nil {
					b.Fatalf("enqueue blocking eligible job: %v", err)
				}

				result := <-results
				totalLatency += time.Since(start)
				if result.err != nil {
					b.Fatalf("blocking dequeue: %v", result.err)
				}
				if result.req == nil || result.req.GetJob().GetId() != hostJob.GetId() {
					b.Fatalf("expected blocking host job, got %#v", result.req)
				}

				deliveryID := result.req.GetJob().GetDeliveryId()
				if _, err := svc.Ack(ctx, &api.AckRequest{DeliveryId: &deliveryID}); err != nil {
					b.Fatalf("ack blocking host job: %v", err)
				}
				runtime.Gosched()
			}

			b.StopTimer()
			if b.N > 0 {
				b.ReportMetric(float64(totalLatency.Nanoseconds())/float64(b.N), "avg_enqueue_to_delivery_ns")
			}
		})
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
			if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, job)); err != nil {
				b.Fatalf("enqueue failed: %v", err)
			}

			got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
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

func TestQueue_BurstEnqueueWakesBlockedCompatibleWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc := NewQueueService(mocks.NopLogger{})
	queue := svc.(*queueServer)

	const workers = 16
	results := make(chan error, workers)
	for range workers {
		go func() {
			got, err := svc.Dequeue(ctx, &api.DequeueRequest{})
			if err != nil {
				results <- err
				return
			}

			if got == nil {
				results <- fmt.Errorf("expected dequeued job")
				return
			}

			results <- nil
		}()
	}

	waitForQueueWaiters(t, queue, workers)
	for i := range workers {
		jobID := fmt.Sprintf("burst-test-job-%d", i)
		if _, err := svc.Enqueue(ctx, queueTestJobRequest(t, &api.Job{Id: &jobID})); err != nil {
			t.Fatalf("enqueue burst job %d: %v", i, err)
		}
	}

	deadline := time.After(2 * time.Second)
	for i := 0; i < workers; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %d blocked workers to receive burst jobs; got %d", workers, i)
		}
	}
}

func BenchmarkQueue_BlockedWorkerBurstDispatch(b *testing.B) {
	for _, workers := range []int{100, 1000, 5000, 10000} {
		b.Run(fmt.Sprintf("blocked_workers_%05d", workers), func(b *testing.B) {
			runBlockedWorkerBurstDispatchBenchmark(b, workers)
		})
	}
}

func BenchmarkQueue_GRPCBlockedWorkerBurstDispatch(b *testing.B) {
	for _, workers := range []int{100, 1000, 5000, 10000} {
		b.Run(fmt.Sprintf("blocked_workers_%05d", workers), func(b *testing.B) {
			runGRPCBlockedWorkerBurstDispatchBenchmark(b, workers)
		})
	}
}

func BenchmarkQueue_GRPCBlockedWorkerBurstDispatch_ManyConns(b *testing.B) {
	for _, workers := range []int{100, 1000, 5000} {
		b.Run(fmt.Sprintf("blocked_workers_%05d", workers), func(b *testing.B) {
			runGRPCBlockedWorkerBurstDispatchManyConnsBenchmark(b, workers)
		})
	}
}

func runBlockedWorkerBurstDispatchBenchmark(b *testing.B, workers int) {
	if workers <= 0 {
		b.Fatal("workers must be positive")
	}

	var measured time.Duration
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		ctx, cancel := context.WithCancel(context.Background())
		svc := NewQueueService(mocks.NopLogger{})
		queue := svc.(*queueServer)
		results := make(chan error, workers)

		for range workers {
			go func() {
				got, err := svc.Dequeue(ctx, &api.DequeueRequest{})
				if err != nil {
					results <- err
					return
				}

				if got == nil {
					results <- fmt.Errorf("expected dequeued job")
					return
				}

				results <- nil
			}()
		}

		waitForQueueWaiters(b, queue, workers)

		b.StartTimer()
		start := time.Now()
		for jobIndex := 0; jobIndex < workers; jobIndex++ {
			jobID := fmt.Sprintf("burst-job-%d-%d", iteration, jobIndex)
			if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, &api.Job{Id: &jobID})); err != nil {
				b.Fatalf("enqueue burst job %d: %v", jobIndex, err)
			}
		}

		deadline := time.After(10 * time.Second)
		for delivered := 0; delivered < workers; delivered++ {
			select {
			case err := <-results:
				if err != nil {
					b.Fatal(err)
				}
			case <-deadline:
				b.Fatalf("timed out waiting for %d blocked workers to receive burst jobs; got %d", workers, delivered)
			}
		}

		measured += time.Since(start)
		b.StopTimer()
		cancel()
	}

	if measured > 0 {
		totalDeliveries := float64(workers * b.N)
		b.ReportMetric(totalDeliveries/measured.Seconds(), "deliveries/s")
	}

	b.ReportMetric(float64(workers), "blocked_workers")
}

func runGRPCBlockedWorkerBurstDispatchBenchmark(b *testing.B, workers int) {
	if workers <= 0 {
		b.Fatal("workers must be positive")
	}

	var measured time.Duration
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		measured += runGRPCBlockedWorkerBurstDispatchIteration(b, iteration, workers)
	}

	if measured > 0 {
		totalDeliveries := float64(workers * b.N)
		b.ReportMetric(totalDeliveries/measured.Seconds(), "deliveries/s")
	}

	b.ReportMetric(float64(workers), "blocked_workers")
	b.ReportMetric(1, "client_conns")
}

func runGRPCBlockedWorkerBurstDispatchIteration(b *testing.B, iteration, workers int) time.Duration {
	b.Helper()
	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	address, queue, cleanupServer := startBenchmarkQueueGRPCServer(b)
	conn, cleanupConn := dialBenchmarkQueueGRPCServer(b, address)
	defer cleanupServer()
	defer cleanupConn()
	defer cancel()

	client := api.NewQueueServiceClient(conn)
	results := make(chan error, workers)

	for range workers {
		go func() {
			got, err := client.Dequeue(ctx, &api.DequeueRequest{})
			if err != nil {
				results <- err
				return
			}

			if got == nil {
				results <- fmt.Errorf("expected dequeued job")
				return
			}

			results <- nil
		}()
	}

	waitForQueueWaiters(b, queue, workers)

	b.StartTimer()
	start := time.Now()
	for jobIndex := 0; jobIndex < workers; jobIndex++ {
		jobID := fmt.Sprintf("grpc-burst-job-%d-%d", iteration, jobIndex)
		if _, err := client.Enqueue(ctx, queueTestJobRequest(b, &api.Job{Id: &jobID})); err != nil {
			b.Fatalf("enqueue grpc burst job %d: %v", jobIndex, err)
		}
	}

	deadline := time.After(20 * time.Second)
	for delivered := 0; delivered < workers; delivered++ {
		select {
		case err := <-results:
			if err != nil {
				b.Fatal(err)
			}
		case <-deadline:
			b.Fatalf("timed out waiting for %d blocked grpc workers to receive burst jobs; got %d", workers, delivered)
		}
	}

	measured := time.Since(start)
	b.StopTimer()

	return measured
}

func runGRPCBlockedWorkerBurstDispatchManyConnsBenchmark(b *testing.B, workers int) {
	if workers <= 0 {
		b.Fatal("workers must be positive")
	}

	b.StopTimer()
	ctx, cancel := context.WithCancel(context.Background())
	address, queue, cleanupServer := startBenchmarkQueueGRPCServer(b)
	enqueuerConn, cleanupEnqueuer := dialBenchmarkQueueGRPCServer(b, address)
	defer cleanupServer()
	defer cleanupEnqueuer()
	defer cancel()

	enqueuer := api.NewQueueServiceClient(enqueuerConn)
	clients := make([]api.QueueServiceClient, 0, workers)
	cleanupConns := make([]func(), 0, workers)
	defer func() {
		for _, cleanupConn := range cleanupConns {
			cleanupConn()
		}
	}()

	for workerIndex := range workers {
		conn, cleanupConn := dialBenchmarkQueueGRPCServer(b, address)
		conn.Connect()
		cleanupConns = append(cleanupConns, cleanupConn)
		clients = append(clients, api.NewQueueServiceClient(conn))
		if workerIndex%128 == 127 {
			time.Sleep(time.Millisecond)
		}
	}

	var measured time.Duration
	var waitersReady time.Duration
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		var iterationReady time.Duration
		var iterationMeasured time.Duration
		iterationMeasured, iterationReady = runGRPCBlockedWorkerBurstDispatchManyConnsIteration(
			b,
			ctx,
			queue,
			enqueuer,
			clients,
			iteration,
		)

		measured += iterationMeasured
		waitersReady += iterationReady
	}

	if measured > 0 {
		totalDeliveries := float64(workers * b.N)
		b.ReportMetric(totalDeliveries/measured.Seconds(), "deliveries/s")
	}

	if waitersReady > 0 {
		totalWaiters := float64(workers * b.N)
		b.ReportMetric(totalWaiters/waitersReady.Seconds(), "waiters_ready/s")
	}

	b.ReportMetric(float64(workers), "blocked_workers")
	b.ReportMetric(float64(workers+1), "client_conns")
}

func runGRPCBlockedWorkerBurstDispatchManyConnsIteration(
	b *testing.B,
	ctx context.Context,
	queue *queueServer,
	enqueuer api.QueueServiceClient,
	clients []api.QueueServiceClient,
	iteration int,
) (time.Duration, time.Duration) {
	b.Helper()
	b.StopTimer()

	workers := len(clients)
	results := make(chan error, workers)

	for workerIndex, client := range clients {
		go func() {
			got, err := client.Dequeue(ctx, &api.DequeueRequest{})
			if err != nil {
				results <- err
				return
			}

			if got == nil {
				results <- fmt.Errorf("expected dequeued job")
				return
			}

			results <- nil
		}()

		if workerIndex%128 == 127 {
			runtime.Gosched()
		}
	}

	waitersReady := waitForQueueWaitersWithin(b, queue, workers, 2*time.Minute)

	b.StartTimer()
	start := time.Now()
	for jobIndex := 0; jobIndex < workers; jobIndex++ {
		jobID := fmt.Sprintf("grpc-manyconns-burst-job-%d-%d", iteration, jobIndex)
		if _, err := enqueuer.Enqueue(ctx, queueTestJobRequest(b, &api.Job{Id: &jobID})); err != nil {
			b.Fatalf("enqueue grpc many-conn burst job %d: %v", jobIndex, err)
		}
	}

	deadline := time.After(30 * time.Second)
	for delivered := 0; delivered < workers; delivered++ {
		select {
		case err := <-results:
			if err != nil {
				b.Fatal(err)
			}
		case <-deadline:
			b.Fatalf("timed out waiting for %d blocked grpc many-conn workers to receive burst jobs; got %d", workers, delivered)
		}
	}

	measured := time.Since(start)
	b.StopTimer()

	return measured, waitersReady
}

func startBenchmarkQueueGRPCServer(tb testing.TB) (string, *queueServer, func()) {
	tb.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen benchmark queue grpc server: %v", err)
	}

	server := grpc.NewServer()
	queueSvc := RegisterQueueService(server, mocks.NopLogger{}, QueueOptions{}, nil)
	queue, ok := queueSvc.(*queueServer)
	if !ok {
		tb.Fatal("expected queue server implementation")
	}

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(listener)
	}()

	cleanup := func() {
		server.Stop()
		_ = listener.Close()
		<-serveDone
	}

	return listener.Addr().String(), queue, cleanup
}

func dialBenchmarkQueueGRPCServer(tb testing.TB, address string) (*grpc.ClientConn, func()) {
	tb.Helper()

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatalf("dial benchmark queue grpc server: %v", err)
	}

	return conn, func() {
		_ = conn.Close()
	}
}

func waitForQueueWaiters(tb testing.TB, queue *queueServer, want int) {
	tb.Helper()
	_ = waitForQueueWaitersWithin(tb, queue, want, 5*time.Second)
}

func waitForQueueWaitersWithin(tb testing.TB, queue *queueServer, want int, timeout time.Duration) time.Duration {
	tb.Helper()

	start := time.Now()
	deadline := start.Add(timeout)
	for {
		queue.mu.Lock()
		got := queue.waiterCount
		queue.mu.Unlock()

		if got >= want {
			return time.Since(start)
		}

		if time.Now().After(deadline) {
			tb.Fatalf("timed out after %v waiting for %d blocked queue waiters; got %d", timeout, want, got)
		}

		runtime.Gosched()
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

				if _, err := svc.Enqueue(ctx, queueTestJobRequest(b, job)); err != nil {
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

				got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
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
