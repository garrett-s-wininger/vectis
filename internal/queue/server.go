package queue

import (
	"context"
	"fmt"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type QueueOptions struct {
	PersistenceDir string
	SnapshotEvery  int
}

type queueServer struct {
	api.UnimplementedQueueServiceServer
	jobs        []*api.Job
	head        int
	size        int
	mu          sync.Mutex
	notify      chan struct{}
	log         interfaces.Logger
	persistence *persistenceStore
}

const initialQueueCapacity = 1024

func NewQueueService(logger interfaces.Logger) api.QueueServiceServer {
	s, err := newQueueServer(logger, QueueOptions{})
	if err != nil {
		panic(err)
	}

	return s
}

func NewQueueServiceWithOptions(logger interfaces.Logger, opts QueueOptions) (api.QueueServiceServer, error) {
	return newQueueServer(logger, opts)
}

func newQueueServer(logger interfaces.Logger, opts QueueOptions) (*queueServer, error) {
	s := &queueServer{
		jobs:   make([]*api.Job, initialQueueCapacity),
		head:   0,
		size:   0,
		notify: make(chan struct{}, 1),
		log:    logger,
	}

	store, state, err := newPersistenceStore(opts.PersistenceDir, opts.SnapshotEvery)
	if err != nil {
		return nil, err
	}
	s.persistence = store

	if len(state.jobs) > 0 {
		s.loadPending(state.jobs)
		s.log.Info("Restored %d pending job(s) from queue persistence", len(state.jobs))
	}

	return s, nil
}

func (s *queueServer) Enqueue(ctx context.Context, req *api.Job) (*api.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.persistence != nil {
		if err := s.persistence.appendEnqueue(req, s.pendingJobsAfterEnqueueLocked(req)); err != nil {
			return nil, fmt.Errorf("persist enqueue: %w", err)
		}
	}

	if s.size == len(s.jobs) {
		s.grow()
	}

	tail := (s.head + s.size) % len(s.jobs)
	s.jobs[tail] = req
	s.size++
	s.log.Info("Enqueued job: %s", req.GetId())

	select {
	case s.notify <- struct{}{}:
	default:
	}

	return &api.Empty{}, nil
}

func (s *queueServer) Dequeue(ctx context.Context, _ *api.Empty) (*api.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.size == 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		s.mu.Unlock()
		select {
		case <-s.notify:
		case <-ctx.Done():
			s.mu.Lock()
			return nil, ctx.Err()
		}
		s.mu.Lock()
	}

	if s.size == 0 {
		return nil, nil
	}

	if s.persistence != nil {
		if err := s.persistence.appendDequeue(s.pendingJobsAfterOneDequeueLocked()); err != nil {
			return nil, fmt.Errorf("persist dequeue: %w", err)
		}
	}

	job := s.jobs[s.head]
	s.jobs[s.head] = nil
	s.head = (s.head + 1) % len(s.jobs)
	s.size--
	s.log.Info("Dequeued job: %s", job.GetId())
	return job, nil
}

func (s *queueServer) TryDequeue(ctx context.Context, _ *api.Empty) (*api.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.size == 0 {
		return nil, nil
	}

	if s.persistence != nil {
		if err := s.persistence.appendDequeue(s.pendingJobsAfterOneDequeueLocked()); err != nil {
			return nil, fmt.Errorf("persist trydequeue: %w", err)
		}
	}

	job := s.jobs[s.head]
	s.jobs[s.head] = nil
	s.head = (s.head + 1) % len(s.jobs)
	s.size--
	s.log.Info("TryDequeue returned job: %s", job.GetId())
	return job, nil
}

func (s *queueServer) pendingJobsLocked() []*api.Job {
	out := make([]*api.Job, 0, s.size)
	for i := 0; i < s.size; i++ {
		out = append(out, s.jobs[(s.head+i)%len(s.jobs)])
	}

	return out
}

func (s *queueServer) pendingJobsAfterEnqueueLocked(job *api.Job) []*api.Job {
	out := s.pendingJobsLocked()
	return append(out, job)
}

func (s *queueServer) pendingJobsAfterOneDequeueLocked() []*api.Job {
	if s.size <= 1 {
		return nil
	}

	out := make([]*api.Job, 0, s.size-1)
	for i := 1; i < s.size; i++ {
		out = append(out, s.jobs[(s.head+i)%len(s.jobs)])
	}

	return out
}

func (s *queueServer) loadPending(jobs []*api.Job) {
	if len(jobs) == 0 {
		return
	}

	capHint := initialQueueCapacity
	for capHint < len(jobs) {
		capHint *= 2
	}

	s.jobs = make([]*api.Job, capHint)
	s.head = 0
	s.size = len(jobs)
	copy(s.jobs, jobs)

	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *queueServer) grow() {
	newCap := len(s.jobs) * 2
	if newCap == 0 {
		newCap = 1
	}

	next := make([]*api.Job, newCap)
	for i := 0; i < s.size; i++ {
		next[i] = s.jobs[(s.head+i)%len(s.jobs)]
	}

	s.jobs = next
	s.head = 0
}

func RegisterQueueService(s grpc.ServiceRegistrar, logger interfaces.Logger) {
	qs := NewQueueService(logger)
	api.RegisterQueueServiceServer(s, qs)

	hs := health.NewServer()
	healthgrpc.RegisterHealthServer(s, hs)
	hs.SetServingStatus("queue", healthpb.HealthCheckResponse_SERVING)
}
