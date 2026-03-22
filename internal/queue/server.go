package queue

import (
	"context"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type queueServer struct {
	api.UnimplementedQueueServiceServer
	jobs   []*api.Job
	head   int
	size   int
	mu     sync.Mutex
	notify chan struct{}
	log    interfaces.Logger
}

const initialQueueCapacity = 1024

func NewQueueService(logger interfaces.Logger) api.QueueServiceServer {
	s := &queueServer{
		jobs:   make([]*api.Job, initialQueueCapacity),
		head:   0,
		size:   0,
		notify: make(chan struct{}, 1),
		log:    logger,
	}
	return s
}

func (s *queueServer) Enqueue(ctx context.Context, req *api.Job) (*api.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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

	job := s.jobs[s.head]
	s.jobs[s.head] = nil
	s.head = (s.head + 1) % len(s.jobs)
	s.size--
	s.log.Info("TryDequeue returned job: %s", job.GetId())
	return job, nil
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
