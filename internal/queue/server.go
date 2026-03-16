package queue

import (
	"context"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/grpc"
)

type queueServer struct {
	api.UnimplementedQueueServiceServer
	jobs   []*api.Job
	mu     sync.Mutex
	notify chan struct{}
	log    interfaces.Logger
}

func NewQueueService(logger interfaces.Logger) api.QueueServiceServer {
	s := &queueServer{
		jobs:   []*api.Job{},
		notify: make(chan struct{}, 1),
		log:    logger,
	}
	return s
}

func (s *queueServer) Enqueue(ctx context.Context, req *api.Job) (*api.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs = append(s.jobs, req)
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

	for len(s.jobs) == 0 {
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

	if len(s.jobs) == 0 {
		return nil, nil
	}

	job := s.jobs[0]
	s.jobs = s.jobs[1:]
	s.log.Info("Dequeued job: %s", job.GetId())
	return job, nil
}

func (s *queueServer) TryDequeue(ctx context.Context, _ *api.Empty) (*api.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.jobs) == 0 {
		return nil, nil
	}

	job := s.jobs[0]
	s.jobs = s.jobs[1:]
	s.log.Info("TryDequeue returned job: %s", job.GetId())
	return job, nil
}

func RegisterQueueService(s grpc.ServiceRegistrar, logger interfaces.Logger) {
	api.RegisterQueueServiceServer(s, NewQueueService(logger))
}
