package queue

import (
	"context"
	"sync"

	api "vectis/api/gen/go"
	"vectis/internal/log"

	"google.golang.org/grpc"
)

type queueServer struct {
	api.UnimplementedQueueServiceServer
	jobs []*api.Job
	mu   sync.Mutex
	cond *sync.Cond
	log  *log.Logger
}

func NewQueueService(logger *log.Logger) api.QueueServiceServer {
	s := &queueServer{
		jobs: []*api.Job{},
		log:  logger,
	}

	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *queueServer) Enqueue(ctx context.Context, req *api.Job) (*api.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs = append(s.jobs, req)
	s.log.Info("Enqueued job: %s", req.GetId())

	// NOTE(garrett): For dequeue, we're using this condition as a barrier for the respective
	// goroutine to wait on.
	s.cond.Signal()

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

		done := make(chan struct{})

		// NOTE(garrett): On wakeup, we'll signal the done channel to wake up the select.
		go func() {
			s.mu.Lock()
			s.cond.Wait()
			s.mu.Unlock()
			close(done)
		}()

		select {
		case <-done:
			// NOTE(garrett): We've been woken up by a new job; loop will re-check len(s.jobs).
		case <-ctx.Done():
			s.cond.Broadcast()
			<-done
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

func RegisterQueueService(s grpc.ServiceRegistrar, logger *log.Logger) {
	api.RegisterQueueServiceServer(s, NewQueueService(logger))
}
