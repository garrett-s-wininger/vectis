package server

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/log"

	"google.golang.org/grpc"
)

type queueServer struct {
	api.UnimplementedQueueServiceServer
	jobs []*api.Job
	log  *log.Logger
}

func NewQueueService(logger *log.Logger) api.QueueServiceServer {
	id := "hello-world-job"
	cmd := "echo 'Hello from queue!'"
	return &queueServer{
		jobs: []*api.Job{
			{
				Id: &id,
				Steps: []*api.Step{
					{Command: &cmd},
				},
			},
		},
		log: logger,
	}
}

func (s *queueServer) Enqueue(ctx context.Context, req *api.Empty) (*api.Empty, error) {
	_ = req
	s.log.Info("Received enqueue request")
	return &api.Empty{}, nil
}

func (s *queueServer) Dequeue(ctx context.Context, req *api.Empty) (*api.Job, error) {
	_ = req
	if len(s.jobs) == 0 {
		return nil, fmt.Errorf("no jobs in queue")
	}
	job := s.jobs[0]
	s.jobs = s.jobs[1:]
	s.log.Info("Dequeued job: %s", *job.Id)
	return job, nil
}

func RegisterQueueService(s grpc.ServiceRegistrar, logger *log.Logger) {
	api.RegisterQueueServiceServer(s, NewQueueService(logger))
}
