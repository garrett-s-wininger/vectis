package server

import (
	"context"
	"fmt"

	api "vectis/api/gen/go"

	"google.golang.org/grpc"
)

type queueServer struct {
	api.UnimplementedQueueServiceServer
	jobs []*api.Job
}

func NewQueueService() api.QueueServiceServer {
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
	}
}

func (s *queueServer) Enqueue(ctx context.Context, req *api.Empty) (*api.Empty, error) {
	_ = req
	fmt.Println("Received enqueue request")
	return &api.Empty{}, nil
}

func (s *queueServer) Dequeue(ctx context.Context, req *api.Empty) (*api.Job, error) {
	_ = req
	if len(s.jobs) == 0 {
		return nil, fmt.Errorf("no jobs in queue")
	}
	job := s.jobs[0]
	s.jobs = s.jobs[1:]
	fmt.Printf("Dequeued job: %s\n", *job.Id)
	return job, nil
}

func RegisterQueueService(s grpc.ServiceRegistrar) {
	api.RegisterQueueServiceServer(s, NewQueueService())
}
