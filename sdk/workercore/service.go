package workercore

import (
	"context"
	"fmt"
	"strings"

	api "vectis/api/gen/go"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ServiceOptions struct{}

type Service struct {
	api.UnimplementedWorkerCoreServiceServer

	core Core
}

func NewService(core Core, _ ServiceOptions) *Service {
	return &Service{core: core}
}

func (s *Service) DescribeCore(ctx context.Context, _ *api.DescribeWorkerCoreRequest) (*api.DescribeWorkerCoreResponse, error) {
	if s == nil || s.core == nil {
		return nil, status.Error(codes.FailedPrecondition, "worker core is not configured")
	}

	desc, err := s.core.Describe(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "describe worker core: %v", err)
	}

	return descriptionProto(desc), nil
}

func (s *Service) ExecuteTask(ctx context.Context, req *api.ExecuteWorkerCoreTaskRequest) (*api.ExecuteWorkerCoreTaskResponse, error) {
	if s == nil || s.core == nil {
		return nil, status.Error(codes.FailedPrecondition, "worker core is not configured")
	}

	task, err := taskFromProto(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	result, err := s.core.ExecuteTask(ctx, task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "execute worker core task: %v", err)
	}

	return resultProto(result), nil
}

func taskFromProto(req *api.ExecuteWorkerCoreTaskRequest) (Task, error) {
	if req == nil {
		return Task{}, fmt.Errorf("execute task request is required")
	}

	if req.GetJob() == nil {
		return Task{}, fmt.Errorf("job is required")
	}

	if strings.TrimSpace(req.GetTaskKey()) == "" {
		return Task{}, fmt.Errorf("task key is required")
	}

	session, err := NewSession(req.GetSession())
	if err != nil {
		return Task{}, err
	}

	return Task{
		Job:     req.GetJob(),
		TaskKey: req.GetTaskKey(),
		Session: session,
	}, nil
}
