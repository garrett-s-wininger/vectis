package orchestrator

import (
	"context"
	"errors"
	"io"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type grpcServer struct {
	api.UnimplementedOrchestratorServiceServer
	service *Service
	logger  interfaces.Logger
}

func RegisterOrchestratorService(s grpc.ServiceRegistrar, service *Service, logger interfaces.Logger) api.OrchestratorServiceServer {
	server := &grpcServer{service: service, logger: logger}

	hs := health.NewServer()
	healthpb.RegisterHealthServer(s, hs)
	hs.SetServingStatus("orchestrator", healthpb.HealthCheckResponse_SERVING)

	api.RegisterOrchestratorServiceServer(s, server)
	return server
}

func (s *grpcServer) LoadRun(ctx context.Context, req *api.LoadRunRequest) (*api.LoadRunResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "load run request is required")
	}

	result, err := s.service.LoadRunWithOptions(ctx, RunSpec{
		RunID:      req.GetRunId(),
		Root:       taskExecutionFromProto(req.GetRoot()),
		CellID:     req.GetCellId(),
		Tasks:      taskSpecsFromProto(req.GetTasks()),
		Executions: taskExecutionSnapshotsFromProto(req.GetExecutions()),
	}, LoadRunOptions{
		OmitPending: req.GetOmitPending(),
	})

	if err != nil {
		return nil, grpcError(err)
	}

	return &api.LoadRunResponse{
		RunId:   stringPtr(result.RunID),
		Root:    taskExecutionToProto(result.Root),
		Pending: taskExecutionsToProto(result.Pending),
		Summary: runTaskCompletionToProto(result.Summary),
	}, nil
}

func (s *grpcServer) ListPending(ctx context.Context, req *api.ListPendingRequest) (*api.ListPendingResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "list pending request is required")
	}

	pending, err := s.service.ListPending(ctx, req.GetRunId(), int(req.GetLimit()))
	if err != nil {
		return nil, grpcError(err)
	}

	return &api.ListPendingResponse{Executions: taskExecutionsToProto(pending)}, nil
}

func (s *grpcServer) ClaimExecution(ctx context.Context, req *api.ClaimExecutionRequest) (*api.ClaimExecutionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "claim execution request is required")
	}

	claim, err := s.service.ClaimExecution(ctx, req.GetRunId(), req.GetExecutionId(), req.GetOwner(), unixNanoTime(req.GetLeaseUntilUnixNano()))
	if err != nil {
		return nil, grpcError(err)
	}

	return &api.ClaimExecutionResponse{
		Claimed:                boolPtr(claim.Claimed),
		ClaimToken:             stringPtr(claim.ClaimToken),
		TransitionedToAccepted: boolPtr(claim.TransitionedToAccepted),
		ExecutionStarted:       boolPtr(claim.ExecutionStarted),
	}, nil
}

func (s *grpcServer) RenewExecutionLease(ctx context.Context, req *api.RenewExecutionLeaseRequest) (*api.Empty, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "renew execution lease request is required")
	}

	err := s.service.RenewExecutionLease(ctx, req.GetRunId(), req.GetExecutionId(), req.GetOwner(), req.GetClaimToken(), unixNanoTime(req.GetLeaseUntilUnixNano()))
	if err != nil {
		return nil, grpcError(err)
	}

	return &api.Empty{}, nil
}

func (s *grpcServer) CompleteExecution(ctx context.Context, req *api.CompleteExecutionRequest) (*api.CompleteExecutionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "complete execution request is required")
	}

	result, err := s.service.CompleteExecutionByClaim(ctx, req.GetRunId(), req.GetExecutionId(), req.GetOwner(), req.GetClaimToken(), req.GetStatus(), req.GetFailureCode(), req.GetReason())
	if err != nil {
		return nil, grpcError(err)
	}

	return &api.CompleteExecutionResponse{
		ExecutionId: stringPtr(result.ExecutionID),
		RunId:       stringPtr(result.RunID),
		Outcome:     stringPtr(string(result.Outcome)),
		Summary:     runTaskCompletionToProto(result.Summary),
		Children:    taskExecutionsToProto(result.Children),
		Activated:   int32Ptr(int32(result.Activated)),
		Executions:  taskExecutionSnapshotsToProto(result.Executions),
	}, nil
}

func (s *grpcServer) GetRunTaskCompletion(ctx context.Context, req *api.GetRunTaskCompletionRequest) (*api.OrchestratorRunTaskCompletion, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "get run task completion request is required")
	}

	summary, err := s.service.GetRunTaskCompletion(ctx, req.GetRunId())
	if err != nil {
		return nil, grpcError(err)
	}

	return runTaskCompletionToProto(summary), nil
}

func (s *grpcServer) GetRunTaskSnapshot(ctx context.Context, req *api.GetRunTaskSnapshotRequest) (*api.GetRunTaskSnapshotResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "get run task snapshot request is required")
	}

	snapshot, err := s.service.GetRunTaskSnapshot(ctx, req.GetRunId(), req.GetCursor(), int(req.GetLimit()))
	if err != nil {
		return nil, grpcError(err)
	}

	return &api.GetRunTaskSnapshotResponse{
		RunId:      stringPtr(snapshot.RunID),
		Executions: taskExecutionSnapshotsToProto(snapshot.Executions),
		Summary:    runTaskCompletionToProto(snapshot.Summary),
		NextCursor: int64Ptr(snapshot.NextCursor),
	}, nil
}

func (s *grpcServer) ExecutionStream(stream grpc.BidiStreamingServer[api.ExecutionStreamRequest, api.ExecutionStreamResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		resp, err := s.executionStreamResponse(stream.Context(), req)
		if err != nil {
			return err
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func (s *grpcServer) executionStreamResponse(ctx context.Context, req *api.ExecutionStreamRequest) (*api.ExecutionStreamResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "execution stream request is required")
	}

	messageCount := 0
	if req.GetClaim() != nil {
		messageCount++
	}

	if req.GetRenew() != nil {
		messageCount++
	}

	if req.GetComplete() != nil {
		messageCount++
	}

	if req.GetLoad() != nil {
		messageCount++
	}

	if messageCount != 1 {
		return nil, status.Error(codes.InvalidArgument, "execution stream request must contain exactly one operation")
	}

	if loadReq := req.GetLoad(); loadReq != nil {
		load, err := s.LoadRun(ctx, loadReq)
		if err != nil {
			return nil, err
		}

		return &api.ExecutionStreamResponse{Load: load}, nil
	}

	if claimReq := req.GetClaim(); claimReq != nil {
		claim, err := s.ClaimExecution(ctx, claimReq)
		if err != nil {
			return nil, err
		}

		return &api.ExecutionStreamResponse{Claim: claim}, nil
	}

	if renewReq := req.GetRenew(); renewReq != nil {
		if _, err := s.RenewExecutionLease(ctx, renewReq); err != nil {
			return nil, err
		}

		return &api.ExecutionStreamResponse{Renew: &api.Empty{}}, nil
	}

	complete, err := s.CompleteExecution(ctx, req.GetComplete())
	if err != nil {
		return nil, err
	}

	return &api.ExecutionStreamResponse{Complete: complete}, nil
}

func taskSpecsFromProto(in []*api.OrchestratorTaskSpec) []TaskSpec {
	out := make([]TaskSpec, 0, len(in))
	for _, spec := range in {
		if spec == nil {
			continue
		}

		out = append(out, TaskSpec{
			TaskKey:       spec.GetTaskKey(),
			ParentTaskKey: spec.GetParentTaskKey(),
			Name:          spec.GetName(),
			CellID:        spec.GetCellId(),
			ChildTaskKeys: append([]string(nil), spec.GetChildTaskKeys()...),
		})
	}

	return out
}

func taskExecutionToProto(in dal.TaskExecutionRecord) *api.OrchestratorTaskExecution {
	return &api.OrchestratorTaskExecution{
		RunId:         stringPtr(in.RunID),
		TaskId:        stringPtr(in.TaskID),
		ParentTaskId:  stringPtr(in.ParentTaskID),
		TaskKey:       stringPtr(in.TaskKey),
		Name:          stringPtr(in.Name),
		TaskAttemptId: stringPtr(in.TaskAttemptID),
		SegmentId:     stringPtr(in.SegmentID),
		SegmentName:   stringPtr(in.SegmentName),
		ExecutionId:   stringPtr(in.ExecutionID),
		CellId:        stringPtr(in.CellID),
		Attempt:       int32Ptr(int32(in.Attempt)),
	}
}

func taskExecutionFromProto(in *api.OrchestratorTaskExecution) dal.TaskExecutionRecord {
	if in == nil {
		return dal.TaskExecutionRecord{}
	}

	return dal.TaskExecutionRecord{
		RunID:         in.GetRunId(),
		TaskID:        in.GetTaskId(),
		ParentTaskID:  in.GetParentTaskId(),
		TaskKey:       in.GetTaskKey(),
		Name:          in.GetName(),
		TaskAttemptID: in.GetTaskAttemptId(),
		SegmentID:     in.GetSegmentId(),
		SegmentName:   in.GetSegmentName(),
		ExecutionID:   in.GetExecutionId(),
		CellID:        in.GetCellId(),
		Attempt:       int(in.GetAttempt()),
	}
}

func taskExecutionsToProto(in []dal.TaskExecutionRecord) []*api.OrchestratorTaskExecution {
	out := make([]*api.OrchestratorTaskExecution, 0, len(in))
	for _, record := range in {
		out = append(out, taskExecutionToProto(record))
	}

	return out
}

func taskExecutionSnapshotsToProto(in []dal.TaskExecutionSnapshot) []*api.OrchestratorTaskExecution {
	out := make([]*api.OrchestratorTaskExecution, 0, len(in))
	for _, snapshot := range in {
		execution := taskExecutionToProto(snapshot.Record)
		if execution == nil {
			continue
		}

		execution.Status = stringPtr(snapshot.Status)
		execution.AcceptedAtUnixNano = int64Ptr(snapshot.AcceptedAtUnixNano)
		execution.StartedAtUnixNano = int64Ptr(snapshot.StartedAtUnixNano)
		execution.FinishedAtUnixNano = int64Ptr(snapshot.FinishedAtUnixNano)
		out = append(out, execution)
	}

	return out
}

func taskExecutionSnapshotsFromProto(in []*api.OrchestratorTaskExecution) []TaskExecutionSnapshot {
	out := make([]TaskExecutionSnapshot, 0, len(in))
	for _, execution := range in {
		if execution == nil {
			continue
		}

		out = append(out, TaskExecutionSnapshot{
			Record:             taskExecutionFromProto(execution),
			Status:             execution.GetStatus(),
			AcceptedAtUnixNano: execution.GetAcceptedAtUnixNano(),
			StartedAtUnixNano:  execution.GetStartedAtUnixNano(),
			FinishedAtUnixNano: execution.GetFinishedAtUnixNano(),
		})
	}

	return out
}

func runTaskCompletionToProto(in dal.RunTaskCompletion) *api.OrchestratorRunTaskCompletion {
	return &api.OrchestratorRunTaskCompletion{
		RunId:          stringPtr(in.RunID),
		Total:          int32Ptr(int32(in.Total)),
		Succeeded:      int32Ptr(int32(in.Succeeded)),
		TerminalFailed: int32Ptr(int32(in.TerminalFailed)),
		Incomplete:     int32Ptr(int32(in.Incomplete)),
	}
}

func stringPtr(v string) *string {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func int32Ptr(v int32) *int32 {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}

func unixNanoTime(v int64) time.Time {
	if v <= 0 {
		return time.Time{}
	}

	return time.Unix(0, v)
}

func grpcError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, dal.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, dal.ErrConflict):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
