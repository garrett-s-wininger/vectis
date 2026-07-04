package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/orchestrator"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type executionChoreographer interface {
	LoadRun(ctx context.Context, job *api.Job, env *cell.ExecutionEnvelope, snapshots []orchestrator.TaskExecutionSnapshot) error
	ClaimAndStartExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error)
	RenewExecutionLease(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken string, leaseUntil time.Time) error
	CompleteExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error)
	RequiresDurableTaskRows() bool
}

type durableCompletionChoreographer interface {
	completesExecutionDurably() bool
}

func choreographerCompletesExecutionDurably(ch executionChoreographer) bool {
	durable, ok := ch.(durableCompletionChoreographer)
	return ok && durable.completesExecutionDurably()
}

type grpcExecutionChoreographer struct {
	client         api.OrchestratorServiceClient
	streamMu       sync.Mutex
	stream         api.OrchestratorService_ExecutionStreamClient
	streamDisabled bool
}

func newGRPCExecutionChoreographer(client api.OrchestratorServiceClient) *grpcExecutionChoreographer {
	return &grpcExecutionChoreographer{client: client}
}

func (c *grpcExecutionChoreographer) LoadRun(ctx context.Context, job *api.Job, env *cell.ExecutionEnvelope, snapshots []orchestrator.TaskExecutionSnapshot) error {
	spec, err := orchestrator.RunSpecFromJobAndEnvelope(job, env)
	if err != nil {
		return err
	}

	tasks := make([]*api.OrchestratorTaskSpec, 0, len(spec.Tasks))
	for _, task := range spec.Tasks {
		tasks = append(tasks, &api.OrchestratorTaskSpec{
			TaskKey:       stringPtr(task.TaskKey),
			ParentTaskKey: stringPtr(task.ParentTaskKey),
			Name:          stringPtr(task.Name),
			CellId:        stringPtr(task.CellID),
			ChildTaskKeys: append([]string(nil), task.ChildTaskKeys...),
			Uses:          stringPtr(task.Uses),
		})
	}

	_, err = c.loadRun(ctx, &api.LoadRunRequest{
		RunId:       stringPtr(spec.RunID),
		Root:        taskExecutionToProto(spec.Root),
		RootUses:    stringPtr(spec.RootUses),
		CellId:      stringPtr(spec.CellID),
		Tasks:       tasks,
		Executions:  taskExecutionSnapshotsToProto(snapshots),
		OmitPending: boolPtr(true),
	})

	return err
}

func (c *grpcExecutionChoreographer) ClaimAndStartExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	req := &api.ClaimExecutionRequest{
		RunId:              stringPtr(env.RunID),
		ExecutionId:        stringPtr(env.ExecutionID),
		Owner:              stringPtr(owner),
		LeaseUntilUnixNano: int64Ptr(leaseUntil.UnixNano()),
	}

	claim, err := c.claimExecution(ctx, req)
	if err != nil {
		return dal.ExecutionClaimResult{}, err
	}

	return dal.ExecutionClaimResult{
		Claimed:                claim.GetClaimed(),
		ClaimToken:             claim.GetClaimToken(),
		TransitionedToAccepted: claim.GetTransitionedToAccepted(),
		ExecutionStarted:       claim.GetExecutionStarted(),
	}, nil
}

func (c *grpcExecutionChoreographer) RenewExecutionLease(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken string, leaseUntil time.Time) error {
	req := &api.RenewExecutionLeaseRequest{
		RunId:              stringPtr(env.RunID),
		ExecutionId:        stringPtr(env.ExecutionID),
		Owner:              stringPtr(owner),
		ClaimToken:         stringPtr(claimToken),
		LeaseUntilUnixNano: int64Ptr(leaseUntil.UnixNano()),
	}

	return c.renewExecutionLease(ctx, req)
}

func (c *grpcExecutionChoreographer) CompleteExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	req := &api.CompleteExecutionRequest{
		RunId:       stringPtr(env.RunID),
		ExecutionId: stringPtr(env.ExecutionID),
		Owner:       stringPtr(owner),
		ClaimToken:  stringPtr(claimToken),
		Status:      stringPtr(status),
		FailureCode: stringPtr(failureCode),
		Reason:      stringPtr(reason),
	}

	result, err := c.completeExecution(ctx, req)
	if err != nil {
		return dal.ExecutionFinalizationResult{}, err
	}

	return dal.ExecutionFinalizationResult{
		ExecutionID: result.GetExecutionId(),
		RunID:       result.GetRunId(),
		Outcome:     dal.ExecutionFinalizationOutcome(result.GetOutcome()),
		Summary:     runTaskCompletionFromProto(result.GetSummary()),
		Children:    taskExecutionsFromProto(result.GetChildren()),
		Executions:  taskExecutionSnapshotsFromProto(result.GetExecutions()),
		Activated:   int(result.GetActivated()),
	}, nil
}

func (c *grpcExecutionChoreographer) RequiresDurableTaskRows() bool {
	return false
}

func (c *grpcExecutionChoreographer) loadRun(ctx context.Context, req *api.LoadRunRequest) (*api.LoadRunResponse, error) {
	resp, usedStream, err := c.tryExecutionStream(ctx, &api.ExecutionStreamRequest{Load: req})
	if err != nil {
		return nil, err
	}

	if !usedStream {
		return c.client.LoadRun(ctx, req)
	}

	if resp.GetLoad() == nil {
		return nil, fmt.Errorf("orchestrator execution stream returned no load response")
	}

	return resp.GetLoad(), nil
}

func (c *grpcExecutionChoreographer) claimExecution(ctx context.Context, req *api.ClaimExecutionRequest) (*api.ClaimExecutionResponse, error) {
	resp, usedStream, err := c.tryExecutionStream(ctx, &api.ExecutionStreamRequest{Claim: req})
	if err != nil {
		return nil, err
	}

	if !usedStream {
		return c.client.ClaimExecution(ctx, req)
	}

	if resp.GetClaim() == nil {
		return nil, fmt.Errorf("orchestrator execution stream returned no claim response")
	}

	return resp.GetClaim(), nil
}

func (c *grpcExecutionChoreographer) renewExecutionLease(ctx context.Context, req *api.RenewExecutionLeaseRequest) error {
	resp, usedStream, err := c.tryExecutionStream(ctx, &api.ExecutionStreamRequest{Renew: req})
	if err != nil {
		return err
	}

	if !usedStream {
		_, err := c.client.RenewExecutionLease(ctx, req)
		return err
	}

	if resp.GetRenew() == nil {
		return fmt.Errorf("orchestrator execution stream returned no renew response")
	}

	return nil
}

func (c *grpcExecutionChoreographer) completeExecution(ctx context.Context, req *api.CompleteExecutionRequest) (*api.CompleteExecutionResponse, error) {
	resp, usedStream, err := c.tryExecutionStream(ctx, &api.ExecutionStreamRequest{Complete: req})
	if err != nil {
		return nil, err
	}

	if !usedStream {
		return c.client.CompleteExecution(ctx, req)
	}

	if resp.GetComplete() == nil {
		return nil, fmt.Errorf("orchestrator execution stream returned no complete response")
	}

	return resp.GetComplete(), nil
}

func (c *grpcExecutionChoreographer) tryExecutionStream(ctx context.Context, req *api.ExecutionStreamRequest) (*api.ExecutionStreamResponse, bool, error) {
	if c == nil {
		return nil, false, fmt.Errorf("execution choreographer is not configured")
	}

	c.streamMu.Lock()
	defer c.streamMu.Unlock()

	if c.streamDisabled {
		return nil, false, nil
	}

	stream, err := c.executionStreamLocked(ctx)
	if err != nil {
		if shouldFallbackBeforeExecutionStreamSend(ctx, err) {
			c.noteExecutionStreamUnavailableLocked(err)
			return nil, false, nil
		}

		return nil, true, err
	}

	if err := stream.Send(req); err != nil {
		c.resetExecutionStreamLocked()
		if status.Code(err) == codes.Unimplemented {
			c.streamDisabled = true
			return nil, false, nil
		}

		return nil, true, err
	}

	resp, err := stream.Recv()
	if err != nil {
		c.resetExecutionStreamLocked()
		if status.Code(err) == codes.Unimplemented {
			c.streamDisabled = true
			return nil, false, nil
		}

		return nil, true, err
	}

	return resp, true, nil
}

func (c *grpcExecutionChoreographer) executionStreamLocked(ctx context.Context) (api.OrchestratorService_ExecutionStreamClient, error) {
	if c.stream != nil {
		return c.stream, nil
	}

	stream, err := c.client.ExecutionStream(ctx)
	if err != nil {
		return nil, err
	}

	c.stream = stream
	return stream, nil
}

func (c *grpcExecutionChoreographer) resetExecutionStreamLocked() {
	if c.stream == nil {
		return
	}

	_ = c.stream.CloseSend()
	c.stream = nil
}

func (c *grpcExecutionChoreographer) noteExecutionStreamUnavailableLocked(err error) {
	if status.Code(err) == codes.Unimplemented {
		c.streamDisabled = true
	}

	c.resetExecutionStreamLocked()
}

func shouldFallbackBeforeExecutionStreamSend(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}

	switch status.Code(err) {
	case codes.Unimplemented, codes.Unavailable:
		return true
	default:
		return false
	}
}

func taskExecutionToProto(in dal.TaskExecutionRecord) *api.OrchestratorTaskExecution {
	if in.ExecutionID == "" {
		return nil
	}

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

func taskExecutionSnapshotsToProto(in []orchestrator.TaskExecutionSnapshot) []*api.OrchestratorTaskExecution {
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

func taskExecutionsFromProto(in []*api.OrchestratorTaskExecution) []dal.TaskExecutionRecord {
	out := make([]dal.TaskExecutionRecord, 0, len(in))
	for _, record := range in {
		if record == nil {
			continue
		}

		out = append(out, dal.TaskExecutionRecord{
			RunID:         record.GetRunId(),
			TaskID:        record.GetTaskId(),
			ParentTaskID:  record.GetParentTaskId(),
			TaskKey:       record.GetTaskKey(),
			Name:          record.GetName(),
			TaskAttemptID: record.GetTaskAttemptId(),
			SegmentID:     record.GetSegmentId(),
			SegmentName:   record.GetSegmentName(),
			ExecutionID:   record.GetExecutionId(),
			CellID:        record.GetCellId(),
			Attempt:       int(record.GetAttempt()),
		})
	}

	return out
}

func taskExecutionSnapshotsFromProto(in []*api.OrchestratorTaskExecution) []dal.TaskExecutionSnapshot {
	out := make([]dal.TaskExecutionSnapshot, 0, len(in))
	for _, record := range in {
		if record == nil {
			continue
		}

		out = append(out, dal.TaskExecutionSnapshot{
			Record: dal.TaskExecutionRecord{
				RunID:         record.GetRunId(),
				TaskID:        record.GetTaskId(),
				ParentTaskID:  record.GetParentTaskId(),
				TaskKey:       record.GetTaskKey(),
				Name:          record.GetName(),
				TaskAttemptID: record.GetTaskAttemptId(),
				SegmentID:     record.GetSegmentId(),
				SegmentName:   record.GetSegmentName(),
				ExecutionID:   record.GetExecutionId(),
				CellID:        record.GetCellId(),
				Attempt:       int(record.GetAttempt()),
			},
			Status:             record.GetStatus(),
			AcceptedAtUnixNano: record.GetAcceptedAtUnixNano(),
			StartedAtUnixNano:  record.GetStartedAtUnixNano(),
			FinishedAtUnixNano: record.GetFinishedAtUnixNano(),
		})
	}

	return out
}

func runTaskCompletionFromProto(in *api.OrchestratorRunTaskCompletion) dal.RunTaskCompletion {
	if in == nil {
		return dal.RunTaskCompletion{}
	}

	return dal.RunTaskCompletion{
		RunID:          in.GetRunId(),
		Total:          int(in.GetTotal()),
		Succeeded:      int(in.GetSucceeded()),
		TerminalFailed: int(in.GetTerminalFailed()),
		Incomplete:     int(in.GetIncomplete()),
	}
}

func stringPtr(v string) *string {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}

func boolPtr(v bool) *bool {
	return &v
}

func int32Ptr(v int32) *int32 {
	return &v
}
