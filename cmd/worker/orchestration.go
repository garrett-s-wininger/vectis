package main

import (
	"context"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/orchestrator"
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
	client api.OrchestratorServiceClient
}

func newGRPCExecutionChoreographer(client api.OrchestratorServiceClient) grpcExecutionChoreographer {
	return grpcExecutionChoreographer{client: client}
}

func (c grpcExecutionChoreographer) LoadRun(ctx context.Context, job *api.Job, env *cell.ExecutionEnvelope, snapshots []orchestrator.TaskExecutionSnapshot) error {
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
		})
	}

	_, err = c.client.LoadRun(ctx, &api.LoadRunRequest{
		RunId:      stringPtr(spec.RunID),
		Root:       taskExecutionToProto(spec.Root),
		CellId:     stringPtr(spec.CellID),
		Tasks:      tasks,
		Executions: taskExecutionSnapshotsToProto(snapshots),
	})

	return err
}

func (c grpcExecutionChoreographer) ClaimAndStartExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	claim, err := c.client.ClaimExecution(ctx, &api.ClaimExecutionRequest{
		RunId:              stringPtr(env.RunID),
		ExecutionId:        stringPtr(env.ExecutionID),
		Owner:              stringPtr(owner),
		LeaseUntilUnixNano: int64Ptr(leaseUntil.UnixNano()),
	})

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

func (c grpcExecutionChoreographer) RenewExecutionLease(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken string, leaseUntil time.Time) error {
	_, err := c.client.RenewExecutionLease(ctx, &api.RenewExecutionLeaseRequest{
		RunId:              stringPtr(env.RunID),
		ExecutionId:        stringPtr(env.ExecutionID),
		Owner:              stringPtr(owner),
		ClaimToken:         stringPtr(claimToken),
		LeaseUntilUnixNano: int64Ptr(leaseUntil.UnixNano()),
	})

	return err
}

func (c grpcExecutionChoreographer) CompleteExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	result, err := c.client.CompleteExecution(ctx, &api.CompleteExecutionRequest{
		RunId:       stringPtr(env.RunID),
		ExecutionId: stringPtr(env.ExecutionID),
		Owner:       stringPtr(owner),
		ClaimToken:  stringPtr(claimToken),
		Status:      stringPtr(status),
		FailureCode: stringPtr(failureCode),
		Reason:      stringPtr(reason),
	})

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

func (c grpcExecutionChoreographer) RequiresDurableTaskRows() bool {
	return false
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

func int32Ptr(v int32) *int32 {
	return &v
}
