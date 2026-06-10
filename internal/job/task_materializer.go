package job

import (
	"context"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/dal"
)

type TaskMaterializationResult struct {
	Tasks   []dal.TaskExecutionRecord
	Created int
}

func EnsureJobTaskExecutions(ctx context.Context, runs dal.RunsRepository, job *api.Job, targetCellID string) (TaskMaterializationResult, error) {
	return EnsureJobTaskExecutionsWithActions(ctx, runs, job, targetCellID, nil)
}

func EnsureJobTaskExecutionsWithActions(ctx context.Context, runs dal.RunsRepository, job *api.Job, targetCellID string, resolver actionregistry.Resolver) (TaskMaterializationResult, error) {
	if runs == nil {
		return TaskMaterializationResult{}, fmt.Errorf("runs repository is required")
	}

	if job == nil {
		return TaskMaterializationResult{}, fmt.Errorf("job is required")
	}

	runID := strings.TrimSpace(job.GetRunId())
	if runID == "" {
		return TaskMaterializationResult{}, fmt.Errorf("job run_id is required")
	}

	plan, err := PlanTaskExecutionsWithActions(job, resolver)
	if err != nil {
		return TaskMaterializationResult{}, err
	}

	return EnsurePlannedTaskExecutions(ctx, runs, runID, plan, targetCellID)
}

func EnsurePlannedTaskExecutions(ctx context.Context, runs dal.RunsRepository, runID string, plan []TaskPlanEntry, targetCellID string) (TaskMaterializationResult, error) {
	if runs == nil {
		return TaskMaterializationResult{}, fmt.Errorf("runs repository is required")
	}

	runID = strings.TrimSpace(runID)
	if runID == "" {
		return TaskMaterializationResult{}, fmt.Errorf("run_id is required")
	}

	taskIDsByKey := map[string]string{
		dal.RootTaskKey: runID + ":" + dal.RootTaskKey,
	}

	result := TaskMaterializationResult{
		Tasks: make([]dal.TaskExecutionRecord, 0, len(plan)),
	}

	for _, entry := range plan {
		parentKey := strings.TrimSpace(entry.ParentTaskKey)
		if parentKey == "" {
			parentKey = dal.RootTaskKey
		}

		parentTaskID, ok := taskIDsByKey[parentKey]
		if !ok {
			return TaskMaterializationResult{}, fmt.Errorf("task %q parent %q has not been materialized", entry.TaskKey, parentKey)
		}

		rec, created, err := runs.EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
			RunID:        runID,
			ParentTaskID: parentTaskID,
			TaskKey:      entry.TaskKey,
			Name:         entry.Name,
			SpecHash:     entry.SpecHash,
			TargetCellID: targetCellID,
		})

		if err != nil {
			return TaskMaterializationResult{}, err
		}

		taskIDsByKey[entry.TaskKey] = rec.TaskID
		result.Tasks = append(result.Tasks, rec)
		if created {
			result.Created++
		}
	}

	return result, nil
}
