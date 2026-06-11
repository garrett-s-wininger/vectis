package orchestrator

import (
	"fmt"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/job"
)

func RunSpecFromJobRequest(req *api.JobRequest) (RunSpec, error) {
	if req == nil || req.GetJob() == nil {
		return RunSpec{}, fmt.Errorf("job request is required")
	}

	env, ok, err := cell.ExecutionEnvelopeFromRequest(req)
	if err != nil {
		return RunSpec{}, fmt.Errorf("decode execution envelope: %w", err)
	}

	if !ok {
		return RunSpec{}, fmt.Errorf("execution envelope is required")
	}

	return RunSpecFromJobAndEnvelope(req.GetJob(), env)
}

func RunSpecFromJobAndEnvelope(j *api.Job, env *cell.ExecutionEnvelope) (RunSpec, error) {
	if j == nil {
		return RunSpec{}, fmt.Errorf("job is required")
	}

	if env == nil {
		return RunSpec{}, fmt.Errorf("execution envelope is required")
	}

	plan, err := job.PlanTaskExecutions(j)
	if err != nil {
		return RunSpec{}, err
	}

	spec := RunSpecFromTaskPlan(env.RunID, plan, env.CellID)
	if env.TaskKey == dal.RootTaskKey {
		spec.Root = TaskExecutionRecordFromEnvelope(env)
	}
	return spec, nil
}

func TaskExecutionRecordFromEnvelope(env *cell.ExecutionEnvelope) dal.TaskExecutionRecord {
	if env == nil {
		return dal.TaskExecutionRecord{}
	}

	return dal.TaskExecutionRecord{
		RunID:         env.RunID,
		TaskID:        env.TaskID,
		TaskKey:       env.TaskKey,
		Name:          env.TaskName,
		TaskAttemptID: env.TaskAttemptID,
		SegmentID:     env.SegmentID,
		SegmentName:   env.TaskName,
		ExecutionID:   env.ExecutionID,
		CellID:        env.CellID,
		Attempt:       env.TaskAttempt,
	}
}
