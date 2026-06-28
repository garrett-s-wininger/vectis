package orchestrator

import (
	"context"
	"fmt"
	"hash/fnv"
	"runtime"
	"strings"
	"sync"
	"time"

	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/job"

	"github.com/google/uuid"
)

const defaultShardBuffer = 1024

type Option func(*Service)

func WithClock(clock interfaces.Clock) Option {
	return func(s *Service) {
		if clock != nil {
			s.clock = clock
		}
	}
}

type Service struct {
	shards    []*shard
	clock     interfaces.Clock
	closeOnce sync.Once
	wg        sync.WaitGroup
}

type RunSpec struct {
	RunID      string
	Root       dal.TaskExecutionRecord
	CellID     string
	Tasks      []TaskSpec
	Executions []TaskExecutionSnapshot
}

type TaskSpec struct {
	TaskKey       string
	ParentTaskKey string
	Name          string
	CellID        string
	ChildTaskKeys []string
}

type TaskExecutionSnapshot = dal.TaskExecutionSnapshot

type LoadResult struct {
	RunID   string
	Root    dal.TaskExecutionRecord
	Pending []dal.TaskExecutionRecord
	Summary dal.RunTaskCompletion
}

type RunTaskSnapshot struct {
	RunID      string
	Executions []dal.TaskExecutionSnapshot
	Summary    dal.RunTaskCompletion
	NextCursor int64
}

type LoadRunOptions struct {
	OmitPending bool
}

type shard struct {
	commands chan command
}

type command struct {
	apply func(*shardState) (any, error)
	resp  chan commandResult
}

type commandResult struct {
	value any
	err   error
}

type shardState struct {
	runs map[string]*runState
}

type runState struct {
	runID      string
	status     string
	tasks      map[string]*taskState
	executions map[string]*taskState
	order      []string
	summary    dal.RunTaskCompletion
}

type taskState struct {
	record        dal.TaskExecutionRecord
	status        string
	parentTaskKey string
	childTaskKeys []string
	leaseOwner    string
	claimToken    string
	leaseUntil    time.Time
	acceptedAt    time.Time
	startedAt     time.Time
	finishedAt    time.Time
}

func New(shardCount int, opts ...Option) *Service {
	if shardCount <= 0 {
		shardCount = runtime.GOMAXPROCS(0)
	}

	s := &Service{
		shards: make([]*shard, shardCount),
		clock:  interfaces.SystemClock{},
	}

	for _, opt := range opts {
		opt(s)
	}

	for i := range s.shards {
		sh := &shard{commands: make(chan command, defaultShardBuffer)}
		s.shards[i] = sh
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			sh.run()
		}()
	}

	return s
}

func RunSpecFromTaskPlan(runID string, plan []job.TaskPlanEntry, cellID string) RunSpec {
	tasks := make([]TaskSpec, 0, len(plan))
	for _, entry := range plan {
		parentTaskKey := strings.TrimSpace(entry.ParentTaskKey)
		if parentTaskKey == "" {
			parentTaskKey = dal.RootTaskKey
		}

		tasks = append(tasks, TaskSpec{
			TaskKey:       entry.TaskKey,
			ParentTaskKey: parentTaskKey,
			Name:          entry.Name,
			CellID:        cellID,
			ChildTaskKeys: append([]string(nil), entry.ChildTaskKeys...),
		})
	}

	return RunSpec{RunID: runID, CellID: cellID, Tasks: tasks}
}

func (s *Service) Close() {
	if s == nil {
		return
	}

	s.closeOnce.Do(func() {
		for _, sh := range s.shards {
			close(sh.commands)
		}
		s.wg.Wait()
	})
}

func (s *Service) LoadRun(ctx context.Context, spec RunSpec) (LoadResult, error) {
	return s.LoadRunWithOptions(ctx, spec, LoadRunOptions{})
}

func (s *Service) LoadRunWithOptions(ctx context.Context, spec RunSpec, opts LoadRunOptions) (LoadResult, error) {
	spec.RunID = strings.TrimSpace(spec.RunID)
	if spec.RunID == "" {
		return LoadResult{}, fmt.Errorf("%w: run_id is required", dal.ErrNotFound)
	}

	value, err := s.call(ctx, spec.RunID, func(state *shardState) (any, error) {
		if run, ok := state.runs[spec.RunID]; ok {
			if len(spec.Executions) > 0 {
				if err := run.applySnapshots(spec.Executions, s.clock.Now()); err != nil {
					return LoadResult{}, err
				}
			}
			return run.loadResult(s.clock.Now(), opts), nil
		}

		run, err := buildRunState(spec)
		if err != nil {
			return LoadResult{}, err
		}

		state.runs[spec.RunID] = run
		return run.loadResult(s.clock.Now(), opts), nil
	})

	if err != nil {
		return LoadResult{}, err
	}

	return value.(LoadResult), nil
}

func (s *Service) ListPending(ctx context.Context, runID string, limit int) ([]dal.TaskExecutionRecord, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("%w: run_id is required", dal.ErrNotFound)
	}

	value, err := s.call(ctx, runID, func(state *shardState) (any, error) {
		run, err := state.getRun(runID)
		if err != nil {
			return nil, err
		}

		return run.pendingRecords(limit), nil
	})

	if err != nil {
		return nil, err
	}

	return value.([]dal.TaskExecutionRecord), nil
}

func (s *Service) ClaimExecution(ctx context.Context, runID, executionID, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	runID = strings.TrimSpace(runID)
	executionID = strings.TrimSpace(executionID)
	owner = strings.TrimSpace(owner)
	if runID == "" || executionID == "" || owner == "" {
		return dal.ExecutionClaimResult{}, fmt.Errorf("%w: run_id, execution_id, and owner are required", dal.ErrConflict)
	}

	now := s.clock.Now()
	value, err := s.call(ctx, runID, func(state *shardState) (any, error) {
		run, err := state.getRun(runID)
		if err != nil {
			return dal.ExecutionClaimResult{}, err
		}

		task, err := run.getExecution(executionID)
		if err != nil {
			return dal.ExecutionClaimResult{}, err
		}

		switch task.status {
		case dal.ExecutionStatusPending:
			token := uuid.NewString()
			task.status = dal.ExecutionStatusRunning

			if task.acceptedAt.IsZero() {
				task.acceptedAt = now
			}

			if task.startedAt.IsZero() {
				task.startedAt = now
			}

			task.leaseOwner = owner
			task.claimToken = token
			task.leaseUntil = leaseUntil
			run.status = dal.RunStatusRunning
			return dal.ExecutionClaimResult{
				Claimed:                true,
				ClaimToken:             token,
				TransitionedToAccepted: true,
				ExecutionStarted:       true,
			}, nil
		case dal.ExecutionStatusRunning:
			if !task.hasActiveClaim(owner, task.claimToken, now) {
				return dal.ExecutionClaimResult{}, nil
			}

			task.leaseUntil = leaseUntil
			return dal.ExecutionClaimResult{
				Claimed:          true,
				ClaimToken:       task.claimToken,
				ExecutionStarted: true,
			}, nil
		default:
			return dal.ExecutionClaimResult{}, nil
		}
	})

	if err != nil {
		return dal.ExecutionClaimResult{}, err
	}

	return value.(dal.ExecutionClaimResult), nil
}

func (s *Service) RenewExecutionLease(ctx context.Context, runID, executionID, owner, claimToken string, leaseUntil time.Time) error {
	runID = strings.TrimSpace(runID)
	executionID = strings.TrimSpace(executionID)
	owner = strings.TrimSpace(owner)
	claimToken = strings.TrimSpace(claimToken)
	if runID == "" || executionID == "" || owner == "" || claimToken == "" {
		return fmt.Errorf("%w: run_id, execution_id, owner, and claim_token are required", dal.ErrConflict)
	}

	_, err := s.call(ctx, runID, func(state *shardState) (any, error) {
		run, err := state.getRun(runID)
		if err != nil {
			return nil, err
		}

		task, err := run.getExecution(executionID)
		if err != nil {
			return nil, err
		}

		if !task.hasActiveClaim(owner, claimToken, s.clock.Now()) {
			return nil, fmt.Errorf("%w: execution %s claim is not active for owner %q", dal.ErrConflict, executionID, owner)
		}

		task.leaseUntil = leaseUntil
		return nil, nil
	})

	return err
}

func (s *Service) CompleteExecutionByClaim(ctx context.Context, runID, executionID, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	runID = strings.TrimSpace(runID)
	executionID = strings.TrimSpace(executionID)
	owner = strings.TrimSpace(owner)
	claimToken = strings.TrimSpace(claimToken)
	status = strings.TrimSpace(status)

	if runID == "" || executionID == "" || owner == "" || claimToken == "" {
		return dal.ExecutionFinalizationResult{}, fmt.Errorf("%w: run_id, execution_id, owner, and claim_token are required", dal.ErrConflict)
	}

	if !isTerminalExecutionStatus(status) {
		return dal.ExecutionFinalizationResult{}, fmt.Errorf("%w: unsupported terminal execution status %s", dal.ErrConflict, status)
	}

	now := s.clock.Now()
	value, err := s.call(ctx, runID, func(state *shardState) (any, error) {
		run, err := state.getRun(runID)
		if err != nil {
			return dal.ExecutionFinalizationResult{}, err
		}

		task, err := run.getExecution(executionID)
		if err != nil {
			return dal.ExecutionFinalizationResult{}, err
		}

		if task.status != dal.ExecutionStatusRunning || !task.hasActiveClaim(owner, claimToken, now) {
			return dal.ExecutionFinalizationResult{}, fmt.Errorf("%w: execution %s claim is not active for owner %q", dal.ErrConflict, executionID, owner)
		}

		task.status = status
		if task.finishedAt.IsZero() {
			task.finishedAt = now
		}

		task.leaseOwner = ""
		task.claimToken = ""
		task.leaseUntil = time.Time{}
		run.applyCompletion(status)

		var children []dal.TaskExecutionRecord
		activated := 0
		if status == dal.ExecutionStatusSucceeded {
			children, activated, err = run.activateChildren(task)
			if err != nil {
				return dal.ExecutionFinalizationResult{}, err
			}
		}

		result := dal.ExecutionFinalizationResult{
			ExecutionID: executionID,
			RunID:       runID,
			Outcome:     run.finalizationOutcome(status, activated),
			Summary:     run.summary,
			Children:    children,
			Activated:   activated,
		}

		run.applyRunStatus(result.Outcome)
		if isTerminalFinalizationOutcome(result.Outcome) {
			result.Executions = run.executionSnapshots()
		}

		return result, nil
	})

	if err != nil {
		return dal.ExecutionFinalizationResult{}, err
	}

	return value.(dal.ExecutionFinalizationResult), nil
}

func (s *Service) GetRunTaskCompletion(ctx context.Context, runID string) (dal.RunTaskCompletion, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return dal.RunTaskCompletion{}, fmt.Errorf("%w: run_id is required", dal.ErrNotFound)
	}

	value, err := s.call(ctx, runID, func(state *shardState) (any, error) {
		run, err := state.getRun(runID)
		if err != nil {
			return dal.RunTaskCompletion{}, err
		}

		return run.summary, nil
	})

	if err != nil {
		return dal.RunTaskCompletion{}, err
	}

	return value.(dal.RunTaskCompletion), nil
}

func (s *Service) GetRunTaskSnapshot(ctx context.Context, runID string, cursor int64, limit int) (RunTaskSnapshot, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return RunTaskSnapshot{}, fmt.Errorf("%w: run_id is required", dal.ErrNotFound)
	}

	value, err := s.call(ctx, runID, func(state *shardState) (any, error) {
		run, err := state.getRun(runID)
		if err != nil {
			return RunTaskSnapshot{}, err
		}

		return run.taskSnapshot(cursor, limit), nil
	})

	if err != nil {
		return RunTaskSnapshot{}, err
	}

	return value.(RunTaskSnapshot), nil
}

func (s *Service) call(ctx context.Context, runID string, apply func(*shardState) (any, error)) (any, error) {
	if s == nil {
		return nil, fmt.Errorf("orchestrator service is required")
	}

	if len(s.shards) == 0 {
		return nil, fmt.Errorf("orchestrator service has no shards")
	}

	resp := make(chan commandResult, 1)
	cmd := command{apply: apply, resp: resp}
	select {
	case s.shardFor(runID).commands <- cmd:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case result := <-resp:
		return result.value, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Service) shardFor(runID string) *shard {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(runID))
	return s.shards[int(hash.Sum32())%len(s.shards)]
}

func (s *shard) run() {
	state := &shardState{runs: map[string]*runState{}}
	for cmd := range s.commands {
		value, err := cmd.apply(state)
		cmd.resp <- commandResult{value: value, err: err}
	}
}

func (s *shardState) getRun(runID string) (*runState, error) {
	run, ok := s.runs[runID]
	if !ok {
		return nil, fmt.Errorf("%w: run %s", dal.ErrNotFound, runID)
	}

	return run, nil
}

func (r *runState) getExecution(executionID string) (*taskState, error) {
	task, ok := r.executions[executionID]
	if !ok {
		return nil, fmt.Errorf("%w: execution %s", dal.ErrNotFound, executionID)
	}

	return task, nil
}

func (t *taskState) hasActiveClaim(owner, claimToken string, now time.Time) bool {
	return t.leaseOwner == owner && t.claimToken == claimToken && !t.leaseUntil.Before(now)
}

func buildRunState(spec RunSpec) (*runState, error) {
	runID := strings.TrimSpace(spec.RunID)
	cellID := normalizeCellID(spec.CellID)
	if runID == "" {
		return nil, fmt.Errorf("%w: run_id is required", dal.ErrNotFound)
	}

	normalized := make([]TaskSpec, 0, len(spec.Tasks))
	seen := map[string]struct{}{dal.RootTaskKey: {}}
	childrenByParent := map[string][]string{}
	for _, task := range spec.Tasks {
		task.TaskKey = strings.TrimSpace(task.TaskKey)
		task.ParentTaskKey = strings.TrimSpace(task.ParentTaskKey)
		task.Name = strings.TrimSpace(task.Name)
		task.CellID = normalizeTargetCellID(task.CellID, cellID)
		if task.TaskKey == "" {
			return nil, fmt.Errorf("%w: task_key is required", dal.ErrConflict)
		}

		if task.TaskKey == dal.RootTaskKey {
			return nil, fmt.Errorf("%w: task_key %q is reserved", dal.ErrConflict, dal.RootTaskKey)
		}

		if _, ok := seen[task.TaskKey]; ok {
			return nil, fmt.Errorf("%w: duplicate task %s", dal.ErrConflict, task.TaskKey)
		}

		seen[task.TaskKey] = struct{}{}
		if task.ParentTaskKey == "" {
			task.ParentTaskKey = dal.RootTaskKey
		}

		if task.Name == "" {
			task.Name = task.TaskKey
		}

		childrenByParent[task.ParentTaskKey] = append(childrenByParent[task.ParentTaskKey], task.TaskKey)
		normalized = append(normalized, task)
	}

	for _, task := range normalized {
		if _, ok := seen[task.ParentTaskKey]; !ok {
			return nil, fmt.Errorf("%w: parent task %s", dal.ErrNotFound, task.ParentTaskKey)
		}
	}

	run := &runState{
		runID:      runID,
		status:     dal.RunStatusQueued,
		tasks:      map[string]*taskState{},
		executions: map[string]*taskState{},
		order:      make([]string, 0, len(normalized)+1),
		summary: dal.RunTaskCompletion{
			RunID:      runID,
			Total:      len(normalized) + 1,
			Incomplete: len(normalized) + 1,
		},
	}

	rootRecord, err := rootTaskExecutionRecord(runID, cellID, spec.Root)
	if err != nil {
		return nil, err
	}

	root := &taskState{
		record:        rootRecord,
		status:        dal.ExecutionStatusPending,
		childTaskKeys: append([]string(nil), childrenByParent[dal.RootTaskKey]...),
	}

	run.addTask(dal.RootTaskKey, root)
	for _, task := range normalized {
		taskState := &taskState{
			record:        taskExecutionRecord(runID, task.TaskKey, task.ParentTaskKey, task.Name, task.CellID),
			status:        dal.ExecutionStatusPlanned,
			parentTaskKey: task.ParentTaskKey,
			childTaskKeys: append([]string(nil), childrenByParent[task.TaskKey]...),
		}
		run.addTask(task.TaskKey, taskState)
	}

	if err := run.applySnapshots(spec.Executions, time.Time{}); err != nil {
		return nil, err
	}

	return run, nil
}

func (r *runState) addTask(taskKey string, task *taskState) {
	r.tasks[taskKey] = task
	r.executions[task.record.ExecutionID] = task
	r.order = append(r.order, taskKey)
}

func (r *runState) applySnapshots(snapshots []TaskExecutionSnapshot, now time.Time) error {
	for _, snapshot := range snapshots {
		status := normalizeSnapshotStatus(snapshot.Status)
		if status == "" {
			continue
		}

		task, err := r.taskForSnapshot(snapshot)
		if err != nil {
			return err
		}

		if err := r.validateSnapshotRecord(task, snapshot.Record); err != nil {
			return err
		}

		if shouldApplySnapshotStatus(task, status, now) {
			task.status = status
			if status == dal.ExecutionStatusRunning {
				task.leaseOwner = strings.TrimSpace(snapshot.LeaseOwner)
				task.claimToken = strings.TrimSpace(snapshot.ClaimToken)
				task.leaseUntil = time.Unix(snapshot.LeaseUntilUnix, 0)
			} else {
				task.leaseOwner = ""
				task.claimToken = ""
				task.leaseUntil = time.Time{}
			}
		}

		applySnapshotTiming(task, snapshot)
		r.applySnapshotRecord(task, snapshot.Record)
	}

	r.recomputeSummary()
	return nil
}

func applySnapshotTiming(task *taskState, snapshot TaskExecutionSnapshot) {
	if task == nil {
		return
	}

	if acceptedAt := snapshotUnixNanoTime(snapshot.AcceptedAtUnixNano); !acceptedAt.IsZero() {
		task.acceptedAt = acceptedAt
	}

	if startedAt := snapshotUnixNanoTime(snapshot.StartedAtUnixNano); !startedAt.IsZero() {
		task.startedAt = startedAt
	}

	if finishedAt := snapshotUnixNanoTime(snapshot.FinishedAtUnixNano); !finishedAt.IsZero() {
		task.finishedAt = finishedAt
	}
}

func (r *runState) taskForSnapshot(snapshot TaskExecutionSnapshot) (*taskState, error) {
	taskKey := strings.TrimSpace(snapshot.Record.TaskKey)
	if taskKey != "" {
		if task, ok := r.tasks[taskKey]; ok {
			return task, nil
		}
	}

	executionID := strings.TrimSpace(snapshot.Record.ExecutionID)
	if executionID != "" {
		if task, ok := r.executions[executionID]; ok {
			return task, nil
		}
	}

	if taskKey != "" {
		return nil, fmt.Errorf("%w: task %s", dal.ErrNotFound, taskKey)
	}
	return nil, fmt.Errorf("%w: execution %s", dal.ErrNotFound, executionID)
}

func (r *runState) validateSnapshotRecord(task *taskState, record dal.TaskExecutionRecord) error {
	if task == nil {
		return fmt.Errorf("%w: snapshot task is required", dal.ErrConflict)
	}

	expected := task.record
	if err := matchSnapshotString("run_id", record.RunID, r.runID); err != nil {
		return err
	}

	if err := matchSnapshotString("task_id", record.TaskID, expected.TaskID); err != nil {
		return err
	}

	if err := matchSnapshotString("parent_task_id", record.ParentTaskID, expected.ParentTaskID); err != nil {
		return err
	}

	if err := matchSnapshotString("task_key", record.TaskKey, expected.TaskKey); err != nil {
		return err
	}

	if err := matchSnapshotAttempt(record, expected.TaskID); err != nil {
		return err
	}

	executionID := strings.TrimSpace(record.ExecutionID)
	if executionID != "" && executionID != expected.ExecutionID {
		if existing, ok := r.executions[executionID]; ok && existing != task {
			return fmt.Errorf("%w: snapshot execution_id %q belongs to another task", dal.ErrConflict, executionID)
		}
	}

	return nil
}

func matchSnapshotString(field, got, want string) error {
	got = strings.TrimSpace(got)
	if got == "" {
		return nil
	}

	if got != want {
		return fmt.Errorf("%w: snapshot %s %q does not match task %s %q", dal.ErrConflict, field, got, field, want)
	}

	return nil
}

func matchSnapshotAttempt(record dal.TaskExecutionRecord, expectedTaskID string) error {
	attemptID := strings.TrimSpace(record.TaskAttemptID)
	if attemptID == "" {
		return nil
	}

	taskID := strings.TrimSpace(record.TaskID)
	if taskID == "" {
		taskID = strings.TrimSpace(expectedTaskID)
	}

	if !strings.HasPrefix(attemptID, taskID+":attempt:") {
		return fmt.Errorf("%w: snapshot task_attempt_id %q does not belong to task_id %q", dal.ErrConflict, attemptID, taskID)
	}

	if record.Attempt <= 0 {
		return nil
	}

	wantSuffix := fmt.Sprintf(":attempt:%d", record.Attempt)
	if !strings.HasSuffix(attemptID, wantSuffix) {
		return fmt.Errorf("%w: snapshot task_attempt_id %q does not match attempt %d", dal.ErrConflict, attemptID, record.Attempt)
	}

	return nil
}

func (r *runState) applySnapshotRecord(task *taskState, record dal.TaskExecutionRecord) {
	oldExecutionID := task.record.ExecutionID

	if record.RunID != "" {
		task.record.RunID = record.RunID
	}
	if record.TaskID != "" {
		task.record.TaskID = record.TaskID
	}
	if record.ParentTaskID != "" {
		task.record.ParentTaskID = record.ParentTaskID
	}
	if record.TaskKey != "" {
		task.record.TaskKey = record.TaskKey
	}
	if record.Name != "" {
		task.record.Name = record.Name
	}
	if record.TaskAttemptID != "" {
		task.record.TaskAttemptID = record.TaskAttemptID
	}
	if record.SegmentID != "" {
		task.record.SegmentID = record.SegmentID
	}
	if record.SegmentName != "" {
		task.record.SegmentName = record.SegmentName
	}
	if record.ExecutionID != "" {
		task.record.ExecutionID = record.ExecutionID
	}
	if record.CellID != "" {
		task.record.CellID = record.CellID
	}
	if record.Attempt > 0 {
		task.record.Attempt = record.Attempt
	}

	if task.record.ExecutionID != "" && task.record.ExecutionID != oldExecutionID {
		delete(r.executions, oldExecutionID)
		r.executions[task.record.ExecutionID] = task
	}
}

func shouldApplySnapshotStatus(task *taskState, status string, now time.Time) bool {
	if task == nil {
		return false
	}

	if task.claimToken != "" && task.hasActiveClaim(task.leaseOwner, task.claimToken, now) {
		return false
	}

	return executionStatusRank(status) >= executionStatusRank(task.status)
}

func (r *runState) recomputeSummary() {
	summary := dal.RunTaskCompletion{
		RunID: r.runID,
		Total: len(r.tasks),
	}

	for _, taskKey := range r.order {
		task := r.tasks[taskKey]
		switch task.status {
		case dal.ExecutionStatusSucceeded:
			summary.Succeeded++
		case dal.ExecutionStatusFailed, dal.ExecutionStatusCancelled, dal.ExecutionStatusAborted:
			summary.TerminalFailed++
		default:
			summary.Incomplete++
		}
	}

	r.summary = summary
}

func (r *runState) executionSnapshots() []dal.TaskExecutionSnapshot {
	out := make([]dal.TaskExecutionSnapshot, 0, len(r.order))
	for _, taskKey := range r.order {
		task := r.tasks[taskKey]
		out = append(out, dal.TaskExecutionSnapshot{
			Record:             task.record,
			Status:             task.status,
			AcceptedAtUnixNano: timeUnixNano(task.acceptedAt),
			StartedAtUnixNano:  timeUnixNano(task.startedAt),
			FinishedAtUnixNano: timeUnixNano(task.finishedAt),
		})
	}

	return out
}

func (r *runState) taskSnapshot(cursor int64, limit int) RunTaskSnapshot {
	start := int(cursor)
	if start < 0 {
		start = 0
	}

	if start > len(r.order) {
		start = len(r.order)
	}

	end := len(r.order)
	if limit > 0 && start+limit < end {
		end = start + limit
	}

	executions := make([]dal.TaskExecutionSnapshot, 0, end-start)
	for _, taskKey := range r.order[start:end] {
		task := r.tasks[taskKey]
		executions = append(executions, dal.TaskExecutionSnapshot{
			Record:             task.record,
			Status:             task.status,
			AcceptedAtUnixNano: timeUnixNano(task.acceptedAt),
			StartedAtUnixNano:  timeUnixNano(task.startedAt),
			FinishedAtUnixNano: timeUnixNano(task.finishedAt),
		})
	}

	var nextCursor int64
	if end < len(r.order) {
		nextCursor = int64(end)
	}

	return RunTaskSnapshot{
		RunID:      r.runID,
		Executions: executions,
		Summary:    r.summary,
		NextCursor: nextCursor,
	}
}

func timeUnixNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}

	return t.UnixNano()
}

func snapshotUnixNanoTime(v int64) time.Time {
	if v <= 0 {
		return time.Time{}
	}

	return time.Unix(0, v)
}

func normalizeSnapshotStatus(status string) string {
	switch strings.TrimSpace(status) {
	case dal.ExecutionStatusPlanned:
		return dal.ExecutionStatusPlanned
	case dal.ExecutionStatusPending:
		return dal.ExecutionStatusPending
	case dal.ExecutionStatusAccepted:
		return dal.ExecutionStatusAccepted
	case dal.ExecutionStatusRunning:
		return dal.ExecutionStatusRunning
	case dal.ExecutionStatusSucceeded:
		return dal.ExecutionStatusSucceeded
	case dal.ExecutionStatusFailed:
		return dal.ExecutionStatusFailed
	case dal.ExecutionStatusCancelled:
		return dal.ExecutionStatusCancelled
	case dal.ExecutionStatusAborted:
		return dal.ExecutionStatusAborted
	default:
		return ""
	}
}

func executionStatusRank(status string) int {
	switch status {
	case dal.ExecutionStatusPlanned:
		return 0
	case dal.ExecutionStatusPending:
		return 1
	case dal.ExecutionStatusAccepted:
		return 2
	case dal.ExecutionStatusRunning:
		return 3
	case dal.ExecutionStatusSucceeded, dal.ExecutionStatusFailed, dal.ExecutionStatusCancelled, dal.ExecutionStatusAborted:
		return 4
	default:
		return -1
	}
}

func (r *runState) loadResult(now time.Time, opts LoadRunOptions) LoadResult {
	var root dal.TaskExecutionRecord
	if task := r.tasks[dal.RootTaskKey]; task != nil {
		root = task.record
	}

	var pending []dal.TaskExecutionRecord
	if !opts.OmitPending {
		pending = r.pendingRecords(0)
	}

	return LoadResult{
		RunID:   r.runID,
		Root:    root,
		Pending: pending,
		Summary: r.summary,
	}
}

func (r *runState) pendingRecords(limit int) []dal.TaskExecutionRecord {
	out := make([]dal.TaskExecutionRecord, 0)
	for _, taskKey := range r.order {
		task := r.tasks[taskKey]
		if task.status != dal.ExecutionStatusPending {
			continue
		}

		out = append(out, task.record)
		if limit > 0 && len(out) >= limit {
			break
		}
	}

	return out
}

func (r *runState) applyCompletion(status string) {
	if r.summary.Incomplete > 0 {
		r.summary.Incomplete--
	}

	if status == dal.ExecutionStatusSucceeded {
		r.summary.Succeeded++
	} else {
		r.summary.TerminalFailed++
	}
}

func (r *runState) activateChildren(parent *taskState) ([]dal.TaskExecutionRecord, int, error) {
	children := make([]dal.TaskExecutionRecord, 0, len(parent.childTaskKeys))
	activated := 0
	for _, childKey := range parent.childTaskKeys {
		child, ok := r.tasks[childKey]
		if !ok {
			return nil, 0, fmt.Errorf("%w: child task %s", dal.ErrNotFound, childKey)
		}

		switch child.status {
		case dal.ExecutionStatusPlanned:
			child.status = dal.ExecutionStatusPending
			children = append(children, child.record)
			activated++
		case dal.ExecutionStatusPending:
			children = append(children, child.record)
		case dal.ExecutionStatusRunning, dal.ExecutionStatusSucceeded, dal.ExecutionStatusFailed, dal.ExecutionStatusCancelled, dal.ExecutionStatusAborted:
			continue
		default:
			return nil, 0, fmt.Errorf("%w: child task %s status %s cannot activate", dal.ErrConflict, childKey, child.status)
		}
	}

	return children, activated, nil
}

func (r *runState) finalizationOutcome(status string, activated int) dal.ExecutionFinalizationOutcome {
	if r.summary.TerminalFailed > 0 {
		if status == dal.ExecutionStatusCancelled || status == dal.ExecutionStatusAborted {
			return dal.ExecutionFinalizationOutcomeRunCancelled
		}

		return dal.ExecutionFinalizationOutcomeRunFailed
	}

	if r.summary.AllSucceeded() {
		return dal.ExecutionFinalizationOutcomeRunSucceeded
	}

	if activated > 0 {
		return dal.ExecutionFinalizationOutcomeContinued
	}

	return dal.ExecutionFinalizationOutcomeWaiting
}

func (r *runState) applyRunStatus(outcome dal.ExecutionFinalizationOutcome) {
	switch outcome {
	case dal.ExecutionFinalizationOutcomeContinued:
		r.status = dal.RunStatusQueued
	case dal.ExecutionFinalizationOutcomeRunSucceeded:
		r.status = dal.RunStatusSucceeded
	case dal.ExecutionFinalizationOutcomeRunFailed:
		r.status = dal.RunStatusFailed
	case dal.ExecutionFinalizationOutcomeRunCancelled:
		r.status = dal.RunStatusCancelled
	default:
		r.status = dal.RunStatusRunning
	}
}

func isTerminalFinalizationOutcome(outcome dal.ExecutionFinalizationOutcome) bool {
	switch outcome {
	case dal.ExecutionFinalizationOutcomeRunSucceeded, dal.ExecutionFinalizationOutcomeRunFailed, dal.ExecutionFinalizationOutcomeRunCancelled:
		return true
	default:
		return false
	}
}

func taskExecutionRecord(runID, taskKey, parentTaskKey, name, cellID string) dal.TaskExecutionRecord {
	taskID := runID + ":" + taskKey
	attempt := 1
	attemptID := taskID + ":attempt:1"
	parentTaskID := ""
	if parentTaskKey != "" {
		parentTaskID = runID + ":" + parentTaskKey
	}

	return dal.TaskExecutionRecord{
		RunID:         runID,
		TaskID:        taskID,
		ParentTaskID:  parentTaskID,
		TaskKey:       taskKey,
		Name:          name,
		TaskAttemptID: attemptID,
		SegmentID:     taskID + ":segment",
		SegmentName:   name,
		ExecutionID:   attemptID + ":execution",
		CellID:        normalizeCellID(cellID),
		Attempt:       attempt,
	}
}

func rootTaskExecutionRecord(runID, cellID string, in dal.TaskExecutionRecord) (dal.TaskExecutionRecord, error) {
	defaultRecord := taskExecutionRecord(runID, dal.RootTaskKey, "", dal.RootTaskKey, cellID)
	if strings.TrimSpace(in.ExecutionID) == "" {
		return defaultRecord, nil
	}

	if in.RunID == "" {
		in.RunID = runID
	} else if in.RunID != runID {
		return dal.TaskExecutionRecord{}, fmt.Errorf("%w: root run_id %q does not match run %q", dal.ErrConflict, in.RunID, runID)
	}

	if in.TaskID == "" {
		in.TaskID = defaultRecord.TaskID
	}

	if in.ParentTaskID == "" {
		in.ParentTaskID = defaultRecord.ParentTaskID
	}

	if in.TaskKey == "" {
		in.TaskKey = dal.RootTaskKey
	} else if in.TaskKey != dal.RootTaskKey {
		return dal.TaskExecutionRecord{}, fmt.Errorf("%w: root task_key must be %q", dal.ErrConflict, dal.RootTaskKey)
	}

	if in.Name == "" {
		in.Name = dal.RootTaskKey
	}

	if in.TaskAttemptID == "" {
		in.TaskAttemptID = defaultRecord.TaskAttemptID
	}

	if in.SegmentID == "" {
		in.SegmentID = defaultRecord.SegmentID
	}

	if in.SegmentName == "" {
		in.SegmentName = in.Name
	}

	if in.CellID == "" {
		in.CellID = normalizeCellID(cellID)
	}

	if in.Attempt <= 0 {
		in.Attempt = 1
	}

	return in, nil
}

func isTerminalExecutionStatus(status string) bool {
	switch status {
	case dal.ExecutionStatusSucceeded, dal.ExecutionStatusFailed, dal.ExecutionStatusCancelled, dal.ExecutionStatusAborted:
		return true
	default:
		return false
	}
}

func normalizeCellID(cellID string) string {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return dal.DefaultCellID
	}

	return cellID
}

func normalizeTargetCellID(cellID, fallback string) string {
	cellID = strings.TrimSpace(cellID)
	if cellID == "" {
		return normalizeCellID(fallback)
	}

	return cellID
}
