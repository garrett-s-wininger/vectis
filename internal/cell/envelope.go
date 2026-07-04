package cell

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/dal"
	"vectis/internal/dispatchmeta"

	"google.golang.org/protobuf/proto"
)

const (
	ExecutionEnvelopeVersion        = 1
	ExecutionEnvelopeMetadataKey    = "vectis.execution_envelope"
	ExecutionTaskKeyMetadataKey     = "vectis.execution_task_key"
	ExecutionPayloadHashMetadataKey = "vectis.execution_payload_hash"
)

type ExecutionEnvelope struct {
	EnvelopeVersion       int                         `json:"envelope_version"`
	RunID                 string                      `json:"run_id"`
	RunIndex              int                         `json:"run_index,omitempty"`
	TaskID                string                      `json:"task_id"`
	TaskKey               string                      `json:"task_key"`
	TaskName              string                      `json:"task_name,omitempty"`
	TaskAttemptID         string                      `json:"task_attempt_id"`
	TaskAttempt           int                         `json:"task_attempt"`
	NamespacePath         string                      `json:"namespace_path,omitempty"`
	SegmentID             string                      `json:"segment_id"`
	ExecutionID           string                      `json:"execution_id"`
	CellID                string                      `json:"cell_id"`
	Attempt               int                         `json:"attempt,omitempty"`
	DefinitionVersion     int                         `json:"definition_version"`
	DefinitionHash        string                      `json:"definition_hash"`
	StartDeadlineUnixNano int64                       `json:"start_deadline_unix_nano,omitempty"`
	Job                   *api.Job                    `json:"job"`
	ActionLocks           []actionregistry.ActionLock `json:"action_locks,omitempty"`
	Metadata              map[string]string           `json:"metadata,omitempty"`
	CreatedAtUnixNano     int64                       `json:"created_at_unix_nano,omitempty"`
}

func NewExecutionEnvelope(dispatch dal.ExecutionDispatchRecord, job *api.Job, metadata map[string]string, createdAtUnixNano int64) (*ExecutionEnvelope, error) {
	return NewExecutionEnvelopeWithActions(dispatch, job, metadata, createdAtUnixNano, nil)
}

func NewExecutionEnvelopeWithActions(dispatch dal.ExecutionDispatchRecord, job *api.Job, metadata map[string]string, createdAtUnixNano int64, resolver actionregistry.Resolver) (*ExecutionEnvelope, error) {
	if job != nil && strings.TrimSpace(dispatch.JobID) != "" && job.GetId() != dispatch.JobID {
		return nil, fmt.Errorf("execution envelope job_id %q does not match job.id %q", dispatch.JobID, job.GetId())
	}

	actionLocks := preservedActionLocks(metadata)
	if len(actionLocks) == 0 && resolver != nil && job != nil {
		locks, err := actionregistry.ResolveJobActions(job, resolver)
		if err != nil {
			return nil, err
		}

		actionLocks = locks
	}

	env := &ExecutionEnvelope{
		EnvelopeVersion:       ExecutionEnvelopeVersion,
		RunID:                 dispatch.RunID,
		RunIndex:              dispatch.RunIndex,
		TaskID:                defaultTaskID(dispatch.RunID, dispatch.TaskID, dispatch.TaskKey),
		TaskKey:               defaultTaskKey(dispatch.TaskKey),
		TaskName:              dispatch.TaskName,
		TaskAttemptID:         defaultTaskAttemptID(dispatch.RunID, dispatch.TaskAttemptID, dispatch.TaskID, dispatch.TaskKey, dispatch.Attempt),
		TaskAttempt:           defaultTaskAttempt(dispatch.Attempt),
		NamespacePath:         normalizeNamespacePath(dispatch.NamespacePath),
		SegmentID:             dispatch.SegmentID,
		ExecutionID:           dispatch.ExecutionID,
		CellID:                dispatch.CellID,
		Attempt:               dispatch.Attempt,
		DefinitionVersion:     dispatch.DefinitionVersion,
		DefinitionHash:        dispatch.DefinitionHash,
		StartDeadlineUnixNano: dispatch.StartDeadlineUnixNano,
		Job:                   job,
		ActionLocks:           actionLocks,
		Metadata:              cloneMetadata(metadata),
		CreatedAtUnixNano:     createdAtUnixNano,
	}

	if err := env.Validate(); err != nil {
		return nil, err
	}

	return env, nil
}

func AttachPendingExecutionEnvelope(ctx context.Context, runs dal.RunsRepository, req *api.JobRequest, runID string, createdAtUnixNano int64) (*ExecutionEnvelope, error) {
	return AttachPendingExecutionEnvelopeWithActions(ctx, runs, req, runID, createdAtUnixNano, nil)
}

func AttachPendingExecutionEnvelopeWithActions(ctx context.Context, runs dal.RunsRepository, req *api.JobRequest, runID string, createdAtUnixNano int64, resolver actionregistry.Resolver) (*ExecutionEnvelope, error) {
	if runs == nil {
		return nil, errors.New("runs repository is required")
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		return nil, err
	}

	return AttachExecutionEnvelopeWithActions(req, dispatch, createdAtUnixNano, resolver)
}

func AttachExecutionEnvelope(req *api.JobRequest, dispatch dal.ExecutionDispatchRecord, createdAtUnixNano int64) (*ExecutionEnvelope, error) {
	return AttachExecutionEnvelopeWithActions(req, dispatch, createdAtUnixNano, nil)
}

func AttachExecutionEnvelopeWithActions(req *api.JobRequest, dispatch dal.ExecutionDispatchRecord, createdAtUnixNano int64, resolver actionregistry.Resolver) (*ExecutionEnvelope, error) {
	if req == nil {
		return nil, errors.New("job request is required")
	}

	dispatchmeta.StampStartDeadline(req, dispatch.StartDeadlineUnixNano)

	env, err := NewExecutionEnvelopeWithActions(dispatch, req.GetJob(), req.GetMetadata(), createdAtUnixNano, resolver)
	if err != nil {
		return nil, err
	}

	payload, err := EncodeExecutionEnvelope(env)
	if err != nil {
		return nil, err
	}

	if req.Metadata == nil {
		req.Metadata = map[string]string{}
	}

	req.Metadata[ExecutionEnvelopeMetadataKey] = string(payload)
	req.Metadata[ExecutionTaskKeyMetadataKey] = env.TaskKey

	return env, nil
}

func ExecutionEnvelopeFromRequest(req *api.JobRequest) (*ExecutionEnvelope, bool, error) {
	if req == nil {
		return nil, false, nil
	}

	payload := req.GetMetadata()[ExecutionEnvelopeMetadataKey]
	if payload == "" {
		return nil, false, nil
	}

	env, err := DecodeExecutionEnvelope([]byte(payload))
	if err != nil {
		return nil, true, err
	}

	if err := validateExecutionEnvelopeRequest(env, req); err != nil {
		return nil, true, err
	}

	return env, true, nil
}

func validateExecutionEnvelopeRequest(env *ExecutionEnvelope, req *api.JobRequest) error {
	if env == nil {
		return errors.New("execution envelope is required")
	}

	if req == nil {
		return errors.New("job request is required")
	}

	job := req.GetJob()
	if job == nil {
		return errors.New("execution envelope request job is required")
	}

	if env.Job.GetId() != job.GetId() {
		return fmt.Errorf("execution envelope job.id %q does not match request job.id %q", env.Job.GetId(), job.GetId())
	}

	if env.RunID != job.GetRunId() {
		return fmt.Errorf("execution envelope run_id %q does not match request job.run_id %q", env.RunID, job.GetRunId())
	}

	taskKey, ok := req.GetMetadata()[ExecutionTaskKeyMetadataKey]
	if !ok {
		return fmt.Errorf("execution envelope task key metadata %q is required", ExecutionTaskKeyMetadataKey)
	}

	if taskKey != env.TaskKey {
		return fmt.Errorf("execution envelope task_key %q does not match request metadata %s %q", env.TaskKey, ExecutionTaskKeyMetadataKey, taskKey)
	}

	if !jobsEqualIgnoringDeliveryID(env.Job, job) {
		return fmt.Errorf("execution envelope job does not match request job")
	}

	return nil
}

func jobsEqualIgnoringDeliveryID(a, b *api.Job) bool {
	a = cloneJobWithoutDeliveryID(a)
	b = cloneJobWithoutDeliveryID(b)
	return proto.Equal(a, b)
}

func executionEnvelopesMatchSubmission(a, b *ExecutionEnvelope) bool {
	if a == nil || b == nil {
		return a == b
	}

	if a.EnvelopeVersion != b.EnvelopeVersion ||
		a.RunID != b.RunID ||
		a.RunIndex != b.RunIndex ||
		a.TaskID != b.TaskID ||
		a.TaskKey != b.TaskKey ||
		a.TaskName != b.TaskName ||
		a.TaskAttemptID != b.TaskAttemptID ||
		a.TaskAttempt != b.TaskAttempt ||
		a.NamespacePath != b.NamespacePath ||
		a.SegmentID != b.SegmentID ||
		a.ExecutionID != b.ExecutionID ||
		a.CellID != b.CellID ||
		a.Attempt != b.Attempt ||
		a.DefinitionVersion != b.DefinitionVersion ||
		a.DefinitionHash != b.DefinitionHash ||
		a.CreatedAtUnixNano != b.CreatedAtUnixNano {
		return false
	}

	if !jobsEqualIgnoringDeliveryID(a.Job, b.Job) {
		return false
	}

	if !actionLocksEqual(a.ActionLocks, b.ActionLocks) {
		return false
	}

	return stringMapsEqual(a.Metadata, b.Metadata)
}

func actionLocksEqual(a, b []actionregistry.ActionLock) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}

	return reflect.DeepEqual(a, b)
}

func stringMapsEqual(a, b map[string]string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}

	return reflect.DeepEqual(a, b)
}

func cloneJobWithoutDeliveryID(job *api.Job) *api.Job {
	if job == nil {
		return nil
	}

	cloned, ok := proto.Clone(job).(*api.Job)
	if !ok {
		return job
	}

	cloned.DeliveryId = nil
	return cloned
}

func EncodeExecutionEnvelope(env *ExecutionEnvelope) ([]byte, error) {
	if err := env.Validate(); err != nil {
		return nil, err
	}

	return json.Marshal(env)
}

func DecodeExecutionEnvelope(payload []byte) (*ExecutionEnvelope, error) {
	var env ExecutionEnvelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return nil, fmt.Errorf("decode execution envelope: %w", err)
	}

	if err := env.Validate(); err != nil {
		return nil, err
	}

	return &env, nil
}

func (e *ExecutionEnvelope) Validate() error {
	if e == nil {
		return errors.New("execution envelope is required")
	}

	if e.EnvelopeVersion != ExecutionEnvelopeVersion {
		return fmt.Errorf("unsupported execution envelope version %d", e.EnvelopeVersion)
	}

	if strings.TrimSpace(e.RunID) == "" {
		return errors.New("execution envelope run_id is required")
	}

	if strings.TrimSpace(e.TaskID) == "" {
		return errors.New("execution envelope task_id is required")
	}

	if strings.TrimSpace(e.TaskKey) == "" {
		return errors.New("execution envelope task_key is required")
	}

	if strings.TrimSpace(e.TaskAttemptID) == "" {
		return errors.New("execution envelope task_attempt_id is required")
	}

	if e.TaskAttempt <= 0 {
		return errors.New("execution envelope task_attempt must be positive")
	}

	e.NamespacePath = normalizeNamespacePath(e.NamespacePath)

	if strings.TrimSpace(e.SegmentID) == "" {
		return errors.New("execution envelope segment_id is required")
	}

	if strings.TrimSpace(e.ExecutionID) == "" {
		return errors.New("execution envelope execution_id is required")
	}

	if strings.TrimSpace(e.CellID) == "" {
		return errors.New("execution envelope cell_id is required")
	}

	if e.Attempt <= 0 {
		e.Attempt = 1
	}

	if e.DefinitionVersion <= 0 {
		return errors.New("execution envelope definition_version must be positive")
	}

	if strings.TrimSpace(e.DefinitionHash) == "" {
		return errors.New("execution envelope definition_hash is required")
	}

	if e.Job == nil {
		return errors.New("execution envelope job is required")
	}

	if strings.TrimSpace(e.Job.GetId()) == "" {
		return errors.New("execution envelope job.id is required")
	}

	if strings.TrimSpace(e.Job.GetRunId()) == "" {
		return errors.New("execution envelope job.run_id is required")
	}

	if e.Job.GetRunId() != e.RunID {
		return fmt.Errorf("execution envelope run_id %q does not match job.run_id %q", e.RunID, e.Job.GetRunId())
	}

	if e.Job.GetRoot() == nil {
		return errors.New("execution envelope job.root is required")
	}

	if err := actionregistry.ValidateActionLocks(e.ActionLocks); err != nil {
		return fmt.Errorf("execution envelope action locks: %w", err)
	}

	return nil
}

func defaultTaskKey(taskKey string) string {
	taskKey = strings.TrimSpace(taskKey)
	if taskKey == "" {
		return dal.RootTaskKey
	}

	return taskKey
}

func defaultTaskID(runID, taskID, taskKey string) string {
	taskID = strings.TrimSpace(taskID)
	if taskID != "" {
		return taskID
	}

	return strings.TrimSpace(runID) + ":" + defaultTaskKey(taskKey)
}

func defaultTaskAttempt(attempt int) int {
	if attempt <= 0 {
		return 1
	}

	return attempt
}

func defaultTaskAttemptID(runID, taskAttemptID, taskID, taskKey string, attempt int) string {
	taskAttemptID = strings.TrimSpace(taskAttemptID)
	if taskAttemptID != "" {
		return taskAttemptID
	}

	return defaultTaskID(runID, taskID, taskKey) + ":attempt:" + fmt.Sprintf("%d", defaultTaskAttempt(attempt))
}

func normalizeNamespacePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return "/"
	}

	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	if len(path) > 1 {
		path = strings.TrimRight(path, "/")
	}

	if path == "" {
		return "/"
	}

	return path
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}

	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if key == ExecutionEnvelopeMetadataKey || key == ExecutionTaskKeyMetadataKey {
			continue
		}

		out[key] = value
	}

	if len(out) == 0 {
		return nil
	}

	return out
}

func preservedActionLocks(metadata map[string]string) []actionregistry.ActionLock {
	payload := metadata[ExecutionEnvelopeMetadataKey]
	if payload == "" {
		return nil
	}

	env, err := DecodeExecutionEnvelope([]byte(payload))
	if err != nil {
		return nil
	}

	return actionregistry.CloneActionLocks(env.ActionLocks)
}
