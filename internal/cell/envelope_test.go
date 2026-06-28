package cell

import (
	"fmt"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/dal"

	"google.golang.org/protobuf/proto"
)

func TestExecutionEnvelopeEncodeDecode(t *testing.T) {
	env := validExecutionEnvelope()

	payload, err := EncodeExecutionEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeExecutionEnvelope: %v", err)
	}

	got, err := DecodeExecutionEnvelope(payload)
	if err != nil {
		t.Fatalf("DecodeExecutionEnvelope: %v", err)
	}

	if got.EnvelopeVersion != ExecutionEnvelopeVersion {
		t.Fatalf("envelope_version: got %d, want %d", got.EnvelopeVersion, ExecutionEnvelopeVersion)
	}

	if got.RunID != env.RunID {
		t.Fatalf("run_id: got %q, want %q", got.RunID, env.RunID)
	}

	if got.RunIndex != env.RunIndex {
		t.Fatalf("run_index: got %d, want %d", got.RunIndex, env.RunIndex)
	}

	if got.TaskID != env.TaskID {
		t.Fatalf("task_id: got %q, want %q", got.TaskID, env.TaskID)
	}

	if got.TaskKey != env.TaskKey {
		t.Fatalf("task_key: got %q, want %q", got.TaskKey, env.TaskKey)
	}

	if got.TaskName != env.TaskName {
		t.Fatalf("task_name: got %q, want %q", got.TaskName, env.TaskName)
	}

	if got.TaskAttemptID != env.TaskAttemptID {
		t.Fatalf("task_attempt_id: got %q, want %q", got.TaskAttemptID, env.TaskAttemptID)
	}

	if got.TaskAttempt != env.TaskAttempt {
		t.Fatalf("task_attempt: got %d, want %d", got.TaskAttempt, env.TaskAttempt)
	}

	if got.NamespacePath != env.NamespacePath {
		t.Fatalf("namespace_path: got %q, want %q", got.NamespacePath, env.NamespacePath)
	}

	if got.SegmentID != env.SegmentID {
		t.Fatalf("segment_id: got %q, want %q", got.SegmentID, env.SegmentID)
	}

	if got.ExecutionID != env.ExecutionID {
		t.Fatalf("execution_id: got %q, want %q", got.ExecutionID, env.ExecutionID)
	}

	if got.CellID != env.CellID {
		t.Fatalf("cell_id: got %q, want %q", got.CellID, env.CellID)
	}

	if got.Attempt != env.Attempt {
		t.Fatalf("attempt: got %d, want %d", got.Attempt, env.Attempt)
	}

	if got.DefinitionVersion != env.DefinitionVersion {
		t.Fatalf("definition_version: got %d, want %d", got.DefinitionVersion, env.DefinitionVersion)
	}

	if got.DefinitionHash != env.DefinitionHash {
		t.Fatalf("definition_hash: got %q, want %q", got.DefinitionHash, env.DefinitionHash)
	}

	if got.CreatedAtUnixNano != env.CreatedAtUnixNano {
		t.Fatalf("created_at_unix_nano: got %d, want %d", got.CreatedAtUnixNano, env.CreatedAtUnixNano)
	}

	if got.Metadata["traceparent"] != env.Metadata["traceparent"] {
		t.Fatalf("traceparent metadata: got %q, want %q", got.Metadata["traceparent"], env.Metadata["traceparent"])
	}

	if got.Job.GetId() != env.Job.GetId() {
		t.Fatalf("job.id: got %q, want %q", got.Job.GetId(), env.Job.GetId())
	}

	if got.Job.GetRunId() != env.Job.GetRunId() {
		t.Fatalf("job.run_id: got %q, want %q", got.Job.GetRunId(), env.Job.GetRunId())
	}

	if got.Job.GetRoot().GetUses() != env.Job.GetRoot().GetUses() {
		t.Fatalf("job.root.uses: got %q, want %q", got.Job.GetRoot().GetUses(), env.Job.GetRoot().GetUses())
	}
}

func TestExecutionEnvelopeValidateRejectsMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name string
		edit func(*ExecutionEnvelope)
		want string
	}{
		{
			name: "version",
			edit: func(env *ExecutionEnvelope) {
				env.EnvelopeVersion = 0
			},
			want: "unsupported execution envelope version",
		},
		{
			name: "run id",
			edit: func(env *ExecutionEnvelope) {
				env.RunID = ""
			},
			want: "run_id is required",
		},
		{
			name: "task id",
			edit: func(env *ExecutionEnvelope) {
				env.TaskID = ""
			},
			want: "task_id is required",
		},
		{
			name: "task key",
			edit: func(env *ExecutionEnvelope) {
				env.TaskKey = ""
			},
			want: "task_key is required",
		},
		{
			name: "task attempt id",
			edit: func(env *ExecutionEnvelope) {
				env.TaskAttemptID = ""
			},
			want: "task_attempt_id is required",
		},
		{
			name: "task attempt",
			edit: func(env *ExecutionEnvelope) {
				env.TaskAttempt = 0
			},
			want: "task_attempt must be positive",
		},
		{
			name: "segment id",
			edit: func(env *ExecutionEnvelope) {
				env.SegmentID = ""
			},
			want: "segment_id is required",
		},
		{
			name: "execution id",
			edit: func(env *ExecutionEnvelope) {
				env.ExecutionID = ""
			},
			want: "execution_id is required",
		},
		{
			name: "cell id",
			edit: func(env *ExecutionEnvelope) {
				env.CellID = ""
			},
			want: "cell_id is required",
		},
		{
			name: "definition version",
			edit: func(env *ExecutionEnvelope) {
				env.DefinitionVersion = 0
			},
			want: "definition_version must be positive",
		},
		{
			name: "definition hash",
			edit: func(env *ExecutionEnvelope) {
				env.DefinitionHash = ""
			},
			want: "definition_hash is required",
		},
		{
			name: "job",
			edit: func(env *ExecutionEnvelope) {
				env.Job = nil
			},
			want: "job is required",
		},
		{
			name: "job id",
			edit: func(env *ExecutionEnvelope) {
				env.Job.Id = stringPtr("")
			},
			want: "job.id is required",
		},
		{
			name: "job run id",
			edit: func(env *ExecutionEnvelope) {
				env.Job.RunId = stringPtr("")
			},
			want: "job.run_id is required",
		},
		{
			name: "job run id mismatch",
			edit: func(env *ExecutionEnvelope) {
				env.Job.RunId = stringPtr("run-other")
			},
			want: "does not match job.run_id",
		},
		{
			name: "job root",
			edit: func(env *ExecutionEnvelope) {
				env.Job.Root = nil
			},
			want: "job.root is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			env := validExecutionEnvelope()
			tt.edit(env)

			err := env.Validate()
			if err == nil {
				t.Fatal("Validate succeeded, want error")
			}

			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate error %q does not contain %q", err.Error(), tt.want)
			}
		})
	}
}

func TestDecodeExecutionEnvelopeRejectsInvalidJSON(t *testing.T) {
	if _, err := DecodeExecutionEnvelope([]byte("{")); err == nil {
		t.Fatal("DecodeExecutionEnvelope succeeded, want error")
	}
}

func TestAttachExecutionEnvelopeBuildsFromDispatchRecord(t *testing.T) {
	runID := "run-1"
	jobID := "job-1"
	req := &api.JobRequest{
		Job: validExecutionEnvelope().Job,
		Metadata: map[string]string{
			"traceparent":                "trace-a",
			ExecutionEnvelopeMetadataKey: "old-envelope",
		},
	}

	req.Job.Id = &jobID
	req.Job.RunId = &runID

	env, err := AttachExecutionEnvelope(req, dal.ExecutionDispatchRecord{
		RunID:             runID,
		RunIndex:          9,
		JobID:             jobID,
		TaskID:            "run-1:root",
		TaskKey:           dal.RootTaskKey,
		TaskName:          dal.RootTaskKey,
		TaskAttemptID:     "run-1:root:attempt:1",
		NamespacePath:     "/teams/build",
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		CellID:            "iad-a",
		Attempt:           1,
		DefinitionVersion: 7,
		DefinitionHash:    "sha256:def456",
		OwningCell:        "iad-a",
	}, 99)
	if err != nil {
		t.Fatalf("AttachExecutionEnvelope: %v", err)
	}

	if env.DefinitionVersion != 7 {
		t.Fatalf("definition version: got %d, want 7", env.DefinitionVersion)
	}

	if env.TaskID != "run-1:root" || env.TaskKey != dal.RootTaskKey || env.TaskAttemptID != "run-1:root:attempt:1" || env.TaskAttempt != 1 {
		t.Fatalf("unexpected task identity: task=%q key=%q attempt_id=%q attempt=%d", env.TaskID, env.TaskKey, env.TaskAttemptID, env.TaskAttempt)
	}

	if env.NamespacePath != "/teams/build" {
		t.Fatalf("namespace path: got %q, want /teams/build", env.NamespacePath)
	}

	payload := req.GetMetadata()[ExecutionEnvelopeMetadataKey]
	if payload == "" {
		t.Fatal("expected execution envelope metadata")
	}

	if got := req.GetMetadata()[ExecutionTaskKeyMetadataKey]; got != dal.RootTaskKey {
		t.Fatalf("execution task key metadata: got %q, want %q", got, dal.RootTaskKey)
	}

	got, err := DecodeExecutionEnvelope([]byte(payload))
	if err != nil {
		t.Fatalf("DecodeExecutionEnvelope: %v", err)
	}

	if got.Metadata["traceparent"] != "trace-a" {
		t.Fatalf("traceparent metadata: got %q, want trace-a", got.Metadata["traceparent"])
	}

	if _, ok := got.Metadata[ExecutionEnvelopeMetadataKey]; ok {
		t.Fatal("envelope metadata recursively included itself")
	}

	if _, ok := got.Metadata[ExecutionTaskKeyMetadataKey]; ok {
		t.Fatal("envelope metadata included reserved task key metadata")
	}
}

func TestAttachExecutionEnvelopeWithActionsResolvesLocks(t *testing.T) {
	runID := "run-1"
	jobID := "job-1"
	uses := "builtins/shell"
	req := &api.JobRequest{
		Job: validExecutionEnvelope().Job,
	}

	req.Job.Id = &jobID
	req.Job.RunId = &runID
	req.Job.Root.Uses = &uses

	env, err := AttachExecutionEnvelopeWithActions(req, dal.ExecutionDispatchRecord{
		RunID:             runID,
		JobID:             jobID,
		TaskID:            "run-1:root",
		TaskKey:           dal.RootTaskKey,
		TaskAttemptID:     "run-1:root:attempt:1",
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		CellID:            "iad-a",
		Attempt:           1,
		DefinitionVersion: 7,
		DefinitionHash:    "sha256:def456",
	}, 99, envelopeResolver{
		"builtins/shell": {
			CanonicalName: "builtins/shell",
			Version:       "v1",
			Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Source:        actionregistry.SourceBuiltin,
			Runtime:       actionregistry.RuntimeBuiltin,
		},
	})

	if err != nil {
		t.Fatalf("AttachExecutionEnvelopeWithActions: %v", err)
	}

	if len(env.ActionLocks) != 1 {
		t.Fatalf("action locks: got %d, want 1: %+v", len(env.ActionLocks), env.ActionLocks)
	}

	lock := env.ActionLocks[0]
	if lock.NodePath != "root" || lock.Uses != "builtins/shell" || lock.Descriptor.Digest == "" {
		t.Fatalf("unexpected action lock: %+v", lock)
	}
}

func TestAttachExecutionEnvelopePreservesExistingActionLocks(t *testing.T) {
	base := validExecutionEnvelope()
	base.ActionLocks = []actionregistry.ActionLock{{
		NodeID:   "root",
		NodePath: "root",
		Uses:     "builtins/shell",
		Descriptor: actionregistry.Descriptor{
			CanonicalName: "builtins/shell",
			Version:       "v1",
			Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Source:        actionregistry.SourceBuiltin,
			Runtime:       actionregistry.RuntimeBuiltin,
		},
	}}

	payload, err := EncodeExecutionEnvelope(base)
	if err != nil {
		t.Fatalf("EncodeExecutionEnvelope: %v", err)
	}

	req := &api.JobRequest{
		Job: base.Job,
		Metadata: map[string]string{
			ExecutionEnvelopeMetadataKey: string(payload),
		},
	}

	env, err := AttachExecutionEnvelopeWithActions(req, dal.ExecutionDispatchRecord{
		RunID:             base.RunID,
		JobID:             base.Job.GetId(),
		TaskID:            base.TaskID,
		TaskKey:           base.TaskKey,
		TaskAttemptID:     base.TaskAttemptID,
		SegmentID:         base.SegmentID,
		ExecutionID:       base.ExecutionID,
		CellID:            base.CellID,
		Attempt:           base.TaskAttempt,
		DefinitionVersion: base.DefinitionVersion,
		DefinitionHash:    base.DefinitionHash,
	}, 99, envelopeResolver{
		"builtins/shell": {
			CanonicalName: "builtins/shell",
			Version:       "v1",
			Digest:        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			Source:        actionregistry.SourceBuiltin,
			Runtime:       actionregistry.RuntimeBuiltin,
		},
	})

	if err != nil {
		t.Fatalf("AttachExecutionEnvelopeWithActions: %v", err)
	}

	if len(env.ActionLocks) != 1 || env.ActionLocks[0].Descriptor.Digest != base.ActionLocks[0].Descriptor.Digest {
		t.Fatalf("action locks were not preserved: got %+v want %+v", env.ActionLocks, base.ActionLocks)
	}
}

func TestNewExecutionEnvelopeRejectsJobIDMismatch(t *testing.T) {
	env := validExecutionEnvelope()
	if _, err := NewExecutionEnvelope(dal.ExecutionDispatchRecord{
		RunID:             env.RunID,
		JobID:             "other-job",
		TaskID:            env.TaskID,
		TaskKey:           env.TaskKey,
		TaskAttemptID:     env.TaskAttemptID,
		Attempt:           env.TaskAttempt,
		SegmentID:         env.SegmentID,
		ExecutionID:       env.ExecutionID,
		CellID:            env.CellID,
		DefinitionVersion: env.DefinitionVersion,
		DefinitionHash:    env.DefinitionHash,
	}, env.Job, nil, 0); err == nil {
		t.Fatal("NewExecutionEnvelope succeeded, want error")
	}
}

func TestExecutionEnvelopeFromRequest(t *testing.T) {
	env := validExecutionEnvelope()
	payload, err := EncodeExecutionEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeExecutionEnvelope: %v", err)
	}

	req := &api.JobRequest{
		Job: env.Job,
		Metadata: map[string]string{
			ExecutionEnvelopeMetadataKey: string(payload),
			ExecutionTaskKeyMetadataKey:  env.TaskKey,
		},
	}

	got, ok, err := ExecutionEnvelopeFromRequest(req)
	if err != nil {
		t.Fatalf("ExecutionEnvelopeFromRequest: %v", err)
	}

	if !ok {
		t.Fatal("expected envelope to be present")
	}

	if got.ExecutionID != env.ExecutionID {
		t.Fatalf("execution id: got %q, want %q", got.ExecutionID, env.ExecutionID)
	}
}

func TestExecutionEnvelopeFromRequestAllowsDeliveryIDMutation(t *testing.T) {
	env := validExecutionEnvelope()
	payload, err := EncodeExecutionEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeExecutionEnvelope: %v", err)
	}

	job, ok := proto.Clone(env.Job).(*api.Job)
	if !ok {
		t.Fatal("clone job")
	}

	deliveryID := "queue-a/delivery-1"
	job.DeliveryId = &deliveryID

	req := &api.JobRequest{
		Job: job,
		Metadata: map[string]string{
			ExecutionEnvelopeMetadataKey: string(payload),
			ExecutionTaskKeyMetadataKey:  env.TaskKey,
		},
	}

	if _, ok, err := ExecutionEnvelopeFromRequest(req); !ok || err != nil {
		t.Fatalf("ExecutionEnvelopeFromRequest() ok=%v err=%v, want present valid envelope", ok, err)
	}
}

func TestExecutionEnvelopeFromRequestMissing(t *testing.T) {
	got, ok, err := ExecutionEnvelopeFromRequest(&api.JobRequest{})
	if err != nil {
		t.Fatalf("ExecutionEnvelopeFromRequest: %v", err)
	}

	if ok || got != nil {
		t.Fatalf("expected missing envelope, got ok=%v env=%+v", ok, got)
	}
}

func TestExecutionEnvelopeFromRequestRejectsRequestMismatch(t *testing.T) {
	env := validExecutionEnvelope()
	payload, err := EncodeExecutionEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeExecutionEnvelope: %v", err)
	}

	otherRunID := "run-other"
	req := &api.JobRequest{
		Job: &api.Job{
			Id:    env.Job.Id,
			RunId: &otherRunID,
			Root:  env.Job.GetRoot(),
		},
		Metadata: map[string]string{
			ExecutionEnvelopeMetadataKey: string(payload),
			ExecutionTaskKeyMetadataKey:  env.TaskKey,
		},
	}

	if _, ok, err := ExecutionEnvelopeFromRequest(req); !ok || err == nil {
		t.Fatalf("expected present mismatched envelope to return error, ok=%v err=%v", ok, err)
	}
}

func TestExecutionEnvelopeFromRequestRejectsMissingRequestJob(t *testing.T) {
	env := validExecutionEnvelope()
	payload, err := EncodeExecutionEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeExecutionEnvelope: %v", err)
	}

	req := &api.JobRequest{
		Metadata: map[string]string{
			ExecutionEnvelopeMetadataKey: string(payload),
			ExecutionTaskKeyMetadataKey:  env.TaskKey,
		},
	}

	if _, ok, err := ExecutionEnvelopeFromRequest(req); !ok || err == nil {
		t.Fatalf("expected present envelope with missing request job to return error, ok=%v err=%v", ok, err)
	}
}

func TestExecutionEnvelopeFromRequestRejectsMissingTaskKeyMetadata(t *testing.T) {
	env := validExecutionEnvelope()
	payload, err := EncodeExecutionEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeExecutionEnvelope: %v", err)
	}

	req := &api.JobRequest{
		Job: env.Job,
		Metadata: map[string]string{
			ExecutionEnvelopeMetadataKey: string(payload),
		},
	}

	if _, ok, err := ExecutionEnvelopeFromRequest(req); !ok || err == nil {
		t.Fatalf("expected present envelope with missing task key metadata to return error, ok=%v err=%v", ok, err)
	}
}

func TestExecutionEnvelopeFromRequestRejectsTaskKeyMetadataMismatch(t *testing.T) {
	env := validExecutionEnvelope()
	payload, err := EncodeExecutionEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeExecutionEnvelope: %v", err)
	}

	req := &api.JobRequest{
		Job: env.Job,
		Metadata: map[string]string{
			ExecutionEnvelopeMetadataKey: string(payload),
			ExecutionTaskKeyMetadataKey:  "other-task",
		},
	}

	if _, ok, err := ExecutionEnvelopeFromRequest(req); !ok || err == nil {
		t.Fatalf("expected present envelope with task key mismatch to return error, ok=%v err=%v", ok, err)
	}
}

func TestExecutionEnvelopeFromRequestRejectsJobBodyMismatch(t *testing.T) {
	env := validExecutionEnvelope()
	payload, err := EncodeExecutionEnvelope(env)
	if err != nil {
		t.Fatalf("EncodeExecutionEnvelope: %v", err)
	}

	job, ok := proto.Clone(env.Job).(*api.Job)
	if !ok {
		t.Fatal("clone job")
	}

	job.Root.Uses = stringPtr("builtins/sequence")
	req := &api.JobRequest{
		Job: job,
		Metadata: map[string]string{
			ExecutionEnvelopeMetadataKey: string(payload),
			ExecutionTaskKeyMetadataKey:  env.TaskKey,
		},
	}

	if _, ok, err := ExecutionEnvelopeFromRequest(req); !ok || err == nil {
		t.Fatalf("expected present envelope with job body mismatch to return error, ok=%v err=%v", ok, err)
	}
}

func validExecutionEnvelope() *ExecutionEnvelope {
	jobID := "job-1"
	runID := "run-1"
	rootID := "root"
	uses := "builtins/shell"

	return &ExecutionEnvelope{
		EnvelopeVersion:   ExecutionEnvelopeVersion,
		RunID:             runID,
		RunIndex:          5,
		TaskID:            runID + ":root",
		TaskKey:           dal.RootTaskKey,
		TaskName:          dal.RootTaskKey,
		TaskAttemptID:     runID + ":root:attempt:1",
		TaskAttempt:       1,
		NamespacePath:     "/teams/build",
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		CellID:            "iad-a",
		Attempt:           2,
		DefinitionVersion: 3,
		DefinitionHash:    "sha256:abc123",
		Job: &api.Job{
			Id:    &jobID,
			RunId: &runID,
			Root: &api.Node{
				Id:   &rootID,
				Uses: &uses,
				With: map[string]string{
					"command": "echo hello",
				},
			},
		},
		Metadata: map[string]string{
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		},
		CreatedAtUnixNano: 42,
	}
}

func stringPtr(s string) *string {
	return &s
}

type envelopeResolver map[string]actionregistry.Descriptor

func (r envelopeResolver) ResolveDescriptor(uses string) (actionregistry.Descriptor, error) {
	descriptor, ok := r[uses]
	if !ok {
		return actionregistry.Descriptor{}, fmt.Errorf("unknown action: %s", uses)
	}

	return descriptor, nil
}
