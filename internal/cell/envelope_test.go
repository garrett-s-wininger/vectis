package cell

import (
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
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

	if got.SegmentID != env.SegmentID {
		t.Fatalf("segment_id: got %q, want %q", got.SegmentID, env.SegmentID)
	}

	if got.ExecutionID != env.ExecutionID {
		t.Fatalf("execution_id: got %q, want %q", got.ExecutionID, env.ExecutionID)
	}

	if got.CellID != env.CellID {
		t.Fatalf("cell_id: got %q, want %q", got.CellID, env.CellID)
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
		JobID:             jobID,
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

	payload := req.GetMetadata()[ExecutionEnvelopeMetadataKey]
	if payload == "" {
		t.Fatal("expected execution envelope metadata")
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
}

func TestNewExecutionEnvelopeRejectsJobIDMismatch(t *testing.T) {
	env := validExecutionEnvelope()
	if _, err := NewExecutionEnvelope(dal.ExecutionDispatchRecord{
		RunID:             env.RunID,
		JobID:             "other-job",
		SegmentID:         env.SegmentID,
		ExecutionID:       env.ExecutionID,
		CellID:            env.CellID,
		DefinitionVersion: env.DefinitionVersion,
		DefinitionHash:    env.DefinitionHash,
	}, env.Job, nil, 0); err == nil {
		t.Fatal("NewExecutionEnvelope succeeded, want error")
	}
}

func validExecutionEnvelope() *ExecutionEnvelope {
	jobID := "job-1"
	runID := "run-1"
	rootID := "root"
	uses := "builtin/shell"

	return &ExecutionEnvelope{
		EnvelopeVersion:   ExecutionEnvelopeVersion,
		RunID:             runID,
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		CellID:            "iad-a",
		DefinitionVersion: 3,
		DefinitionHash:    "sha256:abc123",
		Job: &api.Job{
			Id:    &jobID,
			RunId: &runID,
			Root: &api.Node{
				Id:   &rootID,
				Uses: &uses,
				With: map[string]string{
					"script": "echo hello",
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
