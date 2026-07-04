package cell

import (
	"context"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"

	"google.golang.org/protobuf/encoding/protojson"
)

func TestRecordExecutionHandoffPayloadReturnsRecordedPayload(t *testing.T) {
	ctx := context.Background()
	hash := "sha256:abc123"
	runs := mocks.NewMockRunsRepository()
	recordedReq := validPayloadJobRequest(t, "run-1", hash, "execution-recorded", 42)
	currentReq := validPayloadJobRequest(t, "run-1", hash, "execution-current", 99)
	runs.RecordedPayloads = map[string]string{}
	runs.RecordedPayloads["run-1"] = marshalPayloadJobRequest(t, recordedReq)

	got, err := RecordExecutionHandoffPayload(ctx, runs, "run-1", currentReq, hash)
	if err != nil {
		t.Fatalf("RecordExecutionHandoffPayload: %v", err)
	}

	env, ok, err := ExecutionEnvelopeFromRequest(got)
	if err != nil || !ok {
		t.Fatalf("ExecutionEnvelopeFromRequest returned ok=%v err=%v", ok, err)
	}

	if env.ExecutionID != "execution-recorded" || env.CreatedAtUnixNano != 42 {
		t.Fatalf("expected frozen recorded payload, got execution=%q created=%d", env.ExecutionID, env.CreatedAtUnixNano)
	}
}

func TestRecordExecutionHandoffPayloadRejectsRecordedRunMismatch(t *testing.T) {
	ctx := context.Background()
	hash := "sha256:abc123"
	runs := mocks.NewMockRunsRepository()
	runs.RecordedPayloads = map[string]string{}
	runs.RecordedPayloads["run-1"] = marshalPayloadJobRequest(t, validPayloadJobRequest(t, "run-other", hash, "execution-other", 42))

	_, err := RecordExecutionHandoffPayload(ctx, runs, "run-1", validPayloadJobRequest(t, "run-1", hash, "execution-current", 99), hash)
	if err == nil {
		t.Fatal("RecordExecutionHandoffPayload succeeded, want recorded payload run mismatch")
	}

	if !strings.Contains(err.Error(), "validate recorded execution payload") || !strings.Contains(err.Error(), "run_id") {
		t.Fatalf("RecordExecutionHandoffPayload error %q does not contain recorded run mismatch", err.Error())
	}
}

func TestRecordExecutionHandoffPayloadRejectsRecordedDefinitionHashMismatch(t *testing.T) {
	ctx := context.Background()
	hash := "sha256:abc123"
	runs := mocks.NewMockRunsRepository()
	runs.RecordedPayloads = map[string]string{}
	runs.RecordedPayloads["run-1"] = marshalPayloadJobRequest(t, validPayloadJobRequest(t, "run-1", "sha256:other", "execution-recorded", 42))

	_, err := RecordExecutionHandoffPayload(ctx, runs, "run-1", validPayloadJobRequest(t, "run-1", hash, "execution-current", 99), hash)
	if err == nil {
		t.Fatal("RecordExecutionHandoffPayload succeeded, want recorded payload definition hash mismatch")
	}

	if !strings.Contains(err.Error(), "validate recorded execution payload") || !strings.Contains(err.Error(), "definition_hash") {
		t.Fatalf("RecordExecutionHandoffPayload error %q does not contain recorded definition hash mismatch", err.Error())
	}
}

func TestRecordExecutionHandoffPayloadRejectsInvalidNewPayloadBeforeRecord(t *testing.T) {
	ctx := context.Background()
	runs := mocks.NewMockRunsRepository()
	req := validPayloadJobRequest(t, "run-1", "sha256:abc123", "execution-current", 99)
	delete(req.GetMetadata(), ExecutionEnvelopeMetadataKey)

	if _, err := RecordExecutionHandoffPayload(ctx, runs, "run-1", req, "sha256:abc123"); err == nil {
		t.Fatal("RecordExecutionHandoffPayload succeeded, want missing envelope error")
	}

	if got := len(runs.RecordedPayloads); got != 0 {
		t.Fatalf("invalid payload should not be recorded, got %d payloads", got)
	}
}

func validPayloadJobRequest(t *testing.T, runID, definitionHash, executionID string, createdAtUnixNano int64) *api.JobRequest {
	t.Helper()

	jobID := "job-1"
	rootID := "root"
	uses := "builtins/script"
	req := &api.JobRequest{
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
	}

	if _, err := AttachExecutionEnvelope(req, dal.ExecutionDispatchRecord{
		RunID:             runID,
		RunIndex:          5,
		JobID:             jobID,
		TaskID:            runID + ":root",
		TaskKey:           dal.RootTaskKey,
		TaskName:          dal.RootTaskKey,
		TaskAttemptID:     runID + ":root:attempt:1",
		SegmentID:         runID + ":root:segment",
		ExecutionID:       executionID,
		CellID:            "iad-a",
		Attempt:           1,
		DefinitionVersion: 3,
		DefinitionHash:    definitionHash,
	}, createdAtUnixNano); err != nil {
		t.Fatalf("AttachExecutionEnvelope: %v", err)
	}

	return req
}

func marshalPayloadJobRequest(t *testing.T, req *api.JobRequest) string {
	t.Helper()

	payload, err := protojson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal job request: %v", err)
	}

	return string(payload)
}
