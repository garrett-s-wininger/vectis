package cell

import (
	"context"
	"errors"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/dal"

	"google.golang.org/protobuf/encoding/protojson"
)

func RecordExecutionHandoffPayload(ctx context.Context, runs dal.RunsRepository, runID string, req *api.JobRequest, definitionHash string) (*api.JobRequest, error) {
	if runs == nil {
		return nil, errors.New("runs repository is required")
	}

	if err := validateExecutionHandoffPayload(runID, req, definitionHash); err != nil {
		return nil, err
	}

	payloadJSON, err := protojson.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal execution payload: %w", err)
	}

	_, recordedPayloadJSON, err := runs.RecordExecutionPayload(ctx, runID, string(payloadJSON), strings.TrimSpace(definitionHash))
	if err != nil {
		return nil, fmt.Errorf("record execution payload: %w", err)
	}

	if recordedPayloadJSON == string(payloadJSON) {
		return req, nil
	}

	var recordedReq api.JobRequest
	if err := protojson.Unmarshal([]byte(recordedPayloadJSON), &recordedReq); err != nil {
		return nil, fmt.Errorf("parse recorded execution payload: %w", err)
	}

	if err := validateExecutionHandoffPayload(runID, &recordedReq, definitionHash); err != nil {
		return nil, fmt.Errorf("validate recorded execution payload: %w", err)
	}

	return &recordedReq, nil
}

func validateExecutionHandoffPayload(runID string, req *api.JobRequest, definitionHash string) error {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return errors.New("run_id is required")
	}

	submission, err := NewExecutionSubmission(req)
	if err != nil {
		return err
	}

	if submission.Envelope.RunID != runID {
		return fmt.Errorf("execution payload run_id %q does not match run %q", submission.Envelope.RunID, runID)
	}

	definitionHash = strings.TrimSpace(definitionHash)
	if definitionHash != "" && submission.Envelope.DefinitionHash != definitionHash {
		return fmt.Errorf("execution payload definition_hash %q does not match run definition_hash %q", submission.Envelope.DefinitionHash, definitionHash)
	}

	return nil
}
