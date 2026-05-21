package cell

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	api "vectis/api/gen/go"
)

const ExecutionEnvelopeVersion = 1

type ExecutionEnvelope struct {
	EnvelopeVersion   int               `json:"envelope_version"`
	RunID             string            `json:"run_id"`
	SegmentID         string            `json:"segment_id"`
	ExecutionID       string            `json:"execution_id"`
	CellID            string            `json:"cell_id"`
	DefinitionVersion int               `json:"definition_version"`
	DefinitionHash    string            `json:"definition_hash"`
	Job               *api.Job          `json:"job"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	CreatedAtUnixNano int64             `json:"created_at_unix_nano,omitempty"`
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

	if strings.TrimSpace(e.SegmentID) == "" {
		return errors.New("execution envelope segment_id is required")
	}

	if strings.TrimSpace(e.ExecutionID) == "" {
		return errors.New("execution envelope execution_id is required")
	}

	if strings.TrimSpace(e.CellID) == "" {
		return errors.New("execution envelope cell_id is required")
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

	return nil
}
