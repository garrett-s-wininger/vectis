package dal_test

import (
	"strings"
	"testing"

	"vectis/internal/dal"
)

func TestValidateMirroredExecutionFinalization(t *testing.T) {
	base := dal.ExecutionFinalizationResult{
		ExecutionID: "execution-1",
		RunID:       "run-1",
		Outcome:     dal.ExecutionFinalizationOutcomeContinued,
		Summary: dal.RunTaskCompletion{
			RunID:      "run-1",
			Total:      2,
			Succeeded:  1,
			Incomplete: 1,
		},
		Children: []dal.TaskExecutionRecord{{
			RunID:         "run-1",
			TaskID:        "run-1:child",
			ParentTaskID:  "run-1:root",
			TaskKey:       "child",
			Name:          "child",
			TaskAttemptID: "run-1:child:attempt:1",
			SegmentID:     "segment-child",
			ExecutionID:   "execution-child",
			CellID:        "local",
			Attempt:       1,
		}},
		Activated: 1,
	}

	if err := dal.ValidateMirroredExecutionFinalization(base, base); err != nil {
		t.Fatalf("ValidateMirroredExecutionFinalization identical: %v", err)
	}

	tests := []struct {
		name string
		edit func(*dal.ExecutionFinalizationResult)
		want string
	}{
		{
			name: "execution",
			edit: func(result *dal.ExecutionFinalizationResult) {
				result.ExecutionID = "execution-2"
			},
			want: "execution",
		},
		{
			name: "run",
			edit: func(result *dal.ExecutionFinalizationResult) {
				result.RunID = "run-2"
			},
			want: "run",
		},
		{
			name: "outcome",
			edit: func(result *dal.ExecutionFinalizationResult) {
				result.Outcome = dal.ExecutionFinalizationOutcomeWaiting
			},
			want: "outcome",
		},
		{
			name: "summary",
			edit: func(result *dal.ExecutionFinalizationResult) {
				result.Summary.Incomplete = 0
			},
			want: "summary",
		},
		{
			name: "activated",
			edit: func(result *dal.ExecutionFinalizationResult) {
				result.Activated = 0
			},
			want: "activated",
		},
		{
			name: "child count",
			edit: func(result *dal.ExecutionFinalizationResult) {
				result.Children = nil
			},
			want: "children",
		},
		{
			name: "child identity",
			edit: func(result *dal.ExecutionFinalizationResult) {
				result.Children[0].ExecutionID = "execution-other-child"
			},
			want: "child 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mirror := base
			mirror.Children = append([]dal.TaskExecutionRecord(nil), base.Children...)
			tt.edit(&mirror)

			err := dal.ValidateMirroredExecutionFinalization(base, mirror)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateMirroredExecutionFinalization error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestValidateMirroredExecutionFinalizationTargetAllowsAggregateDrift(t *testing.T) {
	primary := dal.ExecutionFinalizationResult{
		ExecutionID: "execution-1",
		RunID:       "run-1",
		Outcome:     dal.ExecutionFinalizationOutcomeWaiting,
		Summary: dal.RunTaskCompletion{
			RunID:      "run-1",
			Total:      4,
			Succeeded:  2,
			Incomplete: 2,
		},
	}

	mirror := primary
	mirror.Outcome = dal.ExecutionFinalizationOutcomeContinued
	mirror.Summary.Succeeded = 3
	mirror.Summary.Incomplete = 1
	mirror.Activated = 1
	mirror.Children = []dal.TaskExecutionRecord{{RunID: "run-1", ExecutionID: "execution-child"}}

	if err := dal.ValidateMirroredExecutionFinalizationTarget(primary, mirror); err != nil {
		t.Fatalf("target validator should allow aggregate drift: %v", err)
	}

	mirror.ExecutionID = "execution-2"
	if err := dal.ValidateMirroredExecutionFinalizationTarget(primary, mirror); err == nil || !strings.Contains(err.Error(), "execution") {
		t.Fatalf("target validator execution error = %v, want execution mismatch", err)
	}

	mirror = primary
	mirror.RunID = "run-2"
	if err := dal.ValidateMirroredExecutionFinalizationTarget(primary, mirror); err == nil || !strings.Contains(err.Error(), "run") {
		t.Fatalf("target validator run error = %v, want run mismatch", err)
	}
}
