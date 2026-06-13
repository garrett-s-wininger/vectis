package api

import (
	"testing"

	"vectis/internal/dal"
)

func TestRunNextAction(t *testing.T) {
	tests := []struct {
		name           string
		status         string
		taskCompletion dal.RunTaskCompletion
		continuation   bool
		want           *string
	}{
		{
			name:   "non queued",
			status: dal.RunStatusRunning,
		},
		{
			name:   "orphaned task finalization repair succeeded",
			status: dal.RunStatusOrphaned,
			taskCompletion: dal.RunTaskCompletion{
				Total:     2,
				Succeeded: 2,
			},
			want: stringPtr(runNextActionTaskFinalizationRepairPending),
		},
		{
			name:   "orphaned task finalization repair failed",
			status: dal.RunStatusOrphaned,
			taskCompletion: dal.RunTaskCompletion{
				Total:          3,
				Succeeded:      1,
				TerminalFailed: 1,
				Incomplete:     1,
			},
			want: stringPtr(runNextActionTaskFinalizationRepairPending),
		},
		{
			name:   "orphaned incomplete has no next action",
			status: dal.RunStatusOrphaned,
			taskCompletion: dal.RunTaskCompletion{
				Total:      2,
				Succeeded:  1,
				Incomplete: 1,
			},
		},
		{
			name:   "waiting for task completion",
			status: dal.RunStatusQueued,
			taskCompletion: dal.RunTaskCompletion{
				Total:      2,
				Incomplete: 1,
			},
			want: stringPtr(runNextActionTaskCompletionPending),
		},
		{
			name:   "waiting for task continuation redispatch",
			status: dal.RunStatusQueued,
			taskCompletion: dal.RunTaskCompletion{
				Total:      2,
				Succeeded:  1,
				Incomplete: 1,
			},
			continuation: true,
			want:         stringPtr(runNextActionTaskContinuationPending),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runNextAction(tt.status, tt.taskCompletion, tt.continuation)
			if got == nil && tt.want == nil {
				return
			}

			if got == nil || tt.want == nil || *got != *tt.want {
				t.Fatalf("runNextAction() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildDispatchSummary(t *testing.T) {
	failureMessage := "queue unavailable"
	events := []dal.DispatchEvent{
		{Source: dal.DispatchSourceCron, EventType: dal.DispatchEventAttempt, CreatedAt: 10},
		{Source: dal.DispatchSourceCron, EventType: dal.DispatchEventFailure, Message: &failureMessage, CreatedAt: 12},
		{Source: dal.DispatchSourceReconciler, EventType: dal.DispatchEventAttempt, CreatedAt: 20},
		{Source: dal.DispatchSourceReconciler, EventType: dal.DispatchEventSuccess, CreatedAt: 21},
		{Source: dal.DispatchSourceCron, EventType: dal.DispatchEventAttempt, CreatedAt: 30},
		{Source: dal.DispatchSourceCron, EventType: dal.DispatchEventSuccess, CreatedAt: 31},
	}

	got := buildDispatchSummary(events)
	if len(got) != 2 {
		t.Fatalf("summary len: got %d want 2 (%+v)", len(got), got)
	}

	if got[0].Source != dal.DispatchSourceCron || got[0].Attempts != 2 || got[0].Successes != 1 || got[0].Failures != 1 {
		t.Fatalf("cron summary: %+v", got[0])
	}

	if got[0].FirstEventAt != 10 || got[0].LastEventAt != 31 || got[0].LastEventType != dal.DispatchEventSuccess || got[0].LastMessage != nil {
		t.Fatalf("cron last event summary: %+v", got[0])
	}

	if got[1].Source != dal.DispatchSourceReconciler || got[1].Attempts != 1 || got[1].Successes != 1 || got[1].Failures != 0 {
		t.Fatalf("reconciler summary: %+v", got[1])
	}

	if got[1].FirstEventAt != 20 || got[1].LastEventAt != 21 || got[1].LastEventType != dal.DispatchEventSuccess {
		t.Fatalf("reconciler last event summary: %+v", got[1])
	}
}

func stringPtr(value string) *string {
	return &value
}
