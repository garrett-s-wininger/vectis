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
		taskDispatch   *taskDispatchRow
		want           *string
	}{
		{
			name:   "non queued",
			status: dal.RunStatusRunning,
		},
		{
			name:   "pending task dispatch",
			status: dal.RunStatusQueued,
			taskDispatch: &taskDispatchRow{
				Total:   1,
				Pending: 1,
			},
			want: stringPtr(runNextActionTaskDispatchPending),
		},
		{
			name:   "retry pending task dispatch",
			status: dal.RunStatusQueued,
			taskDispatch: &taskDispatchRow{
				Total:  1,
				Failed: 1,
			},
			want: stringPtr(runNextActionTaskDispatchRetryPending),
		},
		{
			name:   "pending beats retry",
			status: dal.RunStatusQueued,
			taskDispatch: &taskDispatchRow{
				Total:   2,
				Pending: 1,
				Failed:  1,
			},
			want: stringPtr(runNextActionTaskDispatchPending),
		},
		{
			name:   "waiting for task completion",
			status: dal.RunStatusQueued,
			taskCompletion: dal.RunTaskCompletion{
				Total:      2,
				Incomplete: 1,
			},
			taskDispatch: &taskDispatchRow{
				Total:    1,
				Enqueued: 1,
			},
			want: stringPtr(runNextActionTaskCompletionPending),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := runNextAction(tt.status, tt.taskCompletion, tt.taskDispatch)
			if got == nil && tt.want == nil {
				return
			}

			if got == nil || tt.want == nil || *got != *tt.want {
				t.Fatalf("runNextAction() = %v, want %v", got, tt.want)
			}
		})
	}
}

func stringPtr(value string) *string {
	return &value
}
