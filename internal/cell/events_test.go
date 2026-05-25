package cell

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"vectis/internal/dal"
)

type recordingCatalogUpdater struct {
	failAt int
	calls  []string
}

func (u *recordingCatalogUpdater) ApplyRunStatusUpdate(ctx context.Context, update dal.RunStatusUpdate) error {
	u.calls = append(u.calls, "run:"+update.RunID+":"+update.Status)
	return u.errIfNeeded()
}

func (u *recordingCatalogUpdater) ApplyExecutionStatusUpdate(ctx context.Context, update dal.ExecutionStatusUpdate) error {
	u.calls = append(u.calls, "execution:"+update.ExecutionID+":"+update.Status)
	return u.errIfNeeded()
}

func (u *recordingCatalogUpdater) errIfNeeded() error {
	if u.failAt > 0 && len(u.calls) == u.failAt {
		return fmt.Errorf("forced update failure")
	}

	return nil
}

func TestCatalogEventConsumer_ApplyBatchAppliesEventsInOrder(t *testing.T) {
	updater := &recordingCatalogUpdater{}
	consumer := NewCatalogEventConsumer(updater)

	events := []CatalogEvent{
		{
			SourceCellID: "iad-a",
			RunStatus: &dal.RunStatusUpdate{
				RunID:  "run-1",
				Status: dal.RunStatusRunning,
			},
		},
		{
			SourceCellID: "iad-a",
			ExecutionStatus: &dal.ExecutionStatusUpdate{
				ExecutionID: "execution-1",
				Status:      dal.ExecutionStatusAccepted,
			},
		},
		{
			SourceCellID: "iad-a",
			RunStatus: &dal.RunStatusUpdate{
				RunID:       "run-1",
				Status:      dal.RunStatusFailed,
				FailureCode: dal.FailureCodeExecution,
				Reason:      "failed in cell",
			},
		},
	}

	if err := consumer.ApplyBatch(context.Background(), events); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	want := []string{
		"run:run-1:running",
		"execution:execution-1:accepted",
		"run:run-1:failed",
	}

	if !reflect.DeepEqual(updater.calls, want) {
		t.Fatalf("calls: got %+v, want %+v", updater.calls, want)
	}
}

func TestCatalogEventConsumer_ApplyBatchStopsOnError(t *testing.T) {
	updater := &recordingCatalogUpdater{failAt: 2}
	consumer := NewCatalogEventConsumer(updater)

	events := []CatalogEvent{
		{
			SourceCellID: "iad-a",
			RunStatus: &dal.RunStatusUpdate{
				RunID:  "run-1",
				Status: dal.RunStatusRunning,
			},
		},
		{
			SourceCellID: "iad-a",
			ExecutionStatus: &dal.ExecutionStatusUpdate{
				ExecutionID: "execution-1",
				Status:      dal.ExecutionStatusAccepted,
			},
		},
		{
			SourceCellID: "iad-a",
			RunStatus: &dal.RunStatusUpdate{
				RunID:  "run-2",
				Status: dal.RunStatusRunning,
			},
		},
	}

	if err := consumer.ApplyBatch(context.Background(), events); err == nil {
		t.Fatal("expected ApplyBatch to fail")
	}

	want := []string{
		"run:run-1:running",
		"execution:execution-1:accepted",
	}

	if !reflect.DeepEqual(updater.calls, want) {
		t.Fatalf("calls: got %+v, want %+v", updater.calls, want)
	}
}

func TestCatalogEventConsumer_RejectsInvalidEvents(t *testing.T) {
	consumer := NewCatalogEventConsumer(&recordingCatalogUpdater{})

	tests := []struct {
		name  string
		event CatalogEvent
	}{
		{
			name: "missing source cell",
			event: CatalogEvent{
				RunStatus: &dal.RunStatusUpdate{RunID: "run-1", Status: dal.RunStatusRunning},
			},
		},
		{
			name: "missing status update",
			event: CatalogEvent{
				SourceCellID: "iad-a",
			},
		},
		{
			name: "multiple status updates",
			event: CatalogEvent{
				SourceCellID:    "iad-a",
				RunStatus:       &dal.RunStatusUpdate{RunID: "run-1", Status: dal.RunStatusRunning},
				ExecutionStatus: &dal.ExecutionStatusUpdate{ExecutionID: "execution-1", Status: dal.ExecutionStatusAccepted},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := consumer.Apply(context.Background(), tt.event)
			if !errors.Is(err, ErrInvalidCatalogEvent) {
				t.Fatalf("expected ErrInvalidCatalogEvent, got %v", err)
			}
		})
	}
}
