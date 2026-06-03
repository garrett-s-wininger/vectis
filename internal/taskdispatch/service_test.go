package taskdispatch_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"vectis/internal/interfaces/mocks"
	"vectis/internal/taskdispatch"
)

type recordingDrainer struct {
	mu     sync.Mutex
	calls  []taskdispatch.DrainOptions
	result taskdispatch.DrainResult
	err    error
	called chan struct{}
}

func newRecordingDrainer() *recordingDrainer {
	return &recordingDrainer{called: make(chan struct{}, 10)}
}

func (d *recordingDrainer) Drain(_ context.Context, opts taskdispatch.DrainOptions) (taskdispatch.DrainResult, error) {
	d.mu.Lock()
	d.calls = append(d.calls, opts)
	result := d.result
	err := d.err
	d.mu.Unlock()

	select {
	case d.called <- struct{}{}:
	default:
	}

	return result, err
}

func (d *recordingDrainer) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

func (d *recordingDrainer) callAt(index int) taskdispatch.DrainOptions {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls[index]
}

func TestServiceProcessDrainsWithOptions(t *testing.T) {
	drainer := newRecordingDrainer()
	drainer.result = taskdispatch.DrainResult{Listed: 2, Enqueued: 1, Failed: 1}

	svc := taskdispatch.NewService(mocks.NewMockLogger(), drainer)
	opts := taskdispatch.DrainOptions{CellID: "iad-a", Limit: 7, RetryCutoffUnixNano: 123}

	result, err := svc.Process(context.Background(), opts)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if result != drainer.result {
		t.Fatalf("result: got %+v, want %+v", result, drainer.result)
	}

	if got := drainer.callCount(); got != 1 {
		t.Fatalf("drain calls: got %d, want 1", got)
	}

	if got := drainer.callAt(0); got != opts {
		t.Fatalf("drain options: got %+v, want %+v", got, opts)
	}
}

func TestServiceRunDrainsOnNotify(t *testing.T) {
	drainer := newRecordingDrainer()
	drainer.result = taskdispatch.DrainResult{Listed: 1, Enqueued: 1}
	logger := mocks.NewMockLogger()
	svc := taskdispatch.NewService(logger, drainer)
	opts := taskdispatch.DrainOptions{CellID: "iad-a", Limit: 3}

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		errs <- svc.Run(ctx, time.Hour, opts)
	}()

	waitForDrain(t, drainer.called)

	svc.Notify()
	waitForDrain(t, drainer.called)

	cancel()
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("service did not stop after cancellation")
	}

	if got := drainer.callCount(); got != 2 {
		t.Fatalf("drain calls: got %d, want 2", got)
	}

	infos := logger.GetInfoCalls()
	if len(infos) < 2 {
		t.Fatalf("expected service info logs, got %+v", infos)
	}
}

func TestServiceProcessRejectsMissingDrainer(t *testing.T) {
	svc := taskdispatch.NewService(nil, nil)

	if _, err := svc.Process(context.Background(), taskdispatch.DrainOptions{}); err == nil {
		t.Fatal("Process should reject missing drainer")
	}
}

func TestServiceRunLogsDrainErrorsAndContinues(t *testing.T) {
	drainer := newRecordingDrainer()
	drainer.err = errors.New("db unavailable")
	logger := mocks.NewMockLogger()
	svc := taskdispatch.NewService(logger, drainer)

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		errs <- svc.Run(ctx, time.Hour, taskdispatch.DrainOptions{CellID: "iad-a"})
	}()

	waitForDrain(t, drainer.called)
	cancel()

	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("service did not stop after cancellation")
	}

	errorLogs := logger.GetErrorCalls()
	if len(errorLogs) != 1 || !strings.Contains(errorLogs[0], "db unavailable") {
		t.Fatalf("error logs: %+v", errorLogs)
	}
}

func waitForDrain(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for drain")
	}
}
