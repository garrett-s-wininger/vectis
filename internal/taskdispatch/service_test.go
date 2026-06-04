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
	mu      sync.Mutex
	calls   []taskdispatch.DrainOptions
	result  taskdispatch.DrainResult
	err     error
	pending bool
	called  chan struct{}
}

type recordingMetrics struct {
	mu      sync.Mutex
	calls   []taskdispatch.DrainOptions
	results []taskdispatch.DrainResult
	errs    []error
}

func (m *recordingMetrics) RecordDrain(_ context.Context, opts taskdispatch.DrainOptions, result taskdispatch.DrainResult, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls = append(m.calls, opts)
	m.results = append(m.results, result)
	m.errs = append(m.errs, err)
}

func (m *recordingMetrics) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.calls)
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

func (d *recordingDrainer) HasPending(context.Context, taskdispatch.DrainOptions) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pending, d.err
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

func TestServiceProcessRecordsMetrics(t *testing.T) {
	drainer := newRecordingDrainer()
	drainer.result = taskdispatch.DrainResult{Listed: 2, Enqueued: 1, Failed: 1}
	metrics := &recordingMetrics{}

	svc := taskdispatch.NewService(nil, drainer)
	svc.SetMetrics(metrics)
	opts := taskdispatch.DrainOptions{CellID: "iad-a", RunID: "run-1", Limit: 7}

	result, err := svc.Process(context.Background(), opts)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if result != drainer.result {
		t.Fatalf("result: got %+v, want %+v", result, drainer.result)
	}

	if got := metrics.callCount(); got != 1 {
		t.Fatalf("metric calls: got %d, want 1", got)
	}

	if metrics.calls[0] != opts || metrics.results[0] != drainer.result || metrics.errs[0] != nil {
		t.Fatalf("metric payload mismatch: calls=%+v results=%+v errs=%+v", metrics.calls, metrics.results, metrics.errs)
	}
}

func TestServiceProcessRecordsMetricsForDrainError(t *testing.T) {
	wantErr := errors.New("db unavailable")
	drainer := newRecordingDrainer()
	drainer.result = taskdispatch.DrainResult{Listed: 1, Failed: 1}
	drainer.err = wantErr
	metrics := &recordingMetrics{}

	svc := taskdispatch.NewService(nil, drainer)
	svc.SetMetrics(metrics)

	if _, err := svc.Process(context.Background(), taskdispatch.DrainOptions{CellID: "iad-a"}); !errors.Is(err, wantErr) {
		t.Fatalf("Process error: got %v, want %v", err, wantErr)
	}

	if got := metrics.callCount(); got != 1 {
		t.Fatalf("metric calls: got %d, want 1", got)
	}

	if !errors.Is(metrics.errs[0], wantErr) || metrics.results[0] != drainer.result {
		t.Fatalf("metric error payload mismatch: results=%+v errs=%+v", metrics.results, metrics.errs)
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

func TestServiceHasPendingUsesDrainer(t *testing.T) {
	drainer := newRecordingDrainer()
	drainer.pending = true
	svc := taskdispatch.NewService(nil, drainer)

	pending, err := svc.HasPending(context.Background(), taskdispatch.DrainOptions{CellID: "iad-a"})
	if err != nil {
		t.Fatalf("HasPending: %v", err)
	}

	if !pending {
		t.Fatal("HasPending returned false, want true")
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
