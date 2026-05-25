package catalog

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"vectis/internal/cell"
	"vectis/internal/interfaces/mocks"
)

type recordingProcessor struct {
	mu       sync.Mutex
	limits   []int
	result   cell.CatalogInboxProcessResult
	err      error
	onCalled func()
}

func (p *recordingProcessor) ProcessPending(ctx context.Context, limit int) (cell.CatalogInboxProcessResult, error) {
	p.mu.Lock()
	p.limits = append(p.limits, limit)
	onCalled := p.onCalled
	result := p.result
	err := p.err
	p.mu.Unlock()

	if onCalled != nil {
		onCalled()
	}

	return result, err
}

func (p *recordingProcessor) calledLimits() []int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]int(nil), p.limits...)
}

func TestServiceProcessPassesBatchSize(t *testing.T) {
	processor := &recordingProcessor{
		result: cell.CatalogInboxProcessResult{Read: 3, Applied: 2, Failed: 1},
	}
	logger := mocks.NewMockLogger()
	svc := NewServiceWithProcessor(logger, processor)

	result, err := svc.Process(context.Background(), 42)
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if result.Read != 3 || result.Applied != 2 || result.Failed != 1 {
		t.Fatalf("unexpected result: %+v", result)
	}

	limits := processor.calledLimits()
	if len(limits) != 1 || limits[0] != 42 {
		t.Fatalf("expected processor limit 42, got %v", limits)
	}

	if len(logger.GetInfoCalls()) == 0 {
		t.Fatal("expected non-empty processing result to be logged")
	}
}

func TestServiceProcessDefaultsBatchSize(t *testing.T) {
	processor := &recordingProcessor{}
	svc := NewServiceWithProcessor(nil, processor)

	if _, err := svc.Process(context.Background(), 0); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	limits := processor.calledLimits()
	if len(limits) != 1 || limits[0] != DefaultBatchSize {
		t.Fatalf("expected default batch size %d, got %v", DefaultBatchSize, limits)
	}
}

func TestServiceProcessRequiresProcessor(t *testing.T) {
	svc := NewServiceWithProcessor(nil, nil)

	if _, err := svc.Process(context.Background(), 1); err == nil {
		t.Fatal("expected error for nil processor")
	}
}

func TestServiceRunProcessesImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	processor := &recordingProcessor{
		onCalled: cancel,
	}
	svc := NewServiceWithProcessor(nil, processor)

	if err := svc.Run(ctx, time.Hour, 7); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	limits := processor.calledLimits()
	if len(limits) != 1 || limits[0] != 7 {
		t.Fatalf("expected one immediate process with limit 7, got %v", limits)
	}
}

func TestServiceRunLogsProcessorErrorsAndContinues(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	processor := &recordingProcessor{
		err:      errors.New("database not ready"),
		onCalled: cancel,
	}
	logger := mocks.NewMockLogger()
	svc := NewServiceWithProcessor(logger, processor)

	if err := svc.Run(ctx, time.Hour, 5); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	errors := logger.GetErrorCalls()
	if len(errors) != 1 {
		t.Fatalf("expected one logged processor error, got %v", errors)
	}
}
