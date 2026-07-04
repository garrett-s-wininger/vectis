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

type recordingFanIn struct {
	mu      sync.Mutex
	limits  []int
	result  FanInResult
	err     error
	called  bool
	onCalls func()
}

func (f *recordingFanIn) IngestPending(ctx context.Context, limit int) (FanInResult, error) {
	f.mu.Lock()
	f.called = true
	f.limits = append(f.limits, limit)
	onCalls := f.onCalls
	result := f.result
	err := f.err
	f.mu.Unlock()

	if onCalls != nil {
		onCalls()
	}

	return result, err
}

func (f *recordingFanIn) calledLimits() []int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]int(nil), f.limits...)
}

type recordingBackfill struct {
	mu      sync.Mutex
	limits  []int
	result  BackfillResult
	err     error
	onCalls func()
}

func (b *recordingBackfill) RepairMissing(ctx context.Context, limit int) (BackfillResult, error) {
	b.mu.Lock()
	b.limits = append(b.limits, limit)
	onCalls := b.onCalls
	result := b.result
	err := b.err
	b.mu.Unlock()

	if onCalls != nil {
		onCalls()
	}

	return result, err
}

func (b *recordingBackfill) calledLimits() []int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return append([]int(nil), b.limits...)
}

type recordingMetrics struct {
	mu      sync.Mutex
	results []cell.CatalogInboxProcessResult
	errors  int
}

func (m *recordingMetrics) RecordProcessResult(ctx context.Context, result cell.CatalogInboxProcessResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.results = append(m.results, result)
}

func (m *recordingMetrics) RecordProcessError(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors++
}

func (m *recordingMetrics) snapshot() ([]cell.CatalogInboxProcessResult, int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]cell.CatalogInboxProcessResult(nil), m.results...), m.errors
}

func TestServiceProcessPassesBatchSize(t *testing.T) {
	processor := &recordingProcessor{
		result: cell.CatalogInboxProcessResult{Read: 3, Applied: 2, Failed: 1},
	}
	logger := mocks.NewMockLogger()
	metrics := &recordingMetrics{}
	svc := NewServiceWithProcessor(logger, processor)
	svc.SetMetrics(metrics)

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

	results, errors := metrics.snapshot()
	if len(results) != 1 || results[0] != result || errors != 0 {
		t.Fatalf("unexpected metrics: results=%+v errors=%d", results, errors)
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

func TestServiceProcessRunsFanInBeforeInboxProcessor(t *testing.T) {
	var order []string
	fanIn := &recordingFanIn{
		result: FanInResult{Sources: 1, Read: 1, Copied: 1},
		onCalls: func() {
			order = append(order, "fan-in")
		},
	}

	processor := &recordingProcessor{
		onCalled: func() {
			order = append(order, "processor")
		},
	}

	logger := mocks.NewMockLogger()
	svc := NewServiceWithProcessor(logger, processor)
	svc.SetFanIn(fanIn)

	if _, err := svc.Process(context.Background(), 5); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if got := fanIn.calledLimits(); len(got) != 1 || got[0] != 5 {
		t.Fatalf("expected fan-in limit 5, got %v", got)
	}

	if len(order) != 2 || order[0] != "fan-in" || order[1] != "processor" {
		t.Fatalf("unexpected call order: %v", order)
	}

	if len(logger.GetInfoCalls()) == 0 {
		t.Fatal("expected fan-in result to be logged")
	}
}

func TestServiceProcessRunsBackfillBeforeFanInAndInboxProcessor(t *testing.T) {
	var order []string
	backfill := &recordingBackfill{
		result: BackfillResult{RunEvents: 1},
		onCalls: func() {
			order = append(order, "backfill")
		},
	}

	fanIn := &recordingFanIn{
		result: FanInResult{Sources: 1, Backfilled: 1, Read: 1, Copied: 1},
		onCalls: func() {
			order = append(order, "fan-in")
		},
	}

	processor := &recordingProcessor{
		onCalled: func() {
			order = append(order, "processor")
		},
	}

	logger := mocks.NewMockLogger()
	svc := NewServiceWithProcessor(logger, processor)
	svc.SetBackfill(backfill)
	svc.SetFanIn(fanIn)

	if _, err := svc.Process(context.Background(), 6); err != nil {
		t.Fatalf("Process() error = %v", err)
	}

	if got := backfill.calledLimits(); len(got) != 1 || got[0] != 6 {
		t.Fatalf("expected backfill limit 6, got %v", got)
	}

	if got := fanIn.calledLimits(); len(got) != 1 || got[0] != 6 {
		t.Fatalf("expected fan-in limit 6, got %v", got)
	}

	if len(order) != 3 || order[0] != "backfill" || order[1] != "fan-in" || order[2] != "processor" {
		t.Fatalf("unexpected call order: %v", order)
	}

	if len(logger.GetInfoCalls()) < 2 {
		t.Fatalf("expected backfill and fan-in results to be logged, got %v", logger.GetInfoCalls())
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
	metrics := &recordingMetrics{}
	svc := NewServiceWithProcessor(logger, processor)
	svc.SetMetrics(metrics)

	if err := svc.Run(ctx, time.Hour, 5); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	errors := logger.GetErrorCalls()
	if len(errors) != 1 {
		t.Fatalf("expected one logged processor error, got %v", errors)
	}

	results, metricErrors := metrics.snapshot()
	if len(results) != 0 || metricErrors != 1 {
		t.Fatalf("unexpected metrics: results=%+v errors=%d", results, metricErrors)
	}
}
