package trigger

import (
	"context"
	"testing"
	"time"

	"vectis/internal/interfaces/mocks"
)

func TestRunnerProcessesImmediatelyAndOnInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor := &countingProcessor{}
	clock := &cancelAfterSecondSleepClock{cancel: cancel}

	err := Runner{
		Name:      "test-trigger",
		Logger:    mocks.NopLogger{},
		Clock:     clock,
		Interval:  time.Second,
		Processor: processor,
	}.Run(ctx)

	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if processor.count != 2 {
		t.Fatalf("process count = %d, want 2", processor.count)
	}

	if clock.sleeps != 2 {
		t.Fatalf("sleep count = %d, want 2", clock.sleeps)
	}
}

type countingProcessor struct {
	count int
}

func (p *countingProcessor) Process(context.Context) error {
	p.count++
	return nil
}

type cancelAfterSecondSleepClock struct {
	cancel func()
	sleeps int
}

func (c *cancelAfterSecondSleepClock) Now() time.Time {
	return time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
}

func (c *cancelAfterSecondSleepClock) Sleep(ctx context.Context, _ time.Duration) error {
	c.sleeps++
	if c.sleeps == 1 {
		return nil
	}

	c.cancel()
	return ctx.Err()
}
