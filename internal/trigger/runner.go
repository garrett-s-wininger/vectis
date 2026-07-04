package trigger

import (
	"context"
	"fmt"
	"strings"
	"time"

	"vectis/internal/interfaces"
)

type Processor interface {
	Process(context.Context) error
}

type Runner struct {
	Name      string
	Logger    interfaces.Logger
	Clock     interfaces.Clock
	Interval  time.Duration
	Processor Processor
}

func (r Runner) Run(ctx context.Context) error {
	if r.Processor == nil {
		return fmt.Errorf("trigger runner processor is required")
	}

	if r.Interval <= 0 {
		return fmt.Errorf("trigger runner interval must be > 0")
	}

	name := strings.TrimSpace(r.Name)
	if name == "" {
		name = "trigger"
	}

	logger := r.Logger
	if logger == nil {
		logger = interfaces.NewLogger(name)
	}

	clock := r.Clock
	if clock == nil {
		clock = interfaces.SystemClock{}
	}

	if err := r.Processor.Process(ctx); err != nil {
		logger.Error("%s: initial processing failed: %v", name, err)
	}

	for {
		if err := clock.Sleep(ctx, r.Interval); err != nil {
			if ctx.Err() != nil {
				logger.Info("%s: shutting down", name)
				return nil
			}

			return err
		}

		if err := r.Processor.Process(ctx); err != nil {
			logger.Error("%s: processing failed: %v", name, err)
		}
	}
}
