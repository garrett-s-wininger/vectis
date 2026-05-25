package cell

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"vectis/internal/dal"
)

var ErrInvalidCatalogEvent = errors.New("invalid catalog event")

type CatalogEvent struct {
	SourceCellID    string
	RunStatus       *dal.RunStatusUpdate
	ExecutionStatus *dal.ExecutionStatusUpdate
}

type CatalogEventConsumer struct {
	updater dal.RunCatalogUpdater
}

func NewCatalogEventConsumer(updater dal.RunCatalogUpdater) CatalogEventConsumer {
	return CatalogEventConsumer{updater: updater}
}

func (c CatalogEventConsumer) ApplyBatch(ctx context.Context, events []CatalogEvent) error {
	for i, event := range events {
		if err := c.Apply(ctx, event); err != nil {
			return fmt.Errorf("apply catalog event %d: %w", i, err)
		}
	}

	return nil
}

func (c CatalogEventConsumer) Apply(ctx context.Context, event CatalogEvent) error {
	if c.updater == nil {
		return errors.New("run catalog updater is required")
	}

	if _, err := event.normalizedSourceCellID(); err != nil {
		return err
	}

	updateCount := 0
	if event.RunStatus != nil {
		updateCount++
	}

	if event.ExecutionStatus != nil {
		updateCount++
	}

	if updateCount != 1 {
		return fmt.Errorf("%w: exactly one status update is required", ErrInvalidCatalogEvent)
	}

	if event.RunStatus != nil {
		return c.updater.ApplyRunStatusUpdate(ctx, *event.RunStatus)
	}

	return c.updater.ApplyExecutionStatusUpdate(ctx, *event.ExecutionStatus)
}

func (e CatalogEvent) normalizedSourceCellID() (string, error) {
	sourceCellID := strings.TrimSpace(e.SourceCellID)
	if sourceCellID == "" {
		return "", fmt.Errorf("%w: source cell is required", ErrInvalidCatalogEvent)
	}

	return sourceCellID, nil
}
