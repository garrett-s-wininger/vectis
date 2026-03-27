package logserver

import (
	"context"
	"vectis/internal/dal"
)

type RunStatusProvider interface {
	GetRunStatus(ctx context.Context, runID string) (status string, found bool, err error)
}

type DALRunStatusProvider struct {
	runs dal.RunsRepository
}

func NewDALRunStatusProvider(runs dal.RunsRepository) *DALRunStatusProvider {
	return &DALRunStatusProvider{runs: runs}
}

func (p *DALRunStatusProvider) GetRunStatus(ctx context.Context, runID string) (string, bool, error) {
	if p == nil || p.runs == nil || runID == "" {
		return "", false, nil
	}

	return p.runs.GetRunStatus(ctx, runID)
}
