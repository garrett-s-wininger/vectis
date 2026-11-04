package api

import (
	"context"
	"vectis/internal/dal"
)

func (s *APIServer) runSourceRepository(ctx context.Context, jobID string, version int) (string, error) {
	if s.sources == nil {
		return "", nil
	}

	source, err := s.sources.GetDefinitionSource(ctx, jobID, version)
	if dal.IsNotFound(err) {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return source.RepositoryID, nil
}
