package api

import (
	"context"
	"errors"

	"vectis/internal/dal"
	sourcepkg "vectis/internal/source"
)

func (s *APIServer) resolveSourceRepositoryJobDefinition(
	ctx context.Context,
	rec dal.SourceRepositoryRecord,
	jobID string,
	target sourcepkg.DefinitionTarget,
	explicit bool,
) (sourcepkg.Definition, error) {
	req := sourcepkg.DefinitionRequest{Ref: target.Ref, Path: target.Path}
	if !explicit {
		req.JobID = jobID
	}

	if loaded, handled, err := sourcepkg.EffectiveSourceJob(ctx, s.sources, rec, jobID, rec.NamespaceID, req); handled {
		return loaded, err
	}

	if registration, err := s.sources.GetSourceJob(ctx, rec.RepositoryID, jobID); err == nil {
		req.JobID = ""
		req.Path = registration.Path
		if req.Ref == "" {
			req.Ref = registration.Ref
		}
	} else if !dal.IsNotFound(err) {
		return sourcepkg.Definition{}, err
	}

	return s.resolveSourceRepositoryDefinition(ctx, rec, req)
}

func sourceJobAuthoringPath(
	ctx context.Context,
	rec dal.SourceRepositoryRecord,
	jobID, ref, fallback string,
) (string, error) {
	if ref == "" {
		ref = rec.DefaultRef
	}

	if ref == "" {
		ref = "HEAD"
	}

	found, _, err := newGitCheckoutForSourceRepository(rec).ResolveJobDefinitionPath(ctx, ref, jobID)
	if errors.Is(err, sourcepkg.ErrNotFound) {
		return fallback, nil
	}

	return found, err
}
