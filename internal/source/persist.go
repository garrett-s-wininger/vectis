package source

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/dal"
	jobvalidation "vectis/internal/job/validation"
)

type RepositoryFactory func(dal.SourceRepositoryRecord) (Repository, error)

type DefinitionPersister struct {
	Jobs                  dal.SourceBackedJobsRepository
	Sources               dal.SourcesRepository
	NewRepository         RepositoryFactory
	NewDefinitionResolver DefinitionResolverFactory
}

type PersistDefinitionRequest struct {
	JobID        string
	NamespaceID  int64
	RepositoryID string
	Ref          string
	Path         string
	Validation   jobvalidation.Options
}

type PersistedDefinition struct {
	JobID      string
	Version    int
	Definition Definition
	Repository dal.SourceRepositoryRecord
}

func (p DefinitionPersister) CreateJob(ctx context.Context, req PersistDefinitionRequest) (PersistedDefinition, error) {
	req = normalizePersistDefinitionRequest(req)
	if p.Jobs == nil {
		return PersistedDefinition{}, fmt.Errorf("%w: jobs repository is required", ErrInvalidReference)
	}

	loaded, repo, sourceRec, err := p.load(ctx, req)
	if err != nil {
		return PersistedDefinition{}, err
	}

	version, err := p.Jobs.CreateWithSource(ctx, req.JobID, loaded.DefinitionJSON, req.NamespaceID, sourceRec)
	if err != nil {
		return PersistedDefinition{}, err
	}

	return PersistedDefinition{
		JobID:      req.JobID,
		Version:    version,
		Definition: loaded,
		Repository: repo,
	}, nil
}

func (p DefinitionPersister) UpdateJob(ctx context.Context, req PersistDefinitionRequest) (PersistedDefinition, error) {
	req = normalizePersistDefinitionRequest(req)
	if p.Jobs == nil {
		return PersistedDefinition{}, fmt.Errorf("%w: jobs repository is required", ErrInvalidReference)
	}

	loaded, repo, sourceRec, err := p.load(ctx, req)
	if err != nil {
		return PersistedDefinition{}, err
	}

	version, err := p.Jobs.UpdateDefinitionWithSource(ctx, req.JobID, loaded.DefinitionJSON, sourceRec)
	if err != nil {
		return PersistedDefinition{}, err
	}

	return PersistedDefinition{
		JobID:      req.JobID,
		Version:    version,
		Definition: loaded,
		Repository: repo,
	}, nil
}

func (p DefinitionPersister) load(ctx context.Context, req PersistDefinitionRequest) (Definition, dal.SourceRepositoryRecord, dal.JobDefinitionSourceRecord, error) {
	if req.JobID == "" {
		return Definition{}, dal.SourceRepositoryRecord{}, dal.JobDefinitionSourceRecord{}, fmt.Errorf("%w: job_id is required", ErrInvalidReference)
	}

	if req.RepositoryID == "" {
		return Definition{}, dal.SourceRepositoryRecord{}, dal.JobDefinitionSourceRecord{}, fmt.Errorf("%w: repository_id is required", ErrInvalidReference)
	}

	if p.Sources == nil {
		return Definition{}, dal.SourceRepositoryRecord{}, dal.JobDefinitionSourceRecord{}, fmt.Errorf("%w: sources repository is required", ErrInvalidReference)
	}

	repoRec, err := p.Sources.GetRepository(ctx, req.RepositoryID)
	if err != nil {
		return Definition{}, dal.SourceRepositoryRecord{}, dal.JobDefinitionSourceRecord{}, err
	}

	if !repoRec.Enabled {
		return Definition{}, dal.SourceRepositoryRecord{}, dal.JobDefinitionSourceRecord{}, fmt.Errorf("%w: source repository %s is disabled", ErrInvalidReference, repoRec.RepositoryID)
	}

	resolver, err := p.definitionResolver(repoRec)
	if err != nil {
		return Definition{}, dal.SourceRepositoryRecord{}, dal.JobDefinitionSourceRecord{}, err
	}

	target, err := ResolveDefinitionTarget(DefinitionTargetRequest{
		JobID:      req.JobID,
		Ref:        req.Ref,
		DefaultRef: repoRec.DefaultRef,
		Path:       req.Path,
	})
	if err != nil {
		return Definition{}, dal.SourceRepositoryRecord{}, dal.JobDefinitionSourceRecord{}, err
	}

	loaded, err := resolver.ResolveDefinition(ctx, DefinitionRequest{
		Ref:        target.Ref,
		Path:       target.Path,
		Validation: req.Validation,
	})
	if err != nil {
		return Definition{}, dal.SourceRepositoryRecord{}, dal.JobDefinitionSourceRecord{}, err
	}

	sourceRec := NewJobDefinitionSourceRecord(req.JobID, repoRec.RepositoryID, loaded)

	return loaded, repoRec, sourceRec, nil
}

func normalizePersistDefinitionRequest(req PersistDefinitionRequest) PersistDefinitionRequest {
	req.JobID = strings.TrimSpace(req.JobID)
	req.RepositoryID = strings.TrimSpace(req.RepositoryID)
	req.Ref = strings.TrimSpace(req.Ref)
	req.Path = strings.TrimSpace(req.Path)
	if req.NamespaceID <= 0 {
		req.NamespaceID = 1
	}

	return req
}

func (p DefinitionPersister) definitionResolver(rec dal.SourceRepositoryRecord) (DefinitionResolver, error) {
	if p.NewDefinitionResolver != nil {
		return p.NewDefinitionResolver(rec)
	}

	if p.NewRepository != nil {
		repo, err := p.NewRepository(rec)
		if err != nil {
			return nil, err
		}

		return NewRepositoryDefinitionResolver(repo), nil
	}

	return NewDefinitionResolverFromRecord(rec)
}

func NewRepositoryFromRecord(rec dal.SourceRepositoryRecord) (Repository, error) {
	return NewGitCheckoutFromRecord(rec)
}
