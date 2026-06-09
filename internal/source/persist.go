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
	Jobs          dal.SourceBackedJobsRepository
	Sources       dal.SourcesRepository
	NewRepository RepositoryFactory
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

	if req.Ref == "" {
		req.Ref = strings.TrimSpace(repoRec.DefaultRef)
	}

	factory := p.NewRepository
	if factory == nil {
		factory = NewRepositoryFromRecord
	}

	repo, err := factory(repoRec)
	if err != nil {
		return Definition{}, dal.SourceRepositoryRecord{}, dal.JobDefinitionSourceRecord{}, err
	}

	loaded, err := LoadDefinition(ctx, repo, DefinitionRequest{
		Ref:        req.Ref,
		Path:       req.Path,
		Validation: req.Validation,
	})
	if err != nil {
		return Definition{}, dal.SourceRepositoryRecord{}, dal.JobDefinitionSourceRecord{}, err
	}

	sourceRec := dal.JobDefinitionSourceRecord{
		JobID:          req.JobID,
		RepositoryID:   repoRec.RepositoryID,
		RequestedRef:   loaded.Source.RequestedRef,
		ResolvedCommit: loaded.Source.Commit,
		DefinitionPath: loaded.Source.Path,
		BlobSHA:        loaded.Source.BlobSHA,
	}

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

func NewRepositoryFromRecord(rec dal.SourceRepositoryRecord) (Repository, error) {
	switch strings.TrimSpace(rec.SourceKind) {
	case dal.SourceKindLocalCheckout:
		checkoutPath := strings.TrimSpace(rec.CheckoutPath)
		if checkoutPath == "" {
			return nil, fmt.Errorf("%w: checkout_path is required for %s", ErrInvalidReference, dal.SourceKindLocalCheckout)
		}

		return NewGitCheckout(checkoutPath), nil
	default:
		return nil, fmt.Errorf("%w: unsupported source_kind %q", ErrInvalidReference, rec.SourceKind)
	}
}
