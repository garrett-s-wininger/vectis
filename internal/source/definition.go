package source

import (
	"context"
	"encoding/json"
	"fmt"

	api "vectis/api/gen/go"
	jobvalidation "vectis/internal/job/validation"
)

type DefinitionRequest struct {
	Ref  string
	Path string

	Validation jobvalidation.Options
}

type Definition struct {
	Job            *api.Job
	DefinitionJSON string
	Source         DefinitionSource
}

type DefinitionSource struct {
	RequestedRef string
	Commit       string
	Path         string
	BlobSHA      string
}

func LoadDefinition(ctx context.Context, repo Repository, req DefinitionRequest) (Definition, error) {
	if repo == nil {
		return Definition{}, fmt.Errorf("%w: repository is required", ErrInvalidReference)
	}

	ref, err := normalizeRef(req.Ref)
	if err != nil {
		return Definition{}, err
	}

	filePath, err := normalizeTreePath(req.Path)
	if err != nil {
		return Definition{}, err
	}

	revision, err := repo.ResolveRevision(ctx, ref)
	if err != nil {
		return Definition{}, err
	}

	file, err := repo.ReadFile(ctx, revision, filePath)
	if err != nil {
		return Definition{}, err
	}

	var job api.Job
	if err := json.Unmarshal(file.Content, &job); err != nil {
		return Definition{}, fmt.Errorf("%w: parse %s at %s: %w", ErrInvalidDefinition, filePath, revision.Commit, err)
	}

	if err := jobvalidation.ValidateJob(&job, req.Validation); err != nil {
		return Definition{}, fmt.Errorf("%w: validate %s at %s: %w", ErrInvalidDefinition, filePath, revision.Commit, err)
	}

	definitionJSON, err := json.Marshal(&job)
	if err != nil {
		return Definition{}, fmt.Errorf("%w: marshal %s at %s: %w", ErrInvalidDefinition, filePath, revision.Commit, err)
	}

	return Definition{
		Job:            &job,
		DefinitionJSON: string(definitionJSON),
		Source: DefinitionSource{
			RequestedRef: ref,
			Commit:       file.Revision.Commit,
			Path:         file.Path,
			BlobSHA:      file.BlobSHA,
		},
	}, nil
}
