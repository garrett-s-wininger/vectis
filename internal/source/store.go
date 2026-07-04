package source

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/dal"
)

type DefinitionResolver interface {
	ResolveDefinition(ctx context.Context, req DefinitionRequest) (Definition, error)
}

type DefinitionStore interface {
	DefinitionResolver
	ReadDefinitionFile(ctx context.Context, req DefinitionFileRequest) (File, error)
	ListDefinitionFiles(ctx context.Context, opts ListDefinitionFilesOptions) (DefinitionFileListing, error)
}

type DefinitionResolverFactory func(dal.SourceRepositoryRecord) (DefinitionResolver, error)

type RepositoryDefinitionResolver struct {
	Repository Repository
}

func NewRepositoryDefinitionResolver(repo Repository) RepositoryDefinitionResolver {
	return RepositoryDefinitionResolver{Repository: repo}
}

func (r RepositoryDefinitionResolver) ResolveDefinition(ctx context.Context, req DefinitionRequest) (Definition, error) {
	return LoadDefinition(ctx, r.Repository, req)
}

func (r RepositoryDefinitionResolver) ReadDefinitionFile(ctx context.Context, req DefinitionFileRequest) (File, error) {
	return ReadDefinitionFile(ctx, r.Repository, req)
}

type GitDefinitionStore struct {
	Checkout *GitCheckout
}

func NewGitDefinitionStore(checkout *GitCheckout) GitDefinitionStore {
	return GitDefinitionStore{Checkout: checkout}
}

func (s GitDefinitionStore) ResolveDefinition(ctx context.Context, req DefinitionRequest) (Definition, error) {
	file, err := s.ReadDefinitionFile(ctx, DefinitionFileRequest{
		Ref:  req.Ref,
		Path: req.Path,
	})

	if err != nil {
		return Definition{}, err
	}

	return ParseDefinitionFile(file, req.Ref, req.Validation)
}

func (s GitDefinitionStore) ReadDefinitionFile(ctx context.Context, req DefinitionFileRequest) (File, error) {
	if s.Checkout == nil {
		return File{}, fmt.Errorf("%w: checkout is required", ErrInvalidReference)
	}

	return s.Checkout.readDefinitionFile(ctx, req)
}

func (s GitDefinitionStore) ListDefinitionFiles(ctx context.Context, opts ListDefinitionFilesOptions) (DefinitionFileListing, error) {
	if s.Checkout == nil {
		return DefinitionFileListing{}, fmt.Errorf("%w: checkout is required", ErrInvalidReference)
	}

	return s.Checkout.ListDefinitionFiles(ctx, opts)
}

func NewDefinitionResolverFromRecord(rec dal.SourceRepositoryRecord) (DefinitionResolver, error) {
	return NewDefinitionStoreFromRecord(rec)
}

func NewDefinitionStoreFromRecord(rec dal.SourceRepositoryRecord) (DefinitionStore, error) {
	checkout, err := NewGitCheckoutFromRecord(rec)
	if err != nil {
		return nil, err
	}

	return NewGitDefinitionStore(checkout), nil
}

func NewGitCheckoutFromRecord(rec dal.SourceRepositoryRecord) (*GitCheckout, error) {
	switch strings.TrimSpace(rec.SourceKind) {
	case dal.SourceKindLocalCheckout:
		checkoutPath := strings.TrimSpace(rec.CheckoutPath)
		if checkoutPath == "" {
			return nil, fmt.Errorf("%w: checkout_path is required for %s", ErrInvalidReference, dal.SourceKindLocalCheckout)
		}

		if strings.TrimSpace(rec.CheckoutMode) == dal.SourceCheckoutModeManaged {
			return NewManagedGitCheckout(checkoutPath), nil
		}

		return NewGitCheckout(checkoutPath), nil
	default:
		return nil, fmt.Errorf("%w: unsupported source_kind %q", ErrInvalidReference, rec.SourceKind)
	}
}

func ReadDefinitionFile(ctx context.Context, repo Repository, req DefinitionFileRequest) (File, error) {
	if repo == nil {
		return File{}, fmt.Errorf("%w: repository is required", ErrInvalidReference)
	}

	filePath, err := normalizeTreePath(req.Path)
	if err != nil {
		return File{}, err
	}

	revision := req.Revision
	if revision.Valid() {
		commit, err := normalizeCommit(revision.Commit)
		if err != nil {
			return File{}, err
		}

		revision = Revision{Commit: commit}
	} else {
		ref, err := normalizeRef(req.Ref)
		if err != nil {
			return File{}, err
		}

		revision, err = repo.ResolveRevision(ctx, ref)
		if err != nil {
			return File{}, err
		}
	}

	return repo.ReadFile(ctx, revision, filePath)
}
