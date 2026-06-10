package source

import (
	"context"
	"fmt"
	"strings"

	"vectis/internal/dal"
)

type DefinitionAuthor interface {
	WriteDefinition(ctx context.Context, req WriteDefinitionRequest) (WrittenDefinition, error)
}

type WriteDefinitionRequest struct {
	Ref            string
	Branch         string
	Path           string
	DefinitionJSON string
	Message        string
	ExpectedHead   string
}

type WrittenDefinition struct {
	RequestedRef string
	Commit       string
	ParentCommit string
	Path         string
	BlobSHA      string
}

type LocalCommitDefinitionAuthor struct {
	Checkout   *GitCheckout
	DefaultRef string
}

func NewDefinitionAuthorFromRecord(rec dal.SourceRepositoryRecord) (DefinitionAuthor, error) {
	mode := strings.TrimSpace(rec.AuthoringMode)
	if mode == "" {
		mode = dal.SourceAuthoringModeReadOnly
	}

	switch mode {
	case dal.SourceAuthoringModeReadOnly:
		return nil, fmt.Errorf("%w: source repository is read-only", ErrAuthoringUnavailable)
	case dal.SourceAuthoringModeLocalCommit:
		if strings.TrimSpace(rec.CheckoutMode) != dal.SourceCheckoutModeManaged {
			return nil, fmt.Errorf("%w: local_commit requires managed checkout", ErrAuthoringUnavailable)
		}

		return LocalCommitDefinitionAuthor{
			Checkout:   NewManagedGitCheckout(rec.CheckoutPath),
			DefaultRef: rec.DefaultRef,
		}, nil
	case dal.SourceAuthoringModeExternalChangeRequest:
		return nil, fmt.Errorf("%w: external change request authoring is not configured", ErrAuthoringUnavailable)
	default:
		return nil, fmt.Errorf("%w: unsupported authoring_mode %q", ErrInvalidReference, rec.AuthoringMode)
	}
}

func (a LocalCommitDefinitionAuthor) WriteDefinition(ctx context.Context, req WriteDefinitionRequest) (WrittenDefinition, error) {
	if a.Checkout == nil {
		return WrittenDefinition{}, fmt.Errorf("%w: checkout is required", ErrInvalidReference)
	}

	targetRef := strings.TrimSpace(req.Branch)
	if targetRef == "" {
		targetRef = strings.TrimSpace(req.Ref)
	}

	if targetRef == "" {
		targetRef = strings.TrimSpace(a.DefaultRef)
	}

	if targetRef == "" {
		targetRef = "HEAD"
	}

	commit, err := a.Checkout.CommitFile(ctx, CommitFileOptions{
		Ref:          targetRef,
		Path:         req.Path,
		Content:      []byte(req.DefinitionJSON),
		Message:      req.Message,
		ExpectedHead: req.ExpectedHead,
	})

	if err != nil {
		return WrittenDefinition{}, err
	}

	return WrittenDefinition{
		RequestedRef: commit.RequestedRef,
		Commit:       commit.Commit,
		ParentCommit: commit.ParentCommit,
		Path:         commit.Path,
		BlobSHA:      commit.BlobSHA,
	}, nil
}
