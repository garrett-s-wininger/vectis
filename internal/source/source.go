package source

import (
	"context"
	"errors"
	"strings"
)

var (
	ErrInvalidReference  = errors.New("invalid source reference")
	ErrInvalidDefinition = errors.New("invalid source definition")
	ErrNotFound          = errors.New("source not found")
	ErrTooLarge          = errors.New("source file too large")
)

const DefaultMaxFileBytes int64 = 1024 * 1024

const DefaultBranchListLimit = 50

// Repository reads immutable source content from a repository-like backing store.
type Repository interface {
	ResolveRevision(ctx context.Context, ref string) (Revision, error)
	ReadFile(ctx context.Context, revision Revision, filePath string) (File, error)
}

type ListBranchesOptions struct {
	Prefix string
	Limit  int
}

type BranchRef struct {
	Name   string
	Ref    string
	Commit string
	Remote string
}

type Revision struct {
	Commit string
}

func (r Revision) String() string {
	return r.Commit
}

func (r Revision) Valid() bool {
	return strings.TrimSpace(r.Commit) != ""
}

type File struct {
	Path     string
	Revision Revision
	BlobSHA  string
	Content  []byte
}
