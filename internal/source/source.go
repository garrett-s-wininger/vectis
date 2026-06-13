package source

import (
	"context"
	"errors"
	"strings"

	"vectis/internal/source/refspec"
)

var (
	ErrInvalidReference     = errors.New("invalid source reference")
	ErrInvalidDefinition    = errors.New("invalid source definition")
	ErrAuthoringUnavailable = errors.New("source authoring unavailable")
	ErrConflict             = errors.New("source conflict")
	ErrNotFound             = errors.New("source not found")
	ErrTooLarge             = errors.New("source file too large")
)

const DefaultMaxFileBytes int64 = 1024 * 1024

const (
	DefaultBranchListLimit = 50
	DefaultTreeListLimit   = 100
	DefaultDefinitionPath  = refspec.DefaultDefinitionPath
)

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

type ListTreeOptions struct {
	Ref       string
	Path      string
	Recursive bool
	Limit     int
}

type TreeListing struct {
	RequestedRef string
	Revision     Revision
	Path         string
	Recursive    bool
	Entries      []TreeEntry
}

type TreeEntry struct {
	Path      string
	Name      string
	Type      string
	Mode      string
	ObjectSHA string
	SizeBytes int64
}

type ListDefinitionFilesOptions struct {
	Ref   string
	Path  string
	Limit int
}

type DefinitionFileListing struct {
	RequestedRef string
	Revision     Revision
	Path         string
	Files        []DefinitionFile
}

type DefinitionFile struct {
	Path      string
	Name      string
	BlobSHA   string
	SizeBytes int64
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

type CommitFileOptions struct {
	Ref          string
	Path         string
	Content      []byte
	Message      string
	ExpectedHead string
}

type FileCommit struct {
	RequestedRef string
	Commit       string
	ParentCommit string
	Path         string
	BlobSHA      string
}
