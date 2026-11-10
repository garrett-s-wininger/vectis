package source

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"vectis/internal/dal"
)

func WriteHotfix(ctx context.Context, store string, definition []byte, manifest dal.SourceJobHotfix) (dal.SourceJobHotfix, error) {
	if err := ctx.Err(); err != nil {
		return manifest, err
	}

	if len(definition) > int(DefaultMaxFileBytes) {
		return manifest, ErrTooLarge
	}

	if !json.Valid(definition) {
		return manifest, ErrInvalidDefinition
	}

	if err := os.MkdirAll(filepath.Dir(store), 0700); err != nil {
		return manifest, err
	}

	repo, err := gogit.PlainInit(store, true)
	if errors.Is(err, gogit.ErrRepositoryAlreadyExists) {
		repo, err = gogit.PlainOpen(store)
	}

	if err != nil {
		return manifest, err
	}

	saveBlob := func(content []byte) (plumbing.Hash, error) {
		obj := repo.Storer.NewEncodedObject()
		obj.SetType(plumbing.BlobObject)
		writer, err := obj.Writer()
		if err != nil {
			return plumbing.ZeroHash, err
		}

		if _, err = writer.Write(content); err != nil {
			_ = writer.Close()
			return plumbing.ZeroHash, err
		}

		if err = writer.Close(); err != nil {
			return plumbing.ZeroHash, err
		}

		return repo.Storer.SetEncodedObject(obj)
	}

	metadata, err := json.Marshal(manifest)
	if err != nil {
		return manifest, err
	}

	definitionHash, err := saveBlob(definition)
	if err != nil {
		return manifest, err
	}

	manifestHash, err := saveBlob(metadata)
	if err != nil {
		return manifest, err
	}

	tree := &object.Tree{Entries: []object.TreeEntry{{Name: "definition.json", Mode: filemode.Regular, Hash: definitionHash}, {Name: "manifest.json", Mode: filemode.Regular, Hash: manifestHash}}}
	treeObject := repo.Storer.NewEncodedObject()
	if err = tree.Encode(treeObject); err != nil {
		return manifest, err
	}

	treeHash, err := repo.Storer.SetEncodedObject(treeObject)
	if err != nil {
		return manifest, err
	}

	signature := object.Signature{Name: "Vectis", Email: "vectis@example.invalid", When: time.Unix(manifest.CreatedAt, 0).UTC()}
	commit := &object.Commit{Author: signature, Committer: signature, Message: manifest.Reason, TreeHash: treeHash}
	commitObject := repo.Storer.NewEncodedObject()
	if err = commit.Encode(commitObject); err != nil {
		return manifest, err
	}

	commitHash, err := repo.Storer.SetEncodedObject(commitObject)
	if err != nil {
		return manifest, err
	}

	if err = repo.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName("refs/vectis/hotfixes/"+commitHash.String()), commitHash)); err != nil {
		return manifest, err
	}

	manifest.Commit = commitHash.String()
	return manifest, nil
}

func LoadHotfix(ctx context.Context, registration dal.SourceJobRecord, validation DefinitionRequest) (Definition, error) {
	if registration.Hotfix == nil {
		return Definition{}, ErrNotFound
	}

	if err := ctx.Err(); err != nil {
		return Definition{}, err
	}

	if _, err := normalizeCommit(registration.Hotfix.Commit); err != nil {
		return Definition{}, err
	}

	repo, err := gogit.PlainOpen(registration.HotfixStore)
	if err != nil {
		return Definition{}, fmt.Errorf("hotfix store unavailable: %w", err)
	}

	commit, err := repo.CommitObject(plumbing.NewHash(registration.Hotfix.Commit))
	if err != nil {
		return Definition{}, err
	}

	tree, err := commit.Tree()
	if err != nil {
		return Definition{}, err
	}

	file, err := tree.File("definition.json")
	if err != nil {
		return Definition{}, err
	}

	if file.Size > DefaultMaxFileBytes {
		return Definition{}, ErrTooLarge
	}

	content, err := file.Contents()
	if err != nil {
		return Definition{}, err
	}

	loaded, err := ParseDefinitionFile(File{Path: registration.Path, Revision: Revision{Commit: registration.Hotfix.Commit}, BlobSHA: file.Hash.String(), Content: []byte(content)}, "refs/vectis/hotfixes/"+registration.Hotfix.Commit, validation.Validation)
	if err != nil {
		return Definition{}, err
	}

	loaded.Source.Hotfix = registration.Hotfix
	return loaded, nil
}

func EffectiveSourceJob(ctx context.Context, repo dal.SourcesRepository, rec dal.SourceRepositoryRecord, jobID string, namespaceID int64, req DefinitionRequest) (Definition, bool, error) {
	registration, err := repo.GetSourceJob(ctx, rec.RepositoryID, jobID)
	if dal.IsNotFound(err) {
		return Definition{}, false, nil
	}

	if err != nil {
		return Definition{}, true, err
	}

	grants, grantErr := repo.RepositoryNamespaces(ctx, rec.RepositoryID)
	if grantErr != nil {
		return Definition{}, true, grantErr
	}

	granted := false
	for _, grant := range grants {
		if grant.ID == registration.NamespaceID {
			granted = true
			break
		}
	}

	if !granted {
		return Definition{}, true, fmt.Errorf("%w: owning namespace no longer has repository access", ErrConflict)
	}

	if namespaceID != 0 && registration.NamespaceID != namespaceID {
		return Definition{}, true, fmt.Errorf("%w: job belongs to %s", ErrConflict, registration.Namespace)
	}

	if req.Path != "" && strings.TrimSpace(req.Path) != registration.Path && req.JobID == "" {
		return Definition{}, true, fmt.Errorf("%w: path differs from the registered job", ErrConflict)
	}

	if registration.Hotfix != nil {
		loaded, err := LoadHotfix(ctx, registration, req)
		return loaded, true, err
	}

	return Definition{}, false, nil
}
