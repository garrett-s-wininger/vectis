package source

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	api "vectis/api/gen/go"
	jobvalidation "vectis/internal/job/validation"
)

func TestLoadDefinitionResolvesReadsAndCanonicalizesJob(t *testing.T) {
	repo := &fakeRepository{
		revision: Revision{Commit: "0123456789abcdef0123456789abcdef01234567"},
		file: File{
			Path:    ".vectis/jobs/build.json",
			BlobSHA: "abcdef0123456789abcdef0123456789abcdef01",
			Content: []byte(`{
				"root": {
					"id": "root",
					"uses": "builtins/shell",
					"with": {"command": "go test ./..."}
				}
			}`),
		},
	}

	loaded, err := LoadDefinition(context.Background(), repo, DefinitionRequest{
		Ref:  "main",
		Path: ".vectis/jobs/build.json",
	})

	if err != nil {
		t.Fatalf("LoadDefinition: %v", err)
	}

	if repo.resolveRef != "main" {
		t.Fatalf("ResolveRevision ref: got %q, want main", repo.resolveRef)
	}

	if repo.readRevision.Commit != repo.revision.Commit {
		t.Fatalf("ReadFile revision: got %q, want %q", repo.readRevision.Commit, repo.revision.Commit)
	}

	if repo.readPath != ".vectis/jobs/build.json" {
		t.Fatalf("ReadFile path: got %q", repo.readPath)
	}

	if loaded.Job.GetId() != "" {
		t.Fatalf("source loader should not assign job id, got %q", loaded.Job.GetId())
	}

	if loaded.Source.RequestedRef != "main" || loaded.Source.Commit != repo.revision.Commit || loaded.Source.BlobSHA != repo.file.BlobSHA {
		t.Fatalf("source provenance mismatch: %+v", loaded.Source)
	}

	var roundTrip api.Job
	if err := json.Unmarshal([]byte(loaded.DefinitionJSON), &roundTrip); err != nil {
		t.Fatalf("canonical definition json did not parse: %v\n%s", err, loaded.DefinitionJSON)
	}

	if roundTrip.GetId() != "" || roundTrip.GetRoot().GetWith()["command"] != "go test ./..." {
		t.Fatalf("canonical definition mismatch: id=%q command=%q", roundTrip.GetId(), roundTrip.GetRoot().GetWith()["command"])
	}
}

func TestLoadDefinitionCanValidateCallerPolicy(t *testing.T) {
	repo := &fakeRepository{
		revision: Revision{Commit: "0123456789abcdef0123456789abcdef01234567"},
		file: File{
			Path:    ".vectis/jobs/build.json",
			BlobSHA: "abcdef0123456789abcdef0123456789abcdef01",
			Content: []byte(`{
				"root": {"id": "root", "uses": "builtins/shell", "with": {"command": "true"}}
			}`),
		},
	}

	_, err := LoadDefinition(context.Background(), repo, DefinitionRequest{
		Ref:  "main",
		Path: ".vectis/jobs/build.json",
		Validation: jobvalidation.Options{
			RequireJobID: true,
		},
	})

	if !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("expected ErrInvalidDefinition, got %v", err)
	}

	var validationErr *jobvalidation.Error
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestLoadDefinitionReportsValidationErrors(t *testing.T) {
	repo := &fakeRepository{
		revision: Revision{Commit: "0123456789abcdef0123456789abcdef01234567"},
		file: File{
			Path:    ".vectis/jobs/build.json",
			BlobSHA: "abcdef0123456789abcdef0123456789abcdef01",
			Content: []byte(`{
				"id": "build",
				"root": {"id": "root", "uses": "builtins/shell"}
			}`),
		},
	}

	_, err := LoadDefinition(context.Background(), repo, DefinitionRequest{
		Ref:  "main",
		Path: ".vectis/jobs/build.json",
		Validation: jobvalidation.Options{
			RequireJobID: true,
		},
	})

	if !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("expected ErrInvalidDefinition, got %v", err)
	}

	var validationErr *jobvalidation.Error
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestLoadDefinitionRejectsInvalidJSON(t *testing.T) {
	repo := &fakeRepository{
		revision: Revision{Commit: "0123456789abcdef0123456789abcdef01234567"},
		file: File{
			Path:    ".vectis/jobs/build.json",
			BlobSHA: "abcdef0123456789abcdef0123456789abcdef01",
			Content: []byte(`not-json`),
		},
	}

	_, err := LoadDefinition(context.Background(), repo, DefinitionRequest{
		Ref:  "main",
		Path: ".vectis/jobs/build.json",
	})

	if !errors.Is(err, ErrInvalidDefinition) {
		t.Fatalf("expected ErrInvalidDefinition, got %v", err)
	}
}

type fakeRepository struct {
	revision Revision
	file     File
	err      error

	resolveRef   string
	readRevision Revision
	readPath     string
}

func (f *fakeRepository) ResolveRevision(_ context.Context, ref string) (Revision, error) {
	f.resolveRef = ref
	if f.err != nil {
		return Revision{}, f.err
	}

	return f.revision, nil
}

func (f *fakeRepository) ReadFile(_ context.Context, revision Revision, filePath string) (File, error) {
	f.readRevision = revision
	f.readPath = filePath
	if f.err != nil {
		return File{}, f.err
	}

	file := f.file
	file.Revision = revision
	return file, nil
}
