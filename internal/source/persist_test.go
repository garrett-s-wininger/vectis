package source

import (
	"context"
	"encoding/json"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestDefinitionPersisterCreateJobFromRegisteredRepository(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	repoPath := initGitRepo(t)
	writeAndCommit(t, repoPath, ".vectis/jobs/build.json", `{
		"root": {"id": "root", "uses": "builtins/shell", "with": {"command": "true"}}
	}`+"\n", "build definition")
	commit := gitOutput(t, repoPath, "rev-parse", "HEAD")
	blob := gitOutput(t, repoPath, "rev-parse", "HEAD:.vectis/jobs/build.json")

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: repoPath,
		DefaultRef:   "HEAD",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	persister := DefinitionPersister{
		Jobs:    repos.Jobs().(dal.SourceBackedJobsRepository),
		Sources: repos.Sources(),
	}

	persisted, err := persister.CreateJob(ctx, PersistDefinitionRequest{
		JobID:        "build",
		NamespaceID:  1,
		RepositoryID: "vectis-local",
		Path:         ".vectis/jobs/build.json",
	})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	if persisted.Version != 1 || persisted.Definition.Source.Commit != commit {
		t.Fatalf("persisted definition mismatch: %+v", persisted)
	}

	definitionJSON, version, err := repos.Jobs().GetDefinition(ctx, "build")
	if err != nil {
		t.Fatalf("GetDefinition: %v", err)
	}

	if version != 1 {
		t.Fatalf("version: got %d, want 1", version)
	}

	var job api.Job
	if err := json.Unmarshal([]byte(definitionJSON), &job); err != nil {
		t.Fatalf("definition json: %v", err)
	}

	if job.GetRoot().GetWith()["command"] != "true" {
		t.Fatalf("stored job definition mismatch: %+v", job)
	}

	sourceRec, err := repos.Sources().GetDefinitionSource(ctx, "build", 1)
	if err != nil {
		t.Fatalf("GetDefinitionSource: %v", err)
	}

	if sourceRec.RepositoryID != "vectis-local" ||
		sourceRec.RequestedRef != "HEAD" ||
		sourceRec.ResolvedCommit != commit ||
		sourceRec.DefinitionPath != ".vectis/jobs/build.json" ||
		sourceRec.BlobSHA != blob {
		t.Fatalf("source provenance mismatch: %+v", sourceRec)
	}
}

func TestDefinitionPersisterUpdateJobFromRegisteredRepository(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	repoPath := initGitRepo(t)
	writeAndCommit(t, repoPath, ".vectis/jobs/build.json", `{
		"root": {"id": "root", "uses": "builtins/shell", "with": {"command": "one"}}
	}`+"\n", "first definition")

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: repoPath,
		DefaultRef:   "HEAD",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	persister := DefinitionPersister{
		Jobs:    repos.Jobs().(dal.SourceBackedJobsRepository),
		Sources: repos.Sources(),
	}

	if _, err := persister.CreateJob(ctx, PersistDefinitionRequest{
		JobID:        "build",
		NamespaceID:  1,
		RepositoryID: "vectis-local",
		Path:         ".vectis/jobs/build.json",
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	writeAndCommit(t, repoPath, ".vectis/jobs/build.json", `{
		"root": {"id": "root", "uses": "builtins/shell", "with": {"command": "two"}}
	}`+"\n", "second definition")
	commit := gitOutput(t, repoPath, "rev-parse", "HEAD")

	persisted, err := persister.UpdateJob(ctx, PersistDefinitionRequest{
		JobID:        "build",
		RepositoryID: "vectis-local",
		Path:         ".vectis/jobs/build.json",
	})
	if err != nil {
		t.Fatalf("UpdateJob: %v", err)
	}

	if persisted.Version != 2 || persisted.Definition.Source.Commit != commit {
		t.Fatalf("persisted definition mismatch: %+v", persisted)
	}

	sourceRec, err := repos.Sources().GetDefinitionSource(ctx, "build", 2)
	if err != nil {
		t.Fatalf("GetDefinitionSource: %v", err)
	}

	if sourceRec.ResolvedCommit != commit {
		t.Fatalf("resolved commit: got %q, want %q", sourceRec.ResolvedCommit, commit)
	}
}

func TestDefinitionPersisterRejectsDisabledRepository(t *testing.T) {
	ctx := context.Background()
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	repoPath := initGitRepo(t)
	writeAndCommit(t, repoPath, ".vectis/jobs/build.json", `{
		"root": {"id": "root", "uses": "builtins/shell", "with": {"command": "true"}}
	}`+"\n", "build definition")

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: repoPath,
		DefaultRef:   "HEAD",
		Enabled:      false,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	persister := DefinitionPersister{
		Jobs:    repos.Jobs().(dal.SourceBackedJobsRepository),
		Sources: repos.Sources(),
	}

	_, err := persister.CreateJob(ctx, PersistDefinitionRequest{
		JobID:        "build",
		NamespaceID:  1,
		RepositoryID: "vectis-local",
		Path:         ".vectis/jobs/build.json",
	})
	if err == nil {
		t.Fatal("expected disabled repository error")
	}
}
