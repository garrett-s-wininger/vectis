package dal_test

import (
	"context"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestSourcesRepository_CreateGetAndListRepository(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	sources := repos.Sources()
	ctx := context.Background()

	created, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		DefaultRef:   "main",
		Enabled:      true,
	})

	if err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	if created.ID == 0 || created.GlobalID == "" {
		t.Fatalf("expected durable IDs, got %+v", created)
	}

	if created.RepositoryID != "vectis-local" || created.CheckoutPath != "/work/vectis" || !created.Enabled {
		t.Fatalf("created repository mismatch: %+v", created)
	}

	got, err := sources.GetRepository(ctx, "vectis-local")
	if err != nil {
		t.Fatalf("GetRepository: %v", err)
	}

	if got.ID != created.ID || got.DefaultRef != "main" {
		t.Fatalf("get repository mismatch: got %+v want %+v", got, created)
	}

	listed, err := sources.ListRepositories(ctx, 1)
	if err != nil {
		t.Fatalf("ListRepositories: %v", err)
	}

	if len(listed) != 1 || listed[0].RepositoryID != "vectis-local" {
		t.Fatalf("expected one listed repository, got %+v", listed)
	}
}

func TestSourcesRepository_CreateRepositoryConflicts(t *testing.T) {
	db := dbtest.NewTestDB(t)
	sources := dal.NewSQLRepositories(db).Sources()
	ctx := context.Background()

	rec := dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}

	if _, err := sources.CreateRepository(ctx, rec); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	if _, err := sources.CreateRepository(ctx, rec); !dal.IsConflict(err) {
		t.Fatalf("expected conflict on duplicate repository, got %v", err)
	}

	duplicatePath := rec
	duplicatePath.RepositoryID = "vectis-alias"
	if _, err := sources.CreateRepository(ctx, duplicatePath); !dal.IsConflict(err) {
		t.Fatalf("expected conflict on duplicate checkout path, got %v", err)
	}

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "missing-path",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		Enabled:      true,
	}); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for missing checkout path, got %v", err)
	}

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "unknown-kind",
		NamespaceID:  1,
		SourceKind:   "remote_git",
		CanonicalURL: "https://example.invalid/repo.git",
		Enabled:      true,
	}); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for unsupported source kind, got %v", err)
	}
}

func TestSourcesRepository_UpdateRepository(t *testing.T) {
	db := dbtest.NewTestDB(t)
	sources := dal.NewSQLRepositories(db).Sources()
	ctx := context.Background()

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		DefaultRef:   "main",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	updated, err := sources.UpdateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID:  "vectis-local",
		SourceKind:    dal.SourceKindLocalCheckout,
		CheckoutPath:  "/work/vectis-next",
		CanonicalURL:  "https://example.invalid/vectis.git",
		DefaultRef:    "release",
		CredentialRef: "secret://git/vectis",
		Enabled:       false,
	})

	if err != nil {
		t.Fatalf("UpdateRepository: %v", err)
	}

	if updated.CheckoutPath != "/work/vectis-next" ||
		updated.CanonicalURL != "https://example.invalid/vectis.git" ||
		updated.DefaultRef != "release" ||
		updated.CredentialRef != "secret://git/vectis" ||
		updated.Enabled {
		t.Fatalf("updated repository mismatch: %+v", updated)
	}

	got, err := sources.GetRepository(ctx, "vectis-local")
	if err != nil {
		t.Fatalf("GetRepository: %v", err)
	}

	if got.CheckoutPath != updated.CheckoutPath || got.DefaultRef != updated.DefaultRef || got.Enabled != updated.Enabled {
		t.Fatalf("persisted update mismatch: got %+v want %+v", got, updated)
	}

	if _, err := sources.UpdateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "missing",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/missing",
		Enabled:      true,
	}); !dal.IsNotFound(err) {
		t.Fatalf("expected not found for missing repository, got %v", err)
	}
}

func TestSourcesRepository_UpdateRepositoryConflicts(t *testing.T) {
	db := dbtest.NewTestDB(t)
	sources := dal.NewSQLRepositories(db).Sources()
	ctx := context.Background()

	for _, rec := range []dal.SourceRepositoryRecord{
		{
			RepositoryID: "vectis-local",
			NamespaceID:  1,
			SourceKind:   dal.SourceKindLocalCheckout,
			CheckoutPath: "/work/vectis",
			Enabled:      true,
		},
		{
			RepositoryID: "other",
			NamespaceID:  1,
			SourceKind:   dal.SourceKindLocalCheckout,
			CheckoutPath: "/work/other",
			Enabled:      true,
		},
	} {
		if _, err := sources.CreateRepository(ctx, rec); err != nil {
			t.Fatalf("CreateRepository(%s): %v", rec.RepositoryID, err)
		}
	}

	if _, err := sources.UpdateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "other",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}); !dal.IsConflict(err) {
		t.Fatalf("expected duplicate checkout path conflict, got %v", err)
	}

	if _, err := sources.UpdateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "other",
		SourceKind:   dal.SourceKindLocalCheckout,
		Enabled:      true,
	}); !dal.IsConflict(err) {
		t.Fatalf("expected missing checkout path conflict, got %v", err)
	}
}

func TestSourcesRepository_RecordAndGetDefinitionSource(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	sources := repos.Sources()
	ctx := context.Background()

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	if err := repos.Jobs().Create(ctx, "build", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	rec := dal.JobDefinitionSourceRecord{
		JobID:          "build",
		Version:        1,
		RepositoryID:   "vectis-local",
		RequestedRef:   "main",
		ResolvedCommit: "0123456789abcdef0123456789abcdef01234567",
		DefinitionPath: ".vectis/jobs/build.json",
		BlobSHA:        "abcdef0123456789abcdef0123456789abcdef01",
	}

	if err := sources.RecordDefinitionSource(ctx, rec); err != nil {
		t.Fatalf("RecordDefinitionSource: %v", err)
	}

	got, err := sources.GetDefinitionSource(ctx, "build", 1)
	if err != nil {
		t.Fatalf("GetDefinitionSource: %v", err)
	}

	if got != rec {
		t.Fatalf("definition source mismatch: got %+v want %+v", got, rec)
	}
}

func TestSourcesRepository_RecordDefinitionSourceRequiresExistingRows(t *testing.T) {
	db := dbtest.NewTestDB(t)
	sources := dal.NewSQLRepositories(db).Sources()
	ctx := context.Background()

	err := sources.RecordDefinitionSource(ctx, dal.JobDefinitionSourceRecord{
		JobID:          "missing",
		Version:        1,
		RepositoryID:   "missing-repo",
		RequestedRef:   "main",
		ResolvedCommit: "0123456789abcdef0123456789abcdef01234567",
		DefinitionPath: ".vectis/jobs/build.json",
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected conflict for missing foreign keys, got %v", err)
	}
}

func TestJobsRepository_CreateWithSourceRecordsProvenance(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	sources := repos.Sources()
	sourceJobs := repos.Jobs().(dal.SourceBackedJobsRepository)
	ctx := context.Background()

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	source := dal.JobDefinitionSourceRecord{
		RepositoryID:   "vectis-local",
		RequestedRef:   "main",
		ResolvedCommit: "0123456789abcdef0123456789abcdef01234567",
		DefinitionPath: ".vectis/jobs/build.json",
		BlobSHA:        "abcdef0123456789abcdef0123456789abcdef01",
	}

	version, err := sourceJobs.CreateWithSource(ctx, "build", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`, 1, source)
	if err != nil {
		t.Fatalf("CreateWithSource: %v", err)
	}

	if version != 1 {
		t.Fatalf("version: got %d, want 1", version)
	}

	got, err := sources.GetDefinitionSource(ctx, "build", 1)
	if err != nil {
		t.Fatalf("GetDefinitionSource: %v", err)
	}

	source.JobID = "build"
	source.Version = 1
	if got != source {
		t.Fatalf("definition source mismatch: got %+v want %+v", got, source)
	}
}

func TestJobsRepository_UpdateDefinitionWithSourceRecordsNextVersion(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	sources := repos.Sources()
	sourceJobs := repos.Jobs().(dal.SourceBackedJobsRepository)
	ctx := context.Background()

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/vectis",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	if err := repos.Jobs().Create(ctx, "build", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	source := dal.JobDefinitionSourceRecord{
		RepositoryID:   "vectis-local",
		RequestedRef:   "main",
		ResolvedCommit: "fedcba9876543210fedcba9876543210fedcba98",
		DefinitionPath: ".vectis/jobs/build.json",
		BlobSHA:        "abcdef0123456789abcdef0123456789abcdef01",
	}

	version, err := sourceJobs.UpdateDefinitionWithSource(ctx, "build", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"echo updated"}}}`, source)
	if err != nil {
		t.Fatalf("UpdateDefinitionWithSource: %v", err)
	}

	if version != 2 {
		t.Fatalf("version: got %d, want 2", version)
	}

	got, err := sources.GetDefinitionSource(ctx, "build", 2)
	if err != nil {
		t.Fatalf("GetDefinitionSource: %v", err)
	}

	source.JobID = "build"
	source.Version = 2
	if got != source {
		t.Fatalf("definition source mismatch: got %+v want %+v", got, source)
	}
}

func TestJobsRepository_CreateWithSourceRollsBackOnMissingRepository(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	sourceJobs := repos.Jobs().(dal.SourceBackedJobsRepository)
	ctx := context.Background()

	_, err := sourceJobs.CreateWithSource(ctx, "build", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`, 1, dal.JobDefinitionSourceRecord{
		RepositoryID:   "missing",
		RequestedRef:   "main",
		ResolvedCommit: "0123456789abcdef0123456789abcdef01234567",
		DefinitionPath: ".vectis/jobs/build.json",
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected conflict, got %v", err)
	}

	if _, _, err := repos.Jobs().GetDefinition(ctx, "build"); !dal.IsNotFound(err) {
		t.Fatalf("expected job creation to roll back, got %v", err)
	}
}
