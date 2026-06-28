package api

import (
	"context"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestGetRunJobNamespacePathUsesSourceRepositoryNamespace(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("Create namespace: %v", err)
	}

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "source-repo",
		NamespaceID:  ns.ID,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: t.TempDir(),
		DefaultRef:   "main",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	runID, _, _, err := repos.CreateSourceDefinitionAndRunInCellWithAudit(ctx, "build", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`, dal.JobDefinitionSourceRecord{
		RepositoryID:   "source-repo",
		RequestedRef:   "main",
		ResolvedCommit: "abc123",
		DefinitionPath: ".vectis/jobs/build.json",
		BlobSHA:        "blob123",
	}, "", dal.RunAuditMetadata{})

	if err != nil {
		t.Fatalf("CreateSourceDefinitionAndRunInCellWithAudit: %v", err)
	}

	got, err := server.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		t.Fatalf("getRunJobNamespacePath: %v", err)
	}

	if got != "/team-a" {
		t.Fatalf("source run namespace: got %q, want /team-a", got)
	}
}

func TestGetRunJobNamespacePathUsesPersistedEphemeralNamespace(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	ctx := context.Background()

	runID, _, err := repos.CreateDefinitionAndRunInCellWithAudit(ctx, "ephemeral-job", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`, nil, "", dal.RunAuditMetadata{
		NamespacePath: dal.EphemeralNamespacePath,
	})

	if err != nil {
		t.Fatalf("CreateDefinitionAndRunInCellWithAudit: %v", err)
	}

	got, err := server.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		t.Fatalf("getRunJobNamespacePath: %v", err)
	}

	if got != dal.EphemeralNamespacePath {
		t.Fatalf("ephemeral run namespace: got %q, want %s", got, dal.EphemeralNamespacePath)
	}
}

func TestGetRunJobNamespacePathKeepsLegacyRootFallback(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	ctx := context.Background()

	runID, _, err := repos.CreateDefinitionAndRunInCellWithAudit(ctx, "legacy-ephemeral-job", `{"root":{"id":"root","uses":"builtins/shell","with":{"command":"true"}}}`, nil, "", dal.RunAuditMetadata{})
	if err != nil {
		t.Fatalf("CreateDefinitionAndRunInCellWithAudit: %v", err)
	}

	got, err := server.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		t.Fatalf("getRunJobNamespacePath: %v", err)
	}

	if got != dal.RootNamespacePath {
		t.Fatalf("legacy run namespace: got %q, want %s", got, dal.RootNamespacePath)
	}
}
