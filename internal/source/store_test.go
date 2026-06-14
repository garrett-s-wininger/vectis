package source

import (
	"context"
	"testing"

	"vectis/internal/dal"
)

func TestGitDefinitionStoreFromRecordResolvesAndListsDefinitions(t *testing.T) {
	repoPath := initGitRepo(t)

	writeAndCommit(t, repoPath, ".vectis/jobs/build.json", `{
		"root": {"id": "root", "uses": "builtins/shell", "with": {"command": "go test ./..."}}
	}`+"\n", "build definition")

	commit := gitOutput(t, repoPath, "rev-parse", "HEAD")
	blob := gitOutput(t, repoPath, "rev-parse", "HEAD:.vectis/jobs/build.json")

	store, err := NewDefinitionStoreFromRecord(dal.SourceRepositoryRecord{
		RepositoryID: "vectis-local",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: repoPath,
	})

	if err != nil {
		t.Fatalf("NewDefinitionStoreFromRecord: %v", err)
	}

	listing, err := store.ListDefinitionFiles(context.Background(), ListDefinitionFilesOptions{
		Ref:   "HEAD",
		Path:  ".vectis/jobs",
		Limit: 10,
	})

	if err != nil {
		t.Fatalf("ListDefinitionFiles: %v", err)
	}

	if listing.Revision.Commit != commit ||
		len(listing.Files) != 1 ||
		listing.Files[0].Path != ".vectis/jobs/build.json" ||
		listing.Files[0].BlobSHA != blob {
		t.Fatalf("definition listing mismatch: %+v", listing)
	}

	file, err := store.ReadDefinitionFile(context.Background(), DefinitionFileRequest{
		Ref:  commit,
		Path: ".vectis/jobs/build.json",
	})

	if err != nil {
		t.Fatalf("ReadDefinitionFile: %v", err)
	}

	if file.Revision.Commit != commit || file.BlobSHA != blob || len(file.Content) == 0 {
		t.Fatalf("raw definition file mismatch: %+v", file)
	}

	loaded, err := store.ResolveDefinition(context.Background(), DefinitionRequest{
		Ref:  "HEAD",
		Path: ".vectis/jobs/build.json",
	})

	if err != nil {
		t.Fatalf("ResolveDefinition: %v", err)
	}

	if loaded.Source.Commit != commit ||
		loaded.Source.BlobSHA != blob ||
		loaded.Job.GetRoot().GetWith()["command"] != "go test ./..." {
		t.Fatalf("loaded definition mismatch: %+v", loaded)
	}
}
