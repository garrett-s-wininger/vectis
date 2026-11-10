package source

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestConventionalJobDiscoveryAndLegacyCompatibility(t *testing.T) {
	t.Parallel()
	repo := initGitRepo(t)
	definition := `{"root":{"id":"root","uses":"builtins/script","with":{"script":"echo example"}}}`
	for _, file := range []string{PrimaryDefinitionPath, ".vectis/build.json", ".vectis/jobs/legacy.json", ".vectis/jobs/team/deploy.json", ".vectis/scripts/settings.json", "package.json"} {
		writeAndCommit(t, repo, file, definition, "add "+file)
	}

	checkout := NewGitCheckout(repo)
	ctx := context.Background()
	expected := []string{
		PrimaryDefinitionPath,
		".vectis/build.json",
		".vectis/jobs/legacy.json",
		".vectis/jobs/team/deploy.json",
	}

	var paths []string
	cursor := ""
	for range 5 {
		listing, err := checkout.ListDefinitionFiles(
			ctx,
			ListDefinitionFilesOptions{Ref: "HEAD", Conventional: true, Limit: 1, Cursor: cursor},
		)

		if err != nil {
			t.Fatal(err)
		}

		if !listing.Conventional {
			t.Fatal("expected conventional listing")
		}

		for _, file := range listing.Files {
			paths = append(paths, file.Path)
		}

		if !listing.Truncated {
			break
		}

		if listing.NextCursor <= cursor {
			t.Fatal("cursor did not advance")
		}

		cursor = listing.NextCursor
	}

	if !reflect.DeepEqual(paths, expected) {
		t.Fatalf("paths = %v, want %v", paths, expected)
	}

	for jobID, expectedPath := range map[string]string{"ci": PrimaryDefinitionPath, "build": ".vectis/build.json", "legacy": ".vectis/jobs/legacy.json", "team.deploy": ".vectis/jobs/team/deploy.json"} {
		found, revision, err := checkout.ResolveJobDefinitionPath(ctx, "HEAD", jobID)
		if err != nil || found != expectedPath || !revision.Valid() {
			t.Fatalf("resolve %s = %s, %v", jobID, found, err)
		}

		loaded, err := NewGitDefinitionStore(
			checkout,
		).ResolveDefinition(ctx, DefinitionRequest{Ref: "HEAD", JobID: jobID})
		if err != nil || loaded.Source.Path != expectedPath {
			t.Fatalf("load %s = %+v, %v", jobID, loaded.Source, err)
		}

		generic, err := LoadDefinition(ctx, checkout, DefinitionRequest{Ref: "HEAD", JobID: jobID})
		if err != nil || generic.Source.Path != expectedPath {
			t.Fatalf("generic load %s = %+v, %v", jobID, generic.Source, err)
		}

	}

	explicit, err := checkout.ListDefinitionFiles(
		ctx,
		ListDefinitionFilesOptions{Ref: "HEAD", Path: DefaultDefinitionPath, Conventional: true},
	)

	if err != nil || len(explicit.Files) != 2 || explicit.Conventional {
		t.Fatalf("explicit legacy listing = %+v, %v", explicit, err)
	}
}

func TestConventionalJobsRejectAmbiguityAndUnsafeIDs(t *testing.T) {
	t.Parallel()
	repo := initGitRepo(t)
	definition := `{"root":{"id":"root","uses":"builtins/script","with":{"script":"true"}}}`
	for _, file := range []string{PrimaryDefinitionPath, ".vectis/ci.json", ".vectis/build.json", ".vectis/jobs/build.json"} {
		writeAndCommit(t, repo, file, definition, "add "+file)
	}

	checkout := NewGitCheckout(repo)
	for _, jobID := range []string{"ci", "build"} {
		if _, _, err := checkout.ResolveJobDefinitionPath(
			context.Background(),
			"HEAD",
			jobID,
		); !errors.Is(
			err,
			ErrInvalidDefinition,
		) {
			t.Fatalf("ambiguous %s: %v", jobID, err)
		}
	}

	for _, jobID := range []string{"../escape", "a/b", "a\\b", "a..b", ""} {
		if _, err := JobDefinitionPaths(jobID); err == nil {
			t.Fatalf("accepted unsafe job ID %q", jobID)
		}
	}

	loaded, err := NewGitDefinitionStore(
		checkout,
	).ResolveDefinition(context.Background(), DefinitionRequest{Ref: "HEAD", Path: PrimaryDefinitionPath})

	if err != nil || loaded.Source.Path != PrimaryDefinitionPath {
		t.Fatalf("explicit path must remain usable: %+v, %v", loaded.Source, err)
	}
}

func TestJobDefinitionLookupBatchesAndKeepsCollisionsVisible(t *testing.T) {
	t.Parallel()
	repo := initGitRepo(t)
	if err := os.MkdirAll(filepath.Join(repo, ".vectis", "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}

	ids := make([]string, 40)
	for i := range ids {
		ids[i] = fmt.Sprintf("job%02d", i)
		if err := os.WriteFile(
			filepath.Join(repo, ".vectis", ids[i]+".json"),
			[]byte(`{"root":{"id":"root"}}`),
			0o644,
		); err != nil {
			t.Fatal(err)
		}
	}

	writeAndCommit(t, repo, ".vectis/jobs/job39.json", `{}`, "add conventional and conflicting legacy jobs")
	checkout := NewGitCheckout(repo)
	revision, err := checkout.ResolveRevision(context.Background(), "HEAD")
	if err != nil {
		t.Fatal(err)
	}

	results, err := checkout.ResolveJobDefinitionPaths(context.Background(), revision, append(ids, "missing"))
	if err != nil {
		t.Fatal(err)
	}

	for _, jobID := range ids[:39] {
		if result := results[jobID]; result.Err != nil || result.Path != ".vectis/"+jobID+".json" {
			t.Fatalf("%s = %+v", jobID, result)
		}
	}

	if !errors.Is(results["job39"].Err, ErrInvalidDefinition) {
		t.Fatalf("collision lost across batches: %+v", results["job39"])
	}

	if !errors.Is(results["missing"].Err, ErrNotFound) {
		t.Fatalf("missing job: %+v", results["missing"])
	}
}
