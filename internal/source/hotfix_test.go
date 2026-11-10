package source

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	gogit "github.com/go-git/go-git/v5"
	"vectis/internal/dal"
)

func TestHotfixStoreKeepsImmutableIndependentDefinitions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := filepath.Join(t.TempDir(), "hotfix.git")
	first := `{"root":{"id":"root","uses":"builtins/script","with":{"script":"echo first"}}}`
	second := strings.Replace(first, "first", "second", 1)
	original, err := WriteHotfix(ctx, store, []byte(first), dal.SourceJobHotfix{Reason: "first incident", Actor: "owner", BaseCommit: strings.Repeat("1", 40), BasePath: ".vectis/build.json", CreatedAt: 1})
	if err != nil {
		t.Fatal(err)
	}

	updated, err := WriteHotfix(ctx, store, []byte(second), dal.SourceJobHotfix{Reason: "second incident", Actor: "owner", BaseCommit: strings.Repeat("2", 40), BasePath: ".vectis/build.json", CreatedAt: 2})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		metadata dal.SourceJobHotfix
		text     string
	}{{original, "echo first"}, {updated, "echo second"}} {
		loaded, err := LoadHotfix(ctx, dal.SourceJobRecord{Path: ".vectis/build.json", HotfixStore: store, Hotfix: &tc.metadata}, DefinitionRequest{})
		if err != nil {
			t.Fatal(err)
		}

		if !strings.Contains(loaded.DefinitionJSON, tc.text) || loaded.Source.Hotfix.Reason != tc.metadata.Reason {
			t.Fatal("old hotfix was overwritten")
		}
	}

	repo, err := gogit.PlainOpen(store)
	if err != nil {
		t.Fatal(err)
	}

	remotes, err := repo.Remotes()
	if err != nil {
		t.Fatal(err)
	}

	if len(remotes) != 0 {
		t.Fatal("hotfix store unexpectedly has upstream remotes")
	}

	if original.Commit == updated.Commit {
		t.Fatal("distinct hotfixes share a commit")
	}
}
