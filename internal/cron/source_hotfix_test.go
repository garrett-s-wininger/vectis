package cron

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"vectis/internal/dal"
	sourcepkg "vectis/internal/source"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/gittest"
)

func TestSourceScheduleLoadsOwnedHotfixBeforeReadingUpstream(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repos := dal.NewSQLRepositories(dbtest.NewTestDB(t))
	sources := repos.Sources()
	checkout := gittest.InitRepository(t)
	definition := `{"root":{"id":"root","uses":"builtins/script","with":{"script":"echo hotfix"}}}`
	if err := os.WriteFile(filepath.Join(checkout, ".vectis.json"), []byte(definition), 0600); err != nil {
		t.Fatal(err)
	}

	commit := gittest.CommitAll(t, checkout, "source")
	rec, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{RepositoryID: "mono", NamespaceID: 1, SourceKind: dal.SourceKindLocalCheckout, CheckoutPath: checkout, DefaultRef: "main", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}

	registration, err := sources.RegisterSourceJob(ctx, dal.SourceJobRecord{RepositoryID: "mono", JobID: "ci", NamespaceID: 1, Path: ".vectis.json"})
	if err != nil {
		t.Fatal(err)
	}

	store := filepath.Join(t.TempDir(), "hotfix.git")
	metadata, err := sourcepkg.WriteHotfix(ctx, store, []byte(definition), dal.SourceJobHotfix{Reason: "schedule incident", BaseCommit: commit, BasePath: ".vectis.json", CreatedAt: 1})
	if err != nil {
		t.Fatal(err)
	}

	registration.Hotfix = &metadata
	registration.HotfixStore = store
	if _, err = sources.SetSourceJobHotfix(ctx, registration, registration.Revision); err != nil {
		t.Fatal(err)
	}

	if err = os.Remove(filepath.Join(checkout, ".vectis.json")); err != nil {
		t.Fatal(err)
	}

	gittest.CommitAll(t, checkout, "remove source")
	service := &CronService{sources: sources}
	schedule := &CronSchedule{NamespaceID: 1, SourceRepositoryID: "mono", SourcePath: ".vectis.json"}
	loaded, provenance, err := service.loadSourceScheduleDefinition(ctx, "ci", schedule)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(loaded.DefinitionJSON, "hotfix") || !strings.Contains(provenance.HotfixJSON, "schedule incident") {
		t.Fatal("schedule lost the hotfix or its attribution")
	}

	schedule.NamespaceID = 999
	if _, _, err = service.loadSourceScheduleDefinition(ctx, "ci", schedule); err == nil {
		t.Fatal("schedule bypassed canonical job ownership")
	}

	schedule.NamespaceID = 1
	rec.Enabled = false
	if _, err = sources.UpdateRepository(ctx, rec); err != nil {
		t.Fatal(err)
	}

	if _, _, err = service.loadSourceScheduleDefinition(ctx, "ci", schedule); err == nil {
		t.Fatal("schedule bypassed disabled source")
	}
}
