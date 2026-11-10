package scmtrigger

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

func TestSCMSourceJobUsesTheCanonicalHotfixAndPreservesSnapshots(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repos := dal.NewSQLRepositories(dbtest.NewTestDB(t))
	sources := repos.Sources()
	checkout := gittest.InitRepository(t)
	original := `{"root":{"id":"root","uses":"builtins/script","with":{"script":"echo upstream"}}}`
	if err := os.WriteFile(filepath.Join(checkout, ".vectis.json"), []byte(original), 0600); err != nil {
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
	metadata, err := sourcepkg.WriteHotfix(ctx, store, []byte(strings.Replace(original, "upstream", "hotfix", 1)), dal.SourceJobHotfix{Reason: "SCM incident", Actor: "owner", BaseCommit: commit, BasePath: ".vectis.json", CreatedAt: 1})
	if err != nil {
		t.Fatal(err)
	}

	registration.HotfixStore = store
	registration.Hotfix = &metadata
	registration, err = sources.SetSourceJobHotfix(ctx, registration, registration.Revision)
	if err != nil {
		t.Fatal(err)
	}

	processor := Processor{Jobs: repos.Jobs()}
	spec := dal.SCMPollTriggerSpec{SourceRepositoryID: "mono", NamespaceID: 1, JobID: "ci"}
	_, version, _, err := processor.eventJobDefinition(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}

	captured, err := sources.GetDefinitionSource(ctx, "ci", version)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(captured.HotfixJSON, "SCM incident") {
		t.Fatal("SCM snapshot lost hotfix attribution")
	}

	registration.Hotfix = nil
	if _, err = sources.SetSourceJobHotfix(ctx, registration, registration.Revision); err != nil {
		t.Fatal(err)
	}

	_, nextVersion, _, err := processor.eventJobDefinition(ctx, spec)
	if err != nil {
		t.Fatal(err)
	}

	body, err := repos.Jobs().GetDefinitionVersion(ctx, "ci", nextVersion)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(body, "upstream") {
		t.Fatal("SCM kept using the cleared hotfix")
	}

	old, err := repos.Jobs().GetDefinitionVersion(ctx, "ci", version)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(old, "hotfix") {
		t.Fatal("clearing changed the old SCM snapshot")
	}

	spec.NamespaceID = 999
	if _, _, _, err = processor.eventJobDefinition(ctx, spec); err == nil {
		t.Fatal("SCM bypassed canonical ownership")
	}

	spec.NamespaceID = 1
	rec.Enabled = false
	if _, err = sources.UpdateRepository(ctx, rec); err != nil {
		t.Fatal(err)
	}

	if _, _, _, err = processor.eventJobDefinition(ctx, spec); err == nil {
		t.Fatal("SCM bypassed disabled source")
	}
}
