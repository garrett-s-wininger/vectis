package dal_test

import (
	"context"
	"sync"
	"testing"
	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestSourceJobsHaveOneOwnerAndRevisionFencedHotfixes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repos := dal.NewSQLRepositories(dbtest.NewTestDB(t))
	sources := repos.Sources()
	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{RepositoryID: "mono", NamespaceID: 1, SourceKind: dal.SourceKindLocalCheckout, CheckoutPath: t.TempDir(), Enabled: true}); err != nil {
		t.Fatal(err)
	}

	second, err := repos.Namespaces().Create(ctx, "other", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = sources.GrantRepositoryNamespace(ctx, "mono", second.ID); err != nil {
		t.Fatal(err)
	}

	saved, err := sources.RegisterSourceJob(ctx, dal.SourceJobRecord{RepositoryID: "mono", JobID: "build", NamespaceID: 1, Path: ".vectis/build.json"})
	if err != nil {
		t.Fatal(err)
	}

	if _, err = sources.RegisterSourceJob(ctx, dal.SourceJobRecord{RepositoryID: "mono", JobID: "build", NamespaceID: second.ID, Path: saved.Path}); !dal.IsConflict(err) {
		t.Fatalf("second owner: %v", err)
	}

	if _, err = sources.RegisterSourceJob(ctx, dal.SourceJobRecord{RepositoryID: "mono", JobID: "alias", NamespaceID: 1, Path: saved.Path}); !dal.IsConflict(err) {
		t.Fatalf("duplicate path alias: %v", err)
	}

	var wg sync.WaitGroup
	outcomes := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Go(func() {
			candidate := saved
			candidate.Hotfix = &dal.SourceJobHotfix{Commit: "abc", Reason: "test"}
			_, err := sources.SetSourceJobHotfix(ctx, candidate, saved.Revision)
			outcomes <- err
		})
	}

	wg.Wait()
	close(outcomes)

	successes := 0
	conflicts := 0
	for err := range outcomes {
		if err == nil {
			successes++
		} else if dal.IsConflict(err) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}

	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}

	if err = sources.DeleteRepository(ctx, "mono"); !dal.IsConflict(err) {
		t.Fatalf("owned source deleted: %v", err)
	}
}
