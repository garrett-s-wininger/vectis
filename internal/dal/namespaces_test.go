package dal

import (
	"context"
	"testing"

	"vectis/internal/testutil/dbtest"
)

func TestNamespacesRepository_Create_and_GetByID(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLNamespacesRepository(db)
	ctx := context.Background()

	rootID := int64(1)
	rec, err := repo.Create(ctx, "test-ns", &rootID)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	if rec.ID == 0 {
		t.Fatal("expected non-zero id")
	}

	if rec.Path != "/test-ns" {
		t.Fatalf("expected /test-ns, got %s", rec.Path)
	}

	got, err := repo.GetByID(ctx, rec.ID)
	if err != nil {
		t.Fatalf("get by id failed: %v", err)
	}

	if got.Name != "test-ns" {
		t.Fatalf("expected test-ns, got %s", got.Name)
	}
}

func TestNamespacesRepository_Create_rootLevel(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLNamespacesRepository(db)
	ctx := context.Background()

	rec, err := repo.Create(ctx, "root-level", nil)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	if rec.Path != "/root-level" {
		t.Fatalf("expected /root-level, got %s", rec.Path)
	}
}

func TestNamespacesRepository_Create_invalidName(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLNamespacesRepository(db)
	ctx := context.Background()

	_, err := repo.Create(ctx, "", nil)
	if !IsInvalidNamespaceName(err) {
		t.Fatalf("expected invalid namespace name error, got %v", err)
	}

	_, err = repo.Create(ctx, "/", nil)
	if !IsInvalidNamespaceName(err) {
		t.Fatalf("expected invalid namespace name error, got %v", err)
	}
}

func TestNamespacesRepository_GetByPath(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLNamespacesRepository(db)
	ctx := context.Background()

	rec, err := repo.Create(ctx, "by-path", nil)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	got, err := repo.GetByPath(ctx, rec.Path)
	if err != nil {
		t.Fatalf("get by path failed: %v", err)
	}

	if got.ID != rec.ID {
		t.Fatalf("expected id %d, got %d", rec.ID, got.ID)
	}
}

func TestNamespacesRepository_List(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLNamespacesRepository(db)
	ctx := context.Background()

	_, err := repo.Create(ctx, "list-a", nil)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	_, err = repo.Create(ctx, "list-b", nil)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	recs, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}

	if len(recs) < 3 { // root + 2 created
		t.Fatalf("expected at least 3 namespaces, got %d", len(recs))
	}
}

func TestNamespacesRepository_ListChildren(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLNamespacesRepository(db)
	ctx := context.Background()

	rootID := int64(1)
	parent, err := repo.Create(ctx, "parent", &rootID)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	_, err = repo.Create(ctx, "child", &parent.ID)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	children, err := repo.ListChildren(ctx, parent.ID)
	if err != nil {
		t.Fatalf("list children failed: %v", err)
	}

	if len(children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(children))
	}
}

func TestNamespacesRepository_Delete(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLNamespacesRepository(db)
	ctx := context.Background()

	rec, err := repo.Create(ctx, "to-delete", nil)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	if err := repo.Delete(ctx, rec.ID); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	_, err = repo.GetByID(ctx, rec.ID)
	if !IsNotFound(err) {
		t.Fatalf("expected not found after delete, got %v", err)
	}
}

func TestNamespacesRepository_Delete_notFound(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLNamespacesRepository(db)
	ctx := context.Background()

	err := repo.Delete(ctx, 99999)
	if !IsNotFound(err) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestNamespacesRepository_HasChildren(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLNamespacesRepository(db)
	ctx := context.Background()

	rootID := int64(1)
	parent, err := repo.Create(ctx, "has-child", &rootID)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	_, err = repo.Create(ctx, "child", &parent.ID)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	has, err := repo.HasChildren(ctx, parent.ID)
	if err != nil {
		t.Fatalf("has children failed: %v", err)
	}

	if !has {
		t.Fatal("expected has children = true")
	}

	has, err = repo.HasChildren(ctx, 99999)
	if err != nil {
		t.Fatalf("has children failed: %v", err)
	}

	if has {
		t.Fatal("expected has children = false for unknown id")
	}
}

func TestNamespacesRepository_HasJobs(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLNamespacesRepository(db)
	ctx := context.Background()

	rootID := int64(1)
	has, err := repo.HasJobs(ctx, rootID)
	if err != nil {
		t.Fatalf("has jobs failed: %v", err)
	}

	if has {
		t.Fatal("expected no jobs on fresh db")
	}
}

func TestBuildNamespacePath(t *testing.T) {
	if got := BuildNamespacePath("/", "foo"); got != "/foo" {
		t.Fatalf("expected /foo, got %s", got)
	}

	if got := BuildNamespacePath("/a", "b"); got != "/a/b" {
		t.Fatalf("expected /a/b, got %s", got)
	}
}

func TestParseNamespacePath(t *testing.T) {
	if got := ParseNamespacePath("/"); len(got) != 1 || got[0] != "/" {
		t.Fatalf("expected [/], got %v", got)
	}

	if got := ParseNamespacePath("/a/b"); len(got) != 3 || got[0] != "/" || got[1] != "/a" || got[2] != "/a/b" {
		t.Fatalf("expected [/ /a /a/b], got %v", got)
	}
}

func TestScanBreakInheritance(t *testing.T) {
	tests := []struct {
		name    string
		in      any
		want    bool
		wantErr bool
	}{
		{name: "bool true", in: true, want: true},
		{name: "bool false", in: false, want: false},
		{name: "int64 one", in: int64(1), want: true},
		{name: "int64 zero", in: int64(0), want: false},
		{name: "int one", in: 1, want: true},
		{name: "bytes false", in: []byte("false"), want: false},
		{name: "bytes one", in: []byte("1"), want: true},
		{name: "string true", in: "true", want: true},
		{name: "string zero", in: "0", want: false},
		{name: "bad string", in: "nope", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := scanBreakInheritance(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (value=%v)", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}
