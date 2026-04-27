package dal

import (
	"context"
	"testing"

	"vectis/internal/testutil/dbtest"
)

func TestRoleBindingsRepository_Create_and_ListByNamespace(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	// Create a user first
	_, err := db.ExecContext(ctx, rebindQueryForPgx(
		"INSERT INTO local_users (username, password_hash) VALUES (?, ?)"),
		"user1", "hash",
	)
	if err != nil {
		t.Fatal(err)
	}

	repo := NewSQLRoleBindingsRepository(db)
	rec, err := repo.Create(ctx, 1, 1, "viewer")
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	if rec.ID == 0 {
		t.Fatal("expected non-zero id")
	}

	if rec.Role != "viewer" {
		t.Fatalf("expected viewer, got %s", rec.Role)
	}

	bindings, err := repo.ListByNamespace(ctx, 1)
	if err != nil {
		t.Fatalf("list by namespace failed: %v", err)
	}

	if len(bindings) != 1 {
		t.Fatalf("expected 1 binding, got %d", len(bindings))
	}
}

func TestRoleBindingsRepository_ListByUser(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, rebindQueryForPgx(
		"INSERT INTO local_users (username, password_hash) VALUES (?, ?)"),
		"user2", "hash",
	)

	if err != nil {
		t.Fatal(err)
	}

	repo := NewSQLRoleBindingsRepository(db)
	_, err = repo.Create(ctx, 1, 1, "admin")
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	bindings, err := repo.ListByUser(ctx, 1)
	if err != nil {
		t.Fatalf("list by user failed: %v", err)
	}

	if len(bindings) != 1 {
		t.Fatalf("expected 1 binding, got %d", len(bindings))
	}
}

func TestRoleBindingsRepository_Delete(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, rebindQueryForPgx(
		"INSERT INTO local_users (username, password_hash) VALUES (?, ?)"),
		"user3", "hash",
	)

	if err != nil {
		t.Fatal(err)
	}

	repo := NewSQLRoleBindingsRepository(db)
	_, err = repo.Create(ctx, 1, 1, "operator")
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	if err := repo.Delete(ctx, 1, 1, "operator"); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	bindings, err := repo.ListByNamespace(ctx, 1)
	if err != nil {
		t.Fatalf("list by namespace failed: %v", err)
	}

	if len(bindings) != 0 {
		t.Fatalf("expected 0 bindings after delete, got %d", len(bindings))
	}
}

func TestRoleBindingsRepository_Delete_notFound(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLRoleBindingsRepository(db)
	ctx := context.Background()

	err := repo.Delete(ctx, 999, 999, "viewer")
	if !IsNotFound(err) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestRoleBindingsRepository_GetUserRolesInNamespace(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	ctx := context.Background()

	_, err := db.ExecContext(ctx, rebindQueryForPgx(
		"INSERT INTO local_users (username, password_hash) VALUES (?, ?)"),
		"user4", "hash",
	)

	if err != nil {
		t.Fatal(err)
	}

	repo := NewSQLRoleBindingsRepository(db)
	_, err = repo.Create(ctx, 1, 1, "viewer")
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	_, err = repo.Create(ctx, 1, 1, "admin")
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	roles, err := repo.GetUserRolesInNamespace(ctx, 1, 1)
	if err != nil {
		t.Fatalf("get user roles failed: %v", err)
	}

	if len(roles) != 2 {
		t.Fatalf("expected 2 roles, got %d", len(roles))
	}
}

func TestRoleBindingsRepository_GetUserRolesInNamespace_empty(t *testing.T) {
	t.Parallel()

	db := dbtest.NewTestDB(t)
	repo := NewSQLRoleBindingsRepository(db)
	ctx := context.Background()

	roles, err := repo.GetUserRolesInNamespace(ctx, 999, 999)
	if err != nil {
		t.Fatalf("get user roles failed: %v", err)
	}

	if len(roles) != 0 {
		t.Fatalf("expected 0 roles, got %d", len(roles))
	}
}
