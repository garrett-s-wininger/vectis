package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/api/authz"
	"vectis/internal/dal"
)

func TestCreateBinding_Success(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	// Insert user directly via SQL
	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash) VALUES (?, ?)`,
		"testuser", "hash")

	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	uid, err := res.LastInsertId()
	if err != nil {
		t.Fatalf("last insert id: %v", err)
	}

	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	body := fmt.Appendf(nil, `{"local_user_id": %d, "role": "viewer"}`, uid)
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/namespaces/%d/bindings", ns.ID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	rec := httptest.NewRecorder()

	server.CreateBinding(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}

	if resp["role"] != "viewer" {
		t.Fatalf("expected role viewer, got %v", resp["role"])
	}

	// Verify in database
	roles, err := repos.RoleBindings().GetUserRolesInNamespace(ctx, uid, ns.ID)
	if err != nil {
		t.Fatalf("get roles: %v", err)
	}

	if len(roles) != 1 || roles[0] != "viewer" {
		t.Fatalf("expected [viewer], got %v", roles)
	}
}

func TestCreateBinding_InvalidRole(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash) VALUES (?, ?)`,
		"testuser", "hash")

	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	uid, _ := res.LastInsertId()
	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	body := fmt.Appendf(nil, `{"local_user_id": %d, "role": "invalid-role"}`, uid)
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/namespaces/%d/bindings", ns.ID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	rec := httptest.NewRecorder()

	server.CreateBinding(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestCreateBinding_MissingUserID(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	body := []byte(`{"role": "viewer"}`)
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/namespaces/%d/bindings", ns.ID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	rec := httptest.NewRecorder()

	server.CreateBinding(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestCreateBinding_Duplicate(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash) VALUES (?, ?)`,
		"testuser", "hash")
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	uid, _ := res.LastInsertId()
	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	// Create first binding
	if _, err := repos.RoleBindings().Create(ctx, uid, ns.ID, "viewer"); err != nil {
		t.Fatalf("create binding: %v", err)
	}

	// Try to create duplicate
	body := fmt.Appendf(nil, `{"local_user_id": %d, "role": "viewer"}`, uid)
	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/namespaces/%d/bindings", ns.ID), bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	rec := httptest.NewRecorder()

	server.CreateBinding(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestListBindings(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash) VALUES (?, ?)`,
		"testuser", "hash")
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	uid, _ := res.LastInsertId()
	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	if _, err := repos.RoleBindings().Create(ctx, uid, ns.ID, "viewer"); err != nil {
		t.Fatalf("create binding: %v", err)
	}

	if _, err := repos.RoleBindings().Create(ctx, uid, ns.ID, "trigger"); err != nil {
		t.Fatalf("create binding: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/namespaces/%d/bindings", ns.ID), nil)
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	rec := httptest.NewRecorder()

	server.ListBindings(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}

	if len(resp) != 2 {
		t.Fatalf("expected 2 bindings, got %d", len(resp))
	}
}

func TestDeleteBinding_Success(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash) VALUES (?, ?)`,
		"testuser", "hash")

	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	uid, _ := res.LastInsertId()
	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	if _, err := repos.RoleBindings().Create(ctx, uid, ns.ID, "viewer"); err != nil {
		t.Fatalf("create binding: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/namespaces/%d/bindings/%d?role=viewer", ns.ID, uid), nil)
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	req.SetPathValue("user_id", fmt.Sprintf("%d", uid))
	rec := httptest.NewRecorder()

	server.DeleteBinding(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify it's gone
	roles, err := repos.RoleBindings().GetUserRolesInNamespace(ctx, uid, ns.ID)
	if err != nil {
		t.Fatalf("get roles: %v", err)
	}

	if len(roles) != 0 {
		t.Fatalf("expected no roles, got %v", roles)
	}
}

func TestDeleteBinding_NotFound(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash) VALUES (?, ?)`,
		"testuser", "hash")

	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	uid, _ := res.LastInsertId()
	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/namespaces/%d/bindings/%d?role=viewer", ns.ID, uid), nil)
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	req.SetPathValue("user_id", fmt.Sprintf("%d", uid))
	rec := httptest.NewRecorder()

	server.DeleteBinding(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestDeleteBinding_MissingRole(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash) VALUES (?, ?)`,
		"testuser", "hash")
	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	uid, _ := res.LastInsertId()
	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/namespaces/%d/bindings/%d", ns.ID, uid), nil)
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	req.SetPathValue("user_id", fmt.Sprintf("%d", uid))
	rec := httptest.NewRecorder()

	server.DeleteBinding(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestHierarchicalRBAC_Integration(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	// Create viewer user directly
	res, err := db.ExecContext(ctx, `INSERT INTO local_users (username, password_hash) VALUES (?, ?)`,
		"viewer", "hash")

	if err != nil {
		t.Fatalf("insert user: %v", err)
	}

	viewerID, _ := res.LastInsertId()

	// Create namespace and assign viewer role
	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	if _, err := repos.RoleBindings().Create(ctx, viewerID, ns.ID, authz.RoleViewer); err != nil {
		t.Fatalf("create binding: %v", err)
	}

	// Create a job in the namespace
	if err := repos.Jobs().Create(ctx, "job-in-ns", `{"id":"job-in-ns"}`, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	// Verify viewer can read the job
	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-in-ns", nil)
	req.SetPathValue("id", "job-in-ns")
	rec := httptest.NewRecorder()

	server.GetJob(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("viewer should be able to read job, got %d: %s", rec.Code, rec.Body.String())
	}
}
