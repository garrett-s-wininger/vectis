package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/dal"
)

func TestCreateNamespace_Success(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	body := []byte(`{"name": "team-a"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/namespaces", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.CreateNamespace(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}

	if resp["name"] != "team-a" {
		t.Fatalf("expected name team-a, got %v", resp["name"])
	}

	if resp["path"] != "/team-a" {
		t.Fatalf("expected path /team-a, got %v", resp["path"])
	}

	// Verify it's in the database
	ns, err := repos.Namespaces().GetByPath(ctx, "/team-a")
	if err != nil {
		t.Fatalf("namespace not found in db: %v", err)
	}

	if ns.Name != "team-a" {
		t.Fatalf("expected name team-a, got %s", ns.Name)
	}
}

func TestCreateNamespace_InvalidName(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	body := []byte(`{"name": "team/a"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/namespaces", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.CreateNamespace(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestCreateNamespace_MissingName(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	body := []byte(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/namespaces", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	server.CreateNamespace(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

func TestListNamespaces(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	// Create some namespaces
	_, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	_, err = repos.Namespaces().Create(ctx, "team-b", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces", nil)
	rec := httptest.NewRecorder()

	server.ListNamespaces(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp []map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}

	if len(resp) != 3 { // root + team-a + team-b
		t.Fatalf("expected 3 namespaces, got %d", len(resp))
	}
}

func TestGetNamespace_Success(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/namespaces/%d", ns.ID), nil)
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	rec := httptest.NewRecorder()

	server.GetNamespace(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("parse response: %v", err)
	}

	if resp["name"] != "team-a" {
		t.Fatalf("expected name team-a, got %v", resp["name"])
	}
}

func TestGetNamespace_NotFound(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/namespaces/99999", nil)
	req.SetPathValue("id", "99999")
	rec := httptest.NewRecorder()

	server.GetNamespace(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestDeleteNamespace_Success(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/namespaces/%d", ns.ID), nil)
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	rec := httptest.NewRecorder()

	server.DeleteNamespace(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify it's gone
	_, err = repos.Namespaces().GetByID(ctx, ns.ID)
	if !dal.IsNotFound(err) {
		t.Fatal("expected namespace to be deleted")
	}
}

func TestDeleteNamespace_HasChildren(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	parent, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	_, err = repos.Namespaces().Create(ctx, "project-1", &parent.ID)
	if err != nil {
		t.Fatalf("create child namespace: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/namespaces/%d", parent.ID), nil)
	req.SetPathValue("id", fmt.Sprintf("%d", parent.ID))
	rec := httptest.NewRecorder()

	server.DeleteNamespace(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDeleteNamespace_Root(t *testing.T) {
	server, _, _, _ := setupTestServer(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/namespaces/1", nil)
	req.SetPathValue("id", "1")
	rec := httptest.NewRecorder()

	server.DeleteNamespace(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for root delete, got %d", rec.Code)
	}
}

func TestDeleteNamespace_HasJobs(t *testing.T) {
	server, _, _, db := setupTestServer(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)

	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	// Create a job in the namespace
	if err := repos.Jobs().Create(ctx, "job-in-ns", `{"id":"job-in-ns"}`, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/namespaces/%d", ns.ID), nil)
	req.SetPathValue("id", fmt.Sprintf("%d", ns.ID))
	rec := httptest.NewRecorder()

	server.DeleteNamespace(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", rec.Code, rec.Body.String())
	}
}
