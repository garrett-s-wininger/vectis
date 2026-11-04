package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/gittest"
)

func TestSharedRepositoryNamespaceIsolation(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")
	t.Setenv("VECTIS_API_AUTHZ_ENGINE", "hierarchical_rbac")
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)

	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.SetQueueClient(mocks.NewMockQueueService())
	h := s.Handler()
	request := func(method, path, token string, body any, want int) *httptest.ResponseRecorder {
		t.Helper()
		var payload []byte

		if body != nil {
			var err error
			payload, err = json.Marshal(body)

			if err != nil {
				t.Fatal(err)
			}
		}

		req := httptest.NewRequest(method, path, bytes.NewReader(payload))
		if method == "POST" && strings.Contains(path, "/trigger") {
			req.Header.Set("Idempotency-Key", "shared-namespace-key")
		}

		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Fatalf("%s %s: got %d want %d: %s", method, path, rec.Code, want, rec.Body.String())
		}

		return rec
	}

	setup := request("POST", "/api/v1/setup/complete", "", map[string]any{"bootstrap_token": "sixteenchars----", "admin_username": "root", "admin_password": "longenough"}, 200)
	var root setupCompleteResponse
	if err := json.Unmarshal(setup.Body.Bytes(), &root); err != nil {
		t.Fatal(err)
	}

	teamA, err := repos.Namespaces().Create(context.Background(), "team-a", nil)
	if err != nil {
		t.Fatal(err)
	}

	teamB, err := repos.Namespaces().Create(context.Background(), "team-b", nil)
	if err != nil {
		t.Fatal(err)
	}

	checkout := gittest.InitRepository(t)
	if err := os.MkdirAll(filepath.Join(checkout, ".vectis", "jobs"), 0700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(checkout, ".vectis", "jobs", "build.json"), []byte(`{"root":{"id":"root","uses":"builtins/script","with":{"script":"true"}}}`), 0600); err != nil {
		t.Fatal(err)
	}

	gittest.CommitAll(t, checkout, "shared source fixture")
	if _, err := repos.Sources().CreateRepository(context.Background(), dal.SourceRepositoryRecord{RepositoryID: "shared", NamespaceID: teamA.ID, SourceKind: dal.SourceKindLocalCheckout, CheckoutPath: checkout, DefaultRef: "HEAD", Enabled: true}); err != nil {
		t.Fatal(err)
	}

	request("PUT", fmt.Sprintf("/api/v1/source-repositories/shared/namespaces/%d", teamB.ID), root.APIToken, nil, 200)
	if rows, err := repos.Sources().ListAllRepositories(context.Background()); err != nil || len(rows) != 1 {
		t.Fatalf("expected one global connection: %+v %v", rows, err)
	}

	create := request("POST", "/api/v1/users", root.APIToken, map[string]any{"username": "namespace-admin", "initial_binding": map[string]any{"namespace_id": teamA.ID, "role": "admin"}}, 201)
	var user createUserResponse
	if err := json.Unmarshal(create.Body.Bytes(), &user); err != nil {
		t.Fatal(err)
	}

	login := postLoginForTest(t, h, map[string]any{"username": "namespace-admin", "password": user.InitialPassword, "return_token": true}, 200)
	token := login.Token
	request("GET", "/api/v1/source-repositories?namespace=%2Fteam-a", token, nil, 200)
	detail := request("GET", "/api/v1/source-repositories/shared?namespace=%2Fteam-a", token, nil, 200)
	if bytes.Contains(detail.Body.Bytes(), []byte(checkout)) {
		t.Fatal("namespace user received private checkout path")
	}

	request("GET", "/api/v1/source-repositories/shared?namespace=%2Fteam-b", token, nil, 403)
	request("GET", "/api/v1/source-repositories?scope=all", token, nil, 403)
	request("POST", "/api/v1/source-repositories/shared/sync", token, nil, 403)
	request("PUT", fmt.Sprintf("/api/v1/source-repositories/shared/namespaces/%d", teamA.ID), token, nil, 403)
	request("PUT", "/api/v1/source-repositories/shared", token, map[string]any{"enabled": false}, 403)
	request("POST", "/api/v1/source-repositories/shared/jobs/build/trigger", root.APIToken, nil, 400)
	trigger := request("POST", "/api/v1/source-repositories/shared/jobs/build/trigger?namespace=%2Fteam-a", token, nil, http.StatusAccepted)
	var run struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(trigger.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}

	if path, err := repos.Runs().GetRunNamespacePath(context.Background(), run.RunID); err != nil || path != "/team-a" {
		t.Fatalf("run namespace: %q %v", path, err)
	}

	other := request("POST", "/api/v1/source-repositories/shared/jobs/build/trigger?namespace=%2Fteam-b", root.APIToken, nil, http.StatusAccepted)
	var second struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(other.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}

	rootA := request("POST", "/api/v1/source-repositories/shared/jobs/build/trigger?namespace=%2Fteam-a", root.APIToken, nil, http.StatusAccepted)
	var rootARun struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(rootA.Body.Bytes(), &rootARun); err != nil {
		t.Fatal(err)
	}

	if rootARun.RunID == second.RunID {
		t.Fatal("idempotency key reused another namespace's run")
	}

	again := request("POST", "/api/v1/source-repositories/shared/jobs/build/trigger?namespace=%2Fteam-a", root.APIToken, nil, http.StatusAccepted)
	if again.Body.String() != rootA.Body.String() {
		t.Fatal("same namespace retry changed accepted run")
	}

	request("GET", "/api/v1/runs/"+second.RunID, token, nil, 404)
	history := request("GET", "/api/v1/source-repositories/shared/jobs/build/runs?namespace=%2Fteam-a&limit=1", token, nil, 200)
	if !bytes.Contains(history.Body.Bytes(), []byte(run.RunID)) || bytes.Contains(history.Body.Bytes(), []byte(second.RunID)) {
		t.Fatalf("cross-namespace history: %s", history.Body.String())
	}

	history = request("GET", "/api/v1/source-repositories/shared/jobs/build/runs?namespace=%2Fteam-a", root.APIToken, nil, 200)
	if bytes.Contains(history.Body.Bytes(), []byte(second.RunID)) {
		t.Fatalf("workspace history leaked: %s", history.Body.String())
	}

	request("GET", "/api/v1/source-repositories/shared/jobs/build/runs/"+second.RunID+"/logs?namespace=%2Fteam-a", token, nil, 404)
	aKey := sourceRepositoryRunBroadcastKey("shared", "build", "/team-a")
	bKey := sourceRepositoryRunBroadcastKey("shared", "build", "/team-b")
	events := s.runBroadcaster.Subscribe(aKey)
	defer s.runBroadcaster.Unsubscribe(aKey, events)

	s.runBroadcaster.Broadcast(bKey, second.RunID, 2)
	select {
	case <-events:
		t.Fatal("received other namespace's run event")
	default:
	}

	s.runBroadcaster.Broadcast(aKey, run.RunID, 1)

	select {
	case <-events:
	default:
		t.Fatal("missing own namespace run event")
	}

	request("DELETE", fmt.Sprintf("/api/v1/source-repositories/shared/namespaces/%d", teamA.ID), root.APIToken, nil, 200)
	request("POST", "/api/v1/source-repositories/shared/jobs/build/trigger?namespace=%2Fteam-a", token, nil, 403)
	request("GET", "/api/v1/runs/"+run.RunID, token, nil, 200)
	if path, err := s.getRunJobNamespacePath(context.Background(), run.RunID); err != nil || path != "/team-a" {
		t.Fatalf("historical run moved after revoke: %q %v", path, err)
	}

	if path, err := s.getRunJobNamespacePath(context.Background(), second.RunID); err != nil || path != "/team-b" {
		t.Fatalf("shared run isolation: %q %v", path, err)
	}
}
