package api_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/testutil/gittest"
)

func TestSourceJobHotfixOwnershipSyncAndSnapshots(t *testing.T) {
	t.Parallel()
	server, _, _, db := setupTestServer(t)
	server.SetSourceCheckoutRoot(filepath.Join(t.TempDir(), "managed"))
	remote := initAPIRemoteGitRepo(t)
	original := `{"root":{"id":"root","uses":"builtins/script","with":{"script":"exit 7"}}}`
	fixed := `{"root":{"id":"root","uses":"builtins/script","with":{"script":"exit 0"}}}`
	writeAPIFileAndCommit(t, remote, ".vectis.json", original, "source")

	h := server.Handler()
	request := func(method, path string, body any, want int) *httptest.ResponseRecorder {
		t.Helper()
		var rec *httptest.ResponseRecorder
		if body == nil {
			rec = httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		} else {
			rec = doJSONRequest(t, h, method, path, body)
		}

		if rec.Code != want {
			t.Fatalf("%s %s: %d want %d: %s", method, path, rec.Code, want, rec.Body.String())
		}

		return rec
	}

	request("POST", "/api/v1/source-repositories", map[string]any{"repository_id": "mono", "namespace": "/", "canonical_url": remote, "checkout_mode": "managed", "default_ref": "main"}, 201)
	request("POST", "/api/v1/source-repositories/mono/sync", nil, 200)
	base := "/api/v1/source-repositories/mono/jobs/ci"
	type lifecycle struct {
		Registration *dal.SourceJobRecord `json:"registration"`
		SourceCommit string               `json:"source_commit"`
		SourceJSON   string               `json:"source_json"`
		Effective    json.RawMessage      `json:"effective_definition"`
	}

	load := func() lifecycle {
		t.Helper()
		rec := request("GET", base+"/lifecycle?namespace=%2F", nil, 200)
		var row lifecycle
		if err := json.Unmarshal(rec.Body.Bytes(), &row); err != nil {
			t.Fatal(err)
		}

		return row
	}

	before := load()
	if before.Registration != nil {
		t.Fatal("unexpected owner")
	}

	request("PUT", base+"/ownership", map[string]string{"namespace": "/"}, 200)
	owned := load()
	if owned.Registration == nil || owned.Registration.Namespace != "/" {
		t.Fatal("missing owner")
	}

	repos := dal.NewSQLRepositories(db)
	other, err := repos.Namespaces().Create(context.Background(), "other", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err = repos.Sources().GrantRepositoryNamespace(context.Background(), "mono", other.ID); err != nil {
		t.Fatal(err)
	}

	request("PUT", base+"/ownership", map[string]string{"namespace": other.Path}, 409)
	request("POST", "/api/v1/jobs/trigger/ci?namespace="+other.Path, map[string]any{"repository_id": "mono"}, 409)
	wrongList := request("GET", "/api/v1/source-repositories/mono/jobs?namespace="+other.Path, nil, 200)
	if len(decodeSourceRepositoryJobsResponse(t, wrongList).Jobs) != 0 {
		t.Fatal("owned job leaked into another namespace's catalog")
	}

	request("PUT", base+"/hotfix?namespace=%2F", map[string]any{"definition": json.RawMessage(fixed), "reason": "repair production", "expected_revision": 0, "expected_source_commit": owned.SourceCommit}, 409)
	request("PUT", base+"/hotfix?namespace=%2F", map[string]any{"definition": map[string]any{"root": map[string]any{}}, "reason": "invalid", "expected_revision": owned.Registration.Revision, "expected_source_commit": owned.SourceCommit}, 400)
	request("PUT", base+"/hotfix?namespace=%2F", map[string]any{"definition": json.RawMessage(fixed), "reason": "repair production", "expected_revision": owned.Registration.Revision, "expected_source_commit": owned.SourceCommit}, 200)
	active := load()
	if active.Registration.Hotfix == nil || !strings.Contains(string(active.Effective), "exit 0") {
		t.Fatal("hotfix not effective")
	}

	trigger := request("POST", "/api/v1/jobs/trigger/ci?namespace=%2F", map[string]any{"repository_id": "mono"}, 202)
	triggered := decodeSourceJobTriggerResponse(t, trigger)
	snapshot := request("GET", "/api/v1/runs/"+triggered.RunID+"/definition", nil, 200)
	if !strings.Contains(snapshot.Body.String(), "repair production") || !strings.Contains(snapshot.Body.String(), "exit 0") {
		t.Fatal("snapshot lost hotfix contents or attribution")
	}

	if err := os.Remove(filepath.Join(remote, ".vectis.json")); err != nil {
		t.Fatal(err)
	}

	gittest.CommitAll(t, remote, "remove upstream definition")
	request("POST", "/api/v1/source-repositories/mono/sync", nil, 200)
	missingSourceList := request("GET", "/api/v1/source-repositories/mono/jobs?namespace=%2F", nil, 200)
	if len(decodeSourceRepositoryJobsResponse(t, missingSourceList).Jobs) != 1 {
		t.Fatal("active hotfix disappeared when upstream file was removed")
	}

	request("POST", "/api/v1/jobs/trigger/ci?namespace=%2F", map[string]any{"repository_id": "mono"}, 202)
	writeAPIFileAndCommit(t, remote, ".vectis.json", `{"root":{"id":"root","uses":"builtins/script","with":{"script":"echo upstream;exit 3"}}}`, "upstream advanced")
	request("POST", "/api/v1/source-repositories/mono/sync", nil, 200)
	afterSync := load()
	if afterSync.SourceCommit == owned.SourceCommit || !strings.Contains(afterSync.SourceJSON, "exit 3") || !strings.Contains(string(afterSync.Effective), "exit 0") {
		t.Fatal("sync replaced hotfix or failed to refresh upstream")
	}

	request("PUT", "/api/v1/source-repositories/mono", map[string]any{"enabled": false, "impact_revision": sourceImpactRevisionForTest(t, h, "mono")}, 200)
	disabledSync := request("POST", "/api/v1/source-repositories/mono/sync", nil, 200)
	if decodeSourceRepositoryResponse(t, disabledSync).Enabled {
		t.Fatal("manual sync re-enabled a disabled connection")
	}

	if load().Registration.Hotfix.Commit != afterSync.Registration.Hotfix.Commit {
		t.Fatal("manual sync of a disabled connection replaced its hotfix")
	}

	request("POST", "/api/v1/jobs/trigger/ci?namespace=%2F", map[string]any{"repository_id": "mono"}, 409)
	request("PUT", "/api/v1/source-repositories/mono", map[string]any{"enabled": true}, 200)
	request("DELETE", base+"/hotfix?namespace=%2F", map[string]any{"expected_revision": afterSync.Registration.Revision, "expected_source_commit": owned.SourceCommit}, 409)
	request("POST", "/api/v1/jobs/trigger/ci?namespace=%2F", map[string]any{"repository_id": "mono", "definition": json.RawMessage(original), "reason": "one diagnostic run", "expected_revision": afterSync.Registration.Revision, "expected_source_commit": afterSync.SourceCommit}, 202)
	if load().Registration.Revision != afterSync.Registration.Revision {
		t.Fatal("one-off run mutated the active pointer")
	}

	impactRevision := sourceImpactRevisionForTest(t, h, "mono")
	request("DELETE", base+"/hotfix?namespace=%2F", map[string]any{"expected_revision": afterSync.Registration.Revision, "expected_source_commit": afterSync.SourceCommit}, 200)
	restored := load()
	if restored.Registration.Hotfix != nil || !strings.Contains(string(restored.Effective), "exit 3") {
		t.Fatal("source restoration did not use current upstream")
	}

	request("PUT", "/api/v1/source-repositories/mono", map[string]any{"enabled": false, "impact_revision": impactRevision}, 409)
	request("PUT", "/api/v1/source-repositories/mono", map[string]any{"enabled": false, "impact_revision": sourceImpactRevisionForTest(t, h, "mono")}, 200)
	request("POST", "/api/v1/jobs/trigger/ci?namespace=%2F", map[string]any{"repository_id": "mono"}, 409)
	historic := request("GET", "/api/v1/runs/"+triggered.RunID+"/definition", nil, 200)
	if !strings.Contains(historic.Body.String(), "repair production") || !strings.Contains(historic.Body.String(), "exit 0") {
		t.Fatal("clearing or disabling changed a historical snapshot")
	}

	request("DELETE", "/api/v1/source-repositories/mono", nil, 409)
}

func TestSourceJobHotfixCanRepairMalformedUpstream(t *testing.T) {
	t.Parallel()
	server, _, _, _ := setupTestServer(t)
	server.SetSourceCheckoutRoot(filepath.Join(t.TempDir(), "checkouts"))
	remote := initAPIRemoteGitRepo(t)
	writeAPIFileAndCommit(t, remote, ".vectis.json", `{"root":`, "broken JSON")

	h := server.Handler()
	created := doJSONRequest(t, h, "POST", "/api/v1/source-repositories", map[string]any{"repository_id": "broken", "checkout_path": remote, "default_ref": "main"})
	if created.Code != 201 {
		server.SetSourceCheckoutRoot(filepath.Dir(remote))
		created = doJSONRequest(t, h, "POST", "/api/v1/source-repositories", map[string]any{"repository_id": "broken", "checkout_path": remote, "default_ref": "main"})
	}

	if created.Code != 201 {
		t.Fatalf("create: %d %s", created.Code, created.Body.String())
	}

	base := "/api/v1/source-repositories/broken/jobs/ci"
	assigned := doJSONRequest(t, h, "PUT", base+"/ownership", map[string]string{"namespace": "/"})
	if assigned.Code != 200 {
		t.Fatalf("assign: %s", assigned.Body.String())
	}

	var registration dal.SourceJobRecord
	if err := json.Unmarshal(assigned.Body.Bytes(), &registration); err != nil {
		t.Fatal(err)
	}

	commit := apiGitOutput(t, remote, "rev-parse", "HEAD")
	fixed := json.RawMessage(`{"root":{"id":"root","uses":"builtins/script","with":{"script":"true"}}}`)
	activated := doJSONRequest(t, h, "PUT", base+"/hotfix?namespace=%2F", map[string]any{"definition": fixed, "reason": "repair invalid JSON", "expected_revision": registration.Revision, "expected_source_commit": commit})
	if activated.Code != 200 {
		t.Fatalf("activate: %d %s", activated.Code, activated.Body.String())
	}

	triggered := doJSONRequest(t, h, "POST", "/api/v1/jobs/trigger/ci?namespace=%2F", map[string]string{"repository_id": "broken"})
	if triggered.Code != 202 {
		t.Fatalf("trigger: %d %s", triggered.Code, triggered.Body.String())
	}
}
