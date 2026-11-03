package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"vectis/internal/api/authz"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
)

func TestAccountNamespaceWorkflow(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")
	t.Setenv("VECTIS_API_AUTHZ_ENGINE", "hierarchical_rbac")
	db := dbtest.NewTestDB(t)

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
	var setupResult setupCompleteResponse
	if err := json.Unmarshal(setup.Body.Bytes(), &setupResult); err != nil {
		t.Fatal(err)
	}

	root := setupResult.APIToken
	repos := dal.NewSQLRepositories(db)
	team, err := repos.Namespaces().Create(context.Background(), "team", nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := repos.Namespaces().Create(context.Background(), "other", nil); err != nil {
		t.Fatal(err)
	}

	if _, err := repos.Namespaces().Create(context.Background(), "child", &team.ID); err != nil {
		t.Fatal(err)
	}

	isolated, err := repos.Namespaces().Create(context.Background(), "isolated", &team.ID)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("UPDATE namespaces SET break_inheritance = ? WHERE id = ?", true, isolated.ID); err != nil {
		t.Fatal(err)
	}

	created := request("POST", "/api/v1/users", root, map[string]any{"username": "operator", "initial_binding": map[string]any{"namespace_id": team.ID, "role": "operator"}}, 201)
	var user createUserResponse
	if err := json.Unmarshal(created.Body.Bytes(), &user); err != nil {
		t.Fatal(err)
	}

	if user.InitialPassword == "" {
		t.Fatal("missing initial password")
	}

	login := postLoginForTest(t, h, map[string]any{"username": "operator", "password": user.InitialPassword, "return_token": true}, 200)
	token := login.Token
	access := request("GET", "/api/v1/access", token, nil, 200)
	var permissions struct {
		Actions    []string            `json:"actions"`
		Namespaces map[string][]string `json:"namespaces"`
	}

	if err := json.Unmarshal(access.Body.Bytes(), &permissions); err != nil {
		t.Fatal(err)
	}

	has := func(actions []string, action string) bool {
		for _, candidate := range actions {
			if candidate == action {
				return true
			}
		}

		return false
	}

	for _, action := range []string{"run:trigger", "run:operator", "job:read", "run:read"} {
		if !has(permissions.Namespaces["/team"], action) {
			t.Fatalf("operator missing %s: %+v", action, permissions)
		}
	}

	for _, action := range []string{"job:write", "admin:*", "user:admin"} {
		if has(permissions.Actions, action) {
			t.Fatalf("operator offered %s", action)
		}
	}

	if _, ok := permissions.Namespaces["/"]; ok {
		t.Fatal("root namespace leaked to scoped operator")
	}

	if _, ok := permissions.Namespaces["/other"]; ok {
		t.Fatal("unrelated namespace leaked to scoped operator")
	}

	if !has(permissions.Namespaces["/team/child"], "run:operator") {
		t.Fatal("inherited operator access missing")
	}

	if _, ok := permissions.Namespaces["/team/isolated"]; ok {
		t.Fatal("access crossed an inheritance break")
	}

	scoped := request("POST", "/api/v1/tokens", token, map[string]any{"label": "read-only", "expires_in": "1w", "scopes": []map[string]any{{"action": "api:any"}, {"action": "run:read", "namespace_path": "/team"}}}, 201)
	var scopedToken createTokenResponse
	if err := json.Unmarshal(scoped.Body.Bytes(), &scopedToken); err != nil {
		t.Fatal(err)
	}

	scopedAccess := request("GET", "/api/v1/access", scopedToken.Token, nil, 200)
	var limited struct {
		Actions    []string            `json:"actions"`
		Namespaces map[string][]string `json:"namespaces"`
	}

	if err := json.Unmarshal(scopedAccess.Body.Bytes(), &limited); err != nil {
		t.Fatal(err)
	}

	if has(limited.Actions, "run:operator") || has(limited.Actions, "run:trigger") || !has(limited.Namespaces["/team"], "run:read") {
		t.Fatalf("access ignored token restrictions: %+v", limited)
	}

	if _, ok := limited.Namespaces["/team/child"]; ok {
		t.Fatal("non-propagating token exposed child access")
	}

	request("GET", "/api/v1/source-repositories?namespace=%2Fteam", token, nil, 200)
	request("GET", "/api/v1/source-repositories", token, nil, 403)
	request("GET", "/api/v1/users", token, nil, 403)
	request("GET", "/api/v1/cells/status", token, nil, 403)
	runBody := func(namespace string) map[string]any {
		return map[string]any{"namespace": namespace, "job": map[string]any{"root": map[string]any{"id": "root", "uses": "builtins/script", "with": map[string]any{"script": "echo scoped"}}}}
	}

	run := request("POST", "/api/v1/jobs/run", token, runBody("/team"), 202)
	var result struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(run.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}

	var namespace string
	if err := db.QueryRow("SELECT namespace_path FROM job_runs WHERE run_id = ?", result.RunID).Scan(&namespace); err != nil || namespace != "/team" {
		t.Fatalf("run namespace: %q %v", namespace, err)
	}

	request("POST", "/api/v1/jobs/run", token, runBody("/"), 403)
	request("POST", "/api/v1/jobs/run", token, runBody("/other"), 403)
	path := fmt.Sprintf("/api/v1/namespaces/%d/bindings/%d", team.ID, user.ID)

	request("PATCH", path, token, map[string]any{"previous_role": "operator", "role": "admin"}, 403)
	before, err := repos.RoleBindings().ListByUser(context.Background(), user.ID)
	if err != nil {
		t.Fatal(err)
	}

	request("PATCH", path, root, map[string]any{"previous_role": "operator", "role": "viewer"}, 200)
	after, err := repos.RoleBindings().ListByUser(context.Background(), user.ID)
	if err != nil {
		t.Fatal(err)
	}

	if len(before) != 1 || len(after) != 1 || after[0].ID != before[0].ID || after[0].Role != "viewer" {
		t.Fatalf("binding was not updated in place: before=%+v after=%+v", before, after)
	}

	access = request("GET", "/api/v1/access", token, nil, 200)
	if err := json.Unmarshal(access.Body.Bytes(), &permissions); err != nil {
		t.Fatal(err)
	}

	if has(permissions.Namespaces["/team"], "run:trigger") || has(permissions.Namespaces["/team"], "run:operator") {
		t.Fatal("downgraded viewer still offered run controls")
	}

	request("POST", "/api/v1/jobs/run", token, runBody("/team"), 403)
	request("PATCH", path, root, map[string]any{"previous_role": "operator", "role": "admin"}, 404)
	request("PATCH", path, root, map[string]any{"previous_role": "viewer", "role": "invalid"}, 400)
	request("POST", "/api/v1/users", root, map[string]any{"username": "invalid-grant", "initial_binding": map[string]any{"namespace_id": team.ID, "role": "invalid"}}, 400)

	var invalidCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM local_users WHERE username = ?", "invalid-grant").Scan(&invalidCount); err != nil || invalidCount != 0 {
		t.Fatalf("invalid grant created an account: %d %v", invalidCount, err)
	}

	if _, err := db.Exec("INSERT INTO audit_log (event_type, actor_id) VALUES (?, ?)", "test.operator.action", user.ID); err != nil {
		t.Fatal(err)
	}

	request("DELETE", fmt.Sprintf("/api/v1/users/%d", user.ID), root, nil, 204)
	request("GET", "/api/v1/namespaces", token, nil, 401)
	request("PUT", fmt.Sprintf("/api/v1/users/%d", user.ID), root, map[string]any{"enabled": true}, 404)
	postLoginForTest(t, h, map[string]any{"username": "operator", "password": user.InitialPassword}, 401)

	var preserved int
	if err := db.QueryRow("SELECT COUNT(*) FROM audit_log a JOIN local_users u ON u.id = a.actor_id WHERE u.id = ? AND u.deleted_at IS NOT NULL AND a.event_type = ?", user.ID, "test.operator.action").Scan(&preserved); err != nil || preserved != 1 {
		t.Fatalf("audit history lost: %d %v", preserved, err)
	}

}

func TestNamespaceAdminCannotReadClusterDiagnostics(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "true")
	t.Setenv("VECTIS_API_AUTH_BOOTSTRAP_TOKEN", "sixteenchars----")
	t.Setenv("VECTIS_API_AUTHZ_ENGINE", "hierarchical_rbac")
	db := dbtest.NewTestDB(t)

	s := NewAPIServer(mocks.NewMockLogger(), db)
	s.MetricsHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h := s.Handler()
	setup := httptest.NewRecorder()
	setupReq := httptest.NewRequest("POST", "/api/v1/setup/complete", bytes.NewBufferString(`{"bootstrap_token":"sixteenchars----","admin_username":"root","admin_password":"longenough"}`))
	setupReq.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(setup, setupReq)
	if setup.Code != 200 {
		t.Fatalf("setup: %d %s", setup.Code, setup.Body.String())
	}

	var root setupCompleteResponse
	if err := json.Unmarshal(setup.Body.Bytes(), &root); err != nil {
		t.Fatal(err)
	}

	repos := dal.NewSQLRepositories(db)
	team, err := repos.Namespaces().Create(context.Background(), "team", nil)
	if err != nil {
		t.Fatal(err)
	}

	create := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/v1/users", bytes.NewBufferString(fmt.Sprintf(`{"username":"team-admin","initial_binding":{"namespace_id":%d,"role":"admin"}}`, team.ID)))
	req.Header.Set("Authorization", "Bearer "+root.APIToken)
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(create, req)
	if create.Code != 201 {
		t.Fatalf("create: %d %s", create.Code, create.Body.String())
	}

	var user createUserResponse
	if err := json.Unmarshal(create.Body.Bytes(), &user); err != nil {
		t.Fatal(err)
	}

	login := postLoginForTest(t, h, map[string]any{"username": "team-admin", "password": user.InitialPassword, "return_token": true}, 200)
	cookieLogin := httptest.NewRecorder()
	cookieBody, err := json.Marshal(map[string]string{"username": "team-admin", "password": user.InitialPassword})
	if err != nil {
		t.Fatal(err)
	}

	cookieReq := httptest.NewRequest("POST", "/api/v1/login", bytes.NewReader(cookieBody))
	cookieReq.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(cookieLogin, cookieReq)
	if cookieLogin.Code != 200 || len(cookieLogin.Result().Cookies()) == 0 {
		t.Fatalf("cookie login: %d %s", cookieLogin.Code, cookieLogin.Body.String())
	}

	request := func(path, token string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest("GET", path, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	for _, spec := range s.routeSpecs(true) {
		if spec.Auth.Action != authz.ActionAdmin || strings.Contains(spec.Pattern, "/namespaces") {
			continue
		}

		if spec.Auth.NamespacePath != "/" {
			t.Errorf("%s must require root administration", spec.Pattern)
		}

		path := strings.TrimPrefix(spec.Pattern, "GET ")
		cookieReq := httptest.NewRequest("GET", path, nil)
		for _, cookie := range cookieLogin.Result().Cookies() {
			cookieReq.AddCookie(cookie)
		}

		cookieResult := httptest.NewRecorder()
		h.ServeHTTP(cookieResult, cookieReq)
		if cookieResult.Code != 403 {
			t.Errorf("cookie %s: got %d want 403: %s", path, cookieResult.Code, cookieResult.Body.String())
		}

		if rec := request(path, login.Token); rec.Code != 403 {
			t.Errorf("%s: got %d want 403: %s", path, rec.Code, rec.Body.String())
		}
	}

	if rec := request("/api/v1/version", root.APIToken); rec.Code != 200 {
		t.Fatalf("root diagnostics: %d %s", rec.Code, rec.Body.String())
	}

	if rec := request(fmt.Sprintf("/api/v1/namespaces/%d/bindings", team.ID), login.Token); rec.Code != 200 {
		t.Fatalf("own bindings: %d %s", rec.Code, rec.Body.String())
	}

	var access struct {
		Actions    []string            `json:"actions"`
		Namespaces map[string][]string `json:"namespaces"`
	}

	rec := request("/api/v1/access", login.Token)
	if rec.Code != 200 {
		t.Fatalf("access: %d %s", rec.Code, rec.Body.String())
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &access); err != nil {
		t.Fatal(err)
	}

	if slices.Contains(access.Actions, "admin:*") || !slices.Contains(access.Namespaces["/team"], "admin:*") {
		t.Fatalf("incorrect admin boundary: %+v", access)
	}
}
