package gerrit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestRunSmokeBootstrapsCursorAgainstFakeGerrit(t *testing.T) {
	var gotPath string
	var gotQuery url.Values
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		gotQuery = r.URL.Query()
		gotAuth = r.Header.Get("Authorization")
		writeSmokeChanges(w, `[{"id":"project~main~Iabc","project":"project","branch":"main","status":"NEW","change_id":"Iabc","current_revision":"rev1","revisions":{"rev1":{"ref":"refs/changes/01/1/1"}}}]`)
	}))
	defer server.Close()

	result, err := RunSmoke(context.Background(), SmokeOptions{
		BaseURL: server.URL,
		Project: "project",
		Branch:  "main",
	})

	if err != nil {
		t.Fatalf("RunSmoke: %v", err)
	}

	if gotPath != "/changes/" {
		t.Fatalf("path = %q, want /changes/", gotPath)
	}

	if gotQuery.Get("q") != "project:project branch:main status:open" {
		t.Fatalf("q = %q", gotQuery.Get("q"))
	}

	if gotAuth != "" {
		t.Fatalf("Authorization = %q, want empty", gotAuth)
	}

	if result.Status != "ok" || result.EventCount != 0 || result.Cursor == "" {
		t.Fatalf("unexpected result: %+v", result)
	}
}

func TestRunSmokeCanEmitExistingChangesWithBasicAuth(t *testing.T) {
	var gotPath string
	var gotUser string
	var gotPassword string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.EscapedPath()
		gotUser, gotPassword, _ = r.BasicAuth()
		writeSmokeChanges(w, `[{"id":"project~main~Iabc","project":"project","branch":"main","status":"NEW","change_id":"Iabc","current_revision":"rev2","revisions":{"rev2":{"ref":"refs/changes/01/1/2"}}}]`)
	}))
	defer server.Close()

	result, err := RunSmoke(context.Background(), SmokeOptions{
		BaseURL:      server.URL,
		Project:      "project",
		Branch:       "main",
		Query:        "change:Iabc",
		Username:     "ci-bot",
		Password:     "secret",
		EmitExisting: true,
		MinEvents:    1,
	})

	if err != nil {
		t.Fatalf("RunSmoke: %v", err)
	}

	if gotPath != "/a/changes/" {
		t.Fatalf("path = %q, want /a/changes/", gotPath)
	}

	if gotUser != "ci-bot" || gotPassword != "secret" {
		t.Fatalf("basic auth = %q/%q", gotUser, gotPassword)
	}

	if result.EventCount != 1 || len(result.Events) != 1 || !strings.Contains(result.Events[0].Key, ":project~main~Iabc:rev2") {
		t.Fatalf("unexpected events: %+v", result.Events)
	}

	var payload struct {
		CurrentRevision string `json:"current_revision"`
		Ref             string `json:"ref"`
	}

	if err := json.Unmarshal(result.Events[0].Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if payload.CurrentRevision != "rev2" || payload.Ref != "refs/changes/01/1/2" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestRunSmokeRejectsPartialAuth(t *testing.T) {
	_, err := RunSmoke(context.Background(), SmokeOptions{
		BaseURL:  "http://gerrit.example.com",
		Username: "ci-bot",
	})

	if err == nil || !strings.Contains(err.Error(), "username and password must be configured together") {
		t.Fatalf("RunSmoke error = %v, want partial auth rejection", err)
	}
}

func writeSmokeChanges(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(")]}'\n" + body))
}
