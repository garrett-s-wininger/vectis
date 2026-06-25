package builtins

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
)

func TestGerritReviewAction_Execute_PostsReviewWithBasicAuth(t *testing.T) {
	var gotPath string
	var gotAuthUser string
	var gotAuthPassword string
	var gotPayload map[string]any

	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.EscapedPath()
		gotAuthUser, gotAuthPassword, _ = r.BasicAuth()

		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}

		if r.Header.Get("Content-Type") != "application/json; charset=UTF-8" {
			t.Errorf("content-type = %q", r.Header.Get("Content-Type"))
		}

		if err := json.NewDecoder(r.Body).Decode(&gotPayload); err != nil {
			t.Errorf("decode body: %v", err)
		}

		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(")]}'\n{\"labels\":{\"Verified\":1}}")),
			Request:    r,
		}, nil
	})}

	workspace := t.TempDir()
	passwordPath := filepath.Join(workspace, ".vectis", "secrets", "gerrit")
	if err := os.MkdirAll(passwordPath, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(passwordPath, "http-password"), []byte("secret-pass\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	actionNode := NewGerritReviewAction(client)
	stream := &mockLogStream{}
	state := createTestState(stream)
	state.Workspace = workspace

	result := actionNode.Execute(context.Background(), state, map[string]any{
		"url":           "http://gerrit.example.com/gerrit/",
		"change":        "project~feature/x~Iabc123",
		"message":       "Vectis run succeeded",
		"label":         "Verified",
		"value":         "+1",
		"username":      "ci-bot",
		"password_file": ".vectis/secrets/gerrit/http-password",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v err=%v", result.Status, result.Error)
	}

	if gotPath != "/gerrit/a/changes/project~feature%2Fx~Iabc123/revisions/current/review" {
		t.Fatalf("path = %q", gotPath)
	}

	if gotAuthUser != "ci-bot" || gotAuthPassword != "secret-pass" {
		t.Fatalf("basic auth = %q/%q", gotAuthUser, gotAuthPassword)
	}

	if gotPayload["message"] != "Vectis run succeeded" {
		t.Fatalf("message payload = %#v", gotPayload["message"])
	}

	if gotPayload["tag"] != gerritReviewDefaultTag {
		t.Fatalf("tag payload = %#v", gotPayload["tag"])
	}

	labels, ok := gotPayload["labels"].(map[string]any)
	if !ok {
		t.Fatalf("labels payload = %#v", gotPayload["labels"])
	}

	if labels["Verified"] != float64(1) {
		t.Fatalf("Verified label = %#v", labels["Verified"])
	}

	if result.Outputs["reviewed"] != true || result.Outputs["change"] != "project~feature/x~Iabc123" || result.Outputs["revision"] != "current" {
		t.Fatalf("unexpected outputs: %#v", result.Outputs)
	}

	foundSuccessLog := false
	for _, chunk := range stream.GetChunks() {
		if chunk.GetStream() == api.Stream_STREAM_STDOUT && strings.Contains(string(chunk.GetData()), "Gerrit review posted successfully") {
			foundSuccessLog = true
		}
	}

	if !foundSuccessLog {
		t.Fatal("expected success log")
	}
}

func TestGerritReviewAction_Execute_FailsWhenLabelIsNotConfirmed(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(strings.NewReader(")]}'\n{\"labels\":{}}")),
			Request:    r,
		}, nil
	})}

	workspace := t.TempDir()
	passwordPath := filepath.Join(workspace, ".vectis", "secrets", "gerrit")
	if err := os.MkdirAll(passwordPath, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(passwordPath, "http-password"), []byte("secret-pass"), 0o600); err != nil {
		t.Fatal(err)
	}

	actionNode := NewGerritReviewAction(client)
	state := createTestState(nil)
	state.Workspace = workspace

	result := actionNode.Execute(context.Background(), state, map[string]any{
		"url":           "http://gerrit.example.com",
		"change":        "1",
		"message":       "review",
		"label":         "Verified",
		"value":         "+1",
		"username":      "ci-bot",
		"password_file": ".vectis/secrets/gerrit/http-password",
	}, nil)

	if result.Status != action.StatusFailure {
		t.Fatalf("expected failure, got %v", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), `did not confirm label "Verified"`) {
		t.Fatalf("expected missing label confirmation error, got %v", result.Error)
	}
}

func TestGerritReviewAction_Execute_RequiresPasswordFileInsideWorkspace(t *testing.T) {
	actionNode := NewGerritReviewAction(nil)
	state := createTestState(nil)
	state.Workspace = t.TempDir()

	result := actionNode.Execute(context.Background(), state, map[string]any{
		"url":           "http://gerrit.example.com",
		"change":        "1",
		"message":       "review",
		"username":      "ci-bot",
		"password_file": "../outside",
	}, nil)

	if result.Status != action.StatusFailure {
		t.Fatalf("expected failure, got %v", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), "must stay inside the workspace") {
		t.Fatalf("expected workspace path error, got %v", result.Error)
	}
}

func TestGerritReviewAction_Execute_ReportsHTTPFailureWithoutXSSIPrefix(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusForbidden,
			Status:     "403 Forbidden",
			Body:       io.NopCloser(strings.NewReader(")]}'\npermission denied")),
			Request:    r,
		}, nil
	})}

	workspace := t.TempDir()
	passwordPath := filepath.Join(workspace, ".vectis", "secrets", "gerrit")
	if err := os.MkdirAll(passwordPath, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(passwordPath, "http-password"), []byte("secret-pass"), 0o600); err != nil {
		t.Fatal(err)
	}

	actionNode := NewGerritReviewAction(client)
	state := createTestState(nil)
	state.Workspace = workspace

	result := actionNode.Execute(context.Background(), state, map[string]any{
		"url":           "http://gerrit.example.com",
		"change":        "1",
		"message":       "review",
		"username":      "ci-bot",
		"password_file": ".vectis/secrets/gerrit/http-password",
	}, nil)

	if result.Status != action.StatusFailure {
		t.Fatalf("expected failure, got %v", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), "permission denied") || strings.Contains(result.Error.Error(), ")]}'") {
		t.Fatalf("expected sanitized Gerrit error, got %v", result.Error)
	}
}

func TestGerritReviewAction_ValidateWith(t *testing.T) {
	actionNode := NewGerritReviewAction(nil)

	errs := actionNode.ValidateWith(map[string]string{
		"url":           "https://user:secret@gerrit.example.com",
		"change":        "1",
		"message":       "review",
		"value":         "+1",
		"username":      "ci",
		"password_file": ".vectis/secrets/gerrit/http-password",
	})

	if len(errs) != 2 {
		t.Fatalf("errs = %+v, want embedded credential and missing label", errs)
	}

	messages := map[string]string{}
	for _, err := range errs {
		messages[err.Field] = err.Message
	}

	if messages["url"] != "must not include embedded credentials" {
		t.Fatalf("url error = %q", messages["url"])
	}

	if messages["value"] != "requires label" {
		t.Fatalf("value error = %q", messages["value"])
	}
}

func TestGerritReviewAction_ValidateWithRejectsNonHTTPURL(t *testing.T) {
	actionNode := NewGerritReviewAction(nil)

	errs := actionNode.ValidateWith(map[string]string{
		"url":           "ssh://gerrit.example.com/project",
		"change":        "1",
		"message":       "review",
		"username":      "ci",
		"password_file": ".vectis/secrets/gerrit/http-password",
	})

	if len(errs) != 1 || errs[0].Field != "url" || errs[0].Message != "must use http or https" {
		t.Fatalf("errs = %+v, want http/https URL error", errs)
	}
}

func TestGerritReviewEndpointEscapesPathSegments(t *testing.T) {
	got, err := gerritReviewEndpoint("http://gerrit.example.com/r/", "project~feature/x~Iabc", "current")
	if err != nil {
		t.Fatal(err)
	}

	want := "http://gerrit.example.com/r/a/changes/project~feature%2Fx~Iabc/revisions/current/review"
	if got != want {
		t.Fatalf("endpoint = %q, want %q", got, want)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
