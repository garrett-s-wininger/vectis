package gerrit

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"vectis/sdk/scm"
)

func TestProviderPollBootstrapsCursorWithoutEvents(t *testing.T) {
	var gotPath string
	var gotQuery url.Values
	provider := NewProvider(WithHTTPClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		gotPath = r.URL.EscapedPath()
		gotQuery = r.URL.Query()
		return gerritChangesResponse(r, `[{"id":"project~main~Iabc","project":"project","branch":"main","status":"NEW","change_id":"Iabc","current_revision":"rev1","revisions":{"rev1":{"ref":"refs/changes/01/1/1"}}}]`), nil
	})}))

	result, err := provider.Poll(context.Background(), scm.PollSpec{
		Provider: "gerrit",
		BaseURL:  "http://gerrit.example.com/r/",
		Project:  "project",
		Branch:   "main",
	})

	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if len(result.Events) != 0 {
		t.Fatalf("events = %+v, want none during cursor bootstrap", result.Events)
	}

	assertCursorChanges(t, result.Cursor, map[string]string{"project~main~Iabc": "rev1"})
	if gotPath != "/r/changes/" {
		t.Fatalf("path = %q", gotPath)
	}

	if gotQuery.Get("q") != "project:project branch:main status:open" {
		t.Fatalf("q = %q", gotQuery.Get("q"))
	}
}

func TestProviderPollEmitsNewAndUpdatedChanges(t *testing.T) {
	cursor, err := encodeCursor(map[string]string{"project~main~Iabc": "rev1"})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	provider := NewProvider(WithHTTPClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return gerritChangesResponse(r, `[
			{"id":"project~main~Iabc","project":"project","branch":"main","status":"NEW","change_id":"Iabc","current_revision":"rev2","revisions":{"rev2":{"ref":"refs/changes/01/1/2"}}},
			{"id":"project~main~Idef","project":"project","branch":"main","status":"NEW","change_id":"Idef","current_revision":"rev1","revisions":{"rev1":{"ref":"refs/changes/02/2/1"}}}
		]`), nil
	})}))

	result, err := provider.Poll(context.Background(), scm.PollSpec{
		Provider: "gerrit",
		BaseURL:  "http://gerrit.example.com",
		Project:  "project",
		Branch:   "main",
		Query:    "status:open label:Verified=0",
		Cursor:   cursor,
	})

	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if len(result.Events) != 2 {
		t.Fatalf("events = %+v, want two events", result.Events)
	}

	if !strings.Contains(result.Events[0].Key, ":project~main~Iabc:rev2") || !strings.Contains(result.Events[1].Key, ":project~main~Idef:rev1") {
		t.Fatalf("event order/keys = %+v", result.Events)
	}

	var payload eventPayload
	if err := json.Unmarshal([]byte(result.Events[0].PayloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if payload.Provider != "gerrit" || payload.Project != "project" || payload.Branch != "main" ||
		payload.ChangeID != "Iabc" || payload.CurrentRevision != "rev2" || payload.PreviousRevision != "rev1" ||
		payload.Ref != "refs/changes/01/1/2" || payload.Query != "project:project branch:main status:open label:Verified=0" {
		t.Fatalf("payload = %+v", payload)
	}

	assertCursorChanges(t, result.Cursor, map[string]string{"project~main~Iabc": "rev2", "project~main~Idef": "rev1"})
}

func TestProviderPollSkipsUnchangedChanges(t *testing.T) {
	cursor, err := encodeCursor(map[string]string{"project~main~Iabc": "rev1"})
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}

	provider := NewProvider(WithHTTPClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return gerritChangesResponse(r, `[{"id":"project~main~Iabc","project":"project","branch":"main","change_id":"Iabc","current_revision":"rev1","revisions":{"rev1":{"ref":"refs/changes/01/1/1"}}}]`), nil
	})}))

	result, err := provider.Poll(context.Background(), scm.PollSpec{
		BaseURL: "http://gerrit.example.com",
		Project: "project",
		Cursor:  cursor,
	})

	if err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if len(result.Events) != 0 {
		t.Fatalf("events = %+v, want unchanged change skipped", result.Events)
	}
}

func TestProviderPollRejectsInvalidCursor(t *testing.T) {
	provider := NewProvider(WithHTTPClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return gerritChangesResponse(r, `[]`), nil
	})}))

	_, err := provider.Poll(context.Background(), scm.PollSpec{
		BaseURL: "http://gerrit.example.com",
		Cursor:  "not-json",
	})

	if err == nil || !strings.Contains(err.Error(), "decode gerrit cursor") {
		t.Fatalf("Poll error = %v, want decode gerrit cursor", err)
	}
}

func TestProviderPollUsesAuthenticatedGerritEndpointWhenConfigured(t *testing.T) {
	var gotPath string
	var gotUser string
	var gotPassword string
	provider := NewProvider(
		WithBasicAuth("ci-bot", "secret"),
		WithHTTPClient(&http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			gotPath = r.URL.EscapedPath()
			gotUser, gotPassword, _ = r.BasicAuth()
			return gerritChangesResponse(r, `[]`), nil
		})}),
	)

	if _, err := provider.Poll(context.Background(), scm.PollSpec{BaseURL: "http://gerrit.example.com", Cursor: `{"version":1,"changes":{}}`}); err != nil {
		t.Fatalf("Poll: %v", err)
	}

	if gotPath != "/a/changes/" {
		t.Fatalf("path = %q", gotPath)
	}

	if gotUser != "ci-bot" || gotPassword != "secret" {
		t.Fatalf("basic auth = %q/%q", gotUser, gotPassword)
	}
}

func TestProviderPollRequiresBaseURL(t *testing.T) {
	_, err := NewProvider().Poll(context.Background(), scm.PollSpec{Project: "project"})
	if err == nil || !strings.Contains(err.Error(), "requires base_url") {
		t.Fatalf("Poll error = %v, want base_url requirement", err)
	}
}

func assertCursorChanges(t *testing.T, raw string, want map[string]string) {
	t.Helper()

	got, bootstrapped, err := decodeCursor(raw)
	if err != nil {
		t.Fatalf("decode cursor: %v", err)
	}

	if !bootstrapped {
		t.Fatalf("cursor bootstrapped = false")
	}

	if len(got) != len(want) {
		t.Fatalf("cursor = %+v, want %+v", got, want)
	}

	for key, value := range want {
		if got[key] != value {
			t.Fatalf("cursor[%q] = %q, want %q in %+v", key, got[key], value, got)
		}
	}
}

func gerritChangesResponse(req *http.Request, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       io.NopCloser(strings.NewReader(")]}'\n" + body)),
		Request:    req,
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
