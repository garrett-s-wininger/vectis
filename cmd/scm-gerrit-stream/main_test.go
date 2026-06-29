package main

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	scmgerrit "vectis/extensions/scm/gerrit"
	"vectis/internal/scmstream"
	"vectis/sdk/scm"
)

func TestRouteGerritStreamEventBuildsTargetFromPayload(t *testing.T) {
	event, ok, err := scmgerrit.NormalizeStreamEvent([]byte(`{
		"type": "patchset-created",
		"change": {"project": "project", "branch": "master", "id": "Iabc", "number": 42, "status": "NEW"},
		"patchSet": {"revision": "abc123", "ref": "refs/changes/42/42/1"}
	}`), scmgerrit.StreamOptions{BaseURL: "http://gerrit.example.com"})

	if err != nil {
		t.Fatalf("NormalizeStreamEvent returned error: %v", err)
	}

	if !ok {
		t.Fatal("NormalizeStreamEvent filtered event")
	}

	router := &recordingGerritStreamRouter{result: scmstream.RouteResult{Candidates: 2, Matched: 1, Handled: 1}}
	result, err := routeGerritStreamEvent(context.Background(), "http://gerrit.example.com/", router, event)
	if err != nil {
		t.Fatalf("routeGerritStreamEvent returned error: %v", err)
	}

	if result.Handled != 1 {
		t.Fatalf("result.Handled = %d, want 1", result.Handled)
	}

	if router.target.Provider != "gerrit" || router.target.BaseURL != "http://gerrit.example.com/" ||
		router.target.Project != "project" || router.target.Branch != "master" {
		t.Fatalf("target = %+v, want Gerrit project/master target", router.target)
	}

	if router.event.Key != event.Key {
		t.Fatalf("event key = %q, want %q", router.event.Key, event.Key)
	}
}

func TestRouteGerritStreamEventPropagatesRouterError(t *testing.T) {
	event, ok, err := scmgerrit.NormalizeStreamEvent([]byte(`{
		"type": "patchset-created",
		"change": {"project": "project", "branch": "master", "id": "Iabc", "number": 42, "status": "NEW"},
		"patchSet": {"revision": "abc123", "ref": "refs/changes/42/42/1"}
	}`), scmgerrit.StreamOptions{BaseURL: "http://gerrit.example.com"})

	if err != nil {
		t.Fatalf("NormalizeStreamEvent returned error: %v", err)
	}

	if !ok {
		t.Fatal("NormalizeStreamEvent filtered event")
	}

	routerErr := errors.New("route failed")
	_, err = routeGerritStreamEvent(context.Background(), "http://gerrit.example.com", &recordingGerritStreamRouter{err: routerErr}, event)
	if !errors.Is(err, routerErr) {
		t.Fatalf("routeGerritStreamEvent error = %v, want %v", err, routerErr)
	}
}

func TestStreamInputAndInstanceDefaults(t *testing.T) {
	if got := streamInputLabel(""); got != "stdin" {
		t.Fatalf("streamInputLabel(empty) = %q, want stdin", got)
	}

	if got := streamInputLabel("-"); got != "stdin" {
		t.Fatalf("streamInputLabel(-) = %q, want stdin", got)
	}

	if got := streamInputLabel("events.json"); got != "events.json" {
		t.Fatalf("streamInputLabel(path) = %q, want path", got)
	}

	if got := streamInstanceID(" \t"); got != "scm-gerrit-stream" {
		t.Fatalf("streamInstanceID(empty) = %q, want default", got)
	}

	if got := streamInstanceID(" stream-a "); got != "stream-a" {
		t.Fatalf("streamInstanceID(custom) = %q, want stream-a", got)
	}
}

func TestOpenStreamInputFile(t *testing.T) {
	path := t.TempDir() + "/events.json"
	if err := os.WriteFile(path, []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write input fixture: %v", err)
	}

	reader, closeInput, err := openStreamInput(path)
	if err != nil {
		t.Fatalf("openStreamInput returned error: %v", err)
	}
	defer func() { _ = closeInput() }()

	var b strings.Builder
	if _, err := io.Copy(&b, reader); err != nil {
		t.Fatalf("read opened input: %v", err)
	}

	if got := b.String(); got != "hello\n" {
		t.Fatalf("input content = %q, want hello newline", got)
	}
}

type recordingGerritStreamRouter struct {
	target scmstream.EventTarget
	event  scm.Event
	result scmstream.RouteResult
	err    error
}

func (r *recordingGerritStreamRouter) HandleEvent(_ context.Context, target scmstream.EventTarget, event scm.Event) (scmstream.RouteResult, error) {
	r.target = target
	r.event = event
	return r.result, r.err
}
