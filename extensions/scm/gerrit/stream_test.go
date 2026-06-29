package gerrit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"vectis/sdk/scm"
)

func TestNormalizeStreamEventPatchSetCreatedUsesPollCompatibleKey(t *testing.T) {
	event, ok, err := NormalizeStreamEvent([]byte(`{
		"type":"patchset-created",
		"change":{
			"project":"project",
			"branch":"master",
			"id":"Iabc",
			"number":"17",
			"status":"NEW"
		},
		"patchSet":{
			"revision":"rev2",
			"ref":"refs/changes/17/17/2"
		}
	}`), StreamOptions{
		BaseURL: "http://gerrit.example.com/",
		Project: "project",
		Branch:  "master",
		Query:   "status:open",
	})

	if err != nil {
		t.Fatalf("NormalizeStreamEvent: %v", err)
	}

	if !ok {
		t.Fatal("expected stream event to be normalized")
	}

	if !strings.Contains(event.Key, ":project~master~Iabc:rev2") {
		t.Fatalf("event key = %q", event.Key)
	}

	var payload eventPayload
	if err := json.Unmarshal([]byte(event.PayloadJSON), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}

	if payload.Provider != "gerrit" || payload.EventType != "patchset-created" || payload.Project != "project" ||
		payload.Branch != "master" || payload.Query != "status:open" || payload.ChangeID != "Iabc" ||
		payload.ID != "project~master~Iabc" || payload.Number != 17 || payload.Status != "NEW" ||
		payload.CurrentRevision != "rev2" || payload.Ref != "refs/changes/17/17/2" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestNormalizeStreamEventFiltersProjectAndBranch(t *testing.T) {
	event, ok, err := NormalizeStreamEvent([]byte(`{
		"type":"comment-added",
		"change":{"project":"project","branch":"master","id":"Iabc","number":17},
		"patchSet":{"revision":"rev2","ref":"refs/changes/17/17/2"}
	}`), StreamOptions{Project: "other", Branch: "master"})

	if err != nil {
		t.Fatalf("NormalizeStreamEvent: %v", err)
	}

	if ok || event.Key != "" {
		t.Fatalf("expected event to be filtered out, got ok=%t event=%+v", ok, event)
	}
}

func TestNormalizeStreamEventSkipsUnsupportedStreamEvent(t *testing.T) {
	event, ok, err := NormalizeStreamEvent([]byte(`{
		"type":"ref-updated",
		"refUpdate":{"project":"project","refName":"refs/heads/master","newRev":"abc"}
	}`), StreamOptions{})

	if err != nil {
		t.Fatalf("NormalizeStreamEvent: %v", err)
	}

	if ok || event.Key != "" {
		t.Fatalf("expected event to be skipped, got ok=%t event=%+v", ok, event)
	}
}

func TestNormalizeStreamEventRejectsMalformedPatchSetEvent(t *testing.T) {
	_, ok, err := NormalizeStreamEvent([]byte(`{
		"type":"patchset-created",
		"change":{"project":"project","branch":"master","id":"Iabc"},
		"patchSet":{"ref":"refs/changes/17/17/2"}
	}`), StreamOptions{BaseURL: "http://gerrit.example.com"})

	if err == nil || !strings.Contains(err.Error(), "missing patchSet.revision") {
		t.Fatalf("NormalizeStreamEvent error = %v, want missing revision", err)
	}

	if ok {
		t.Fatal("expected malformed event not to normalize")
	}
}

func TestNormalizeStreamEventRequiresBaseURLForChangeEvents(t *testing.T) {
	_, ok, err := NormalizeStreamEvent([]byte(`{
		"type":"patchset-created",
		"change":{"project":"project","branch":"master","id":"Iabc"},
		"patchSet":{"revision":"rev2","ref":"refs/changes/17/17/2"}
	}`), StreamOptions{})

	if err == nil || !strings.Contains(err.Error(), "requires base_url") {
		t.Fatalf("NormalizeStreamEvent error = %v, want base_url requirement", err)
	}

	if ok {
		t.Fatal("expected event without base_url not to normalize")
	}
}

func TestConsumeStreamEmitsNormalizedEvents(t *testing.T) {
	input := strings.NewReader(`
{"type":"ref-updated","refUpdate":{"project":"project"}}
{"type":"patchset-created","change":{"project":"project","branch":"master","id":"Iabc","number":17},"patchSet":{"revision":"rev2","ref":"refs/changes/17/17/2"}}

{"type":"comment-added","change":{"project":"project","branch":"master","id":"Iabc","number":17},"patchSet":{"revision":"rev2","ref":"refs/changes/17/17/2"}}
`)

	var events []string
	err := ConsumeStream(context.Background(), input, StreamOptions{BaseURL: "http://gerrit.example.com"}, func(_ context.Context, event scm.Event) error {
		events = append(events, event.Key)
		return nil
	})

	if err != nil {
		t.Fatalf("ConsumeStream: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("events = %+v, want two normalized change events", events)
	}

	if events[0] != events[1] {
		t.Fatalf("events should dedupe to the same revision key, got %+v", events)
	}
}

func TestStreamEventMatchesQueryConservatively(t *testing.T) {
	event, ok, err := NormalizeStreamEvent([]byte(`{
		"type":"patchset-created",
		"change":{"project":"project","branch":"master","id":"Iabc","number":17,"status":"NEW"},
		"patchSet":{"revision":"rev2","ref":"refs/changes/17/17/2"}
	}`), StreamOptions{BaseURL: "http://gerrit.example.com"})

	if err != nil {
		t.Fatalf("NormalizeStreamEvent: %v", err)
	}

	if !ok {
		t.Fatal("expected stream event")
	}

	for _, query := range []string{"", "status:open", "is:open"} {
		if !StreamEventMatchesQuery(event, query) {
			t.Fatalf("StreamEventMatchesQuery(%q) = false, want true", query)
		}
	}

	for _, query := range []string{"label:Verified=0", "status:merged"} {
		if StreamEventMatchesQuery(event, query) {
			t.Fatalf("StreamEventMatchesQuery(%q) = true, want false", query)
		}
	}
}

func TestStreamEventInfoFromEvent(t *testing.T) {
	event, ok, err := NormalizeStreamEvent([]byte(`{
		"type":"patchset-created",
		"change":{"project":"project","branch":"master","id":"Iabc","number":17,"status":"NEW"},
		"patchSet":{"revision":"rev2","ref":"refs/changes/17/17/2"}
	}`), StreamOptions{BaseURL: "http://gerrit.example.com"})

	if err != nil {
		t.Fatalf("NormalizeStreamEvent: %v", err)
	}

	if !ok {
		t.Fatal("expected stream event")
	}

	info, err := StreamEventInfoFromEvent(event)
	if err != nil {
		t.Fatalf("StreamEventInfoFromEvent: %v", err)
	}

	if info.Provider != "gerrit" || info.EventType != "patchset-created" || info.Project != "project" ||
		info.Branch != "master" || info.ChangeID != "Iabc" || info.ID != "project~master~Iabc" ||
		info.Number != 17 || info.Status != "NEW" || info.CurrentRevision != "rev2" || info.Ref != "refs/changes/17/17/2" {
		t.Fatalf("info = %+v", info)
	}
}
