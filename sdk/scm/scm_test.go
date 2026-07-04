package scm

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestCurrentRevisionRef(t *testing.T) {
	change := Change{
		CurrentRevision: "abc",
		Revisions: map[string]Revision{
			"abc": {Ref: "refs/changes/01/1/1"},
		},
	}

	revision, ref, err := change.CurrentRevisionRef()
	if err != nil {
		t.Fatalf("CurrentRevisionRef: %v", err)
	}

	if revision != "abc" || ref != "refs/changes/01/1/1" {
		t.Fatalf("revision/ref = %q/%q", revision, ref)
	}
}

func TestFindChangeMatchesChangeIDOrProviderID(t *testing.T) {
	changes := []Change{
		{ID: "provider-id", ChangeID: "Iabc"},
	}

	if _, ok := FindChange(changes, "Iabc"); !ok {
		t.Fatal("FindChange did not match change_id")
	}

	if _, ok := FindChange(changes, "provider-id"); !ok {
		t.Fatal("FindChange did not match provider id")
	}
}

func TestPollChangeRetriesUntilProviderFindsChange(t *testing.T) {
	provider := &fakeProvider{
		responses: [][]Change{
			nil,
			{{ID: "project~master~Iabc", ChangeID: "Iabc"}},
		},
	}

	change, err := PollChange(context.Background(), provider, PollOptions{
		Query:    Query{Project: "project", Branch: "master", Status: "open"},
		ChangeID: "Iabc",
		Timeout:  time.Second,
		Interval: time.Millisecond,
	})

	if err != nil {
		t.Fatalf("PollChange: %v", err)
	}

	if change.ChangeID != "Iabc" {
		t.Fatalf("change = %+v", change)
	}

	if provider.calls != 2 {
		t.Fatalf("calls = %d, want 2", provider.calls)
	}

	if provider.queries[0].ChangeID != "Iabc" {
		t.Fatalf("query ChangeID = %q, want filled from options", provider.queries[0].ChangeID)
	}
}

func TestPollChangeRequiresChangeID(t *testing.T) {
	_, err := PollChange(context.Background(), &fakeProvider{}, PollOptions{})
	if err == nil || !strings.Contains(err.Error(), "change_id is required") {
		t.Fatalf("PollChange error = %v, want change_id required", err)
	}
}

type fakeProvider struct {
	responses [][]Change
	queries   []Query
	calls     int
}

func (p *fakeProvider) QueryChanges(_ context.Context, query Query) ([]Change, error) {
	p.calls++
	p.queries = append(p.queries, query)

	if len(p.responses) == 0 {
		return nil, nil
	}

	response := p.responses[0]
	p.responses = p.responses[1:]
	return response, nil
}
