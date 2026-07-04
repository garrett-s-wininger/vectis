package scmstream

import (
	"context"
	"errors"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/scmtrigger"
	"vectis/sdk/scm"
)

func TestRouterRoutesMatchingSpecs(t *testing.T) {
	specs := &fakeSpecRepository{specs: []dal.SCMPollTriggerSpec{
		{TriggerID: 1, Provider: "gerrit", BaseURL: "http://gerrit.example.com/", Project: "project", Branch: "master", Query: "status:open"},
		{TriggerID: 2, Provider: "gerrit", BaseURL: "http://gerrit.example.com", Project: "project", Branch: "stable"},
		{TriggerID: 3, Provider: "gerrit", BaseURL: "http://other.example.com", Project: "project", Branch: "master"},
		{TriggerID: 4, Provider: "git", BaseURL: "http://gerrit.example.com", Project: "project", Branch: "master"},
	}}

	processor := &fakeEventProcessor{result: scmtrigger.HandleResult{
		EventCreated: true,
		RunCreated:   true,
		Dispatched:   true,
	}}

	router := Router{
		Specs:     specs,
		Processor: processor,
		Matcher: func(spec dal.SCMPollTriggerSpec, event scm.Event) bool {
			return spec.Query == "status:open" && event.Key == "event-1"
		},
	}

	result, err := router.HandleEvent(context.Background(), EventTarget{
		Provider: "gerrit",
		BaseURL:  "http://gerrit.example.com",
		Project:  "project",
		Branch:   "master",
	}, scm.Event{Key: "event-1", PayloadJSON: `{"change_id":"Iabc"}`})

	if err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	if result.Candidates != 4 || result.Matched != 1 || result.Handled != 1 {
		t.Fatalf("result = %+v", result)
	}

	if result.EventsCreated != 1 || result.RunsCreated != 1 || result.Dispatched != 1 || result.AlreadyDispatched != 0 {
		t.Fatalf("route outcomes = %+v", result)
	}

	if len(processor.calls) != 1 || processor.calls[0].TriggerID != 1 {
		t.Fatalf("processor calls = %+v", processor.calls)
	}

	if specs.provider != "gerrit" || specs.limit != DefaultSpecLimit {
		t.Fatalf("spec lookup = provider %q limit %d", specs.provider, specs.limit)
	}
}

func TestRouterAllowsWildcardProjectAndBranch(t *testing.T) {
	specs := &fakeSpecRepository{specs: []dal.SCMPollTriggerSpec{
		{TriggerID: 1, Provider: "gerrit", BaseURL: "http://gerrit.example.com"},
	}}

	processor := &fakeEventProcessor{}
	router := Router{Specs: specs, Processor: processor}

	result, err := router.HandleEvent(context.Background(), EventTarget{
		Provider: "gerrit",
		BaseURL:  "http://gerrit.example.com",
		Project:  "project",
		Branch:   "master",
	}, scm.Event{Key: "event-1"})

	if err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	if result.Handled != 1 || len(processor.calls) != 1 {
		t.Fatalf("result=%+v calls=%+v", result, processor.calls)
	}
}

func TestRouterCountsAlreadyDispatchedEvents(t *testing.T) {
	specs := &fakeSpecRepository{specs: []dal.SCMPollTriggerSpec{
		{TriggerID: 1, Provider: "gerrit", BaseURL: "http://gerrit.example.com"},
	}}

	processor := &fakeEventProcessor{result: scmtrigger.HandleResult{
		EventKey:          "event-1",
		AlreadyDispatched: true,
	}}

	router := Router{Specs: specs, Processor: processor}
	result, err := router.HandleEvent(context.Background(), EventTarget{
		Provider: "gerrit",
		BaseURL:  "http://gerrit.example.com",
	}, scm.Event{Key: "event-1"})

	if err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	if result.Handled != 1 || result.Dispatched != 0 || result.AlreadyDispatched != 1 {
		t.Fatalf("result = %+v", result)
	}
}

func TestRouterPropagatesProcessorError(t *testing.T) {
	wantErr := errors.New("dispatch failed")
	specs := &fakeSpecRepository{specs: []dal.SCMPollTriggerSpec{
		{TriggerID: 1, Provider: "gerrit", BaseURL: "http://gerrit.example.com"},
	}}

	processor := &fakeEventProcessor{err: wantErr}
	router := Router{Specs: specs, Processor: processor}

	result, err := router.HandleEvent(context.Background(), EventTarget{
		Provider: "gerrit",
		BaseURL:  "http://gerrit.example.com",
	}, scm.Event{Key: "event-1"})

	if !errors.Is(err, wantErr) {
		t.Fatalf("HandleEvent error = %v, want %v", err, wantErr)
	}

	if result.Matched != 1 || result.Handled != 0 {
		t.Fatalf("result = %+v", result)
	}
}

type fakeSpecRepository struct {
	provider string
	limit    int
	specs    []dal.SCMPollTriggerSpec
	err      error
}

func (r *fakeSpecRepository) ListEnabledByProvider(_ context.Context, provider string, limit int) ([]dal.SCMPollTriggerSpec, error) {
	r.provider = provider
	r.limit = limit
	return r.specs, r.err
}

type fakeEventProcessor struct {
	calls  []dal.SCMPollTriggerSpec
	result scmtrigger.HandleResult
	err    error
}

func (p *fakeEventProcessor) HandleEvent(_ context.Context, spec dal.SCMPollTriggerSpec, _ scm.Event) (scmtrigger.HandleResult, error) {
	if p.err != nil {
		return scmtrigger.HandleResult{}, p.err
	}

	p.calls = append(p.calls, spec)
	return p.result, nil
}
