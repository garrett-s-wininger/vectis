package scmpoller

import (
	"context"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
)

func TestServiceProcessClaimsPollsRecordsEventsAndCompletes(t *testing.T) {
	clock := mocks.NewMockClock()
	now := clock.Now().UTC().Truncate(time.Second)
	repo := &fakePollRepository{
		ready: []dal.SCMPollTriggerSpec{{
			ID:         7,
			TriggerID:  42,
			JobID:      "build-main",
			Provider:   "gerrit",
			BaseURL:    "http://gerrit.example.com",
			Project:    "project",
			Branch:     "master",
			Interval:   2 * time.Minute,
			NextPollAt: now.Add(-time.Minute),
			Cursor:     "cursor-0",
		}},
	}

	provider := &fakeProvider{
		result: PollResult{
			Events: []Event{{
				Key:         "gerrit:project:master:Iabc:rev1",
				PayloadJSON: `{"change_id":"Iabc","revision":"rev1"}`,
			}},
			Cursor: "cursor-1",
		},
	}

	svc := NewServiceWithRepository(mocks.NopLogger{}, repo, clock)
	svc.SetInstanceID("poller-a")
	svc.SetClaimTTL(5 * time.Minute)
	svc.RegisterProvider("gerrit", provider)

	if err := svc.Process(context.Background()); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if len(repo.claims) != 1 {
		t.Fatalf("claims = %+v", repo.claims)
	}

	if repo.claims[0].specID != 7 || repo.claims[0].observedNextPoll != now.Add(-time.Minute).UTC() {
		t.Fatalf("claim = %+v", repo.claims[0])
	}

	if repo.claims[0].claimedUntil != now.Add(5*time.Minute).UTC() {
		t.Fatalf("claim until = %s", repo.claims[0].claimedUntil)
	}

	if provider.got.TriggerID != 42 || provider.got.Cursor != "cursor-0" || provider.got.Project != "project" {
		t.Fatalf("provider spec = %+v", provider.got)
	}

	if len(repo.events) != 1 || repo.events[0].EventKey != "gerrit:project:master:Iabc:rev1" {
		t.Fatalf("events = %+v", repo.events)
	}

	if len(repo.completions) != 1 {
		t.Fatalf("completions = %+v", repo.completions)
	}

	if repo.completions[0].cursor != "cursor-1" {
		t.Fatalf("completion cursor = %q", repo.completions[0].cursor)
	}

	if repo.completions[0].nextPoll != now.Add(2*time.Minute).UTC() {
		t.Fatalf("completion next poll = %s", repo.completions[0].nextPoll)
	}

	if len(repo.releases) != 0 {
		t.Fatalf("unexpected releases: %+v", repo.releases)
	}
}

type fakeProvider struct {
	got    PollSpec
	result PollResult
	err    error
}

func (p *fakeProvider) Poll(_ context.Context, spec PollSpec) (PollResult, error) {
	p.got = spec
	return p.result, p.err
}

type fakePollRepository struct {
	ready       []dal.SCMPollTriggerSpec
	claims      []claimCall
	completions []completeCall
	releases    []releaseCall
	events      []dal.SCMTriggerEvent
}

type claimCall struct {
	specID           int64
	observedNextPoll time.Time
	claimToken       string
	claimedUntil     time.Time
	now              time.Time
}

type completeCall struct {
	specID     int64
	claimToken string
	nextPoll   time.Time
	cursor     string
}

type releaseCall struct {
	specID     int64
	claimToken string
}

func (r *fakePollRepository) GetReady(_ context.Context, _ time.Time, _ int) ([]dal.SCMPollTriggerSpec, error) {
	return r.ready, nil
}

func (r *fakePollRepository) ClaimDue(_ context.Context, specID int64, observedNextPoll time.Time, claimToken string, claimedUntil, now time.Time) (bool, error) {
	r.claims = append(r.claims, claimCall{
		specID:           specID,
		observedNextPoll: observedNextPoll,
		claimToken:       claimToken,
		claimedUntil:     claimedUntil,
		now:              now,
	})

	return true, nil
}

func (r *fakePollRepository) CompleteClaim(_ context.Context, specID int64, claimToken string, nextPoll time.Time, cursor string) (bool, error) {
	r.completions = append(r.completions, completeCall{
		specID:     specID,
		claimToken: claimToken,
		nextPoll:   nextPoll,
		cursor:     cursor,
	})

	return true, nil
}

func (r *fakePollRepository) ReleaseClaim(_ context.Context, specID int64, claimToken string) error {
	r.releases = append(r.releases, releaseCall{specID: specID, claimToken: claimToken})
	return nil
}

func (r *fakePollRepository) RecordEvent(_ context.Context, event dal.SCMTriggerEvent) (dal.SCMTriggerEventRecord, bool, error) {
	r.events = append(r.events, event)
	return dal.SCMTriggerEventRecord{
		TriggerID:   event.TriggerID,
		EventKey:    event.EventKey,
		PayloadJSON: event.PayloadJSON,
	}, true, nil
}
