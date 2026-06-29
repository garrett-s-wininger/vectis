package scmpoller

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"vectis/internal/action/builtins"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
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

func TestServiceProcessCreatesRunAndDispatchesNewSCMEvent(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	jobID := "scm-dispatch"
	definition := `{"id":"scm-dispatch","root":{"uses":"builtins/script","with":{"script":"echo scm"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, definition); err != nil {
		t.Fatalf("create job: %v", err)
	}

	now := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	triggerID := insertSCMPollTestTrigger(t, ctx, db, jobID, "gerrit", "project", "master", now.Add(-time.Minute))

	clock := mocks.NewMockClock()
	clock.SetNow(now)
	ingress := &recordingIngress{}
	provider := &fakeProvider{
		result: PollResult{
			Events: []Event{{
				Key:         "gerrit:project:master:Iabc:rev1",
				PayloadJSON: `{"change_id":"Iabc","revision":"rev1"}`,
			}},
			Cursor: "cursor-1",
		},
	}

	logger := mocks.NewMockLogger()
	svc := NewService(logger, db)
	svc.SetInstanceID("scm-poller-a")
	svc.SetClock(clock)
	svc.SetClaimTTL(5 * time.Minute)
	svc.SetExecutionIngress(ingress)
	svc.SetActionDescriptorResolver(builtins.NewRegistry())
	svc.RegisterProvider("gerrit", provider)

	if err := svc.Process(ctx); err != nil {
		t.Fatalf("Process: %v", err)
	}

	if len(ingress.submissions) != 1 {
		t.Fatalf("expected one execution submission, got %d; errors=%v", len(ingress.submissions), logger.GetErrorCalls())
	}

	submission := ingress.submissions[0]
	runID := submission.Envelope.RunID
	if runID == "" {
		t.Fatal("expected dispatched submission to include a run id")
	}

	if submission.Request.GetJob().GetId() != jobID || submission.Request.GetJob().GetRunId() != runID {
		t.Fatalf("unexpected dispatched job: %+v", submission.Request.GetJob())
	}

	var eventRunID sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT run_id
		FROM scm_trigger_events
		WHERE trigger_id = ? AND event_key = ?
	`, triggerID, "gerrit:project:master:Iabc:rev1").Scan(&eventRunID); err != nil {
		t.Fatalf("read scm trigger event run_id: %v", err)
	}

	if !eventRunID.Valid || eventRunID.String != runID {
		t.Fatalf("expected scm event run_id %q, got %+v", runID, eventRunID)
	}

	run, err := repos.Runs().GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	if run.TriggerInvocationID == nil || run.TriggerType == nil || *run.TriggerType != dal.TriggerTypeSCMPoll {
		t.Fatalf("expected scm poll trigger metadata on run, got %+v", run)
	}

	if run.TriggerSourceInstance == nil || *run.TriggerSourceInstance != "scm-poller-a" {
		t.Fatalf("expected scm poll trigger source instance on run, got %+v", run.TriggerSourceInstance)
	}

	if run.ExecutionPayloadHash == "" {
		t.Fatalf("expected execution payload hash on run %+v", run)
	}

	dispatches, err := repos.DispatchEvents().ListByRun(ctx, runID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if len(dispatches) != 2 || dispatches[0].EventType != dal.DispatchEventAttempt || dispatches[1].EventType != dal.DispatchEventSuccess {
		t.Fatalf("unexpected dispatch events: %+v", dispatches)
	}

	if dispatches[0].Source != dal.DispatchSourceSCMPoller || dispatches[1].Source != dal.DispatchSourceSCMPoller {
		t.Fatalf("unexpected dispatch event sources: %+v", dispatches)
	}

	if dispatches[0].SourceInstance != "scm-poller-a" || dispatches[1].SourceInstance != "scm-poller-a" {
		t.Fatalf("unexpected dispatch source instances: %+v", dispatches)
	}
}

func insertSCMPollTestTrigger(t *testing.T, ctx context.Context, db *sql.DB, jobID, provider, project, branch string, nextPoll time.Time) int64 {
	t.Helper()

	result, err := db.ExecContext(ctx,
		"INSERT INTO job_triggers (job_id, trigger_type) VALUES (?, ?)",
		jobID,
		dal.TriggerTypeSCMPoll,
	)

	if err != nil {
		t.Fatalf("insert scm poll trigger: %v", err)
	}

	triggerID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("scm poll trigger id: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO scm_poll_trigger_specs (trigger_id, provider, project, branch, interval_seconds, next_poll_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, triggerID, provider, project, branch, 60, nextPoll.Format(time.RFC3339)); err != nil {
		t.Fatalf("insert scm poll trigger spec: %v", err)
	}

	return triggerID
}

type recordingIngress struct {
	submissions []cell.ExecutionSubmission
}

func (i *recordingIngress) SubmitExecution(_ context.Context, submission cell.ExecutionSubmission) error {
	i.submissions = append(i.submissions, submission)
	return nil
}

var _ cell.ExecutionIngress = (*recordingIngress)(nil)

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

func (r *fakePollRepository) ListEnabledByProvider(_ context.Context, _ string, _ int) ([]dal.SCMPollTriggerSpec, error) {
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
