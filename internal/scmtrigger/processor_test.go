package scmtrigger

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/testutil/dbtest"
	"vectis/sdk/scm"
)

func TestProcessorHandleEventCreatesRunAndDispatchesWithSource(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	jobID := "scm-trigger-dispatch"
	definition := `{"id":"scm-trigger-dispatch","root":{"uses":"builtins/shell","with":{"command":"echo scm"}}}`
	if err := repos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	triggerID := insertSCMTriggerTestSpec(t, ctx, db, jobID, "gerrit", "http://gerrit.example.com", "project", "master")
	clock := mocks.NewMockClock()
	clock.SetNow(time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC))
	ingress := &recordingIngress{}

	processor := Processor{
		Events:         repos.SCMPollTriggers(),
		Jobs:           repos.Jobs(),
		Runs:           repos.Runs(),
		Dispatch:       repos.DispatchEvents(),
		Invocations:    repos.TriggerInvocations(),
		Ingress:        ingress,
		Logger:         mocks.NopLogger{},
		Clock:          clock,
		Source:         dal.DispatchSourceSCMGerritStream,
		SourceInstance: "gerrit-stream-a",
	}

	event := scm.Event{
		Key:         "gerrit:server:project~master~Iabc:rev1",
		PayloadJSON: `{"change_id":"Iabc","current_revision":"rev1"}`,
	}

	spec := dal.SCMPollTriggerSpec{
		TriggerID: triggerID,
		JobID:     jobID,
		Provider:  "gerrit",
		BaseURL:   "http://gerrit.example.com",
		Project:   "project",
		Branch:    "master",
		Query:     "status:open",
	}

	if err := processor.HandleEvent(ctx, spec, event); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}

	if len(ingress.submissions) != 1 {
		t.Fatalf("expected one execution submission, got %d", len(ingress.submissions))
	}

	runID := ingress.submissions[0].Envelope.RunID
	if runID == "" {
		t.Fatal("expected dispatched submission to include a run id")
	}

	var eventRunID sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT run_id
		FROM scm_trigger_events
		WHERE trigger_id = ? AND event_key = ?
	`, triggerID, event.Key).Scan(&eventRunID); err != nil {
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
		t.Fatalf("expected scm trigger metadata on run, got %+v", run)
	}

	if run.TriggerSourceInstance == nil || *run.TriggerSourceInstance != "gerrit-stream-a" {
		t.Fatalf("expected stream source instance on run, got %+v", run.TriggerSourceInstance)
	}

	dispatches, err := repos.DispatchEvents().ListByRun(ctx, runID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if len(dispatches) != 2 || dispatches[0].EventType != dal.DispatchEventAttempt || dispatches[1].EventType != dal.DispatchEventSuccess {
		t.Fatalf("unexpected dispatch events: %+v", dispatches)
	}

	if dispatches[0].Source != dal.DispatchSourceSCMGerritStream || dispatches[1].Source != dal.DispatchSourceSCMGerritStream {
		t.Fatalf("unexpected dispatch sources: %+v", dispatches)
	}

	if dispatches[0].SourceInstance != "gerrit-stream-a" || dispatches[1].SourceInstance != "gerrit-stream-a" {
		t.Fatalf("unexpected dispatch source instances: %+v", dispatches)
	}
}

func TestProcessorHandleEventDedupesAcrossSources(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	jobID := "scm-trigger-duplicate"
	definition := `{"id":"scm-trigger-duplicate","root":{"uses":"builtins/shell","with":{"command":"echo scm"}}}`
	if err := repos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	triggerID := insertSCMTriggerTestSpec(t, ctx, db, jobID, "gerrit", "http://gerrit.example.com", "project", "master")
	ingress := &recordingIngress{}
	processor := Processor{
		Events:         repos.SCMPollTriggers(),
		Jobs:           repos.Jobs(),
		Runs:           repos.Runs(),
		Dispatch:       repos.DispatchEvents(),
		Invocations:    repos.TriggerInvocations(),
		Ingress:        ingress,
		Logger:         mocks.NopLogger{},
		Source:         dal.DispatchSourceSCMGerritStream,
		SourceInstance: "gerrit-stream-a",
	}
	pollerProcessor := processor
	pollerProcessor.Source = dal.DispatchSourceSCMPoller
	pollerProcessor.SourceInstance = "scm-poller-a"

	spec := dal.SCMPollTriggerSpec{TriggerID: triggerID, JobID: jobID, Provider: "gerrit", BaseURL: "http://gerrit.example.com", Project: "project", Branch: "master"}
	event := scm.Event{Key: "gerrit:server:project~master~Iabc:rev1", PayloadJSON: `{"change_id":"Iabc"}`}

	if err := processor.HandleEvent(ctx, spec, event); err != nil {
		t.Fatalf("first HandleEvent: %v", err)
	}

	if err := pollerProcessor.HandleEvent(ctx, spec, event); err != nil {
		t.Fatalf("second HandleEvent: %v", err)
	}

	if len(ingress.submissions) != 1 {
		t.Fatalf("expected duplicate event not to dispatch again, got %d submissions", len(ingress.submissions))
	}

	run, err := repos.Runs().GetRun(ctx, ingress.submissions[0].Envelope.RunID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	if run.TriggerSourceInstance == nil || *run.TriggerSourceInstance != "gerrit-stream-a" {
		t.Fatalf("expected first source instance to remain on run, got %+v", run.TriggerSourceInstance)
	}

	var firstSource, firstInstance, lastSource, lastInstance string
	var observationCount int
	if err := db.QueryRowContext(ctx, `
		SELECT first_observed_source, first_observed_source_instance, last_observed_source, last_observed_source_instance, observation_count
		FROM scm_trigger_events
		WHERE trigger_id = ? AND event_key = ?
	`, triggerID, event.Key).Scan(&firstSource, &firstInstance, &lastSource, &lastInstance, &observationCount); err != nil {
		t.Fatalf("read scm trigger event observations: %v", err)
	}

	if firstSource != dal.DispatchSourceSCMGerritStream || firstInstance != "gerrit-stream-a" ||
		lastSource != dal.DispatchSourceSCMPoller || lastInstance != "scm-poller-a" ||
		observationCount != 2 {
		t.Fatalf("unexpected observation metadata: first=%s/%s last=%s/%s count=%d", firstSource, firstInstance, lastSource, lastInstance, observationCount)
	}
}

func insertSCMTriggerTestSpec(t *testing.T, ctx context.Context, db *sql.DB, jobID, provider, baseURL, project, branch string) int64 {
	t.Helper()

	result, err := db.ExecContext(ctx,
		"INSERT INTO job_triggers (job_id, trigger_type) VALUES (?, ?)",
		jobID,
		dal.TriggerTypeSCMPoll,
	)

	if err != nil {
		t.Fatalf("insert scm trigger: %v", err)
	}

	triggerID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("scm trigger id: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO scm_poll_trigger_specs (trigger_id, provider, base_url, project, branch, interval_seconds, next_poll_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, triggerID, provider, baseURL, project, branch, 60, time.Now().UTC().Format(time.RFC3339)); err != nil {
		t.Fatalf("insert scm trigger spec: %v", err)
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
