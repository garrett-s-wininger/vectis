package dal_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"
)

func TestJobsRepository_CRUDAndConflict(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	jobs := repos.Jobs()
	ctx := context.Background()

	jobID := "job-a"
	def1 := `{"id":"job-a","root":{"uses":"builtins/shell"}}`
	def2 := `{"id":"job-a","root":{"uses":"builtins/shell","with":{"command":"echo hi"}}}`

	if err := jobs.Create(ctx, jobID, def1, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	if err := jobs.Create(ctx, jobID, def1, 1); !dal.IsConflict(err) {
		t.Fatalf("expected conflict on duplicate create, got: %v", err)
	}

	gotDef, version, err := jobs.GetDefinition(ctx, jobID)
	if err != nil {
		t.Fatalf("get definition: %v", err)
	}

	if gotDef != def1 {
		t.Fatalf("definition mismatch: got %q want %q", gotDef, def1)
	}

	if version != 1 {
		t.Fatalf("expected initial version 1, got %d", version)
	}

	newVersion, err := jobs.UpdateDefinition(ctx, jobID, def2)
	if err != nil {
		t.Fatalf("update definition: %v", err)
	}

	if newVersion != 2 {
		t.Fatalf("expected version 2 after update, got %d", newVersion)
	}

	gotDef, version, err = jobs.GetDefinition(ctx, jobID)
	if err != nil {
		t.Fatalf("get definition after update: %v", err)
	}

	if gotDef != def2 {
		t.Fatalf("updated definition mismatch: got %q want %q", gotDef, def2)
	}

	if version != 2 {
		t.Fatalf("expected version 2 in DB, got %d", version)
	}

	gotV1, err := jobs.GetDefinitionVersion(ctx, jobID, 1)
	if err != nil {
		t.Fatalf("get definition version 1: %v", err)
	}

	if gotV1 != def1 {
		t.Fatalf("definition version 1 mismatch: got %q want %q", gotV1, def1)
	}

	gotV2, err := jobs.GetDefinitionVersion(ctx, jobID, 2)
	if err != nil {
		t.Fatalf("get definition version 2: %v", err)
	}

	if gotV2 != def2 {
		t.Fatalf("definition version 2 mismatch: got %q want %q", gotV2, def2)
	}

	var storedHash, versionHash string
	if err := db.QueryRowContext(ctx, "SELECT definition_hash FROM stored_jobs WHERE job_id = ?", jobID).Scan(&storedHash); err != nil {
		t.Fatalf("scan stored job hash: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT definition_hash FROM job_definitions WHERE job_id = ? AND version = 2", jobID).Scan(&versionHash); err != nil {
		t.Fatalf("scan version hash: %v", err)
	}

	if want := dal.DefinitionHash(def2); storedHash != want || versionHash != want {
		t.Fatalf("definition hash mismatch: stored=%q version=%q want=%q", storedHash, versionHash, want)
	}

	list, _, err := jobs.List(ctx, 0, 100)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(list) != 1 {
		t.Fatalf("expected 1 job in list, got %d", len(list))
	}

	if err := jobs.Delete(ctx, jobID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, _, err = jobs.GetDefinition(ctx, jobID)
	if !dal.IsNotFound(err) {
		t.Fatalf("expected not found after delete, got: %v", err)
	}

	def3 := `{"id":"job-a","root":{"uses":"builtins/shell","with":{"command":"echo recreated"}}}`
	if err := jobs.Create(ctx, jobID, def3, 1); err != nil {
		t.Fatalf("recreate job: %v", err)
	}

	_, version, err = jobs.GetDefinition(ctx, jobID)
	if err != nil {
		t.Fatalf("get recreated definition: %v", err)
	}

	if version != 3 {
		t.Fatalf("expected recreated job to continue immutable version history at 3, got %d", version)
	}
}

func TestSQLRepositoriesWithCellID_WritesHomeAndOwningCell(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-cell-owned"
	def := `{"id":"job-cell-owned","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	var namespaceCell, jobCell, definitionCell, runCell, executionCell string
	if err := db.QueryRowContext(ctx, "SELECT home_cell FROM namespaces WHERE id = ?", ns.ID).Scan(&namespaceCell); err != nil {
		t.Fatalf("query namespace cell: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT home_cell FROM stored_jobs WHERE job_id = ?", jobID).Scan(&jobCell); err != nil {
		t.Fatalf("query job cell: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT home_cell FROM job_definitions WHERE job_id = ? AND version = 1", jobID).Scan(&definitionCell); err != nil {
		t.Fatalf("query definition cell: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT owning_cell FROM job_runs WHERE run_id = ?", runID).Scan(&runCell); err != nil {
		t.Fatalf("query run cell: %v", err)
	}

	var segmentCount, executionCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM run_segments WHERE run_id = ?", runID).Scan(&segmentCount); err != nil {
		t.Fatalf("query segment count: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*), cell_id FROM segment_executions WHERE run_id = ? GROUP BY cell_id", runID).Scan(&executionCount, &executionCell); err != nil {
		t.Fatalf("query execution count/cell: %v", err)
	}

	if segmentCount != 1 || executionCount != 1 {
		t.Fatalf("expected one segment and one execution, got segments=%d executions=%d", segmentCount, executionCount)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if dispatch.RunID != runID {
		t.Fatalf("dispatch run id: got %q, want %q", dispatch.RunID, runID)
	}

	if dispatch.JobID != jobID {
		t.Fatalf("dispatch job id: got %q, want %q", dispatch.JobID, jobID)
	}

	if dispatch.SegmentID == "" {
		t.Fatal("dispatch segment id is empty")
	}

	if dispatch.SegmentName != "root" {
		t.Fatalf("dispatch segment name: got %q, want root", dispatch.SegmentName)
	}

	if dispatch.SegmentStatus != dal.SegmentStatusPending {
		t.Fatalf("dispatch segment status: got %q, want %q", dispatch.SegmentStatus, dal.SegmentStatusPending)
	}

	if dispatch.ExecutionID == "" {
		t.Fatal("dispatch execution id is empty")
	}

	if dispatch.ExecutionStatus != dal.ExecutionStatusPending {
		t.Fatalf("dispatch execution status: got %q, want %q", dispatch.ExecutionStatus, dal.ExecutionStatusPending)
	}

	if dispatch.CellID != "iad-a" {
		t.Fatalf("dispatch cell id: got %q, want iad-a", dispatch.CellID)
	}

	if dispatch.Attempt != 1 {
		t.Fatalf("dispatch attempt: got %d, want 1", dispatch.Attempt)
	}

	if dispatch.DefinitionVersion != 1 {
		t.Fatalf("dispatch definition version: got %d, want 1", dispatch.DefinitionVersion)
	}

	if dispatch.DefinitionHash != dal.DefinitionHash(def) {
		t.Fatalf("dispatch definition hash: got %q, want %q", dispatch.DefinitionHash, dal.DefinitionHash(def))
	}

	if dispatch.OwningCell != "iad-a" {
		t.Fatalf("dispatch owning cell: got %q, want iad-a", dispatch.OwningCell)
	}

	for name, got := range map[string]string{
		"namespace":  namespaceCell,
		"job":        jobCell,
		"definition": definitionCell,
		"run":        runCell,
		"execution":  executionCell,
	} {
		if got != "iad-a" {
			t.Fatalf("%s cell: got %q", name, got)
		}
	}
}

func TestRunsRepository_CreateRunInCell_TargetsExecutionCell(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-target", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-target-cell"
	def := `{"id":"job-target-cell","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRunInCell(ctx, jobID, nil, 1, "pdx-b")
	if err != nil {
		t.Fatalf("CreateRunInCell: %v", err)
	}

	var jobHomeCell string
	if err := db.QueryRowContext(ctx, "SELECT home_cell FROM stored_jobs WHERE job_id = ?", jobID).Scan(&jobHomeCell); err != nil {
		t.Fatalf("query job home cell: %v", err)
	}

	if jobHomeCell != "global-a" {
		t.Fatalf("job home cell: got %q, want global-a", jobHomeCell)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if dispatch.CellID != "pdx-b" {
		t.Fatalf("dispatch cell id: got %q, want pdx-b", dispatch.CellID)
	}

	if dispatch.OwningCell != "pdx-b" {
		t.Fatalf("dispatch owning cell: got %q, want pdx-b", dispatch.OwningCell)
	}

	run, err := repos.Runs().GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	if run.OwningCell != "pdx-b" {
		t.Fatalf("run owning cell: got %q, want pdx-b", run.OwningCell)
	}

	defaultRunID, _, err := repos.Runs().CreateRunInCell(ctx, jobID, nil, 1, "")
	if err != nil {
		t.Fatalf("CreateRunInCell default target: %v", err)
	}

	defaultDispatch, err := repos.Runs().GetPendingExecution(ctx, defaultRunID)
	if err != nil {
		t.Fatalf("get default pending execution: %v", err)
	}

	if defaultDispatch.CellID != "global-a" {
		t.Fatalf("default dispatch cell id: got %q, want global-a", defaultDispatch.CellID)
	}
}

func TestRunsRepository_CreateRunsInCells_FanoutTargetsExecutionCells(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-fanout", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-fanout-cells"
	def := `{"id":"job-fanout-cells","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	created, err := repos.Runs().CreateRunsInCells(ctx, jobID, nil, 1, []string{"iad-a", "pdx-b"})
	if err != nil {
		t.Fatalf("CreateRunsInCells: %v", err)
	}

	if len(created) != 2 {
		t.Fatalf("created runs: got %d, want 2", len(created))
	}

	if created[0].RunIndex != 1 || created[1].RunIndex != 2 {
		t.Fatalf("run indexes: got %d/%d, want 1/2", created[0].RunIndex, created[1].RunIndex)
	}

	if created[0].TargetCellID != "iad-a" || created[1].TargetCellID != "pdx-b" {
		t.Fatalf("target cells: got %q/%q, want iad-a/pdx-b", created[0].TargetCellID, created[1].TargetCellID)
	}

	for _, createdRun := range created {
		dispatch, err := repos.Runs().GetPendingExecution(ctx, createdRun.RunID)
		if err != nil {
			t.Fatalf("get pending execution for %s: %v", createdRun.RunID, err)
		}

		if dispatch.CellID != createdRun.TargetCellID {
			t.Fatalf("dispatch cell for %s: got %q, want %q", createdRun.RunID, dispatch.CellID, createdRun.TargetCellID)
		}

		if dispatch.OwningCell != createdRun.TargetCellID {
			t.Fatalf("owning cell for %s: got %q, want %q", createdRun.RunID, dispatch.OwningCell, createdRun.TargetCellID)
		}
	}

	iadRuns, _, err := repos.Runs().ListByJob(ctx, jobID, nil, nil, "iad-a", 0, 100)
	if err != nil {
		t.Fatalf("list iad-a runs: %v", err)
	}

	if len(iadRuns) != 1 || iadRuns[0].RunID != created[0].RunID || iadRuns[0].OwningCell != "iad-a" {
		t.Fatalf("expected only iad-a run in list, got %+v", iadRuns)
	}

	pdxRuns, _, err := repos.Runs().ListByJob(ctx, jobID, nil, nil, "pdx-b", 0, 100)
	if err != nil {
		t.Fatalf("list pdx-b runs: %v", err)
	}

	if len(pdxRuns) != 1 || pdxRuns[0].RunID != created[1].RunID || pdxRuns[0].OwningCell != "pdx-b" {
		t.Fatalf("expected only pdx-b run in list, got %+v", pdxRuns)
	}
}

func TestSQLRepositories_CreateDefinitionAndRunInCell_TargetsExecutionCell(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	jobID := "ephemeral-target-cell"
	def := `{"id":"ephemeral-target-cell","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	runID, _, err := repos.CreateDefinitionAndRunInCell(ctx, jobID, def, nil, "pdx-b")
	if err != nil {
		t.Fatalf("CreateDefinitionAndRunInCell: %v", err)
	}

	var definitionHomeCell string
	if err := db.QueryRowContext(ctx, "SELECT home_cell FROM job_definitions WHERE job_id = ? AND version = 1", jobID).Scan(&definitionHomeCell); err != nil {
		t.Fatalf("query definition home cell: %v", err)
	}

	if definitionHomeCell != "global-a" {
		t.Fatalf("definition home cell: got %q, want global-a", definitionHomeCell)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if dispatch.CellID != "pdx-b" {
		t.Fatalf("dispatch cell id: got %q, want pdx-b", dispatch.CellID)
	}

	if dispatch.OwningCell != "pdx-b" {
		t.Fatalf("dispatch owning cell: got %q, want pdx-b", dispatch.OwningCell)
	}
}

func TestRunsRepository_GetPendingExecution_NotFound(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	if _, err := repos.Runs().GetPendingExecution(ctx, "missing-run"); !dal.IsNotFound(err) {
		t.Fatalf("expected missing run to return ErrNotFound, got %v", err)
	}

	ns, err := repos.Namespaces().Create(ctx, "team-pending", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-no-pending-execution"
	def := `{"id":"job-no-pending-execution","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if _, err := db.ExecContext(ctx, "UPDATE segment_executions SET status = 'accepted' WHERE run_id = ?", runID); err != nil {
		t.Fatalf("mark execution accepted: %v", err)
	}

	if _, err := repos.Runs().GetPendingExecution(ctx, runID); !dal.IsNotFound(err) {
		t.Fatalf("expected accepted execution to return ErrNotFound, got %v", err)
	}
}

func TestRunsRepository_ExecutionTransitions(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-transitions", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-execution-transitions"
	def := `{"id":"job-execution-transitions","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if err := repos.Runs().MarkExecutionAccepted(ctx, dispatch.ExecutionID); err != nil {
		t.Fatalf("mark accepted: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)

	if err := repos.Runs().MarkExecutionStarted(ctx, dispatch.ExecutionID); err != nil {
		t.Fatalf("mark started: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusRunning, dal.SegmentStatusRunning, 2)

	if err := repos.Runs().MarkExecutionTerminal(ctx, dispatch.ExecutionID, dal.ExecutionStatusSucceeded); err != nil {
		t.Fatalf("mark terminal: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 3)

	if err := repos.Runs().MarkExecutionStarted(ctx, dispatch.ExecutionID); !dal.IsConflict(err) {
		t.Fatalf("expected conflict restarting terminal execution, got %v", err)
	}
}

func TestRunsRepository_ExecutionTransitionsRejectInvalidTargets(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	if err := repos.Runs().MarkExecutionAccepted(ctx, "missing-execution"); !dal.IsNotFound(err) {
		t.Fatalf("expected missing execution to return ErrNotFound, got %v", err)
	}

	if err := repos.Runs().MarkExecutionTerminal(ctx, "missing-execution", dal.ExecutionStatusRunning); !dal.IsConflict(err) {
		t.Fatalf("expected non-terminal status to return ErrConflict, got %v", err)
	}
}

func assertExecutionAndSegmentStatus(t *testing.T, db *sql.DB, executionID, segmentID, wantExecutionStatus, wantSegmentStatus string, wantEventSequence int64) {
	t.Helper()

	var executionStatus string
	var eventSequence int64
	var acceptedAt, startedAt, finishedAt sql.NullString
	var lastObservedAt sql.NullInt64
	if err := db.QueryRow("SELECT status, accepted_at, started_at, finished_at, last_observed_at, event_sequence FROM segment_executions WHERE execution_id = ?", executionID).
		Scan(&executionStatus, &acceptedAt, &startedAt, &finishedAt, &lastObservedAt, &eventSequence); err != nil {
		t.Fatalf("query execution status: %v", err)
	}

	if executionStatus != wantExecutionStatus {
		t.Fatalf("execution status: got %q, want %q", executionStatus, wantExecutionStatus)
	}

	if eventSequence != wantEventSequence {
		t.Fatalf("event sequence: got %d, want %d", eventSequence, wantEventSequence)
	}

	if !lastObservedAt.Valid || lastObservedAt.Int64 == 0 {
		t.Fatalf("last_observed_at was not set")
	}

	if !acceptedAt.Valid {
		t.Fatalf("accepted_at was not set")
	}

	if wantExecutionStatus == dal.ExecutionStatusRunning && !startedAt.Valid {
		t.Fatalf("started_at was not set")
	}

	if wantExecutionStatus == dal.ExecutionStatusSucceeded && !finishedAt.Valid {
		t.Fatalf("finished_at was not set")
	}

	var segmentStatus string
	if err := db.QueryRow("SELECT status FROM run_segments WHERE segment_id = ?", segmentID).Scan(&segmentStatus); err != nil {
		t.Fatalf("query segment status: %v", err)
	}

	if segmentStatus != wantSegmentStatus {
		t.Fatalf("segment status: got %q, want %q", segmentStatus, wantSegmentStatus)
	}
}

func TestJobsRepository_UpdateBackfillsLegacyCurrentVersion(t *testing.T) {
	db := dbtest.NewTestDB(t)
	jobs := dal.NewSQLRepositories(db).Jobs()
	ctx := context.Background()

	jobID := "legacy-job"
	def1 := `{"id":"legacy-job","root":{"uses":"builtins/shell","with":{"command":"echo old"}}}`
	def2 := `{"id":"legacy-job","root":{"uses":"builtins/shell","with":{"command":"echo new"}}}`
	if _, err := db.ExecContext(ctx, "INSERT INTO stored_jobs (job_id, definition_json, version) VALUES (?, ?, 1)", jobID, def1); err != nil {
		t.Fatalf("insert legacy job: %v", err)
	}

	newVersion, err := jobs.UpdateDefinition(ctx, jobID, def2)
	if err != nil {
		t.Fatalf("update legacy job: %v", err)
	}

	if newVersion != 2 {
		t.Fatalf("expected updated version 2, got %d", newVersion)
	}

	gotV1, err := jobs.GetDefinitionVersion(ctx, jobID, 1)
	if err != nil {
		t.Fatalf("get backfilled version 1: %v", err)
	}

	if gotV1 != def1 {
		t.Fatalf("backfilled version mismatch: got %q want %q", gotV1, def1)
	}

	var hash string
	if err := db.QueryRowContext(ctx, "SELECT definition_hash FROM job_definitions WHERE job_id = ? AND version = 1", jobID).Scan(&hash); err != nil {
		t.Fatalf("scan backfilled hash: %v", err)
	}

	if want := dal.DefinitionHash(def1); hash != want {
		t.Fatalf("backfilled hash: want %q, got %q", want, hash)
	}
}

func TestRunsRepository_CreateRunAndListSinceOrdered(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID1, idx1, err := runs.CreateRun(ctx, "job-order", nil, 1)
	if err != nil {
		t.Fatalf("create run 1: %v", err)
	}

	runID2, idx2, err := runs.CreateRun(ctx, "job-order", nil, 1)
	if err != nil {
		t.Fatalf("create run 2: %v", err)
	}

	if idx1 != 1 || idx2 != 2 {
		t.Fatalf("unexpected run indexes: idx1=%d idx2=%d", idx1, idx2)
	}

	all, _, err := runs.ListByJob(ctx, "job-order", nil, nil, "", 0, 100)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}

	if len(all) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(all))
	}

	if all[0].RunIndex != 1 || all[1].RunIndex != 2 {
		t.Fatalf("runs not ordered asc by run_index: %+v", all)
	}

	since := 1
	after, _, err := runs.ListByJob(ctx, "job-order", &since, nil, "", 0, 100)
	if err != nil {
		t.Fatalf("list since: %v", err)
	}

	if len(after) != 1 || after[0].RunIndex != 2 {
		t.Fatalf("expected only run_index 2 after since=1, got %+v", after)
	}

	if _, err := db.ExecContext(ctx, `UPDATE job_runs SET created_at = ? WHERE run_id = ?`, "2026-05-15 10:00:00", runID1); err != nil {
		t.Fatalf("set first created_at: %v", err)
	}

	if _, err := db.ExecContext(ctx, `UPDATE job_runs SET created_at = ? WHERE run_id = ?`, "2026-05-16 10:00:00", runID2); err != nil {
		t.Fatalf("set second created_at: %v", err)
	}

	sinceTime := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)
	recent, _, err := runs.ListByJob(ctx, "job-order", nil, &sinceTime, "", 0, 100)
	if err != nil {
		t.Fatalf("list since time: %v", err)
	}

	if len(recent) != 1 || recent[0].RunID != runID2 {
		t.Fatalf("expected only run %s since %s, got %+v", runID2, sinceTime, recent)
	}
}

func TestRunsRepository_ClaimRenewAndDispatchQueries(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-claim", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	queued, err := runs.ListQueuedBeforeDispatchCutoff(ctx, time.Now().Unix()+60)
	if err != nil {
		t.Fatalf("list queued before cutoff: %v", err)
	}

	if len(queued) != 1 || queued[0].RunID != runID {
		t.Fatalf("expected queued run %s, got %+v", runID, queued)
	}

	if queued[0].DefinitionVersion != 1 {
		t.Fatalf("expected definition_version 1, got %d", queued[0].DefinitionVersion)
	}

	if queued[0].OwningCell != dal.DefaultCellID {
		t.Fatalf("expected owning_cell %q, got %q", dal.DefaultCellID, queued[0].OwningCell)
	}

	beforeClaim, err := runs.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run before claim: %v", err)
	}
	if beforeClaim.CreatedAt == nil {
		t.Fatal("created_at should be set when run is created")
	}
	if beforeClaim.StartedAt != nil {
		t.Fatalf("started_at should be empty before claim, got %s", *beforeClaim.StartedAt)
	}

	claimed, claimToken, err := runs.TryClaim(ctx, runID, "worker-1", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim first: %v", err)
	}

	if !claimed {
		t.Fatal("expected first claim to succeed")
	}

	if claimToken == "" {
		t.Fatal("expected non-empty claim token on successful claim")
	}

	afterClaim, err := runs.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run after claim: %v", err)
	}

	if afterClaim.StartedAt == nil {
		t.Fatal("started_at should be set when run is claimed")
	}

	cancelRec, err := runs.GetRunForCancel(ctx, runID)
	if err != nil {
		t.Fatalf("get run for cancel: %v", err)
	}
	if cancelRec.CancelToken != claimToken {
		t.Fatalf("cancel token should match worker claim token, got cancel=%q claim=%q", cancelRec.CancelToken, claimToken)
	}

	claimed, _, err = runs.TryClaim(ctx, runID, "worker-2", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim second: %v", err)
	}

	if claimed {
		t.Fatal("expected second claim to fail")
	}

	if err := runs.RenewLease(ctx, runID, "worker-1", claimToken, time.Now().Add(2*time.Minute)); err != nil {
		t.Fatalf("renew lease for owner: %v", err)
	}

	if err := runs.RenewLease(ctx, runID, "worker-2", claimToken, time.Now().Add(2*time.Minute)); err == nil {
		t.Fatal("expected renew lease by non-owner to fail")
	}

	if err := runs.TouchDispatched(ctx, runID); err != nil {
		t.Fatalf("touch dispatched: %v", err)
	}

	queued, err = runs.ListQueuedBeforeDispatchCutoff(ctx, time.Now().Unix()+60)
	if err != nil {
		t.Fatalf("list queued after claim/touch: %v", err)
	}

	if len(queued) != 0 {
		t.Fatalf("expected no queued rows after claim, got %+v", queued)
	}

	if _, err := db.ExecContext(ctx, `UPDATE job_runs SET status = 'orphaned' WHERE run_id = ?`, runID); err != nil {
		t.Fatalf("force orphaned status: %v", err)
	}

	if err := runs.RenewLease(ctx, runID, "worker-1", claimToken, time.Now().Add(3*time.Minute)); err != nil {
		t.Fatalf("renew lease should recover orphaned run: %v", err)
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status: %v", err)
	}

	if status != "running" {
		t.Fatalf("expected status running after orphaned renew, got %q", status)
	}
}

func TestDispatchEventsRepository_RecordAndListByRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()
	dispatch := repos.DispatchEvents()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-dispatch", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	msg := "queue unavailable"
	if err := dispatch.Record(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventAttempt, nil); err != nil {
		t.Fatalf("record attempt: %v", err)
	}

	if err := dispatch.Record(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventFailure, &msg); err != nil {
		t.Fatalf("record failure: %v", err)
	}

	events, err := dispatch.ListByRun(ctx, runID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %+v", events)
	}

	if events[0].EventType != dal.DispatchEventAttempt || events[0].Message != nil {
		t.Fatalf("unexpected attempt event: %+v", events[0])
	}

	if events[1].Source != dal.DispatchSourceAPI || events[1].EventType != dal.DispatchEventFailure {
		t.Fatalf("unexpected failure event: %+v", events[1])
	}

	if events[1].Message == nil || *events[1].Message != msg {
		t.Fatalf("unexpected failure message: %+v", events[1].Message)
	}

	if events[0].CreatedAt == 0 || events[1].CreatedAt == 0 {
		t.Fatalf("expected created_at values: %+v", events)
	}
}

func TestRunsRepository_MarkExpiredRunningAsOrphaned(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()
	ctx := context.Background()

	runA, _, err := runs.CreateRun(ctx, "job-orphan-a", nil, 1)
	if err != nil {
		t.Fatalf("create run A: %v", err)
	}

	runB, _, err := runs.CreateRun(ctx, "job-orphan-b", nil, 1)
	if err != nil {
		t.Fatalf("create run B: %v", err)
	}

	leaseExpired := time.Now().Add(-1 * time.Minute).Unix()
	leaseFuture := time.Now().Add(10 * time.Minute).Unix()

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'running', lease_owner = 'worker-a', lease_until = ?
		WHERE run_id = ?
	`, leaseExpired, runA); err != nil {
		t.Fatalf("seed run A running expired lease: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'running', lease_owner = 'worker-b', lease_until = ?
		WHERE run_id = ?
	`, leaseFuture, runB); err != nil {
		t.Fatalf("seed run B running active lease: %v", err)
	}

	orphaned, err := runs.MarkExpiredRunningAsOrphaned(ctx, time.Now().Unix())
	if err != nil {
		t.Fatalf("MarkExpiredRunningAsOrphaned: %v", err)
	}

	if len(orphaned) != 1 || orphaned[0] != runA {
		t.Fatalf("expected only runA orphaned, got %+v", orphaned)
	}

	var statusA, statusB string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runA).Scan(&statusA); err != nil {
		t.Fatalf("scan run A status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runB).Scan(&statusB); err != nil {
		t.Fatalf("scan run B status: %v", err)
	}

	if statusA != "orphaned" {
		t.Fatalf("expected run A orphaned, got %q", statusA)
	}
	if statusB != "running" {
		t.Fatalf("expected run B running, got %q", statusB)
	}
}

func TestRunsRepository_MarkRunSucceeded_FromOrphaned(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-orphan-finish", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'orphaned', lease_owner = 'worker-a', lease_until = ?
		WHERE run_id = ?
	`, time.Now().Add(-1*time.Minute).Unix(), runID); err != nil {
		t.Fatalf("seed orphaned run: %v", err)
	}

	if err := runs.MarkRunSucceeded(ctx, runID, ""); err != nil {
		t.Fatalf("MarkRunSucceeded from orphaned: %v", err)
	}

	var status string
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	var finishedAt sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, lease_owner, lease_until, CAST(finished_at AS TEXT)
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&status, &leaseOwner, &leaseUntil, &finishedAt); err != nil {
		t.Fatalf("query run state: %v", err)
	}

	if status != "succeeded" {
		t.Fatalf("expected status succeeded, got %q", status)
	}

	if leaseOwner.Valid || leaseUntil.Valid {
		t.Fatalf("expected lease owner/until cleared, got owner=%v lease_until=%v", leaseOwner, leaseUntil)
	}

	if !finishedAt.Valid || finishedAt.String == "" {
		t.Fatal("expected finished_at set")
	}
}

func TestRunsRepository_FencingTokenRejectsStaleFinalizeAndRenew(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-fencing", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, tokenA, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim worker-a: %v", err)
	}

	if !claimed || tokenA == "" {
		t.Fatalf("expected worker-a claim and token, got claimed=%v token=%q", claimed, tokenA)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'queued', lease_owner = NULL, lease_until = NULL, claim_token = NULL
		WHERE run_id = ?
	`, runID); err != nil {
		t.Fatalf("force requeue: %v", err)
	}

	claimed, tokenB, err := runs.TryClaim(ctx, runID, "worker-b", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim worker-b: %v", err)
	}

	if !claimed || tokenB == "" {
		t.Fatalf("expected worker-b claim and token, got claimed=%v token=%q", claimed, tokenB)
	}

	if tokenA == tokenB {
		t.Fatal("expected distinct claim tokens across attempts")
	}

	var attempt int
	if err := db.QueryRowContext(ctx, `SELECT attempt FROM job_runs WHERE run_id = ?`, runID).Scan(&attempt); err != nil {
		t.Fatalf("scan attempt: %v", err)
	}
	if attempt != 2 {
		t.Fatalf("expected attempt=2 after two successful claims, got %d", attempt)
	}

	if err := runs.RenewLease(ctx, runID, "worker-b", tokenA, time.Now().Add(2*time.Minute)); err == nil {
		t.Fatal("expected stale token renew to fail")
	}

	if err := runs.MarkRunSucceeded(ctx, runID, tokenA); err == nil {
		t.Fatal("expected stale token finalize to fail")
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status after stale finalize: %v", err)
	}

	if status != "running" {
		t.Fatalf("expected status to remain running for active token, got %q", status)
	}

	if err := runs.MarkRunSucceeded(ctx, runID, tokenB); err != nil {
		t.Fatalf("expected active token finalize to succeed: %v", err)
	}

	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status after active finalize: %v", err)
	}

	if status != "succeeded" {
		t.Fatalf("expected status succeeded after active token finalize, got %q", status)
	}
}

func TestRunsRepository_FencingTokenRejectsStaleFailedAndOrphaned(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-fencing-stale-fail", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, tokenA, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim worker-a: %v", err)
	}

	if !claimed || tokenA == "" {
		t.Fatalf("expected worker-a claim and token, got claimed=%v token=%q", claimed, tokenA)
	}

	if err := runs.MarkRunFailed(ctx, runID, tokenA, dal.FailureCodeExecution, "first attempt failed"); err != nil {
		t.Fatalf("mark failed for first attempt: %v", err)
	}

	if err := runs.RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatalf("requeue run for retry: %v", err)
	}

	claimed, tokenB, err := runs.TryClaim(ctx, runID, "worker-b", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim worker-b: %v", err)
	}

	if !claimed || tokenB == "" {
		t.Fatalf("expected worker-b claim and token, got claimed=%v token=%q", claimed, tokenB)
	}

	if err := runs.MarkRunFailed(ctx, runID, tokenA, dal.FailureCodeExecution, "stale token fail"); err == nil {
		t.Fatal("expected stale token MarkRunFailed to fail")
	}

	if err := runs.MarkRunOrphaned(ctx, runID, tokenA, dal.OrphanReasonAckUncertain); err == nil {
		t.Fatal("expected stale token MarkRunOrphaned to fail")
	}

	if err := runs.MarkRunAborted(ctx, runID, tokenA, dal.CancelReasonAPI); err == nil {
		t.Fatal("expected stale token MarkRunAborted to fail")
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status after stale transitions: %v", err)
	}

	if status != "running" {
		t.Fatalf("expected status running after stale transitions, got %q", status)
	}

	if err := runs.MarkRunSucceeded(ctx, runID, tokenB); err != nil {
		t.Fatalf("mark succeeded with active token: %v", err)
	}
}

func TestRunsRepository_MarkRunAborted_SetsCancelledTerminalState(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-abort-run", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, token, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("try claim: %v", err)
	}
	if !claimed || token == "" {
		t.Fatalf("expected claim token, got claimed=%v token=%q", claimed, token)
	}

	if err := runs.MarkRunAborted(ctx, runID, token, dal.CancelReasonAPI); err != nil {
		t.Fatalf("mark run aborted: %v", err)
	}

	var status string
	var failureCode string
	var failure sql.NullString
	var finishedAt sql.NullString
	var claimToken sql.NullString
	var cancelToken sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason, CAST(finished_at AS TEXT), claim_token, cancel_token, lease_owner, lease_until
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&status, &failureCode, &failure, &finishedAt, &claimToken, &cancelToken, &leaseOwner, &leaseUntil); err != nil {
		t.Fatalf("query aborted run: %v", err)
	}

	if status != dal.RunStatusCancelled {
		t.Fatalf("expected cancelled status, got %q", status)
	}

	if failureCode != "" {
		t.Fatalf("expected empty failure_code, got %q", failureCode)
	}

	if !failure.Valid || failure.String != dal.CancelReasonAPI {
		t.Fatalf("expected failure_reason %q, got %v", dal.CancelReasonAPI, failure)
	}

	if !finishedAt.Valid {
		t.Fatal("expected finished_at to be set")
	}

	if claimToken.Valid || cancelToken.Valid || leaseOwner.Valid || leaseUntil.Valid {
		t.Fatalf("expected abort to clear runtime fields; got claim=%v cancel=%v owner=%v lease_until=%v", claimToken, cancelToken, leaseOwner, leaseUntil)
	}

	if err := runs.RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatalf("expected aborted run to be requeueable: %v", err)
	}

	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("query requeued aborted run: %v", err)
	}

	if status != dal.RunStatusQueued {
		t.Fatalf("expected queued status after requeue, got %q", status)
	}
}

func TestRunsRepository_RequeueRunForRetry_ClearsLeaseAndToken(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-requeue-retry", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, token, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("try claim: %v", err)
	}
	if !claimed || token == "" {
		t.Fatalf("expected claim token, got claimed=%v token=%q", claimed, token)
	}

	if err := runs.MarkRunFailed(ctx, runID, token, dal.FailureCodeExecution, "test failure"); err != nil {
		t.Fatalf("mark run failed: %v", err)
	}

	if err := runs.RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatalf("RequeueRunForRetry: %v", err)
	}

	var status string
	var failureCode string
	var failure sql.NullString
	var claimToken sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	var lastDispatched sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason, claim_token, lease_owner, lease_until, last_dispatched_at
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&status, &failureCode, &failure, &claimToken, &leaseOwner, &leaseUntil, &lastDispatched); err != nil {
		t.Fatalf("query requeued run: %v", err)
	}

	if status != "queued" {
		t.Fatalf("expected queued status, got %q", status)
	}

	if failureCode != "" || failure.Valid || claimToken.Valid || leaseOwner.Valid || leaseUntil.Valid || lastDispatched.Valid {
		t.Fatalf("expected queue retry to clear runtime fields; got failure_code=%q failure=%v token=%v owner=%v lease_until=%v dispatched=%v",
			failureCode, failure, claimToken, leaseOwner, leaseUntil, lastDispatched)
	}
}

func TestRunsRepository_RepairMarkRunAbandoned_OnlyFromOrphaned(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runningRunID, _, err := runs.CreateRun(ctx, "job-repair-running", nil, 1)
	if err != nil {
		t.Fatalf("create running run: %v", err)
	}
	if claimed, _, err := runs.TryClaim(ctx, runningRunID, "worker-a", time.Now().Add(time.Minute)); err != nil || !claimed {
		t.Fatalf("claim running run claimed=%v err=%v", claimed, err)
	}
	if err := runs.RepairMarkRunAbandoned(ctx, runningRunID, "worker deleted"); !dal.IsConflict(err) {
		t.Fatalf("expected running run conflict, got %v", err)
	}

	orphanRunID, _, err := runs.CreateRun(ctx, "job-repair-orphan", nil, 1)
	if err != nil {
		t.Fatalf("create orphan run: %v", err)
	}
	claimed, token, err := runs.TryClaim(ctx, orphanRunID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim orphan run: %v", err)
	}
	if !claimed {
		t.Fatal("expected orphan run claim")
	}
	if err := runs.MarkRunOrphaned(ctx, orphanRunID, token, dal.OrphanReasonLeaseExpired); err != nil {
		t.Fatalf("mark orphaned: %v", err)
	}
	if err := runs.RepairMarkRunAbandoned(ctx, orphanRunID, "worker deleted"); err != nil {
		t.Fatalf("repair mark abandoned: %v", err)
	}

	var status string
	var reason sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_reason FROM job_runs WHERE run_id = ?`, orphanRunID).Scan(&status, &reason); err != nil {
		t.Fatalf("query repaired run: %v", err)
	}
	if status != dal.RunStatusAbandoned {
		t.Fatalf("expected abandoned status, got %q", status)
	}
	if !reason.Valid || reason.String != "worker deleted" {
		t.Fatalf("expected repair reason, got %v", reason)
	}
}

func TestRunsRepository_RequeueRunForRetry_RejectsRunning(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-requeue-running", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, _, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("try claim: %v", err)
	}

	if !claimed {
		t.Fatal("expected claim to succeed")
	}

	err = runs.RequeueRunForRetry(ctx, runID)
	if !dal.IsConflict(err) {
		t.Fatalf("expected conflict requeueing running run, got %v", err)
	}
}

func TestRunsRepository_MarkRunOrphaned_WithClaimToken(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-mark-orphaned", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, token, err := runs.TryClaim(ctx, runID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("try claim: %v", err)
	}

	if !claimed || token == "" {
		t.Fatalf("expected claim token, got claimed=%v token=%q", claimed, token)
	}

	if err := runs.MarkRunOrphaned(ctx, runID, token, dal.OrphanReasonAckUncertain); err != nil {
		t.Fatalf("MarkRunOrphaned: %v", err)
	}

	var status string
	var reason sql.NullString
	var orphanReason sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	var claimToken sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_reason, orphan_reason, lease_owner, lease_until, claim_token
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&status, &reason, &orphanReason, &leaseOwner, &leaseUntil, &claimToken); err != nil {
		t.Fatalf("query run: %v", err)
	}

	if status != "orphaned" {
		t.Fatalf("expected orphaned status, got %q", status)
	}

	if !reason.Valid || reason.String != dal.OrphanReasonAckUncertain {
		t.Fatalf("expected orphan reason, got %v", reason)
	}
	if !orphanReason.Valid || orphanReason.String != dal.OrphanReasonAckUncertain {
		t.Fatalf("expected orphan_reason, got %v", orphanReason)
	}

	if leaseOwner.Valid || leaseUntil.Valid || claimToken.Valid {
		t.Fatalf("expected lease/token cleared, got owner=%v lease=%v token=%v", leaseOwner, leaseUntil, claimToken)
	}
}

func TestRunsRepository_CreateRunWithExplicitRunIndex(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-explicit", nil, 1)
	if err != nil {
		t.Fatalf("create initial run: %v", err)
	}

	idx := 10
	runID2, outIdx, err := runs.CreateRun(ctx, "job-explicit", &idx, 1)
	if err != nil {
		t.Fatalf("create explicit run_index: %v", err)
	}

	if outIdx != idx {
		t.Fatalf("expected run_index %d, got %d", idx, outIdx)
	}

	all, _, err := runs.ListByJob(ctx, "job-explicit", nil, nil, "", 0, 100)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}

	if len(all) != 2 {
		t.Fatalf("expected 2 runs, got %+v", all)
	}

	if all[0].RunID != runID || all[1].RunID != runID2 {
		t.Fatalf("unexpected run ids in list: %+v", all)
	}
}

func TestSQLRepositories_CreateDefinitionAndRun_AndGetDefinitionVersion(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	jobID := "ephemeral-job-id"
	def := `{"id":"ephemeral-job-id","root":{"uses":"builtins/shell","with":{"command":"echo x"}}}`
	idx := 1

	runID, outIdx, err := repos.CreateDefinitionAndRun(ctx, jobID, def, &idx)
	if err != nil {
		t.Fatalf("CreateDefinitionAndRun: %v", err)
	}

	if outIdx != idx {
		t.Fatalf("run index: want %d, got %d", idx, outIdx)
	}

	got, err := repos.Jobs().GetDefinitionVersion(ctx, jobID, 1)
	if err != nil {
		t.Fatalf("GetDefinitionVersion: %v", err)
	}

	if got != def {
		t.Fatalf("definition mismatch: got %q", got)
	}

	var dv int
	if err := db.QueryRowContext(ctx, "SELECT definition_version FROM job_runs WHERE run_id = ?", runID).Scan(&dv); err != nil {
		t.Fatalf("scan definition_version: %v", err)
	}

	if dv != 1 {
		t.Fatalf("job_runs.definition_version: want 1, got %d", dv)
	}

	var runHash, owningCell string
	if err := db.QueryRowContext(ctx, "SELECT definition_hash, owning_cell FROM job_runs WHERE run_id = ?", runID).Scan(&runHash, &owningCell); err != nil {
		t.Fatalf("scan run foundation fields: %v", err)
	}

	if want := dal.DefinitionHash(def); runHash != want {
		t.Fatalf("job_runs.definition_hash: want %q, got %q", want, runHash)
	}

	if owningCell != dal.DefaultCellID {
		t.Fatalf("job_runs.owning_cell: want %q, got %q", dal.DefaultCellID, owningCell)
	}

	var segmentCount, executionCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM run_segments WHERE run_id = ?", runID).Scan(&segmentCount); err != nil {
		t.Fatalf("scan segment count: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM segment_executions WHERE run_id = ?", runID).Scan(&executionCount); err != nil {
		t.Fatalf("scan execution count: %v", err)
	}

	if segmentCount != 1 || executionCount != 1 {
		t.Fatalf("expected one segment and one execution, got segments=%d executions=%d", segmentCount, executionCount)
	}
}

func TestIdempotencyRepository_ReserveCompleteAndReplay(t *testing.T) {
	db := dbtest.NewTestDB(t)
	idempotency := dal.NewSQLRepositories(db).Idempotency()
	ctx := context.Background()

	rec, created, err := idempotency.Reserve(ctx, "scope-a", "key-a", "hash-a")
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}

	if !created {
		t.Fatal("expected first reserve to create record")
	}

	if rec.ResponseJSON != nil {
		t.Fatalf("expected no response on new record, got %q", *rec.ResponseJSON)
	}

	if err := idempotency.Complete(ctx, "scope-a", "key-a", `{"run_id":"run-a"}`); err != nil {
		t.Fatalf("complete: %v", err)
	}

	rec, created, err = idempotency.Reserve(ctx, "scope-a", "key-a", "hash-a")
	if err != nil {
		t.Fatalf("replay reserve: %v", err)
	}

	if created {
		t.Fatal("expected replay reserve to read existing record")
	}

	if rec.ResponseJSON == nil || *rec.ResponseJSON != `{"run_id":"run-a"}` {
		t.Fatalf("expected stored response, got %+v", rec.ResponseJSON)
	}

	rec, created, err = idempotency.Reserve(ctx, "scope-a", "key-a", "hash-b")
	if err != nil {
		t.Fatalf("mismatch reserve: %v", err)
	}

	if created {
		t.Fatal("expected mismatched reserve to read existing record")
	}

	if rec.RequestHash != "hash-a" {
		t.Fatalf("expected original hash, got %q", rec.RequestHash)
	}
}

func TestIdempotencyRepository_ReleaseIncomplete(t *testing.T) {
	db := dbtest.NewTestDB(t)
	idempotency := dal.NewSQLRepositories(db).Idempotency()
	ctx := context.Background()

	if _, created, err := idempotency.Reserve(ctx, "scope-a", "key-a", "hash-a"); err != nil || !created {
		t.Fatalf("reserve created=%v err=%v", created, err)
	}

	if err := idempotency.Release(ctx, "scope-a", "key-a"); err != nil {
		t.Fatalf("release: %v", err)
	}

	if _, created, err := idempotency.Reserve(ctx, "scope-a", "key-a", "hash-a"); err != nil || !created {
		t.Fatalf("reserve after release created=%v err=%v", created, err)
	}
}

func TestJobsRepository_GetDefinitionVersion_NotFound(t *testing.T) {
	db := dbtest.NewTestDB(t)
	jobs := dal.NewSQLRepositories(db).Jobs()
	ctx := context.Background()

	_, err := jobs.GetDefinitionVersion(ctx, "missing", 1)
	if err == nil || !dal.IsNotFound(err) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestSchedulesRepository_GetReadyClaimAndComplete(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	jobs := repos.Jobs()
	schedules := repos.Schedules()
	ctx := context.Background()

	if err := jobs.Create(ctx, "cron-job", `{"id":"cron-job"}`, 1); err != nil {
		t.Fatalf("create stored job: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	past := now.Add(-1 * time.Minute)
	future := now.Add(5 * time.Minute)

	if _, err := db.ExecContext(ctx,
		"INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"cron-job", "* * * * *", past.Format(time.RFC3339)); err != nil {
		t.Fatalf("insert past schedule: %v", err)
	}

	if _, err := db.ExecContext(ctx,
		"INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"cron-job", "0 * * * *", future.Format(time.RFC3339)); err != nil {
		t.Fatalf("insert future schedule: %v", err)
	}

	ready, err := schedules.GetReady(ctx, now)
	if err != nil {
		t.Fatalf("get ready schedules: %v", err)
	}

	if len(ready) != 1 || ready[0].CronSpec != "* * * * *" {
		t.Fatalf("expected one ready schedule for '* * * * *', got %+v", ready)
	}

	updatedNext := now.Add(10 * time.Minute)
	claimToken := "claim-ready"
	claimed, err := schedules.ClaimDue(ctx, ready[0].ID, ready[0].NextRunAt, claimToken, now.Add(5*time.Minute), now)
	if err != nil {
		t.Fatalf("claim next run: %v", err)
	}

	if !claimed {
		t.Fatal("expected schedule claim to succeed")
	}

	readyClaimed, err := schedules.GetReady(ctx, now)
	if err != nil {
		t.Fatalf("get ready schedules after claim: %v", err)
	}

	if len(readyClaimed) != 0 {
		t.Fatalf("expected claimed schedule to be hidden from ready list, got %+v", readyClaimed)
	}

	completed, err := schedules.CompleteClaim(ctx, ready[0].ID, claimToken, updatedNext)
	if err != nil {
		t.Fatalf("complete claim: %v", err)
	}

	if !completed {
		t.Fatal("expected schedule completion to succeed")
	}

	readyAfter, err := schedules.GetReady(ctx, now)
	if err != nil {
		t.Fatalf("get ready schedules after update: %v", err)
	}

	if len(readyAfter) != 0 {
		t.Fatalf("expected no ready schedules after update, got %+v", readyAfter)
	}
}

func TestSchedulesRepository_ClaimDueCompleteAndRelease(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	jobs := repos.Jobs()
	schedules := repos.Schedules()
	ctx := context.Background()

	if err := jobs.Create(ctx, "cron-job", `{"id":"cron-job"}`, 1); err != nil {
		t.Fatalf("create stored job: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	observed := now.Add(-1 * time.Minute)
	next := now.Add(10 * time.Minute)

	result, err := db.ExecContext(ctx,
		"INSERT INTO job_cron_schedules (job_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		"cron-job", "* * * * *", observed.Format(time.RFC3339))
	if err != nil {
		t.Fatalf("insert schedule: %v", err)
	}

	scheduleID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("schedule id: %v", err)
	}

	claimed, err := schedules.ClaimDue(ctx, scheduleID, observed, "claim-1", now.Add(5*time.Minute), now)
	if err != nil {
		t.Fatalf("claim due: %v", err)
	}

	if !claimed {
		t.Fatal("expected first claim to claim schedule")
	}

	claimed, err = schedules.ClaimDue(ctx, scheduleID, observed, "claim-2", now.Add(5*time.Minute), now)
	if err != nil {
		t.Fatalf("duplicate claim due: %v", err)
	}

	if claimed {
		t.Fatal("expected duplicate claim to lose schedule claim")
	}

	if err := schedules.ReleaseClaim(ctx, scheduleID, "claim-1"); err != nil {
		t.Fatalf("release claim: %v", err)
	}

	claimed, err = schedules.ClaimDue(ctx, scheduleID, observed, "claim-3", now.Add(5*time.Minute), now)
	if err != nil {
		t.Fatalf("claim after release: %v", err)
	}

	if !claimed {
		t.Fatal("expected claim after release to succeed")
	}

	completed, err := schedules.CompleteClaim(ctx, scheduleID, "claim-3", next)
	if err != nil {
		t.Fatalf("complete claim: %v", err)
	}

	if !completed {
		t.Fatal("expected complete claim to succeed")
	}

	var nextRunStr string
	if err := db.QueryRowContext(ctx, "SELECT next_run_at FROM job_cron_schedules WHERE id = ?", scheduleID).Scan(&nextRunStr); err != nil {
		t.Fatalf("read next_run_at: %v", err)
	}

	if nextRunStr != next.Format(time.RFC3339) {
		t.Fatalf("expected next_run_at %q, got %q", next.Format(time.RFC3339), nextRunStr)
	}
}
