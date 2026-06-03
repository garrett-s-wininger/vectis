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

	var currentVersion int
	var versionHash string
	if err := db.QueryRowContext(ctx, "SELECT current_version FROM stored_jobs WHERE job_id = ?", jobID).Scan(&currentVersion); err != nil {
		t.Fatalf("scan stored job current version: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT definition_hash FROM job_definitions WHERE job_id = ? AND version = 2", jobID).Scan(&versionHash); err != nil {
		t.Fatalf("scan version hash: %v", err)
	}

	if currentVersion != 2 {
		t.Fatalf("current version: got %d, want 2", currentVersion)
	}

	if want := dal.DefinitionHash(def2); versionHash != want {
		t.Fatalf("definition hash mismatch: version=%q want=%q", versionHash, want)
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

func TestTriggerInvocations_CreateRunAuditAndPayloadLedger(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	jobID := "job-audit"
	definitionJSON := `{"id":"job-audit","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, definitionJSON, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	rec, err := repos.TriggerInvocations().Record(ctx, dal.TriggerInvocation{
		JobID:              jobID,
		TriggerType:        dal.TriggerTypeManual,
		TriggerPayloadHash: dal.PayloadHash(`{"target_cell_ids":["iad-a","pdx-b"]}`),
		RequestedCells:     []string{"iad-a", "pdx-b"},
	})

	if err != nil {
		t.Fatalf("record trigger invocation: %v", err)
	}

	created, err := repos.Runs().CreateRunsInCellsWithAudit(ctx, jobID, nil, 1, []string{"iad-a", "pdx-b"}, dal.RunAuditMetadata{
		TriggerInvocationID: rec.InvocationID,
	})
	if err != nil {
		t.Fatalf("create audited runs: %v", err)
	}

	if len(created) != 2 {
		t.Fatalf("created runs: got %d, want 2", len(created))
	}

	for _, run := range created {
		var invocationID, payloadHash string
		if err := db.QueryRowContext(ctx, "SELECT trigger_invocation_id, execution_payload_hash FROM job_runs WHERE run_id = ?", run.RunID).Scan(&invocationID, &payloadHash); err != nil {
			t.Fatalf("query run audit fields: %v", err)
		}

		if invocationID != rec.InvocationID {
			t.Fatalf("run %s invocation: got %q want %q", run.RunID, invocationID, rec.InvocationID)
		}

		if payloadHash != "" {
			t.Fatalf("run %s should not have payload hash before dispatch, got %q", run.RunID, payloadHash)
		}
	}

	payload1 := `{"job":{"id":"job-audit","runId":"` + created[0].RunID + `"},"metadata":{"attempt":"1"}}`
	payloadHash1, recordedPayload1, err := repos.Runs().RecordExecutionPayload(ctx, created[0].RunID, payload1, dal.DefinitionHash(definitionJSON))
	if err != nil {
		t.Fatalf("record execution payload: %v", err)
	}

	if recordedPayload1 != payload1 {
		t.Fatalf("recorded payload: got %q want %q", recordedPayload1, payload1)
	}

	payload2 := `{"job":{"id":"job-audit","runId":"` + created[0].RunID + `"},"metadata":{"attempt":"2"}}`
	payloadHash2, recordedPayload2, err := repos.Runs().RecordExecutionPayload(ctx, created[0].RunID, payload2, dal.DefinitionHash(definitionJSON))
	if err != nil {
		t.Fatalf("record replacement execution payload: %v", err)
	}

	if payloadHash2 != payloadHash1 {
		t.Fatalf("replacement payload hash: got %q want frozen %q", payloadHash2, payloadHash1)
	}

	if recordedPayload2 != payload1 {
		t.Fatalf("replacement payload should replay original: got %q want %q", recordedPayload2, payload1)
	}

	var currentPayloadHash string
	if err := db.QueryRowContext(ctx, "SELECT execution_payload_hash FROM job_runs WHERE run_id = ?", created[0].RunID).Scan(&currentPayloadHash); err != nil {
		t.Fatalf("query current payload hash: %v", err)
	}

	if currentPayloadHash != payloadHash1 {
		t.Fatalf("current payload hash: got %q want %q", currentPayloadHash, payloadHash1)
	}

	attemptedPayloadHash2 := dal.ExecutionPayloadHash(payload2)
	var payloadRows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM execution_payloads WHERE payload_hash IN (?, ?)", payloadHash1, attemptedPayloadHash2).Scan(&payloadRows); err != nil {
		t.Fatalf("count payload ledger rows: %v", err)
	}

	if payloadRows != 1 {
		t.Fatalf("expected only the frozen payload ledger row, got %d", payloadRows)
	}

	runRec, err := repos.Runs().GetRun(ctx, created[0].RunID)
	if err != nil {
		t.Fatalf("get audited run: %v", err)
	}

	if runRec.TriggerInvocationID == nil || *runRec.TriggerInvocationID != rec.InvocationID {
		t.Fatalf("run trigger invocation id: got %+v want %q", runRec.TriggerInvocationID, rec.InvocationID)
	}

	if runRec.TriggerType == nil || *runRec.TriggerType != dal.TriggerTypeManual {
		t.Fatalf("run trigger type: got %+v want %q", runRec.TriggerType, dal.TriggerTypeManual)
	}

	if runRec.ExecutionPayloadHash != payloadHash1 {
		t.Fatalf("run execution payload hash: got %q want %q", runRec.ExecutionPayloadHash, payloadHash1)
	}

	if len(runRec.RequestedCells) != 2 || runRec.RequestedCells[0] != "iad-a" || runRec.RequestedCells[1] != "pdx-b" {
		t.Fatalf("run requested cells: got %+v", runRec.RequestedCells)
	}

	payloadByRun, err := repos.Runs().GetExecutionPayloadForRun(ctx, created[0].RunID)
	if err != nil {
		t.Fatalf("get payload by run: %v", err)
	}

	if payloadByRun.PayloadHash != payloadHash1 || payloadByRun.PayloadJSON != payload1 {
		t.Fatalf("payload by run: got hash=%q payload=%q", payloadByRun.PayloadHash, payloadByRun.PayloadJSON)
	}

	payloadByHash, err := repos.Runs().GetExecutionPayloadByHash(ctx, payloadHash1)
	if err != nil {
		t.Fatalf("get payload by hash: %v", err)
	}

	if payloadByHash.PayloadHash != payloadHash1 || payloadByHash.PayloadJSON != payload1 {
		t.Fatalf("payload by hash: got hash=%q payload=%q", payloadByHash.PayloadHash, payloadByHash.PayloadJSON)
	}
}

func TestCellExecutionAcceptances_AcceptExecutionMaterializesLocalRows(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	acceptance := dal.CellExecutionAcceptance{
		ExecutionID:       "execution-1",
		RunID:             "run-1",
		JobID:             "job-1",
		RunIndex:          7,
		SegmentID:         "segment-1",
		SegmentName:       "root",
		CellID:            "iad-a",
		Attempt:           1,
		DefinitionVersion: 3,
		DefinitionHash:    "sha256:abc123",
		DefinitionJSON:    `{"id":"job-1","root":{"uses":"builtins/shell"}}`,
		RequestJSON:       `{"job":{"id":"job-1","runId":"run-1"}}`,
	}

	created, err := repos.CellExecutionAcceptances().AcceptExecution(ctx, acceptance)
	if err != nil {
		t.Fatalf("AcceptExecution: %v", err)
	}

	if !created {
		t.Fatal("first AcceptExecution should create a receipt")
	}

	created, err = repos.CellExecutionAcceptances().AcceptExecution(ctx, acceptance)
	if err != nil {
		t.Fatalf("duplicate AcceptExecution: %v", err)
	}

	if created {
		t.Fatal("duplicate AcceptExecution should be idempotent")
	}

	var receiptHash string
	if err := db.QueryRowContext(ctx, "SELECT acceptance_hash FROM cell_execution_acceptances WHERE execution_id = ?", acceptance.ExecutionID).Scan(&receiptHash); err != nil {
		t.Fatalf("query acceptance receipt: %v", err)
	}

	if receiptHash == "" {
		t.Fatal("acceptance hash was not recorded")
	}

	var runIndex int
	var runStatus, owningCell string
	if err := db.QueryRowContext(ctx, "SELECT run_index, status, owning_cell FROM job_runs WHERE run_id = ?", acceptance.RunID).Scan(&runIndex, &runStatus, &owningCell); err != nil {
		t.Fatalf("query job run: %v", err)
	}

	if runIndex != acceptance.RunIndex || runStatus != dal.RunStatusQueued || owningCell != acceptance.CellID {
		t.Fatalf("unexpected run row: run_index=%d status=%q owning_cell=%q", runIndex, runStatus, owningCell)
	}

	var segmentStatus string
	if err := db.QueryRowContext(ctx, "SELECT status FROM run_segments WHERE segment_id = ?", acceptance.SegmentID).Scan(&segmentStatus); err != nil {
		t.Fatalf("query run segment: %v", err)
	}

	if segmentStatus != dal.SegmentStatusAccepted {
		t.Fatalf("segment status: got %q, want %q", segmentStatus, dal.SegmentStatusAccepted)
	}

	var executionStatus string
	if err := db.QueryRowContext(ctx, "SELECT status FROM segment_executions WHERE execution_id = ?", acceptance.ExecutionID).Scan(&executionStatus); err != nil {
		t.Fatalf("query segment execution: %v", err)
	}

	if executionStatus != dal.ExecutionStatusAccepted {
		t.Fatalf("execution status: got %q, want %q", executionStatus, dal.ExecutionStatusAccepted)
	}

	assertExecutionTaskLink(t, db, acceptance.ExecutionID, acceptance.RunID+":root", acceptance.RunID+":root:attempt:1")
	assertRootTaskAndAttemptStatus(t, db, acceptance.RunID, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)

	var definitionHash string
	if err := db.QueryRowContext(ctx, "SELECT definition_hash FROM job_definitions WHERE job_id = ? AND version = ?", acceptance.JobID, acceptance.DefinitionVersion).Scan(&definitionHash); err != nil {
		t.Fatalf("query job definition: %v", err)
	}

	if definitionHash != acceptance.DefinitionHash {
		t.Fatalf("definition hash: got %q, want %q", definitionHash, acceptance.DefinitionHash)
	}

	payloadHash := dal.ExecutionPayloadHash(acceptance.RequestJSON)
	var storedPayload string
	if err := db.QueryRowContext(ctx, "SELECT payload_json FROM execution_payloads WHERE payload_hash = ?", payloadHash).Scan(&storedPayload); err != nil {
		t.Fatalf("query execution payload: %v", err)
	}

	if storedPayload != acceptance.RequestJSON {
		t.Fatalf("execution payload: got %q, want %q", storedPayload, acceptance.RequestJSON)
	}
}

func TestCellExecutionAcceptances_QueueHandoffMarkers(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	acceptance := dal.CellExecutionAcceptance{
		ExecutionID:       "execution-repair",
		RunID:             "run-repair",
		JobID:             "job-repair",
		RunIndex:          2,
		SegmentID:         "segment-repair",
		SegmentName:       "root",
		CellID:            "iad-a",
		Attempt:           1,
		DefinitionVersion: 1,
		DefinitionHash:    "sha256:repair",
		DefinitionJSON:    `{"id":"job-repair","root":{"uses":"builtins/shell"}}`,
		RequestJSON:       `{"job":{"id":"job-repair","runId":"run-repair"}}`,
	}

	if _, err := repos.CellExecutionAcceptances().AcceptExecution(ctx, acceptance); err != nil {
		t.Fatalf("AcceptExecution: %v", err)
	}

	pending, err := repos.CellExecutionAcceptances().ListPendingQueueHandoffs(ctx, time.Now().Add(time.Minute).UnixNano(), 10)
	if err != nil {
		t.Fatalf("ListPendingQueueHandoffs: %v", err)
	}

	if len(pending) != 1 || pending[0].ExecutionID != acceptance.ExecutionID || pending[0].RequestJSON != acceptance.RequestJSON {
		t.Fatalf("pending handoffs: got %+v", pending)
	}

	failedAt := time.Now().UnixNano()
	if err := repos.CellExecutionAcceptances().MarkEnqueueFailed(ctx, acceptance.ExecutionID, failedAt, "queue closed"); err != nil {
		t.Fatalf("MarkEnqueueFailed: %v", err)
	}

	pending, err = repos.CellExecutionAcceptances().ListPendingQueueHandoffs(ctx, failedAt-1, 10)
	if err != nil {
		t.Fatalf("ListPendingQueueHandoffs after failed marker: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("expected failed handoff to be throttled, got %+v", pending)
	}

	pending, err = repos.CellExecutionAcceptances().ListPendingQueueHandoffs(ctx, failedAt+1, 10)
	if err != nil {
		t.Fatalf("ListPendingQueueHandoffs after cutoff: %v", err)
	}

	if len(pending) != 1 || pending[0].EnqueueAttempts != 1 {
		t.Fatalf("expected retryable handoff with one attempt, got %+v", pending)
	}

	enqueuedAt := failedAt + int64(time.Second)
	if err := repos.CellExecutionAcceptances().MarkEnqueued(ctx, acceptance.ExecutionID, enqueuedAt); err != nil {
		t.Fatalf("MarkEnqueued: %v", err)
	}

	pending, err = repos.CellExecutionAcceptances().ListPendingQueueHandoffs(ctx, enqueuedAt+1, 10)
	if err != nil {
		t.Fatalf("ListPendingQueueHandoffs after enqueue: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("expected enqueued handoff to disappear, got %+v", pending)
	}

	var storedEnqueuedAt int64
	var attempts int
	var lastErr sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT enqueued_at, enqueue_attempts, last_enqueue_error
		FROM cell_execution_acceptances
		WHERE execution_id = ?
	`, acceptance.ExecutionID).Scan(&storedEnqueuedAt, &attempts, &lastErr); err != nil {
		t.Fatalf("query receipt handoff markers: %v", err)
	}

	if storedEnqueuedAt != enqueuedAt || attempts != 2 || lastErr.Valid {
		t.Fatalf("unexpected handoff markers: enqueued_at=%d attempts=%d last_err=%v", storedEnqueuedAt, attempts, lastErr)
	}
}

func TestCellExecutionAcceptances_AcceptsExistingPendingExecution(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	jobID := "job-existing"
	definitionJSON := `{"id":"job-existing","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, definitionJSON, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, runIndex, err := repos.Runs().CreateRunInCell(ctx, jobID, nil, 1, "iad-a")
	if err != nil {
		t.Fatalf("CreateRunInCell: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("GetPendingExecution: %v", err)
	}

	created, err := repos.CellExecutionAcceptances().AcceptExecution(ctx, dal.CellExecutionAcceptance{
		ExecutionID:       dispatch.ExecutionID,
		RunID:             runID,
		JobID:             jobID,
		RunIndex:          runIndex,
		SegmentID:         dispatch.SegmentID,
		SegmentName:       dispatch.SegmentName,
		CellID:            "iad-a",
		Attempt:           dispatch.Attempt,
		DefinitionVersion: dispatch.DefinitionVersion,
		DefinitionHash:    dispatch.DefinitionHash,
		DefinitionJSON:    definitionJSON,
		RequestJSON:       `{"job":{"id":"job-existing","runId":"` + runID + `"}}`,
	})

	if err != nil {
		t.Fatalf("AcceptExecution: %v", err)
	}

	if !created {
		t.Fatal("first AcceptExecution should create a receipt")
	}

	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
}

func TestCellExecutionAcceptances_RejectsConflictingDuplicate(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	acceptance := dal.CellExecutionAcceptance{
		ExecutionID:       "execution-1",
		RunID:             "run-1",
		JobID:             "job-1",
		RunIndex:          1,
		SegmentID:         "segment-1",
		CellID:            "iad-a",
		DefinitionVersion: 1,
		DefinitionHash:    "sha256:abc123",
		DefinitionJSON:    `{"id":"job-1","root":{"uses":"builtins/shell"}}`,
		RequestJSON:       `{"job":{"id":"job-1","runId":"run-1"}}`,
	}

	if _, err := repos.CellExecutionAcceptances().AcceptExecution(ctx, acceptance); err != nil {
		t.Fatalf("AcceptExecution: %v", err)
	}

	acceptance.DefinitionHash = "sha256:different"
	if _, err := repos.CellExecutionAcceptances().AcceptExecution(ctx, acceptance); !dal.IsConflict(err) {
		t.Fatalf("expected conflicting duplicate to return ErrConflict, got %v", err)
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

	var namespaceCell, jobCell, runCell, executionCell string
	if err := db.QueryRowContext(ctx, "SELECT home_cell FROM namespaces WHERE id = ?", ns.ID).Scan(&namespaceCell); err != nil {
		t.Fatalf("query namespace cell: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT home_cell FROM stored_jobs WHERE job_id = ?", jobID).Scan(&jobCell); err != nil {
		t.Fatalf("query job cell: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT owning_cell FROM job_runs WHERE run_id = ?", runID).Scan(&runCell); err != nil {
		t.Fatalf("query run cell: %v", err)
	}

	var segmentCount, executionCount, taskCount, taskAttemptCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM run_segments WHERE run_id = ?", runID).Scan(&segmentCount); err != nil {
		t.Fatalf("query segment count: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*), cell_id FROM segment_executions WHERE run_id = ? GROUP BY cell_id", runID).Scan(&executionCount, &executionCell); err != nil {
		t.Fatalf("query execution count/cell: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM run_tasks WHERE run_id = ? AND task_key = ?", runID, dal.RootTaskKey).Scan(&taskCount); err != nil {
		t.Fatalf("query task count: %v", err)
	}

	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM task_attempts WHERE run_id = ? AND attempt = 1", runID).Scan(&taskAttemptCount); err != nil {
		t.Fatalf("query task attempt count: %v", err)
	}

	if segmentCount != 1 || executionCount != 1 || taskCount != 1 || taskAttemptCount != 1 {
		t.Fatalf("expected one root execution and task, got segments=%d executions=%d tasks=%d attempts=%d", segmentCount, executionCount, taskCount, taskAttemptCount)
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

	if dispatch.TaskID != runID+":root" {
		t.Fatalf("dispatch task id: got %q, want %s:root", dispatch.TaskID, runID)
	}

	if dispatch.TaskKey != dal.RootTaskKey || dispatch.TaskName != dal.RootTaskKey {
		t.Fatalf("dispatch task key/name: got %q/%q, want root/root", dispatch.TaskKey, dispatch.TaskName)
	}

	if dispatch.TaskAttemptID != runID+":root:attempt:1" {
		t.Fatalf("dispatch task attempt id: got %q, want %s:root:attempt:1", dispatch.TaskAttemptID, runID)
	}
	assertExecutionTaskLink(t, db, dispatch.ExecutionID, dispatch.TaskID, dispatch.TaskAttemptID)

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
		"namespace": namespaceCell,
		"job":       jobCell,
		"run":       runCell,
		"execution": executionCell,
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

func TestRunsRepository_CreateReplayRun_UsesSourceSnapshot(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-replay", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-replay"
	defV1 := `{"id":"job-replay","root":{"uses":"builtins/shell","with":{"command":"echo old"}}}`
	defV2 := `{"id":"job-replay","root":{"uses":"builtins/shell","with":{"command":"echo new"}}}`
	if err := repos.Jobs().Create(ctx, jobID, defV1, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	sourceRunID, _, err := repos.Runs().CreateRunInCell(ctx, jobID, nil, 1, "pdx-b")
	if err != nil {
		t.Fatalf("create source run: %v", err)
	}

	if err := repos.Runs().MarkRunFailed(ctx, sourceRunID, "", dal.FailureCodeExecution, "environment failed"); err != nil {
		t.Fatalf("mark source failed: %v", err)
	}

	if _, err := repos.Jobs().UpdateDefinition(ctx, jobID, defV2); err != nil {
		t.Fatalf("update job definition: %v", err)
	}

	invocation, err := repos.TriggerInvocations().Record(ctx, dal.TriggerInvocation{
		JobID:              jobID,
		TriggerType:        dal.TriggerTypeReplay,
		TriggerPayloadHash: dal.PayloadHash(`{"source_run_id":"` + sourceRunID + `"}`),
		RequestedCells:     []string{"pdx-b"},
	})

	if err != nil {
		t.Fatalf("record replay invocation: %v", err)
	}

	replay, err := repos.Runs().CreateReplayRun(ctx, sourceRunID, "", dal.RunAuditMetadata{
		TriggerInvocationID: invocation.InvocationID,
	})

	if err != nil {
		t.Fatalf("create replay run: %v", err)
	}

	if replay.RunID == sourceRunID {
		t.Fatal("replay should create a new run id")
	}

	if replay.RunIndex != 2 || replay.TargetCellID != "pdx-b" {
		t.Fatalf("replay created run: got index=%d cell=%q", replay.RunIndex, replay.TargetCellID)
	}

	run, err := repos.Runs().GetRun(ctx, replay.RunID)
	if err != nil {
		t.Fatalf("get replay run: %v", err)
	}

	if run.DefinitionVersion != 1 || run.DefinitionHash != dal.DefinitionHash(defV1) {
		t.Fatalf("replay definition snapshot: got version=%d hash=%q", run.DefinitionVersion, run.DefinitionHash)
	}

	if run.ReplayOfRunID == nil || *run.ReplayOfRunID != sourceRunID {
		t.Fatalf("replay source: got %+v want %q", run.ReplayOfRunID, sourceRunID)
	}

	if run.TriggerType == nil || *run.TriggerType != dal.TriggerTypeReplay {
		t.Fatalf("replay trigger type: got %+v want %q", run.TriggerType, dal.TriggerTypeReplay)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, replay.RunID)
	if err != nil {
		t.Fatalf("get replay pending execution: %v", err)
	}

	if dispatch.CellID != "pdx-b" || dispatch.DefinitionVersion != 1 {
		t.Fatalf("replay dispatch: got cell=%q version=%d", dispatch.CellID, dispatch.DefinitionVersion)
	}

	activeRunID, _, err := repos.Runs().CreateRunInCell(ctx, jobID, nil, 1, "pdx-b")
	if err != nil {
		t.Fatalf("create active source run: %v", err)
	}

	if _, err := repos.Runs().CreateReplayRun(ctx, activeRunID, "", dal.RunAuditMetadata{}); !dal.IsConflict(err) {
		t.Fatalf("expected active source replay to conflict, got %v", err)
	}
}

func TestRunsRepository_CountByStatusByCell(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-status-cells", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-status-cells"
	def := `{"id":"job-status-cells","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	created, err := repos.Runs().CreateRunsInCells(ctx, jobID, nil, 1, []string{"iad-a", "iad-a", "pdx-b", "sfo-c"})
	if err != nil {
		t.Fatalf("CreateRunsInCells: %v", err)
	}

	if len(created) != 4 {
		t.Fatalf("created runs: got %d, want 4", len(created))
	}

	if _, err := db.ExecContext(ctx, "UPDATE job_runs SET status = 'running' WHERE run_id = ?", created[3].RunID); err != nil {
		t.Fatalf("mark non-queued run: %v", err)
	}

	counts, err := repos.Runs().CountByStatusByCell(ctx, dal.RunStatusQueued)
	if err != nil {
		t.Fatalf("CountByStatusByCell: %v", err)
	}

	want := []dal.RunCountByCell{
		{CellID: "iad-a", Count: 2},
		{CellID: "pdx-b", Count: 1},
	}

	if len(counts) != len(want) {
		t.Fatalf("counts len: got %d want %d (%+v)", len(counts), len(want), counts)
	}

	for i := range want {
		if counts[i] != want[i] {
			t.Fatalf("counts[%d]: got %+v want %+v", i, counts[i], want[i])
		}
	}
}

func TestRunsRepository_CountStuckBeforeDispatchCutoffByCell(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-stuck-cells", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-stuck-cells"
	def := `{"id":"job-stuck-cells","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	created, err := repos.Runs().CreateRunsInCells(ctx, jobID, nil, 1, []string{"iad-a", "iad-a", "pdx-b", "sfo-c"})
	if err != nil {
		t.Fatalf("CreateRunsInCells: %v", err)
	}

	if len(created) != 4 {
		t.Fatalf("created runs: got %d, want 4", len(created))
	}

	if err := repos.Runs().TouchDispatched(ctx, created[3].RunID); err != nil {
		t.Fatalf("touch recently dispatched run: %v", err)
	}

	cutoff := time.Now().Add(-1 * time.Minute).Unix()
	total, err := repos.Runs().CountStuckBeforeDispatchCutoff(ctx, cutoff)
	if err != nil {
		t.Fatalf("CountStuckBeforeDispatchCutoff: %v", err)
	}
	if total != 3 {
		t.Fatalf("stuck total: got %d, want 3", total)
	}

	counts, err := repos.Runs().CountStuckBeforeDispatchCutoffByCell(ctx, cutoff)
	if err != nil {
		t.Fatalf("CountStuckBeforeDispatchCutoffByCell: %v", err)
	}

	want := []dal.RunCountByCell{
		{CellID: "iad-a", Count: 2},
		{CellID: "pdx-b", Count: 1},
	}
	if len(counts) != len(want) {
		t.Fatalf("counts len: got %d want %d (%+v)", len(counts), len(want), counts)
	}
	for i := range want {
		if counts[i] != want[i] {
			t.Fatalf("counts[%d]: got %+v want %+v", i, counts[i], want[i])
		}
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

func TestRunsRepository_ListRunTasks_ReturnsRootTaskAndAttempt(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-tasks", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-root-task-list"
	def := `{"id":"job-root-task-list","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	tasks, nextCursor, err := repos.Runs().ListRunTasks(ctx, runID, 0, 50)
	if err != nil {
		t.Fatalf("list run tasks: %v", err)
	}

	if nextCursor != 0 {
		t.Fatalf("next cursor: got %d, want 0", nextCursor)
	}

	if len(tasks) != 1 {
		t.Fatalf("tasks: got %d, want 1", len(tasks))
	}

	task := tasks[0]
	if task.TaskID != runID+":root" || task.RunID != runID || task.TaskKey != dal.RootTaskKey || task.Name != dal.RootTaskKey || task.Status != dal.TaskStatusPending {
		t.Fatalf("unexpected root task: %+v", task)
	}

	if task.ParentTaskID != nil {
		t.Fatalf("root task parent: got %+v, want nil", task.ParentTaskID)
	}

	if len(task.Attempts) != 1 {
		t.Fatalf("attempts: got %d, want 1", len(task.Attempts))
	}

	attempt := task.Attempts[0]
	if attempt.AttemptID != runID+":root:attempt:1" || attempt.TaskID != task.TaskID || attempt.RunID != runID || attempt.CellID != "iad-a" || attempt.Attempt != 1 || attempt.Status != dal.TaskStatusPending {
		t.Fatalf("unexpected root task attempt: %+v", attempt)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO run_tasks (task_id, run_id, parent_task_id, task_key, name, status)
		VALUES (?, ?, ?, ?, ?, ?)
	`, runID+":child", runID, task.TaskID, "child", "child", dal.TaskStatusPending); err != nil {
		t.Fatalf("insert child task: %v", err)
	}

	firstPage, nextCursor, err := repos.Runs().ListRunTasks(ctx, runID, 0, 1)
	if err != nil {
		t.Fatalf("list first task page: %v", err)
	}

	if len(firstPage) != 1 || firstPage[0].TaskID != task.TaskID || nextCursor == 0 {
		t.Fatalf("first task page: tasks=%+v next=%d", firstPage, nextCursor)
	}

	secondPage, nextCursor, err := repos.Runs().ListRunTasks(ctx, runID, nextCursor, 1)
	if err != nil {
		t.Fatalf("list second task page: %v", err)
	}

	if len(secondPage) != 1 || secondPage[0].TaskID != runID+":child" || nextCursor != 0 {
		t.Fatalf("second task page: tasks=%+v next=%d", secondPage, nextCursor)
	}

	if _, _, err := repos.Runs().ListRunTasks(ctx, "missing-run", 0, 50); !dal.IsNotFound(err) {
		t.Fatalf("expected missing run to return ErrNotFound, got %v", err)
	}
}

func TestRunsRepository_EnsurePendingTaskExecutionCreatesLinkedRows(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-task-create", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-create"
	def := `{"id":"job-task-create","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	child, created, err := repos.Runs().EnsurePendingTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:    runID,
		TaskKey:  "child",
		Name:     "child task",
		SpecHash: "sha256:child",
	})
	if err != nil {
		t.Fatalf("EnsurePendingTaskExecution: %v", err)
	}

	if !created {
		t.Fatal("first EnsurePendingTaskExecution should create rows")
	}

	wantTaskID := runID + ":child"
	wantAttemptID := wantTaskID + ":attempt:1"
	if child.RunID != runID || child.TaskID != wantTaskID || child.ParentTaskID != runID+":root" || child.TaskKey != "child" || child.Name != "child task" {
		t.Fatalf("unexpected child task record: %+v", child)
	}

	if child.TaskAttemptID != wantAttemptID || child.SegmentID != wantTaskID+":segment" || child.ExecutionID != wantAttemptID+":execution" || child.CellID != "iad-a" || child.Attempt != 1 {
		t.Fatalf("unexpected child execution record: %+v", child)
	}
	assertExecutionTaskLink(t, db, child.ExecutionID, child.TaskID, child.TaskAttemptID)

	tasks, _, err := repos.Runs().ListRunTasks(ctx, runID, 0, 10)
	if err != nil {
		t.Fatalf("list run tasks: %v", err)
	}

	if len(tasks) != 2 {
		t.Fatalf("tasks: got %d, want 2: %+v", len(tasks), tasks)
	}

	var childTask *dal.TaskRecord
	for i := range tasks {
		if tasks[i].TaskID == child.TaskID {
			childTask = &tasks[i]
			break
		}
	}

	if childTask == nil {
		t.Fatalf("child task %q missing from list: %+v", child.TaskID, tasks)
	}

	if childTask.ParentTaskID == nil || *childTask.ParentTaskID != runID+":root" || childTask.SpecHash != "sha256:child" {
		t.Fatalf("unexpected listed child task: %+v", childTask)
	}

	if len(childTask.Attempts) != 1 || childTask.Attempts[0].AttemptID != child.TaskAttemptID || childTask.Attempts[0].CellID != "iad-a" || childTask.Attempts[0].Status != dal.TaskStatusPending {
		t.Fatalf("unexpected listed child attempts: %+v", childTask.Attempts)
	}

	again, created, err := repos.Runs().EnsurePendingTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:    runID,
		TaskKey:  "child",
		Name:     "child task",
		SpecHash: "sha256:child",
	})
	if err != nil {
		t.Fatalf("idempotent EnsurePendingTaskExecution: %v", err)
	}

	if created {
		t.Fatal("duplicate EnsurePendingTaskExecution should not create rows")
	}

	if again != child {
		t.Fatalf("idempotent record mismatch: got %+v, want %+v", again, child)
	}

	if _, _, err := repos.Runs().EnsurePendingTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:    runID,
		TaskKey:  "child",
		Name:     "different",
		SpecHash: "sha256:child",
	}); !dal.IsConflict(err) {
		t.Fatalf("expected conflicting duplicate to fail, got %v", err)
	}
}

func TestRunsRepository_EnsurePlannedTaskExecutionCreatesNonDispatchableRows(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-task-plan", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-plan"
	def := `{"id":"job-task-plan","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root pending execution: %v", err)
	}

	child, created, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		TaskKey:      "child",
		Name:         "child task",
		SpecHash:     "sha256:child",
		TargetCellID: "pdx-b",
	})
	if err != nil {
		t.Fatalf("EnsurePlannedTaskExecution: %v", err)
	}

	if !created {
		t.Fatal("first EnsurePlannedTaskExecution should create rows")
	}

	if child.CellID != "pdx-b" {
		t.Fatalf("child cell: got %q, want pdx-b", child.CellID)
	}

	assertTaskExecutionStatuses(t, db, child, dal.TaskStatusPlanned, dal.SegmentStatusPlanned, dal.ExecutionStatusPlanned, 0)

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution after planned child: %v", err)
	}

	if dispatch.ExecutionID != rootDispatch.ExecutionID || dispatch.TaskKey != dal.RootTaskKey {
		t.Fatalf("planned child should not dispatch before root: got %+v, want root execution %q", dispatch, rootDispatch.ExecutionID)
	}

	if err := repos.Runs().MarkExecutionAccepted(ctx, rootDispatch.ExecutionID); err != nil {
		t.Fatalf("mark root accepted: %v", err)
	}

	if _, err := repos.Runs().GetPendingExecution(ctx, runID); !dal.IsNotFound(err) {
		t.Fatalf("planned child should not dispatch after root accepts, got %v", err)
	}

	if err := repos.Runs().MarkExecutionAccepted(ctx, child.ExecutionID); !dal.IsConflict(err) {
		t.Fatalf("planned execution should reject dispatch transition, got %v", err)
	}

	again, created, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		TaskKey:      "child",
		Name:         "child task",
		SpecHash:     "sha256:child",
		TargetCellID: "pdx-b",
	})
	if err != nil {
		t.Fatalf("idempotent EnsurePlannedTaskExecution: %v", err)
	}

	if created {
		t.Fatal("duplicate EnsurePlannedTaskExecution should not create rows")
	}

	if again != child {
		t.Fatalf("idempotent record mismatch: got %+v, want %+v", again, child)
	}

	activated, didActivate, err := repos.Runs().ActivatePlannedTaskExecution(ctx, child.TaskID)
	if err != nil {
		t.Fatalf("ActivatePlannedTaskExecution: %v", err)
	}

	if !didActivate {
		t.Fatal("planned task activation should report a transition")
	}

	if activated != child {
		t.Fatalf("activated record mismatch: got %+v, want %+v", activated, child)
	}

	assertTaskExecutionStatuses(t, db, child, dal.TaskStatusPending, dal.SegmentStatusPending, dal.ExecutionStatusPending, 0)

	dispatch, err = repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution after activation: %v", err)
	}

	if dispatch.ExecutionID != child.ExecutionID || dispatch.TaskID != child.TaskID || dispatch.TaskKey != "child" {
		t.Fatalf("activated child dispatch mismatch: %+v", dispatch)
	}

	activated, didActivate, err = repos.Runs().ActivatePlannedTaskExecution(ctx, child.TaskID)
	if err != nil {
		t.Fatalf("idempotent ActivatePlannedTaskExecution: %v", err)
	}

	if didActivate {
		t.Fatal("pending task activation should be idempotent")
	}

	if activated != child {
		t.Fatalf("idempotent activation record mismatch: got %+v, want %+v", activated, child)
	}

	if err := repos.Runs().MarkExecutionAccepted(ctx, child.ExecutionID); err != nil {
		t.Fatalf("mark activated child accepted: %v", err)
	}

	assertExecutionAndSegmentStatus(t, db, child.ExecutionID, child.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertTaskAndAttemptStatus(t, db, child.TaskID, 1, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)

	if _, _, err := repos.Runs().ActivatePlannedTaskExecution(ctx, child.TaskID); !dal.IsConflict(err) {
		t.Fatalf("accepted task activation should conflict, got %v", err)
	}
}

func TestRunsRepository_ActivatePlannedChildTaskExecutionsFansOutDirectChildren(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-task-child-activate", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-child-activate"
	def := `{"id":"job-task-child-activate","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root pending execution: %v", err)
	}

	setup, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:    runID,
		TaskKey:  "setup",
		SpecHash: "sha256:setup",
	})
	if err != nil {
		t.Fatalf("ensure setup: %v", err)
	}

	build, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:    runID,
		TaskKey:  "build",
		SpecHash: "sha256:build",
	})
	if err != nil {
		t.Fatalf("ensure build: %v", err)
	}

	compile, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: setup.TaskID,
		TaskKey:      "compile",
		SpecHash:     "sha256:compile",
	})
	if err != nil {
		t.Fatalf("ensure compile: %v", err)
	}

	if err := repos.Runs().MarkExecutionAccepted(ctx, rootDispatch.ExecutionID); err != nil {
		t.Fatalf("mark root accepted: %v", err)
	}

	if _, err := repos.Runs().GetPendingExecution(ctx, runID); !dal.IsNotFound(err) {
		t.Fatalf("planned descendants should not dispatch before fan-out, got %v", err)
	}

	children, activated, err := repos.Runs().ActivatePlannedChildTaskExecutions(ctx, rootDispatch.TaskID)
	if err != nil {
		t.Fatalf("ActivatePlannedChildTaskExecutions: %v", err)
	}

	if activated != 2 || len(children) != 2 {
		t.Fatalf("activated root children: activated=%d children=%+v", activated, children)
	}

	byKey := taskExecutionRecordsByKey(children)
	if byKey["setup"] != setup || byKey["build"] != build {
		t.Fatalf("activated root children mismatch: %+v", children)
	}

	assertTaskExecutionStatuses(t, db, setup, dal.TaskStatusPending, dal.SegmentStatusPending, dal.ExecutionStatusPending, 0)
	assertTaskExecutionStatuses(t, db, build, dal.TaskStatusPending, dal.SegmentStatusPending, dal.ExecutionStatusPending, 0)
	assertTaskExecutionStatuses(t, db, compile, dal.TaskStatusPlanned, dal.SegmentStatusPlanned, dal.ExecutionStatusPlanned, 0)

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution after root fan-out: %v", err)
	}

	if dispatch.TaskKey != "setup" && dispatch.TaskKey != "build" {
		t.Fatalf("pending execution after root fan-out: %+v", dispatch)
	}

	children, activated, err = repos.Runs().ActivatePlannedChildTaskExecutions(ctx, rootDispatch.TaskID)
	if err != nil {
		t.Fatalf("idempotent ActivatePlannedChildTaskExecutions: %v", err)
	}

	if activated != 0 || len(children) != 2 {
		t.Fatalf("idempotent root fan-out: activated=%d children=%+v", activated, children)
	}

	if err := repos.Runs().MarkExecutionAccepted(ctx, setup.ExecutionID); err != nil {
		t.Fatalf("mark setup accepted: %v", err)
	}

	children, activated, err = repos.Runs().ActivatePlannedChildTaskExecutions(ctx, rootDispatch.TaskID)
	if err != nil {
		t.Fatalf("replayed root fan-out after setup accepted: %v", err)
	}

	if activated != 0 || len(children) != 1 || children[0] != build {
		t.Fatalf("replayed root fan-out should return only dispatchable build: activated=%d children=%+v", activated, children)
	}

	grandchildren, activated, err := repos.Runs().ActivatePlannedChildTaskExecutions(ctx, setup.TaskID)
	if err != nil {
		t.Fatalf("activate setup children: %v", err)
	}

	if activated != 1 || len(grandchildren) != 1 || grandchildren[0] != compile {
		t.Fatalf("activated setup children: activated=%d grandchildren=%+v", activated, grandchildren)
	}

	assertTaskExecutionStatuses(t, db, compile, dal.TaskStatusPending, dal.SegmentStatusPending, dal.ExecutionStatusPending, 0)

	if _, _, err := repos.Runs().ActivatePlannedChildTaskExecutions(ctx, "missing-parent"); !dal.IsNotFound(err) {
		t.Fatalf("missing parent fan-out should return not found, got %v", err)
	}
}

func TestRunsRepository_MarkExecutionSucceededAndActivateChildren(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-task-success-fanout", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-success-fanout"
	def := `{"id":"job-task-success-fanout","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root pending execution: %v", err)
	}

	setup, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:    runID,
		TaskKey:  "setup",
		SpecHash: "sha256:setup",
	})
	if err != nil {
		t.Fatalf("ensure setup: %v", err)
	}

	build, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:    runID,
		TaskKey:  "build",
		SpecHash: "sha256:build",
	})
	if err != nil {
		t.Fatalf("ensure build: %v", err)
	}

	compile, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: setup.TaskID,
		TaskKey:      "compile",
		SpecHash:     "sha256:compile",
	})
	if err != nil {
		t.Fatalf("ensure compile: %v", err)
	}

	children, activated, err := repos.Runs().MarkExecutionSucceededAndActivateChildren(ctx, rootDispatch.ExecutionID)
	if err != nil {
		t.Fatalf("MarkExecutionSucceededAndActivateChildren root: %v", err)
	}

	if activated != 2 || len(children) != 2 {
		t.Fatalf("root success fan-out: activated=%d children=%+v", activated, children)
	}

	byKey := taskExecutionRecordsByKey(children)
	if byKey["setup"] != setup || byKey["build"] != build {
		t.Fatalf("root success children mismatch: %+v", children)
	}

	assertExecutionAndSegmentStatus(t, db, rootDispatch.ExecutionID, rootDispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 1)
	assertRootTaskAndAttemptStatus(t, db, runID, dal.TaskStatusSucceeded, dal.TaskStatusSucceeded, 1)
	assertTaskExecutionStatuses(t, db, setup, dal.TaskStatusPending, dal.SegmentStatusPending, dal.ExecutionStatusPending, 0)
	assertTaskExecutionStatuses(t, db, build, dal.TaskStatusPending, dal.SegmentStatusPending, dal.ExecutionStatusPending, 0)
	assertTaskExecutionStatuses(t, db, compile, dal.TaskStatusPlanned, dal.SegmentStatusPlanned, dal.ExecutionStatusPlanned, 0)

	intents, err := repos.TaskDispatchIntents().ListPending(ctx, "iad-a", 0, 100)
	if err != nil {
		t.Fatalf("list root fan-out dispatch intents: %v", err)
	}

	if len(intents) != 2 {
		t.Fatalf("root fan-out dispatch intents: got %d, want 2: %+v", len(intents), intents)
	}

	intentsByExecution := taskDispatchIntentsByExecutionID(intents)
	for _, child := range []dal.TaskExecutionRecord{setup, build} {
		intent, ok := intentsByExecution[child.ExecutionID]
		if !ok {
			t.Fatalf("missing dispatch intent for child execution %s: %+v", child.ExecutionID, intents)
		}

		if intent.SourceExecutionID != rootDispatch.ExecutionID || intent.RunID != runID || intent.TaskID != child.TaskID || intent.TaskAttemptID != child.TaskAttemptID || intent.CellID != "iad-a" {
			t.Fatalf("dispatch intent for %s mismatch: %+v", child.ExecutionID, intent)
		}
	}

	children, activated, err = repos.Runs().MarkExecutionSucceededAndActivateChildren(ctx, rootDispatch.ExecutionID)
	if err != nil {
		t.Fatalf("idempotent root success fan-out: %v", err)
	}

	if activated != 0 || len(children) != 2 {
		t.Fatalf("idempotent root success fan-out: activated=%d children=%+v", activated, children)
	}

	intents, err = repos.TaskDispatchIntents().ListPending(ctx, "iad-a", 0, 100)
	if err != nil {
		t.Fatalf("list idempotent root fan-out dispatch intents: %v", err)
	}

	if len(intents) != 2 {
		t.Fatalf("idempotent root fan-out should not duplicate dispatch intents, got %d: %+v", len(intents), intents)
	}

	grandchildren, activated, err := repos.Runs().MarkExecutionSucceededAndActivateChildren(ctx, setup.ExecutionID)
	if err != nil {
		t.Fatalf("setup success fan-out: %v", err)
	}

	if activated != 1 || len(grandchildren) != 1 || grandchildren[0] != compile {
		t.Fatalf("setup success children: activated=%d grandchildren=%+v", activated, grandchildren)
	}

	assertExecutionAndSegmentStatus(t, db, setup.ExecutionID, setup.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 1)
	assertTaskAndAttemptStatus(t, db, setup.TaskID, 1, dal.TaskStatusSucceeded, dal.TaskStatusSucceeded, 1)
	assertTaskExecutionStatuses(t, db, compile, dal.TaskStatusPending, dal.SegmentStatusPending, dal.ExecutionStatusPending, 0)

	intents, err = repos.TaskDispatchIntents().ListPending(ctx, "iad-a", 0, 100)
	if err != nil {
		t.Fatalf("list setup fan-out dispatch intents: %v", err)
	}

	if len(intents) != 2 {
		t.Fatalf("setup fan-out should leave build and compile dispatchable, got %d: %+v", len(intents), intents)
	}

	intentsByExecution = taskDispatchIntentsByExecutionID(intents)
	if _, ok := intentsByExecution[setup.ExecutionID]; ok {
		t.Fatalf("succeeded setup execution should no longer be pending dispatch: %+v", intents)
	}

	compileIntent, ok := intentsByExecution[compile.ExecutionID]
	if !ok {
		t.Fatalf("missing compile dispatch intent: %+v", intents)
	}

	if compileIntent.SourceExecutionID != setup.ExecutionID {
		t.Fatalf("compile dispatch source: got %q, want %q", compileIntent.SourceExecutionID, setup.ExecutionID)
	}

	if err := repos.Runs().MarkExecutionTerminal(ctx, build.ExecutionID, dal.ExecutionStatusFailed); err != nil {
		t.Fatalf("mark build failed: %v", err)
	}

	if _, _, err := repos.Runs().MarkExecutionSucceededAndActivateChildren(ctx, build.ExecutionID); !dal.IsConflict(err) {
		t.Fatalf("failed execution success fan-out should conflict, got %v", err)
	}

	if _, _, err := repos.Runs().MarkExecutionSucceededAndActivateChildren(ctx, "missing-execution"); !dal.IsNotFound(err) {
		t.Fatalf("missing execution success fan-out should return not found, got %v", err)
	}
}

func TestTaskDispatchIntentsRepository_Lifecycle(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-task-dispatch-intents", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-dispatch-intents"
	def := `{"id":"job-task-dispatch-intents","root":{"uses":"builtins/shell"}}`
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

	intents := repos.TaskDispatchIntents()
	intent, created, err := intents.Ensure(ctx, dal.TaskDispatchIntentCreate{
		ExecutionID:       dispatch.ExecutionID,
		RunID:             dispatch.RunID,
		TaskID:            dispatch.TaskID,
		TaskAttemptID:     dispatch.TaskAttemptID,
		SourceExecutionID: "source-execution",
		CellID:            dispatch.CellID,
	})
	if err != nil {
		t.Fatalf("ensure dispatch intent: %v", err)
	}
	if !created {
		t.Fatal("first ensure should create dispatch intent")
	}
	if intent.ExecutionID != dispatch.ExecutionID || intent.RunID != runID || intent.TaskID != dispatch.TaskID || intent.TaskAttemptID != dispatch.TaskAttemptID || intent.SourceExecutionID != "source-execution" || intent.CellID != "iad-a" {
		t.Fatalf("dispatch intent mismatch: %+v", intent)
	}

	pending, err := intents.ListPending(ctx, "iad-a", 0, 10)
	if err != nil {
		t.Fatalf("list pending dispatch intents: %v", err)
	}
	if len(pending) != 1 || pending[0].ExecutionID != dispatch.ExecutionID {
		t.Fatalf("pending dispatch intents: %+v", pending)
	}

	if err := intents.MarkEnqueueFailed(ctx, dispatch.ExecutionID, 1000, "queue unavailable"); err != nil {
		t.Fatalf("mark enqueue failed: %v", err)
	}

	pending, err = intents.ListPending(ctx, "iad-a", 999, 10)
	if err != nil {
		t.Fatalf("list pending before retry cutoff: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("intent should wait for retry cutoff, got %+v", pending)
	}

	pending, err = intents.ListPending(ctx, "iad-a", 1000, 10)
	if err != nil {
		t.Fatalf("list pending at retry cutoff: %v", err)
	}
	if len(pending) != 1 || pending[0].EnqueueAttempts != 1 || pending[0].LastEnqueueAttemptAt == nil || *pending[0].LastEnqueueAttemptAt != 1000 || pending[0].LastEnqueueError == nil {
		t.Fatalf("pending dispatch intent after failure mismatch: %+v", pending)
	}

	again, created, err := intents.Ensure(ctx, dal.TaskDispatchIntentCreate{
		ExecutionID:       dispatch.ExecutionID,
		RunID:             dispatch.RunID,
		TaskID:            dispatch.TaskID,
		TaskAttemptID:     dispatch.TaskAttemptID,
		SourceExecutionID: "source-execution",
		CellID:            dispatch.CellID,
	})
	if err != nil {
		t.Fatalf("idempotent ensure dispatch intent: %v", err)
	}
	if created || again.ID != intent.ID || again.EnqueueAttempts != 1 {
		t.Fatalf("idempotent ensure mismatch: created=%v again=%+v first=%+v", created, again, intent)
	}

	if err := intents.MarkEnqueued(ctx, dispatch.ExecutionID, 2000); err != nil {
		t.Fatalf("mark enqueued: %v", err)
	}

	pending, err = intents.ListPending(ctx, "iad-a", 3000, 10)
	if err != nil {
		t.Fatalf("list pending after enqueue: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("enqueued intent should not remain pending: %+v", pending)
	}

	if err := intents.MarkEnqueued(ctx, "missing-execution", 0); !dal.IsNotFound(err) {
		t.Fatalf("mark missing enqueued should return not found, got %v", err)
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
	assertRootTaskAndAttemptStatus(t, db, runID, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)

	if err := repos.Runs().MarkExecutionStarted(ctx, dispatch.ExecutionID); err != nil {
		t.Fatalf("mark started: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusRunning, dal.SegmentStatusRunning, 2)
	assertRootTaskAndAttemptStatus(t, db, runID, dal.TaskStatusRunning, dal.TaskStatusRunning, 2)

	if err := repos.Runs().MarkExecutionTerminal(ctx, dispatch.ExecutionID, dal.ExecutionStatusSucceeded); err != nil {
		t.Fatalf("mark terminal: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 3)
	assertRootTaskAndAttemptStatus(t, db, runID, dal.TaskStatusSucceeded, dal.TaskStatusSucceeded, 3)

	if err := repos.Runs().MarkExecutionStarted(ctx, dispatch.ExecutionID); !dal.IsConflict(err) {
		t.Fatalf("expected conflict restarting terminal execution, got %v", err)
	}
}

func TestRunsRepository_DispatchAndTransitionsUseLinkedTaskAttempt(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-linked-task", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-linked-task"
	def := `{"id":"job-linked-task","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, ns.ID); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root pending execution: %v", err)
	}

	child, created, err := repos.Runs().EnsurePendingTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:   runID,
		TaskKey: "child",
	})
	if err != nil {
		t.Fatalf("ensure child task execution: %v", err)
	}

	if !created {
		t.Fatal("child task execution should be created")
	}

	if err := repos.Runs().MarkExecutionAccepted(ctx, rootDispatch.ExecutionID); err != nil {
		t.Fatalf("mark root accepted: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get child pending execution: %v", err)
	}

	if dispatch.ExecutionID != child.ExecutionID {
		t.Fatalf("dispatch execution id: got %q, want %q", dispatch.ExecutionID, child.ExecutionID)
	}

	if dispatch.TaskID != child.TaskID || dispatch.TaskKey != "child" || dispatch.TaskName != "child" || dispatch.TaskAttemptID != child.TaskAttemptID {
		t.Fatalf("dispatch task identity: got task=%q key=%q name=%q attempt=%q", dispatch.TaskID, dispatch.TaskKey, dispatch.TaskName, dispatch.TaskAttemptID)
	}

	if err := repos.Runs().MarkExecutionAccepted(ctx, child.ExecutionID); err != nil {
		t.Fatalf("mark child accepted: %v", err)
	}

	assertExecutionAndSegmentStatus(t, db, child.ExecutionID, child.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertTaskAndAttemptStatus(t, db, child.TaskID, 1, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)
	assertRootTaskAndAttemptStatus(t, db, runID, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)
}

func TestRunCatalogUpdater_AppliesRunAndExecutionStatusUpdates(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-catalog-updates", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-catalog-updates"
	def := `{"id":"job-catalog-updates","root":{"uses":"builtins/shell"}}`
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

	var updater dal.RunCatalogUpdater = repos.Runs()
	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: dispatch.ExecutionID,
		Status:      dal.ExecutionStatusAccepted,
	}); err != nil {
		t.Fatalf("apply accepted execution update: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)

	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: dispatch.ExecutionID,
		Status:      dal.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("apply running execution update: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusRunning, dal.SegmentStatusRunning, 2)

	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: dispatch.ExecutionID,
		Status:      dal.ExecutionStatusFailed,
	}); err != nil {
		t.Fatalf("apply failed execution update: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusFailed, dal.SegmentStatusFailed, 3)

	if err := updater.ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{
		RunID:  runID,
		Status: dal.RunStatusRunning,
	}); err != nil {
		t.Fatalf("apply running run update: %v", err)
	}

	if err := updater.ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{
		RunID:       runID,
		Status:      dal.RunStatusFailed,
		FailureCode: dal.FailureCodeExecution,
		Reason:      "cell reported failure",
	}); err != nil {
		t.Fatalf("apply failed run update: %v", err)
	}

	run, err := repos.Runs().GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	if run.Status != dal.RunStatusFailed {
		t.Fatalf("run status: got %q, want %q", run.Status, dal.RunStatusFailed)
	}

	if run.FailureCode == nil || *run.FailureCode != dal.FailureCodeExecution {
		t.Fatalf("failure code: got %+v, want %q", run.FailureCode, dal.FailureCodeExecution)
	}

	if run.FailureReason == nil || *run.FailureReason != "cell reported failure" {
		t.Fatalf("failure reason: got %+v", run.FailureReason)
	}
}

func TestRunsRepository_ExecutionTransitionsRejectInvalidTargets(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	runID := "run-invalid-update"
	if err := repos.Runs().ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{RunID: runID, Status: "waiting"}); !dal.IsConflict(err) {
		t.Fatalf("expected unsupported run status to return ErrConflict, got %v", err)
	}

	if err := repos.Runs().MarkExecutionAccepted(ctx, "missing-execution"); !dal.IsNotFound(err) {
		t.Fatalf("expected missing execution to return ErrNotFound, got %v", err)
	}

	if err := repos.Runs().MarkExecutionTerminal(ctx, "missing-execution", dal.ExecutionStatusRunning); !dal.IsConflict(err) {
		t.Fatalf("expected non-terminal status to return ErrConflict, got %v", err)
	}

	if err := repos.Runs().ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{ExecutionID: "execution-invalid-update", Status: dal.ExecutionStatusPending}); !dal.IsConflict(err) {
		t.Fatalf("expected unsupported execution status to return ErrConflict, got %v", err)
	}
}

func TestCatalogEventsRepository_RecordListAndMark(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	events := repos.CatalogEvents()
	ctx := context.Background()

	first, created, err := events.Record(ctx, "iad-a", "event-1", "run.status", []byte(`{"run_id":"run-1","status":"running"}`))
	if err != nil {
		t.Fatalf("record first event: %v", err)
	}

	if !created {
		t.Fatal("expected first record to create an event")
	}

	if first.ID == 0 || first.Status != dal.CatalogEventStatusPending || first.Attempts != 0 {
		t.Fatalf("unexpected first event record: %+v", first)
	}

	if string(first.Payload) != `{"run_id":"run-1","status":"running"}` {
		t.Fatalf("unexpected first payload: %s", first.Payload)
	}

	duplicate, created, err := events.Record(ctx, "iad-a", "event-1", "run.status", []byte(`{"run_id":"run-1","status":"failed"}`))
	if err != nil {
		t.Fatalf("record duplicate event: %v", err)
	}

	if created {
		t.Fatal("expected duplicate record to be idempotent")
	}

	if duplicate.ID != first.ID || string(duplicate.Payload) != string(first.Payload) {
		t.Fatalf("duplicate should return original event, got %+v want %+v", duplicate, first)
	}

	second, created, err := events.Record(ctx, "iad-a", "event-2", "execution.status", []byte(`{"execution_id":"execution-1","status":"accepted"}`))
	if err != nil {
		t.Fatalf("record second event: %v", err)
	}

	if !created {
		t.Fatal("expected second record to create an event")
	}

	pending, err := events.ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}

	if len(pending) != 2 || pending[0].ID != first.ID || pending[1].ID != second.ID {
		t.Fatalf("expected pending events in insertion order, got %+v", pending)
	}

	if err := events.MarkApplied(ctx, first.ID); err != nil {
		t.Fatalf("mark applied: %v", err)
	}

	if err := events.MarkFailed(ctx, second.ID, "apply failed"); err != nil {
		t.Fatalf("mark failed: %v", err)
	}

	pending, err = events.ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list pending after marks: %v", err)
	}

	if len(pending) != 0 {
		t.Fatalf("expected no pending events after marks, got %+v", pending)
	}

	var firstStatus string
	var firstAttempts int
	var firstAppliedAt sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT status, attempts, applied_at FROM cell_catalog_events WHERE id = ?", first.ID).
		Scan(&firstStatus, &firstAttempts, &firstAppliedAt); err != nil {
		t.Fatalf("query applied event: %v", err)
	}

	if firstStatus != dal.CatalogEventStatusApplied || firstAttempts != 1 || !firstAppliedAt.Valid {
		t.Fatalf("unexpected applied event state: status=%q attempts=%d applied_at=%+v", firstStatus, firstAttempts, firstAppliedAt)
	}

	var secondStatus string
	var secondAttempts int
	var secondError sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT status, attempts, last_error FROM cell_catalog_events WHERE id = ?", second.ID).
		Scan(&secondStatus, &secondAttempts, &secondError); err != nil {
		t.Fatalf("query failed event: %v", err)
	}

	if secondStatus != dal.CatalogEventStatusFailed || secondAttempts != 1 || !secondError.Valid || secondError.String != "apply failed" {
		t.Fatalf("unexpected failed event state: status=%q attempts=%d error=%+v", secondStatus, secondAttempts, secondError)
	}

	summary, err := events.Summary(ctx)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}

	if summary.Pending != 0 || summary.Applied != 1 || summary.Failed != 1 || summary.Total != 2 {
		t.Fatalf("unexpected summary counts: %+v", summary)
	}

	if summary.LastReceivedUnix == nil || *summary.LastReceivedUnix < first.ReceivedAt || *summary.LastReceivedUnix < second.ReceivedAt {
		t.Fatalf("unexpected last received timestamp: %+v", summary.LastReceivedUnix)
	}

	if summary.LastAppliedUnix == nil {
		t.Fatalf("expected last applied timestamp, got %+v", summary.LastAppliedUnix)
	}
}

func TestCatalogEventsRepository_SummaryBySource(t *testing.T) {
	db := dbtest.NewTestDB(t)
	events := dal.NewSQLRepositories(db).CatalogEvents()
	ctx := context.Background()

	iadApplied, _, err := events.Record(ctx, "iad-a", "iad-applied", "run.status", []byte(`{"run_id":"run-1","status":"running"}`))
	if err != nil {
		t.Fatalf("record iad applied: %v", err)
	}
	iadFailed, _, err := events.Record(ctx, "iad-a", "iad-failed", "execution.status", []byte(`{"execution_id":"execution-1","status":"failed"}`))
	if err != nil {
		t.Fatalf("record iad failed: %v", err)
	}
	if _, _, err := events.Record(ctx, "pdx-b", "pdx-pending", "run.status", []byte(`{"run_id":"run-2","status":"queued"}`)); err != nil {
		t.Fatalf("record pdx pending: %v", err)
	}

	if err := events.MarkApplied(ctx, iadApplied.ID); err != nil {
		t.Fatalf("mark iad applied: %v", err)
	}
	if err := events.MarkFailed(ctx, iadFailed.ID, "apply failed"); err != nil {
		t.Fatalf("mark iad failed: %v", err)
	}

	summaries, err := events.SummaryBySource(ctx)
	if err != nil {
		t.Fatalf("SummaryBySource: %v", err)
	}

	if len(summaries) != 2 {
		t.Fatalf("summaries len: got %d want 2 (%+v)", len(summaries), summaries)
	}

	if summaries[0].SourceCell != "iad-a" || summaries[0].Pending != 0 || summaries[0].Applied != 1 || summaries[0].Failed != 1 || summaries[0].Total != 2 || summaries[0].LastReceivedUnix == nil || summaries[0].LastAppliedUnix == nil {
		t.Fatalf("unexpected iad summary: %+v", summaries[0])
	}

	if summaries[1].SourceCell != "pdx-b" || summaries[1].Pending != 1 || summaries[1].Applied != 0 || summaries[1].Failed != 0 || summaries[1].Total != 1 || summaries[1].LastReceivedUnix == nil || summaries[1].LastAppliedUnix != nil {
		t.Fatalf("unexpected pdx summary: %+v", summaries[1])
	}
}

func TestCatalogEventsRepository_RejectsInvalidRecords(t *testing.T) {
	db := dbtest.NewTestDB(t)
	events := dal.NewSQLRepositories(db).CatalogEvents()
	ctx := context.Background()

	tests := []struct {
		name       string
		sourceCell string
		eventKey   string
		eventType  string
		payload    []byte
	}{
		{name: "source cell", eventKey: "event-1", eventType: "run.status", payload: []byte(`{}`)},
		{name: "event key", sourceCell: "iad-a", eventType: "run.status", payload: []byte(`{}`)},
		{name: "event type", sourceCell: "iad-a", eventKey: "event-1", payload: []byte(`{}`)},
		{name: "payload", sourceCell: "iad-a", eventKey: "event-1", eventType: "run.status"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := events.Record(ctx, tt.sourceCell, tt.eventKey, tt.eventType, tt.payload); !dal.IsConflict(err) {
				t.Fatalf("expected conflict, got %v", err)
			}
		})
	}

	if err := events.MarkApplied(ctx, 404); !dal.IsNotFound(err) {
		t.Fatalf("expected missing applied mark to return not found, got %v", err)
	}
}

func assertExecutionTaskLink(t *testing.T, db *sql.DB, executionID, wantTaskID, wantTaskAttemptID string) {
	t.Helper()

	var taskID, taskAttemptID string
	if err := db.QueryRow("SELECT task_id, task_attempt_id FROM segment_executions WHERE execution_id = ?", executionID).Scan(&taskID, &taskAttemptID); err != nil {
		t.Fatalf("query execution task link: %v", err)
	}

	if taskID != wantTaskID || taskAttemptID != wantTaskAttemptID {
		t.Fatalf("execution task link: got task=%q attempt=%q, want task=%q attempt=%q", taskID, taskAttemptID, wantTaskID, wantTaskAttemptID)
	}
}

func taskExecutionRecordsByKey(records []dal.TaskExecutionRecord) map[string]dal.TaskExecutionRecord {
	out := make(map[string]dal.TaskExecutionRecord, len(records))
	for _, rec := range records {
		out[rec.TaskKey] = rec
	}

	return out
}

func taskDispatchIntentsByExecutionID(intents []dal.TaskDispatchIntent) map[string]dal.TaskDispatchIntent {
	out := make(map[string]dal.TaskDispatchIntent, len(intents))
	for _, intent := range intents {
		out[intent.ExecutionID] = intent
	}

	return out
}

func assertTaskExecutionStatuses(t *testing.T, db *sql.DB, rec dal.TaskExecutionRecord, wantTaskStatus, wantSegmentStatus, wantExecutionStatus string, wantEventSequence int64) {
	t.Helper()

	var taskStatus string
	if err := db.QueryRow("SELECT status FROM run_tasks WHERE task_id = ?", rec.TaskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status: %v", err)
	}

	if taskStatus != wantTaskStatus {
		t.Fatalf("task status: got %q, want %q", taskStatus, wantTaskStatus)
	}

	var attemptStatus string
	var attemptEventSequence int64
	var attemptLastObservedAt sql.NullInt64
	if err := db.QueryRow("SELECT status, last_observed_at, event_sequence FROM task_attempts WHERE attempt_id = ?", rec.TaskAttemptID).
		Scan(&attemptStatus, &attemptLastObservedAt, &attemptEventSequence); err != nil {
		t.Fatalf("query task attempt status: %v", err)
	}

	if attemptStatus != wantTaskStatus {
		t.Fatalf("task attempt status: got %q, want %q", attemptStatus, wantTaskStatus)
	}

	if attemptEventSequence != wantEventSequence {
		t.Fatalf("task attempt event sequence: got %d, want %d", attemptEventSequence, wantEventSequence)
	}

	if wantEventSequence == 0 && attemptLastObservedAt.Valid {
		t.Fatalf("task attempt last_observed_at should not be set before dispatch")
	}

	var segmentStatus string
	if err := db.QueryRow("SELECT status FROM run_segments WHERE segment_id = ?", rec.SegmentID).Scan(&segmentStatus); err != nil {
		t.Fatalf("query segment status: %v", err)
	}

	if segmentStatus != wantSegmentStatus {
		t.Fatalf("segment status: got %q, want %q", segmentStatus, wantSegmentStatus)
	}

	var executionStatus string
	var executionEventSequence int64
	var executionLastObservedAt sql.NullInt64
	if err := db.QueryRow("SELECT status, last_observed_at, event_sequence FROM segment_executions WHERE execution_id = ?", rec.ExecutionID).
		Scan(&executionStatus, &executionLastObservedAt, &executionEventSequence); err != nil {
		t.Fatalf("query execution status: %v", err)
	}

	if executionStatus != wantExecutionStatus {
		t.Fatalf("execution status: got %q, want %q", executionStatus, wantExecutionStatus)
	}

	if executionEventSequence != wantEventSequence {
		t.Fatalf("execution event sequence: got %d, want %d", executionEventSequence, wantEventSequence)
	}

	if wantEventSequence == 0 && executionLastObservedAt.Valid {
		t.Fatalf("execution last_observed_at should not be set before dispatch")
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

func assertRootTaskAndAttemptStatus(t *testing.T, db *sql.DB, runID, wantTaskStatus, wantAttemptStatus string, wantEventSequence int64) {
	t.Helper()

	assertTaskAndAttemptStatus(t, db, runID+":"+dal.RootTaskKey, 1, wantTaskStatus, wantAttemptStatus, wantEventSequence)
}

func assertTaskAndAttemptStatus(t *testing.T, db *sql.DB, taskID string, attempt int, wantTaskStatus, wantAttemptStatus string, wantEventSequence int64) {
	t.Helper()

	var taskStatus string
	if err := db.QueryRow("SELECT status FROM run_tasks WHERE task_id = ?", taskID).Scan(&taskStatus); err != nil {
		t.Fatalf("query task status: %v", err)
	}

	if taskStatus != wantTaskStatus {
		t.Fatalf("task status: got %q, want %q", taskStatus, wantTaskStatus)
	}

	var attemptStatus string
	var eventSequence int64
	var acceptedAt, startedAt, finishedAt sql.NullString
	var lastObservedAt sql.NullInt64
	if err := db.QueryRow("SELECT status, accepted_at, started_at, finished_at, last_observed_at, event_sequence FROM task_attempts WHERE task_id = ? AND attempt = ?", taskID, attempt).
		Scan(&attemptStatus, &acceptedAt, &startedAt, &finishedAt, &lastObservedAt, &eventSequence); err != nil {
		t.Fatalf("query task attempt status: %v", err)
	}

	if attemptStatus != wantAttemptStatus {
		t.Fatalf("task attempt status: got %q, want %q", attemptStatus, wantAttemptStatus)
	}

	if eventSequence != wantEventSequence {
		t.Fatalf("task attempt event sequence: got %d, want %d", eventSequence, wantEventSequence)
	}

	if !lastObservedAt.Valid || lastObservedAt.Int64 == 0 {
		t.Fatalf("task attempt last_observed_at was not set")
	}

	if !acceptedAt.Valid {
		t.Fatalf("task attempt accepted_at was not set")
	}

	if wantAttemptStatus == dal.TaskStatusRunning && !startedAt.Valid {
		t.Fatalf("task attempt started_at was not set")
	}

	if wantAttemptStatus == dal.TaskStatusSucceeded && !finishedAt.Valid {
		t.Fatalf("task attempt finished_at was not set")
	}
}

func insertCronTriggerSpec(t *testing.T, ctx context.Context, db *sql.DB, jobID, cronSpec string, nextRun time.Time) int64 {
	t.Helper()

	result, err := db.ExecContext(ctx,
		"INSERT INTO job_triggers (job_id, trigger_type) VALUES (?, ?)",
		jobID,
		"cron",
	)
	if err != nil {
		t.Fatalf("insert cron trigger: %v", err)
	}

	triggerID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("cron trigger id: %v", err)
	}

	result, err = db.ExecContext(ctx,
		"INSERT INTO cron_trigger_specs (trigger_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		triggerID,
		cronSpec,
		nextRun.Format(time.RFC3339),
	)

	if err != nil {
		t.Fatalf("insert cron trigger spec: %v", err)
	}

	specID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("cron trigger spec id: %v", err)
	}

	return specID
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

func TestRunsRepository_CreateScheduledRunIdempotentByScheduleTick(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	jobs := repos.Jobs()
	runs := repos.Runs()
	ctx := context.Background()

	jobID := "cron-idempotent"
	if err := jobs.Create(ctx, jobID, `{"id":"cron-idempotent"}`, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	scheduledFor := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	scheduleID := insertCronTriggerSpec(t, ctx, db, jobID, "* * * * *", scheduledFor)

	runID1, idx1, created, err := runs.CreateScheduledRun(ctx, scheduleID, scheduledFor, jobID, 1, dal.RunAuditMetadata{})
	if err != nil {
		t.Fatalf("create scheduled run: %v", err)
	}

	if !created {
		t.Fatal("expected first scheduled run call to create a run")
	}

	if idx1 != 1 {
		t.Fatalf("expected first scheduled run index 1, got %d", idx1)
	}

	runID2, idx2, created, err := runs.CreateScheduledRun(ctx, scheduleID, scheduledFor, jobID, 1, dal.RunAuditMetadata{})
	if err != nil {
		t.Fatalf("create scheduled run duplicate: %v", err)
	}

	if created {
		t.Fatal("expected duplicate scheduled run call to reuse existing run")
	}

	if runID2 != runID1 || idx2 != idx1 {
		t.Fatalf("expected duplicate to reuse run %s/%d, got %s/%d", runID1, idx1, runID2, idx2)
	}

	nextScheduledFor := scheduledFor.Add(time.Minute)
	runID3, idx3, created, err := runs.CreateScheduledRun(ctx, scheduleID, nextScheduledFor, jobID, 1, dal.RunAuditMetadata{})
	if err != nil {
		t.Fatalf("create next scheduled run: %v", err)
	}

	if !created {
		t.Fatal("expected next scheduled tick to create a run")
	}

	if runID3 == runID1 || idx3 != 2 {
		t.Fatalf("expected new run index 2 for next tick, got run=%s index=%d", runID3, idx3)
	}

	var runCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_runs WHERE job_id = ?", jobID).Scan(&runCount); err != nil {
		t.Fatalf("count job runs: %v", err)
	}

	if runCount != 2 {
		t.Fatalf("expected two job_runs rows after duplicate and next tick, got %d", runCount)
	}

	var fireCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cron_schedule_fires WHERE schedule_id = ?", scheduleID).Scan(&fireCount); err != nil {
		t.Fatalf("count schedule fires: %v", err)
	}

	if fireCount != 2 {
		t.Fatalf("expected two cron_schedule_fires rows, got %d", fireCount)
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

func TestRunsRepository_RequestRunCancel_SetsDurableIntent(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-cancel-intent", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimed, token, err := runs.TryClaim(ctx, runID, "worker-cancel", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("try claim: %v", err)
	}

	if !claimed || token == "" {
		t.Fatalf("expected claim token, got claimed=%v token=%q", claimed, token)
	}

	rec, err := runs.RequestRunCancel(ctx, runID, dal.CancelReasonAPI)
	if err != nil {
		t.Fatalf("request run cancel: %v", err)
	}

	if rec.Status != dal.RunStatusRunning {
		t.Fatalf("expected running status, got %q", rec.Status)
	}

	if rec.LeaseOwner != "worker-cancel" {
		t.Fatalf("expected lease owner worker-cancel, got %q", rec.LeaseOwner)
	}

	if rec.CancelToken != token {
		t.Fatalf("cancel token should match worker claim token, got cancel=%q claim=%q", rec.CancelToken, token)
	}

	if rec.CancelRequestedAt == nil || *rec.CancelRequestedAt <= 0 {
		t.Fatalf("expected cancel_requested_at to be set, got %v", rec.CancelRequestedAt)
	}
	if rec.CancelReason != dal.CancelReasonAPI {

		t.Fatalf("expected cancel reason %q, got %q", dal.CancelReasonAPI, rec.CancelReason)
	}

	requested, err := runs.RunCancelRequested(ctx, runID, token)
	if err != nil {
		t.Fatalf("run cancel requested: %v", err)
	}

	if !requested {
		t.Fatal("expected cancel request to be visible to active claim")
	}

	requested, err = runs.RunCancelRequested(ctx, runID, "stale-token")
	if err != nil {
		t.Fatalf("run cancel requested with stale token: %v", err)
	}

	if requested {
		t.Fatal("expected stale token not to see cancel request")
	}

	if err := runs.MarkRunAborted(ctx, runID, token, dal.CancelReasonAPI); err != nil {
		t.Fatalf("mark run aborted: %v", err)
	}

	requested, err = runs.RunCancelRequested(ctx, runID, token)
	if err != nil {
		t.Fatalf("run cancel requested after terminal transition: %v", err)
	}

	if requested {
		t.Fatal("expected terminal transition to clear cancel request")
	}
}

func TestRunsRepository_LogShardAssignment(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-log-shard", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	if shardID, assigned, err := runs.GetLogShard(ctx, runID); err != nil {
		t.Fatalf("get empty log shard: %v", err)
	} else if assigned || shardID != "" {
		t.Fatalf("expected no assignment, got assigned=%v shard=%q", assigned, shardID)
	}

	assigned, err := runs.AssignLogShard(ctx, runID, "log-a")
	if err != nil {
		t.Fatalf("assign log shard: %v", err)
	}
	if assigned != "log-a" {
		t.Fatalf("assigned shard = %q, want log-a", assigned)
	}

	assigned, err = runs.AssignLogShard(ctx, runID, "log-b")
	if err != nil {
		t.Fatalf("assign log shard again: %v", err)
	}

	if assigned != "log-a" {
		t.Fatalf("assigned shard after second write = %q, want log-a", assigned)
	}

	if shardID, assigned, err := runs.GetLogShard(ctx, runID); err != nil {
		t.Fatalf("get assigned log shard: %v", err)
	} else if !assigned || shardID != "log-a" {
		t.Fatalf("expected log-a assignment, got assigned=%v shard=%q", assigned, shardID)
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
	var cancelRequestedAt sql.NullInt64
	var cancelReason sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64

	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason, CAST(finished_at AS TEXT), claim_token, cancel_token, cancel_requested_at, cancel_reason, lease_owner, lease_until
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&status, &failureCode, &failure, &finishedAt, &claimToken, &cancelToken, &cancelRequestedAt, &cancelReason, &leaseOwner, &leaseUntil); err != nil {
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

	if claimToken.Valid || cancelToken.Valid || cancelRequestedAt.Valid || cancelReason.Valid || leaseOwner.Valid || leaseUntil.Valid {
		t.Fatalf("expected abort to clear runtime fields; got claim=%v cancel=%v cancel_at=%v cancel_reason=%v owner=%v lease_until=%v",
			claimToken, cancelToken, cancelRequestedAt, cancelReason, leaseOwner, leaseUntil)
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

func TestRunsRepository_MarkRunQueuedForContinuation_ReleasesClaimedRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-continuation", nil, 1)
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

	if err := runs.MarkRunQueuedForContinuation(ctx, runID, token); err != nil {
		t.Fatalf("MarkRunQueuedForContinuation: %v", err)
	}

	var status string
	var claimToken sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	var lastDispatched sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT status, claim_token, lease_owner, lease_until, last_dispatched_at
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&status, &claimToken, &leaseOwner, &leaseUntil, &lastDispatched); err != nil {
		t.Fatalf("query continuation run: %v", err)
	}

	if status != dal.RunStatusQueued {
		t.Fatalf("status: got %q, want %q", status, dal.RunStatusQueued)
	}

	if claimToken.Valid || leaseOwner.Valid || leaseUntil.Valid || lastDispatched.Valid {
		t.Fatalf("expected continuation to clear runtime dispatch fields; got claim=%v owner=%v lease_until=%v last_dispatched=%v",
			claimToken, leaseOwner, leaseUntil, lastDispatched)
	}

	claimed, nextToken, err := runs.TryClaim(ctx, runID, "worker-b", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("try claim continuation: %v", err)
	}

	if !claimed || nextToken == "" || nextToken == token {
		t.Fatalf("expected fresh continuation claim, got claimed=%v token=%q old=%q", claimed, nextToken, token)
	}

	if err := runs.MarkRunQueuedForContinuation(ctx, runID, token); err == nil {
		t.Fatal("expected stale continuation token to fail")
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

	insertCronTriggerSpec(t, ctx, db, "cron-job", "* * * * *", past)
	insertCronTriggerSpec(t, ctx, db, "cron-job", "0 * * * *", future)

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

	scheduleID := insertCronTriggerSpec(t, ctx, db, "cron-job", "* * * * *", observed)

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
	if err := db.QueryRowContext(ctx, "SELECT next_run_at FROM cron_trigger_specs WHERE id = ?", scheduleID).Scan(&nextRunStr); err != nil {
		t.Fatalf("read next_run_at: %v", err)
	}

	if nextRunStr != next.Format(time.RFC3339) {
		t.Fatalf("expected next_run_at %q, got %q", next.Format(time.RFC3339), nextRunStr)
	}
}

func TestServiceLeasesRepository_TryAcquireRenewAndExpire(t *testing.T) {
	db := dbtest.NewTestDB(t)
	leases := dal.NewSQLRepositories(db).ServiceLeases()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)
	acquired, err := leases.TryAcquire(ctx, "reconciler", "owner-a", now, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	if !acquired {
		t.Fatal("expected owner-a to acquire empty lease")
	}

	acquired, err = leases.TryAcquire(ctx, "reconciler", "owner-b", now.Add(10*time.Second), now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("competing acquire: %v", err)
	}

	if acquired {
		t.Fatal("expected owner-b to lose unexpired lease")
	}

	acquired, err = leases.TryAcquire(ctx, "reconciler", "owner-a", now.Add(20*time.Second), now.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("renew acquire: %v", err)
	}

	if !acquired {
		t.Fatal("expected current owner to renew lease")
	}

	acquired, err = leases.TryAcquire(ctx, "reconciler", "owner-b", now.Add(4*time.Minute), now.Add(5*time.Minute))
	if err != nil {
		t.Fatalf("expired acquire: %v", err)
	}

	if !acquired {
		t.Fatal("expected owner-b to acquire expired lease")
	}

	if err := leases.Release(ctx, "reconciler", "owner-a"); err != nil {
		t.Fatalf("release non-owner: %v", err)
	}

	acquired, err = leases.TryAcquire(ctx, "reconciler", "owner-a", now.Add(4*time.Minute+10*time.Second), now.Add(6*time.Minute))
	if err != nil {
		t.Fatalf("acquire after non-owner release: %v", err)
	}

	if acquired {
		t.Fatal("expected owner-a to still lose after non-owner release")
	}

	if err := leases.Release(ctx, "reconciler", "owner-b"); err != nil {
		t.Fatalf("release owner: %v", err)
	}

	acquired, err = leases.TryAcquire(ctx, "reconciler", "owner-a", now.Add(4*time.Minute+20*time.Second), now.Add(6*time.Minute))
	if err != nil {
		t.Fatalf("acquire after owner release: %v", err)
	}

	if !acquired {
		t.Fatal("expected owner-a to acquire after owner-b release")
	}
}
