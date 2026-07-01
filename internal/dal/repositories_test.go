package dal_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	jobexec "vectis/internal/job"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/runfixture"

	"google.golang.org/protobuf/encoding/protojson"
)

func claimExecutionAccepted(t testing.TB, ctx context.Context, runs dal.RunsRepository, executionID, owner string) dal.ExecutionClaimResult {
	t.Helper()

	claim, err := runs.TryClaimExecution(ctx, executionID, owner, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim execution %s: %v", executionID, err)
	}
	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected execution %s to be claimable, claim=%+v", executionID, claim)
	}

	return claim
}

func claimPendingRunExecution(t testing.TB, ctx context.Context, runs dal.RunsRepository, runID, owner string, leaseUntil time.Time) (dal.ExecutionDispatchRecord, dal.ExecutionClaimResult) {
	t.Helper()

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution for run %s: %v", runID, err)
	}

	claim, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, owner, leaseUntil)
	if err != nil {
		t.Fatalf("claim pending execution %s: %v", dispatch.ExecutionID, err)
	}
	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected pending execution %s to be claimable, claim=%+v", dispatch.ExecutionID, claim)
	}

	return dispatch, claim
}

func assertCreatedRunRootDispatch(t testing.TB, createdRun dal.CreatedRun, dispatch dal.ExecutionDispatchRecord) {
	t.Helper()

	if createdRun.RootDispatch != dispatch {
		t.Fatalf("created run root dispatch mismatch:\n got  %+v\n want %+v", createdRun.RootDispatch, dispatch)
	}
}

func TestJobsRepository_DefinitionSnapshotsAppendImmutableVersions(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	jobs := repos.Jobs()
	ctx := context.Background()

	jobID := "job-a"
	def1 := `{"id":"job-a","root":{"uses":"builtins/script"}}`
	def2 := `{"id":"job-a","root":{"uses":"builtins/script","with":{"script":"echo hi"}}}`

	if err := jobs.CreateDefinitionSnapshot(ctx, jobID, def1); err != nil {
		t.Fatalf("create definition snapshot v1: %v", err)
	}

	if err := jobs.CreateDefinitionSnapshot(ctx, jobID, def2); err != nil {
		t.Fatalf("create definition snapshot v2: %v", err)
	}

	var versionHash string
	if err := db.QueryRowContext(ctx, "SELECT definition_hash FROM job_definitions WHERE job_id = ? AND version = 2", jobID).Scan(&versionHash); err != nil {
		t.Fatalf("scan version hash: %v", err)
	}

	if want := dal.DefinitionHash(def2); versionHash != want {
		t.Fatalf("definition hash mismatch: version=%q want=%q", versionHash, want)
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

	def3 := `{"id":"job-a","root":{"uses":"builtins/script","with":{"script":"echo recreated"}}}`
	if err := jobs.CreateDefinitionSnapshot(ctx, jobID, def3); err != nil {
		t.Fatalf("create definition snapshot v3: %v", err)
	}

	gotV3, err := jobs.GetDefinitionVersion(ctx, jobID, 3)
	if err != nil {
		t.Fatalf("get definition version 3: %v", err)
	}

	if gotV3 != def3 {
		t.Fatalf("definition version 3 mismatch: got %q want %q", gotV3, def3)
	}
}

func TestTriggerInvocations_CreateRunAuditAndPayloadLedger(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	jobID := "job-audit"
	definitionJSON := `{"id":"job-audit","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, definitionJSON); err != nil {
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

	startDeadlineUnixNano := int64(123456789)
	created, err := repos.Runs().CreateRunsInCellsWithAudit(ctx, jobID, nil, 1, []string{"iad-a", "pdx-b"}, dal.RunAuditMetadata{
		TriggerInvocationID:   rec.InvocationID,
		StartDeadlineUnixNano: startDeadlineUnixNano,
	})
	if err != nil {
		t.Fatalf("create audited runs: %v", err)
	}

	if len(created) != 2 {
		t.Fatalf("created runs: got %d, want 2", len(created))
	}

	byInvocation, err := repos.Runs().ListCreatedByTriggerInvocation(ctx, rec.InvocationID)
	if err != nil {
		t.Fatalf("list runs by trigger invocation: %v", err)
	}

	if len(byInvocation) != 2 {
		t.Fatalf("runs by trigger invocation: got %d, want 2", len(byInvocation))
	}

	for i, run := range byInvocation {
		if run.RunID != created[i].RunID || run.JobID != jobID || run.RunIndex != created[i].RunIndex || run.TargetCellID != created[i].TargetCellID {
			t.Fatalf("run by invocation %d: got %+v, want run=%+v job=%s", i, run, created[i], jobID)
		}
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

		dispatch, err := repos.Runs().GetPendingExecution(ctx, run.RunID)
		if err != nil {
			t.Fatalf("get pending execution for %s: %v", run.RunID, err)
		}
		if dispatch.StartDeadlineUnixNano != startDeadlineUnixNano {
			t.Fatalf("run %s start deadline: got %d want %d", run.RunID, dispatch.StartDeadlineUnixNano, startDeadlineUnixNano)
		}
		assertCreatedRunRootDispatch(t, run, dispatch)
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

	payloadHashByRun, err := repos.Runs().GetExecutionPayloadHashForRun(ctx, created[0].RunID)
	if err != nil {
		t.Fatalf("get payload hash by run: %v", err)
	}

	if payloadHashByRun != payloadHash1 {
		t.Fatalf("payload hash by run: got %q want %q", payloadHashByRun, payloadHash1)
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

func TestTriggerInvocations_RecordIsIdempotentForSameInvocationID(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	create := dal.TriggerInvocation{
		InvocationID:       "trigger-idempotent",
		JobID:              "job-idempotent",
		TriggerType:        dal.TriggerTypeReaction,
		TriggerPayloadHash: dal.PayloadHash(`{"reaction_invocation_id":"trigger-idempotent"}`),
		RequestedCells:     []string{"iad-a", "pdx-b"},
	}

	first, err := repos.TriggerInvocations().Record(ctx, create)
	if err != nil {
		t.Fatalf("record first trigger invocation: %v", err)
	}

	duplicate, err := repos.TriggerInvocations().Record(ctx, create)
	if err != nil {
		t.Fatalf("record duplicate trigger invocation: %v", err)
	}

	if duplicate.ID != first.ID || duplicate.InvocationID != first.InvocationID {
		t.Fatalf("duplicate trigger invocation: got %+v want %+v", duplicate, first)
	}

	create.TriggerPayloadHash = dal.PayloadHash(`{"reaction_invocation_id":"different"}`)
	if _, err := repos.TriggerInvocations().Record(ctx, create); !dal.IsConflict(err) {
		t.Fatalf("expected conflicting trigger invocation, got %v", err)
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
		DefinitionJSON:    `{"id":"job-1","root":{"uses":"builtins/script"}}`,
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
		DefinitionJSON:    `{"id":"job-repair","root":{"uses":"builtins/script"}}`,
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
	if pending[0].RunID != acceptance.RunID ||
		pending[0].JobID != acceptance.JobID ||
		pending[0].RunIndex != acceptance.RunIndex ||
		pending[0].TaskID != "run-repair:root" ||
		pending[0].TaskKey != dal.RootTaskKey ||
		pending[0].TaskName != dal.RootTaskKey ||
		pending[0].TaskAttemptID != "run-repair:root:attempt:1" ||
		pending[0].SegmentID != acceptance.SegmentID ||
		pending[0].SegmentName != acceptance.SegmentName ||
		pending[0].CellID != acceptance.CellID ||
		pending[0].Attempt != acceptance.Attempt ||
		pending[0].DefinitionVersion != acceptance.DefinitionVersion ||
		pending[0].DefinitionHash != acceptance.DefinitionHash {
		t.Fatalf("pending handoff identity mismatch: got %+v", pending[0])
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
	definitionJSON := `{"id":"job-existing","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, definitionJSON); err != nil {
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
		DefinitionJSON:    `{"id":"job-1","root":{"uses":"builtins/script"}}`,
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
	def := `{"id":"job-cell-owned","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	var namespaceCell, runCell, executionCell string
	if err := db.QueryRowContext(ctx, "SELECT home_cell FROM namespaces WHERE id = ?", ns.ID).Scan(&namespaceCell); err != nil {
		t.Fatalf("query namespace cell: %v", err)
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

	if dispatch.NamespacePath != "/" {
		t.Fatalf("dispatch namespace path: got %q, want /", dispatch.NamespacePath)
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

	jobID := "job-target-cell"
	def := `{"id":"job-target-cell","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRunInCell(ctx, jobID, nil, 1, "pdx-b")
	if err != nil {
		t.Fatalf("CreateRunInCell: %v", err)
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

	jobID := "job-replay"
	defV1 := `{"id":"job-replay","root":{"uses":"builtins/script","with":{"script":"echo old"}}}`
	defV2 := `{"id":"job-replay","root":{"uses":"builtins/script","with":{"script":"echo new"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, defV1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	sourceRunID, _, err := repos.Runs().CreateRunInCell(ctx, jobID, nil, 1, "pdx-b")
	if err != nil {
		t.Fatalf("create source run: %v", err)
	}

	if err := repos.Runs().MarkRunFailed(ctx, sourceRunID, dal.FailureCodeExecution, "environment failed"); err != nil {
		t.Fatalf("mark source failed: %v", err)
	}

	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, defV2); err != nil {
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
	assertCreatedRunRootDispatch(t, replay, dispatch)

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

	_, err := repos.Namespaces().Create(ctx, "team-status-cells", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-status-cells"
	def := `{"id":"job-status-cells","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	_, err := repos.Namespaces().Create(ctx, "team-stuck-cells", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-stuck-cells"
	def := `{"id":"job-stuck-cells","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	_, err := repos.Namespaces().Create(ctx, "team-fanout", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-fanout-cells"
	def := `{"id":"job-fanout-cells","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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
		assertCreatedRunRootDispatch(t, createdRun, dispatch)
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

	_, err := repos.Namespaces().Create(ctx, "team-tasks", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-root-task-list"
	def := `{"id":"job-root-task-list","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	if attempt.ExecutionID == "" || attempt.ExecutionStatus != dal.ExecutionStatusPending {
		t.Fatalf("unexpected root execution identity: %+v", attempt)
	}

	if attempt.LeaseOwner != nil || attempt.LeaseUntil != nil {
		t.Fatalf("unclaimed execution should not expose lease fields: %+v", attempt)
	}

	secretCount := 2
	fileCount := 1
	if err := repos.Runs().RecordExecutionSecurityEvent(ctx, dal.RecordExecutionSecurityEventParams{
		RunID:         runID,
		TaskID:        attempt.TaskID,
		TaskAttemptID: attempt.AttemptID,
		ExecutionID:   attempt.ExecutionID,
		EventType:     dal.ExecutionSecurityEventSecretResolution,
		Outcome:       "success",
		Reason:        "ok",
		Provider:      "encryptedfs",
		SecretCount:   &secretCount,
		FileCount:     &fileCount,
		CreatedAt:     123,
	}); err != nil {
		t.Fatalf("record execution security event: %v", err)
	}

	if err := repos.Runs().RecordExecutionSecurityEvent(ctx, dal.RecordExecutionSecurityEventParams{
		RunID:         runID,
		TaskID:        attempt.TaskID,
		TaskAttemptID: attempt.AttemptID,
		ExecutionID:   attempt.ExecutionID,
		EventType:     dal.ExecutionSecurityEventSecretResolution,
		Outcome:       "success",
		Reason:        "ok",
		Provider:      "encryptedfs",
		SecretCount:   &secretCount,
		FileCount:     &fileCount,
		CreatedAt:     123,
	}); err != nil {
		t.Fatalf("record duplicate execution security event: %v", err)
	}

	tasks, _, err = repos.Runs().ListRunTasks(ctx, runID, 0, 50)
	if err != nil {
		t.Fatalf("list run tasks with security event: %v", err)
	}
	if len(tasks) != 1 || len(tasks[0].Attempts) != 1 || len(tasks[0].Attempts[0].SecurityEvents) != 1 {
		t.Fatalf("listed security events missing: %+v", tasks)
	}
	securityEvent := tasks[0].Attempts[0].SecurityEvents[0]
	if securityEvent.EventType != dal.ExecutionSecurityEventSecretResolution || securityEvent.Outcome != "success" || securityEvent.Reason != "ok" {
		t.Fatalf("unexpected security event result: %+v", securityEvent)
	}
	if securityEvent.Provider == nil || *securityEvent.Provider != "encryptedfs" || securityEvent.SecretCount == nil || *securityEvent.SecretCount != 2 || securityEvent.FileCount == nil || *securityEvent.FileCount != 1 {
		t.Fatalf("unexpected security event details: %+v", securityEvent)
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

	_, err := repos.Namespaces().Create(ctx, "team-task-create", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-create"
	def := `{"id":"job-task-create","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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
		return
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

	_, err := repos.Namespaces().Create(ctx, "team-task-plan", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-plan"
	def := `{"id":"job-task-plan","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	claimExecutionAccepted(t, ctx, repos.Runs(), rootDispatch.ExecutionID, "worker-root")

	if _, err := repos.Runs().GetPendingExecution(ctx, runID); !dal.IsNotFound(err) {
		t.Fatalf("planned child should not dispatch after root accepts, got %v", err)
	}

	plannedClaim, err := repos.Runs().TryClaimExecution(ctx, child.ExecutionID, "worker-planned", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim planned child execution: %v", err)
	}

	if plannedClaim.Claimed {
		t.Fatalf("planned execution should not be claimable, claim=%+v", plannedClaim)
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

	claimExecutionAccepted(t, ctx, repos.Runs(), child.ExecutionID, "worker-child")

	assertExecutionAndSegmentStatus(t, db, child.ExecutionID, child.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertTaskAndAttemptStatus(t, db, child.TaskID, 1, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)

	if _, _, err := repos.Runs().ActivatePlannedTaskExecution(ctx, child.TaskID); !dal.IsConflict(err) {
		t.Fatalf("accepted task activation should conflict, got %v", err)
	}
}

func TestRunsRepository_MirrorExecutionClaimAcceptsPlannedChildClaim(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-mirror-child-claim", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-mirror-child-claim"
	def := `{"id":"job-mirror-child-claim","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	child, created, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: runID + ":" + dal.RootTaskKey,
		TaskKey:      "child",
		Name:         "child task",
		SpecHash:     "sha256:child",
		TargetCellID: "iad-a",
	})
	if err != nil {
		t.Fatalf("ensure planned child: %v", err)
	}
	if !created {
		t.Fatal("expected planned child to be created")
	}

	leaseUntil := time.Now().Add(time.Minute)
	const token = "orchestrator-child-token"
	if err := repos.Runs().MirrorExecutionClaim(ctx, child.ExecutionID, "worker-orchestrator", token, leaseUntil); err != nil {
		t.Fatalf("mirror planned child claim: %v", err)
	}

	assertExecutionAndSegmentStatus(t, db, child.ExecutionID, child.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertTaskAndAttemptStatus(t, db, child.TaskID, 1, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)
	assertExecutionClaim(t, db, child.ExecutionID, "worker-orchestrator", leaseUntil.Unix(), token)
	assertRunExecutionOwner(t, db, runID, dal.RunStatusRunning, "worker-orchestrator", leaseUntil.Unix())

	if err := repos.Runs().ValidateActiveExecutionClaim(ctx, runID, child.ExecutionID, token); err != nil {
		t.Fatalf("validate mirrored active claim: %v", err)
	}

	recoveredUntil := time.Now().Add(2 * time.Minute)
	const recoveredToken = "orchestrator-recovered-token"
	if err := repos.Runs().MirrorExecutionClaim(ctx, child.ExecutionID, "worker-orchestrator", recoveredToken, recoveredUntil); err != nil {
		t.Fatalf("mirror recovered child claim: %v", err)
	}

	assertExecutionClaim(t, db, child.ExecutionID, "worker-orchestrator", recoveredUntil.Unix(), recoveredToken)
	if err := repos.Runs().ValidateActiveExecutionClaim(ctx, runID, child.ExecutionID, token); !dal.IsConflict(err) {
		t.Fatalf("expected stale mirrored claim token to fail validation, got %v", err)
	}

	if err := repos.Runs().MirrorExecutionClaim(ctx, child.ExecutionID, "worker-other", "other-token", time.Now().Add(3*time.Minute)); !dal.IsConflict(err) {
		t.Fatalf("expected conflicting active mirrored claim, got %v", err)
	}

	if err := repos.Runs().MarkExecutionStarted(ctx, child.ExecutionID); err != nil {
		t.Fatalf("mark mirrored child started: %v", err)
	}

	assertExecutionAndSegmentStatus(t, db, child.ExecutionID, child.SegmentID, dal.ExecutionStatusRunning, dal.SegmentStatusRunning, 2)
	assertTaskAndAttemptStatus(t, db, child.TaskID, 1, dal.TaskStatusRunning, dal.TaskStatusRunning, 2)
}

func TestRunsRepository_ActivatePlannedChildTaskExecutionsFansOutDirectChildren(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-task-child-activate", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-child-activate"
	def := `{"id":"job-task-child-activate","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	claimExecutionAccepted(t, ctx, repos.Runs(), rootDispatch.ExecutionID, "worker-root")

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

	claimExecutionAccepted(t, ctx, repos.Runs(), setup.ExecutionID, "worker-setup")

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

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_SequenceActivatesOneReadySubtreeAtATime(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-task-sequence-activate", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-sequence-activate"
	j := durableSequenceJob(jobID, "")
	definitionJSON, err := protojson.Marshal(j)
	if err != nil {
		t.Fatalf("marshal definition payload: %v", err)
	}

	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, string(definitionJSON)); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	j = durableSequenceJob(jobID, runID)
	if _, err := jobexec.EnsureJobTaskExecutions(ctx, repos.Runs(), j, "iad-a"); err != nil {
		t.Fatalf("materialize job tasks: %v", err)
	}

	recordExecutionPayloadForJob(t, ctx, repos, runID, j)

	tasks, _, err := repos.Runs().ListRunTasks(ctx, runID, 0, 20)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}

	byKey := map[string]dal.TaskRecord{}
	for _, task := range tasks {
		byKey[task.TaskKey] = task
	}

	rootDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root pending execution: %v", err)
	}

	result := runfixture.FinalizeExecutionByClaim(t, ctx, repos, rootDispatch.ExecutionID, dal.ExecutionStatusSucceeded)
	assertActivatedTaskKeys(t, result.Children, "build")
	assertTaskStatusByKey(t, db, byKey, "build", dal.TaskStatusPending)
	assertTaskStatusByKey(t, db, byKey, "compile", dal.TaskStatusPlanned)
	assertTaskStatusByKey(t, db, byKey, "test", dal.TaskStatusPlanned)
	assertTaskStatusByKey(t, db, byKey, "deploy", dal.TaskStatusPlanned)

	result = runfixture.FinalizeExecutionByClaim(t, ctx, repos, taskExecutionIDForKey(t, byKey, "build"), dal.ExecutionStatusSucceeded)
	assertActivatedTaskKeys(t, result.Children, "compile", "test")
	assertTaskStatusByKey(t, db, byKey, "build", dal.TaskStatusSucceeded)
	assertTaskStatusByKey(t, db, byKey, "compile", dal.TaskStatusPending)
	assertTaskStatusByKey(t, db, byKey, "test", dal.TaskStatusPending)
	assertTaskStatusByKey(t, db, byKey, "deploy", dal.TaskStatusPlanned)

	result = runfixture.FinalizeExecutionByClaim(t, ctx, repos, taskExecutionIDForKey(t, byKey, "compile"), dal.ExecutionStatusSucceeded)
	assertActivatedTaskKeys(t, result.Children, "test")
	assertTaskStatusByKey(t, db, byKey, "deploy", dal.TaskStatusPlanned)

	result = runfixture.FinalizeExecutionByClaim(t, ctx, repos, taskExecutionIDForKey(t, byKey, "test"), dal.ExecutionStatusSucceeded)
	assertActivatedTaskKeys(t, result.Children, "deploy")
	assertTaskStatusByKey(t, db, byKey, "deploy", dal.TaskStatusPending)

	result = runfixture.FinalizeExecutionByClaim(t, ctx, repos, taskExecutionIDForKey(t, byKey, "deploy"), dal.ExecutionStatusSucceeded)
	if result.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		t.Fatalf("final outcome: got %q, want %q; result=%+v", result.Outcome, dal.ExecutionFinalizationOutcomeRunSucceeded, result)
	}
	assertActivatedTaskKeys(t, result.Children)
}

func TestRunsRepository_GetRunTaskCompletion(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-task-completion", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-task-completion"
	def := `{"id":"job-task-completion","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	child, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      "child",
		SpecHash:     "sha256:child",
	})

	if err != nil {
		t.Fatalf("ensure child: %v", err)
	}

	summary, err := repos.Runs().GetRunTaskCompletion(ctx, runID)
	if err != nil {
		t.Fatalf("initial GetRunTaskCompletion: %v", err)
	}

	if summary.RunID != runID || summary.Total != 2 || summary.Succeeded != 0 || summary.TerminalFailed != 0 || summary.Incomplete != 2 || summary.AllSucceeded() {
		t.Fatalf("initial summary mismatch: %+v", summary)
	}

	runfixture.FinalizeExecutionByClaim(t, ctx, repos, rootDispatch.ExecutionID, dal.ExecutionStatusSucceeded)

	summary, err = repos.Runs().GetRunTaskCompletion(ctx, runID)
	if err != nil {
		t.Fatalf("after root success GetRunTaskCompletion: %v", err)
	}

	if summary.Total != 2 || summary.Succeeded != 1 || summary.TerminalFailed != 0 || summary.Incomplete != 1 || summary.AllSucceeded() {
		t.Fatalf("after root success summary mismatch: %+v", summary)
	}

	runfixture.FinalizeExecutionByClaimWithFailure(t, ctx, repos, child.ExecutionID, dal.ExecutionStatusFailed, dal.FailureCodeExecution, "child failed")

	summary, err = repos.Runs().GetRunTaskCompletion(ctx, runID)
	if err != nil {
		t.Fatalf("after child failed GetRunTaskCompletion: %v", err)
	}

	if summary.Total != 2 || summary.Succeeded != 1 || summary.TerminalFailed != 1 || summary.Incomplete != 0 || summary.AllSucceeded() {
		t.Fatalf("after child failed summary mismatch: %+v", summary)
	}

	if _, err := repos.Runs().GetRunTaskCompletion(ctx, "missing-run"); !dal.IsNotFound(err) {
		t.Fatalf("missing run should return not found, got %v", err)
	}
}

func TestSQLRepositories_CreateDefinitionAndRunInCell_TargetsExecutionCell(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "global-a")
	ctx := context.Background()

	jobID := "ephemeral-target-cell"
	def := `{"id":"ephemeral-target-cell","root":{"uses":"builtins/script","with":{"script":"echo x"}}}`
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

	_, err := repos.Namespaces().Create(ctx, "team-pending", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-no-pending-execution"
	def := `{"id":"job-no-pending-execution","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	_, err := repos.Namespaces().Create(ctx, "team-transitions", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-execution-transitions"
	def := `{"id":"job-execution-transitions","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	claimExecutionAccepted(t, ctx, repos.Runs(), dispatch.ExecutionID, "worker-dispatch")
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertRootTaskAndAttemptStatus(t, db, runID, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)

	if err := repos.Runs().MarkExecutionStarted(ctx, dispatch.ExecutionID); err != nil {
		t.Fatalf("mark started: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusRunning, dal.SegmentStatusRunning, 2)
	assertRootTaskAndAttemptStatus(t, db, runID, dal.TaskStatusRunning, dal.TaskStatusRunning, 2)

	if err := repos.Runs().ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{ExecutionID: dispatch.ExecutionID, Status: dal.ExecutionStatusSucceeded}); err != nil {
		t.Fatalf("apply terminal execution status: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 3)
	assertRootTaskAndAttemptStatus(t, db, runID, dal.TaskStatusSucceeded, dal.TaskStatusSucceeded, 3)

	if err := repos.Runs().MarkExecutionStarted(ctx, dispatch.ExecutionID); !dal.IsConflict(err) {
		t.Fatalf("expected conflict restarting terminal execution, got %v", err)
	}
}

func TestRunsRepository_ExecutionClaims(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-execution-claims", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-execution-claims"
	def := `{"id":"job-execution-claims","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	leaseUntil := time.Now().Add(time.Minute)
	claim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-a", leaseUntil)
	if err != nil {
		t.Fatalf("claim execution: %v", err)
	}

	if !claim.Claimed || claim.ClaimToken == "" || !claim.TransitionedToAccepted {
		t.Fatalf("expected execution claim to accept with token, claim=%+v", claim)
	}

	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertRootTaskAndAttemptStatus(t, db, runID, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)
	assertExecutionClaim(t, db, dispatch.ExecutionID, "worker-a", leaseUntil.Unix(), claim.ClaimToken)
	assertRunExecutionOwner(t, db, runID, dal.RunStatusRunning, "worker-a", leaseUntil.Unix())

	tasks, _, err := repos.Runs().ListRunTasks(ctx, runID, 0, 10)
	if err != nil {
		t.Fatalf("list run tasks after execution claim: %v", err)
	}

	if len(tasks) != 1 || len(tasks[0].Attempts) != 1 {
		t.Fatalf("claimed task attempts: %+v", tasks)
	}

	claimedAttempt := tasks[0].Attempts[0]
	if claimedAttempt.ExecutionID != dispatch.ExecutionID || claimedAttempt.ExecutionStatus != dal.ExecutionStatusAccepted {
		t.Fatalf("claimed attempt execution state: %+v", claimedAttempt)
	}

	if claimedAttempt.LeaseOwner == nil || *claimedAttempt.LeaseOwner != "worker-a" || claimedAttempt.LeaseUntil == nil || *claimedAttempt.LeaseUntil != leaseUntil.Unix() {
		t.Fatalf("claimed attempt lease fields: %+v", claimedAttempt)
	}

	duplicateClaim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-b", time.Now().Add(2*time.Minute))
	if err != nil {
		t.Fatalf("duplicate claim execution: %v", err)
	}

	if duplicateClaim.Claimed || duplicateClaim.ClaimToken != "" || duplicateClaim.TransitionedToAccepted {
		t.Fatalf("active execution lease should not be claimable, claim=%+v", duplicateClaim)
	}

	renewedUntil := time.Now().Add(3 * time.Minute)
	if err := repos.Runs().RenewExecutionLease(ctx, dispatch.ExecutionID, "worker-a", claim.ClaimToken, renewedUntil); err != nil {
		t.Fatalf("renew execution lease: %v", err)
	}
	assertExecutionClaim(t, db, dispatch.ExecutionID, "worker-a", renewedUntil.Unix(), claim.ClaimToken)

	if err := repos.Runs().RenewExecutionLease(ctx, dispatch.ExecutionID, "worker-b", claim.ClaimToken, time.Now().Add(4*time.Minute)); !dal.IsConflict(err) {
		t.Fatalf("expected conflicting execution lease renewal, got %v", err)
	}

	if _, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, dispatch.ExecutionID, "worker-a", claim.ClaimToken, dal.ExecutionStatusSucceeded, "", ""); err != nil {
		t.Fatalf("complete execution by claim: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 2)
	assertExecutionClaimCleared(t, db, dispatch.ExecutionID)

	terminalClaim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-c", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim terminal execution: %v", err)
	}

	if terminalClaim.Claimed || terminalClaim.ClaimToken != "" || terminalClaim.TransitionedToAccepted {
		t.Fatalf("terminal execution should not be claimable, claim=%+v", terminalClaim)
	}
}

func TestRunsRepository_TryClaimExecutionExpiresPastStartDeadline(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-execution-deadline-claim", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-execution-deadline-claim"
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, `{"id":"job-execution-deadline-claim","root":{"uses":"builtins/script"}}`); err != nil {
		t.Fatalf("create job: %v", err)
	}

	deadline := time.Now().Add(-time.Second).UnixNano()
	created, err := repos.Runs().CreateRunsInCellsWithAudit(ctx, jobID, nil, 1, []string{dal.DefaultCellID}, dal.RunAuditMetadata{
		StartDeadlineUnixNano: deadline,
	})

	if err != nil {
		t.Fatalf("create run with deadline: %v", err)
	}

	runID := created[0].RunID
	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if dispatch.StartDeadlineUnixNano != deadline {
		t.Fatalf("dispatch deadline: got %d want %d", dispatch.StartDeadlineUnixNano, deadline)
	}

	claim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim expired execution: %v", err)
	}

	if !claim.Expired || claim.Claimed || claim.RunID != runID || claim.ExecutionID != dispatch.ExecutionID {
		t.Fatalf("expected expired claim result, got %+v", claim)
	}

	assertDispatchExpired(t, db, runID, dispatch.ExecutionID, dispatch.SegmentID)
}

func TestRunsRepository_MirrorExecutionClaimExpiresPastStartDeadline(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-execution-deadline-mirror", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-execution-deadline-mirror"
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, `{"id":"job-execution-deadline-mirror","root":{"uses":"builtins/script"}}`); err != nil {
		t.Fatalf("create definition snapshot: %v", err)
	}

	deadline := time.Now().Add(-time.Second).UnixNano()
	created, err := repos.Runs().CreateRunsInCellsWithAudit(ctx, jobID, nil, 1, []string{dal.DefaultCellID}, dal.RunAuditMetadata{
		StartDeadlineUnixNano: deadline,
	})

	if err != nil {
		t.Fatalf("create run with deadline: %v", err)
	}

	runID := created[0].RunID
	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	err = repos.Runs().MirrorExecutionClaim(ctx, dispatch.ExecutionID, "worker-orchestrator", "claim-token", time.Now().Add(time.Minute))
	if !dal.IsConflict(err) || !dal.IsDispatchExpired(err) {
		t.Fatalf("mirror expired execution claim: got %v, want dispatch-expired conflict", err)
	}

	assertDispatchExpired(t, db, runID, dispatch.ExecutionID, dispatch.SegmentID)
}

func TestRunsRepository_MarkExpiredQueuedExecutionsFailed(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-execution-deadline-sweep", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-execution-deadline-sweep"
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, `{"id":"job-execution-deadline-sweep","root":{"uses":"builtins/script"}}`); err != nil {
		t.Fatalf("create job: %v", err)
	}

	now := time.Now()
	expiredCreated, err := repos.Runs().CreateRunsInCellsWithAudit(ctx, jobID, nil, 1, []string{dal.DefaultCellID}, dal.RunAuditMetadata{
		StartDeadlineUnixNano: now.Add(-time.Second).UnixNano(),
	})

	if err != nil {
		t.Fatalf("create expired run: %v", err)
	}

	futureCreated, err := repos.Runs().CreateRunsInCellsWithAudit(ctx, jobID, nil, 1, []string{dal.DefaultCellID}, dal.RunAuditMetadata{
		StartDeadlineUnixNano: now.Add(time.Hour).UnixNano(),
	})

	if err != nil {
		t.Fatalf("create future run: %v", err)
	}

	runningCreated, err := repos.Runs().CreateRunsInCellsWithAudit(ctx, jobID, nil, 1, []string{dal.DefaultCellID}, dal.RunAuditMetadata{
		StartDeadlineUnixNano: now.Add(time.Hour).UnixNano(),
	})

	if err != nil {
		t.Fatalf("create running run: %v", err)
	}

	orphanedCreated, err := repos.Runs().CreateRunsInCellsWithAudit(ctx, jobID, nil, 1, []string{dal.DefaultCellID}, dal.RunAuditMetadata{
		StartDeadlineUnixNano: now.Add(time.Hour).UnixNano(),
	})

	if err != nil {
		t.Fatalf("create orphaned run: %v", err)
	}

	expiredDispatch, err := repos.Runs().GetPendingExecution(ctx, expiredCreated[0].RunID)
	if err != nil {
		t.Fatalf("get expired dispatch: %v", err)
	}

	futureDispatch, err := repos.Runs().GetPendingExecution(ctx, futureCreated[0].RunID)
	if err != nil {
		t.Fatalf("get future dispatch: %v", err)
	}

	runningDispatch, claim := claimPendingRunExecution(t, ctx, repos.Runs(), runningCreated[0].RunID, "worker-running", time.Now().Add(time.Minute))
	if err := repos.Runs().MarkExecutionStarted(ctx, runningDispatch.ExecutionID); err != nil {
		t.Fatalf("mark running execution started: %v", err)
	}

	orphanedDispatch, _ := claimPendingRunExecution(t, ctx, repos.Runs(), orphanedCreated[0].RunID, "worker-orphaned", time.Now().Add(-time.Minute))
	if err := repos.Runs().MarkRunOrphaned(ctx, orphanedCreated[0].RunID, dal.OrphanReasonLeaseExpired); err != nil {
		t.Fatalf("mark orphaned run: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE segment_executions
		SET start_deadline_unix_nano = ?, lease_until = ?
		WHERE execution_id = ?
	`, now.Add(-time.Second).UnixNano(), now.Add(-time.Minute).Unix(), runningDispatch.ExecutionID); err != nil {
		t.Fatalf("force running execution expired deadline: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE segment_executions
		SET start_deadline_unix_nano = ?, lease_until = ?
		WHERE execution_id = ?
	`, now.Add(-time.Second).UnixNano(), now.Add(-time.Minute).Unix(), orphanedDispatch.ExecutionID); err != nil {
		t.Fatalf("force orphaned execution expired deadline: %v", err)
	}

	expired, err := repos.Runs().MarkExpiredQueuedExecutionsFailed(ctx, now.UnixNano(), 10)
	if err != nil {
		t.Fatalf("mark expired queued executions failed: %v", err)
	}

	if len(expired) != 1 || expired[0].RunID != expiredCreated[0].RunID || expired[0].ExecutionID != expiredDispatch.ExecutionID {
		t.Fatalf("expired executions: got %+v", expired)
	}

	assertDispatchExpired(t, db, expiredCreated[0].RunID, expiredDispatch.ExecutionID, expiredDispatch.SegmentID)

	var futureRunStatus, futureExecutionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, futureCreated[0].RunID).Scan(&futureRunStatus); err != nil {
		t.Fatalf("scan future run status: %v", err)
	}

	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, futureDispatch.ExecutionID).Scan(&futureExecutionStatus); err != nil {
		t.Fatalf("scan future execution status: %v", err)
	}

	if futureRunStatus != dal.RunStatusQueued || futureExecutionStatus != dal.ExecutionStatusPending {
		t.Fatalf("future execution should remain queued/pending, run=%q execution=%q", futureRunStatus, futureExecutionStatus)
	}

	var runningRunStatus, runningExecutionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runningCreated[0].RunID).Scan(&runningRunStatus); err != nil {
		t.Fatalf("scan running run status: %v", err)
	}

	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, runningDispatch.ExecutionID).Scan(&runningExecutionStatus); err != nil {
		t.Fatalf("scan running execution status: %v", err)
	}

	if runningRunStatus != dal.RunStatusRunning || runningExecutionStatus != dal.ExecutionStatusRunning {
		t.Fatalf("running execution should not dispatch-expire after it starts, run=%q execution=%q claim=%+v", runningRunStatus, runningExecutionStatus, claim)
	}

	var orphanedRunStatus, orphanedExecutionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, orphanedCreated[0].RunID).Scan(&orphanedRunStatus); err != nil {
		t.Fatalf("scan orphaned run status: %v", err)
	}

	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, orphanedDispatch.ExecutionID).Scan(&orphanedExecutionStatus); err != nil {
		t.Fatalf("scan orphaned execution status: %v", err)
	}

	if orphanedRunStatus != dal.RunStatusOrphaned || orphanedExecutionStatus != dal.ExecutionStatusAccepted {
		t.Fatalf("orphaned execution should not dispatch-expire automatically, run=%q execution=%q", orphanedRunStatus, orphanedExecutionStatus)
	}
}

func TestRunsRepository_RequeueRunForRetry_RestoresDispatchExpiredExecution(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-retry-dispatch-expired", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-retry-dispatch-expired"
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, `{"id":"job-retry-dispatch-expired","root":{"uses":"builtins/script"}}`); err != nil {
		t.Fatalf("create job: %v", err)
	}

	now := time.Now()
	created, err := repos.Runs().CreateRunsInCellsWithAudit(ctx, jobID, nil, 1, []string{dal.DefaultCellID}, dal.RunAuditMetadata{
		StartDeadlineUnixNano: now.Add(-time.Second).UnixNano(),
	})

	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	runID := created[0].RunID
	expiredDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get expired dispatch: %v", err)
	}

	expired, err := repos.Runs().MarkExpiredQueuedExecutionsFailed(ctx, now.UnixNano(), 10)
	if err != nil {
		t.Fatalf("mark expired queued executions failed: %v", err)
	}

	if len(expired) != 1 || expired[0].RunID != runID || expired[0].ExecutionID != expiredDispatch.ExecutionID {
		t.Fatalf("expired executions: got %+v", expired)
	}

	if _, err := repos.Runs().GetPendingExecution(ctx, runID); !dal.IsNotFound(err) {
		t.Fatalf("expected expired run to have no pending execution before retry, got %v", err)
	}

	if err := repos.Runs().RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatalf("requeue run for retry: %v", err)
	}

	retryDispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get retry dispatch: %v", err)
	}

	if retryDispatch.ExecutionID == expiredDispatch.ExecutionID {
		t.Fatalf("expected retry to create a fresh execution attempt, got original execution %s", retryDispatch.ExecutionID)
	}

	if retryDispatch.Attempt != 2 {
		t.Fatalf("expected retry attempt 2, got %d", retryDispatch.Attempt)
	}

	if retryDispatch.StartDeadlineUnixNano != 0 {
		t.Fatalf("expected retry dispatch to start without stale deadline, got %d", retryDispatch.StartDeadlineUnixNano)
	}

	newDeadline := now.Add(time.Minute).UnixNano()
	gotDeadline, err := repos.Runs().EnsureExecutionStartDeadline(ctx, retryDispatch.ExecutionID, newDeadline)
	if err != nil {
		t.Fatalf("ensure retry deadline: %v", err)
	}

	if gotDeadline != newDeadline {
		t.Fatalf("retry deadline: got %d want %d", gotDeadline, newDeadline)
	}

	var runStatus, failureCode string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_code, failure_reason FROM job_runs WHERE run_id = ?`, runID).
		Scan(&runStatus, &failureCode, &failureReason); err != nil {
		t.Fatalf("query requeued run: %v", err)
	}

	if runStatus != dal.RunStatusQueued || failureCode != "" || failureReason.Valid {
		t.Fatalf("expected requeued run to clear failure state, status=%q failure_code=%q failure_reason=%v", runStatus, failureCode, failureReason)
	}

	var oldExecutionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, expiredDispatch.ExecutionID).
		Scan(&oldExecutionStatus); err != nil {
		t.Fatalf("query old execution: %v", err)
	}

	if oldExecutionStatus != dal.ExecutionStatusFailed {
		t.Fatalf("expected old execution to remain failed, got %q", oldExecutionStatus)
	}
}

func TestRunsRepository_EnsureExecutionStartDeadlineAdoptsMissingDeadline(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-execution-deadline-adopt", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-execution-deadline-adopt"
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, `{"id":"job-execution-deadline-adopt","root":{"uses":"builtins/script"}}`); err != nil {
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

	if dispatch.StartDeadlineUnixNano != 0 {
		t.Fatalf("new legacy-style run should not have deadline, got %d", dispatch.StartDeadlineUnixNano)
	}

	deadline := time.Now().Add(time.Hour).UnixNano()
	got, err := repos.Runs().EnsureExecutionStartDeadline(ctx, dispatch.ExecutionID, deadline)
	if err != nil {
		t.Fatalf("ensure start deadline: %v", err)
	}

	if got != deadline {
		t.Fatalf("ensured deadline: got %d want %d", got, deadline)
	}

	again, err := repos.Runs().EnsureExecutionStartDeadline(ctx, dispatch.ExecutionID, deadline+int64(time.Hour))
	if err != nil {
		t.Fatalf("ensure start deadline again: %v", err)
	}

	if again != deadline {
		t.Fatalf("deadline should be stable after adoption: got %d want %d", again, deadline)
	}

	refetched, err := repos.Runs().GetExecutionDispatch(ctx, dispatch.ExecutionID)
	if err != nil {
		t.Fatalf("get execution dispatch: %v", err)
	}

	if refetched.StartDeadlineUnixNano != deadline {
		t.Fatalf("refetched deadline: got %d want %d", refetched.StartDeadlineUnixNano, deadline)
	}
}

func TestRunsRepository_ValidateActiveExecutionClaim(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	validator, ok := repos.Runs().(interface {
		ValidateActiveExecutionClaim(context.Context, string, string, string) error
	})
	if !ok {
		t.Fatalf("runs repository cannot validate active execution claims")
	}
	activeDispatches, ok := repos.Runs().(interface {
		GetActiveExecutionDispatch(context.Context, string, string) (dal.ExecutionDispatchRecord, error)
	})
	if !ok {
		t.Fatalf("runs repository cannot load active execution dispatches")
	}

	_, err := repos.Namespaces().Create(ctx, "team-validate-execution-claims", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-validate-execution-claims"
	def := `{"id":"job-validate-execution-claims","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, claim := claimPendingRunExecution(t, ctx, repos.Runs(), runID, "worker-a", time.Now().Add(time.Minute))
	if err := validator.ValidateActiveExecutionClaim(ctx, runID, dispatch.ExecutionID, claim.ClaimToken); err != nil {
		t.Fatalf("validate active execution claim: %v", err)
	}

	activeDispatch, err := activeDispatches.GetActiveExecutionDispatch(ctx, runID, dispatch.ExecutionID)
	if err != nil {
		t.Fatalf("get active execution dispatch: %v", err)
	}

	if activeDispatch.RunID != runID || activeDispatch.ExecutionID != dispatch.ExecutionID || activeDispatch.JobID != jobID || activeDispatch.NamespacePath != "/" {
		t.Fatalf("active execution dispatch = %+v", activeDispatch)
	}

	if err := validator.ValidateActiveExecutionClaim(ctx, runID, dispatch.ExecutionID, "wrong-token"); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for wrong execution claim token, got %v", err)
	}

	if err := validator.ValidateActiveExecutionClaim(ctx, "missing-run", dispatch.ExecutionID, claim.ClaimToken); !dal.IsNotFound(err) {
		t.Fatalf("expected not found for wrong run, got %v", err)
	}

	orphanedRunID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create orphaned run: %v", err)
	}

	orphanedDispatch, orphanedClaim := claimPendingRunExecution(t, ctx, repos.Runs(), orphanedRunID, "worker-orphaned", time.Now().Add(time.Minute))
	if err := repos.Runs().MarkRunOrphaned(ctx, orphanedRunID, dal.OrphanReasonAckUncertain); err != nil {
		t.Fatalf("mark run orphaned: %v", err)
	}

	if err := validator.ValidateActiveExecutionClaim(ctx, orphanedRunID, orphanedDispatch.ExecutionID, orphanedClaim.ClaimToken); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for orphaned execution claim, got %v", err)
	}

	if _, err := activeDispatches.GetActiveExecutionDispatch(ctx, orphanedRunID, orphanedDispatch.ExecutionID); !dal.IsNotFound(err) {
		t.Fatalf("expected not found for orphaned active dispatch, got %v", err)
	}

	expiredRunID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create expired run: %v", err)
	}

	expiredDispatch, expiredClaim := claimPendingRunExecution(t, ctx, repos.Runs(), expiredRunID, "worker-b", time.Now().Add(-time.Minute))
	if err := validator.ValidateActiveExecutionClaim(ctx, expiredRunID, expiredDispatch.ExecutionID, expiredClaim.ClaimToken); !dal.IsConflict(err) {
		t.Fatalf("expected conflict for expired execution claim, got %v", err)
	}
}

func TestRunsRepository_ExecutionClaimsRejectExpiredAcceptedReclaim(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-execution-reclaim", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-execution-reclaim"
	def := `{"id":"job-execution-reclaim","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	expiredUntil := time.Now().Add(-time.Minute)
	firstClaim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-a", expiredUntil)
	if err != nil {
		t.Fatalf("claim execution with expired lease: %v", err)
	}

	if !firstClaim.Claimed || firstClaim.ClaimToken == "" || !firstClaim.TransitionedToAccepted {
		t.Fatalf("expected first execution claim to accept, claim=%+v", firstClaim)
	}

	reclaimedUntil := time.Now().Add(time.Minute)
	secondClaim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-b", reclaimedUntil)
	if err != nil {
		t.Fatalf("reclaim execution: %v", err)
	}

	if secondClaim.Claimed || secondClaim.ClaimToken != "" {
		t.Fatalf("expected expired execution lease reclaim to be rejected, first=%+v second=%+v", firstClaim, secondClaim)
	}

	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertExecutionClaim(t, db, dispatch.ExecutionID, "worker-a", expiredUntil.Unix(), firstClaim.ClaimToken)
}

type claimedExecutionFinalizationRun struct {
	RunID      string
	Dispatch   dal.ExecutionDispatchRecord
	Token      string
	LeaseUntil int64
}

func setupClaimedExecutionFinalizationRun(t *testing.T, ctx context.Context, repos *dal.SQLRepositories, suffix string) claimedExecutionFinalizationRun {
	t.Helper()

	_, err := repos.Namespaces().Create(ctx, "team-execution-finalize-"+suffix, nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-execution-finalize-" + suffix
	def := `{"id":"` + jobID + `","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	executionLeaseUntil := time.Now().Add(time.Minute)
	claim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-a", executionLeaseUntil)
	if err != nil {
		t.Fatalf("claim execution: %v", err)
	}

	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected execution claim with token, claim=%+v", claim)
	}

	return claimedExecutionFinalizationRun{
		RunID:      runID,
		Dispatch:   dispatch,
		Token:      claim.ClaimToken,
		LeaseUntil: executionLeaseUntil.Unix(),
	}
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_Succeeds(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "success")

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("CompleteExecutionAndFinalizeRunByClaim success: %v", err)
	}

	if result.ExecutionID != setup.Dispatch.ExecutionID || result.RunID != setup.RunID || result.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		t.Fatalf("success result mismatch: %+v", result)
	}

	if result.Summary.Total != 1 || result.Summary.Succeeded != 1 || result.Summary.TerminalFailed != 0 || result.Summary.Incomplete != 0 || !result.Summary.AllSucceeded() {
		t.Fatalf("success summary mismatch: %+v", result.Summary)
	}

	if result.Activated != 0 || len(result.Children) != 0 {
		t.Fatalf("single task success should not activate children: %+v", result)
	}

	status, found, err := repos.Runs().GetRunStatus(ctx, setup.RunID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}

	if !found || status != dal.RunStatusSucceeded {
		t.Fatalf("run status: found=%v status=%q", found, status)
	}

	assertExecutionAndSegmentStatus(t, db, setup.Dispatch.ExecutionID, setup.Dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 2)
	assertRootTaskAndAttemptStatus(t, db, setup.RunID, dal.TaskStatusSucceeded, dal.TaskStatusSucceeded, 2)
	assertExecutionClaimCleared(t, db, setup.Dispatch.ExecutionID)
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_SuccessCanWinCancelRace(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "success-after-cancel")

	if _, err := repos.Runs().RequestRunCancel(ctx, setup.RunID, dal.CancelReasonAPI); err != nil {
		t.Fatalf("request cancel: %v", err)
	}

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("CompleteExecutionAndFinalizeRunByClaim success after cancel: %v", err)
	}

	if result.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded || !result.Summary.AllSucceeded() {
		t.Fatalf("success after cancel result mismatch: %+v", result)
	}

	status, found, err := repos.Runs().GetRunStatus(ctx, setup.RunID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}
	if !found || status != dal.RunStatusSucceeded {
		t.Fatalf("run status after cancel/success race: found=%v status=%q", found, status)
	}

	requested, err := repos.Runs().RunCancelRequested(ctx, setup.RunID)
	if err != nil {
		t.Fatalf("run cancel requested after success: %v", err)
	}
	if requested {
		t.Fatal("terminal success should clear durable cancel request")
	}
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_FailsRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "failed")

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusFailed, dal.FailureCodeExecution, "boom")
	if err != nil {
		t.Fatalf("CompleteExecutionAndFinalizeRunByClaim failed: %v", err)
	}

	if result.Outcome != dal.ExecutionFinalizationOutcomeRunFailed || result.Summary.Total != 1 || result.Summary.TerminalFailed != 1 {
		t.Fatalf("failed result mismatch: %+v", result)
	}

	var status string
	var failureCode string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason
		FROM job_runs
		WHERE run_id = ?
	`, setup.RunID).Scan(&status, &failureCode, &failureReason); err != nil {
		t.Fatalf("query failed run: %v", err)
	}

	if status != dal.RunStatusFailed || failureCode != dal.FailureCodeExecution || !failureReason.Valid || failureReason.String != "boom" {
		t.Fatalf("failed run state mismatch: status=%q code=%q reason=%v", status, failureCode, failureReason)
	}

	assertExecutionAndSegmentStatus(t, db, setup.Dispatch.ExecutionID, setup.Dispatch.SegmentID, dal.ExecutionStatusFailed, dal.SegmentStatusFailed, 2)
	assertRootTaskAndAttemptStatus(t, db, setup.RunID, dal.TaskStatusFailed, dal.TaskStatusFailed, 2)
	assertExecutionClaimCleared(t, db, setup.Dispatch.ExecutionID)
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_TerminalClearsSiblingClaims(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "terminal-clears-sibling")

	sibling, created, err := repos.Runs().EnsurePendingTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:    setup.RunID,
		TaskKey:  "sibling",
		SpecHash: "sha256:sibling",
	})
	if err != nil {
		t.Fatalf("ensure sibling: %v", err)
	}
	if !created {
		t.Fatal("expected sibling task to be created")
	}

	claimExecutionAccepted(t, ctx, repos.Runs(), sibling.ExecutionID, "worker-sibling")

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusFailed, dal.FailureCodeExecution, "boom")
	if err != nil {
		t.Fatalf("CompleteExecutionAndFinalizeRunByClaim failed: %v", err)
	}
	if result.Outcome != dal.ExecutionFinalizationOutcomeRunFailed {
		t.Fatalf("failed result mismatch: %+v", result)
	}

	assertExecutionClaimCleared(t, db, setup.Dispatch.ExecutionID)
	assertExecutionClaimCleared(t, db, sibling.ExecutionID)
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_CancelsRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "cancelled")

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusAborted, "", dal.CancelReasonAPI)
	if err != nil {
		t.Fatalf("CompleteExecutionAndFinalizeRunByClaim aborted: %v", err)
	}

	if result.Outcome != dal.ExecutionFinalizationOutcomeRunCancelled || result.Summary.Total != 1 || result.Summary.TerminalFailed != 1 {
		t.Fatalf("cancelled result mismatch: %+v", result)
	}

	var status string
	var failureCode string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason
		FROM job_runs
		WHERE run_id = ?
	`, setup.RunID).Scan(&status, &failureCode, &failureReason); err != nil {
		t.Fatalf("query cancelled run: %v", err)
	}

	if status != dal.RunStatusCancelled || failureCode != "" || !failureReason.Valid || failureReason.String != dal.CancelReasonAPI {
		t.Fatalf("cancelled run state mismatch: status=%q code=%q reason=%v", status, failureCode, failureReason)
	}

	assertExecutionAndSegmentStatus(t, db, setup.Dispatch.ExecutionID, setup.Dispatch.SegmentID, dal.ExecutionStatusAborted, dal.SegmentStatusAborted, 2)
	assertRootTaskAndAttemptStatus(t, db, setup.RunID, dal.TaskStatusAborted, dal.TaskStatusAborted, 2)
	assertExecutionClaimCleared(t, db, setup.Dispatch.ExecutionID)
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_WaitsForIncompleteTasks(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "waiting")

	sibling, created, err := repos.Runs().EnsurePendingTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:    setup.RunID,
		TaskKey:  "sibling",
		SpecHash: "sha256:sibling",
	})

	if err != nil {
		t.Fatalf("ensure sibling: %v", err)
	}

	if !created {
		t.Fatal("expected sibling task to be created")
	}

	claimExecutionAccepted(t, ctx, repos.Runs(), sibling.ExecutionID, "worker-sibling")

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("CompleteExecutionAndFinalizeRunByClaim waiting: %v", err)
	}

	if result.Outcome != dal.ExecutionFinalizationOutcomeWaiting || result.Summary.Total != 2 || result.Summary.Succeeded != 1 || result.Summary.Incomplete != 1 {
		t.Fatalf("waiting result mismatch: %+v", result)
	}

	status, found, err := repos.Runs().GetRunStatus(ctx, setup.RunID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}

	if !found || status != dal.RunStatusQueued {
		t.Fatalf("run should be queued while sibling is incomplete: found=%v status=%q", found, status)
	}

	assertExecutionAndSegmentStatus(t, db, setup.Dispatch.ExecutionID, setup.Dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 2)
	assertExecutionAndSegmentStatus(t, db, sibling.ExecutionID, sibling.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertTaskAndAttemptStatus(t, db, sibling.TaskID, 1, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)
	assertExecutionClaimCleared(t, db, setup.Dispatch.ExecutionID)
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_ActivatesChildren(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "continued")

	child, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        setup.RunID,
		ParentTaskID: setup.Dispatch.TaskID,
		TaskKey:      "child",
		SpecHash:     "sha256:child",
	})

	if err != nil {
		t.Fatalf("ensure child: %v", err)
	}

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("CompleteExecutionAndFinalizeRunByClaim continued: %v", err)
	}

	if result.Outcome != dal.ExecutionFinalizationOutcomeContinued || result.Activated != 1 || len(result.Children) != 1 || result.Children[0] != child {
		t.Fatalf("continued result mismatch: %+v", result)
	}

	if result.Summary.Total != 2 || result.Summary.Succeeded != 1 || result.Summary.Incomplete != 1 {
		t.Fatalf("continued summary mismatch: %+v", result.Summary)
	}

	status, found, err := repos.Runs().GetRunStatus(ctx, setup.RunID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}

	if !found || status != dal.RunStatusQueued {
		t.Fatalf("run should be queued after fan-out: found=%v status=%q", found, status)
	}

	assertTaskExecutionStatuses(t, db, child, dal.TaskStatusPending, dal.SegmentStatusPending, dal.ExecutionStatusPending, 0)
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_CancelRequestStopsContinuation(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "continued-cancelled")

	child, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        setup.RunID,
		ParentTaskID: setup.Dispatch.TaskID,
		TaskKey:      "child",
		SpecHash:     "sha256:child",
	})

	if err != nil {
		t.Fatalf("ensure child: %v", err)
	}

	if _, err := repos.Runs().RequestRunCancel(ctx, setup.RunID, dal.CancelReasonAPI); err != nil {
		t.Fatalf("request cancel: %v", err)
	}

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("CompleteExecutionAndFinalizeRunByClaim after cancel: %v", err)
	}

	if result.Outcome != dal.ExecutionFinalizationOutcomeRunCancelled || result.Activated != 0 || len(result.Children) != 0 {
		t.Fatalf("cancelled continuation result mismatch: %+v", result)
	}

	status, found, err := repos.Runs().GetRunStatus(ctx, setup.RunID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}

	if !found || status != dal.RunStatusCancelled {
		t.Fatalf("run should be cancelled after cancel races with continuation: found=%v status=%q", found, status)
	}

	requested, err := repos.Runs().RunCancelRequested(ctx, setup.RunID)
	if err != nil {
		t.Fatalf("run cancel requested after terminal transition: %v", err)
	}

	if requested {
		t.Fatal("terminal cancellation should clear durable cancel request")
	}

	assertExecutionAndSegmentStatus(t, db, setup.Dispatch.ExecutionID, setup.Dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 2)
	assertTaskExecutionStatuses(t, db, child, dal.TaskStatusPlanned, dal.SegmentStatusPlanned, dal.ExecutionStatusPlanned, 0)
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_RejectsDuplicateAfterActivatingChildren(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "continued-duplicate")

	child, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        setup.RunID,
		ParentTaskID: setup.Dispatch.TaskID,
		TaskKey:      "child",
		SpecHash:     "sha256:child",
	})

	if err != nil {
		t.Fatalf("ensure child: %v", err)
	}

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("CompleteExecutionAndFinalizeRunByClaim continued: %v", err)
	}

	if result.Outcome != dal.ExecutionFinalizationOutcomeContinued || result.Activated != 1 || len(result.Children) != 1 {
		t.Fatalf("continued result mismatch: %+v", result)
	}

	if _, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusSucceeded, "", ""); !dal.IsConflict(err) {
		t.Fatalf("expected duplicate completion conflict, got %v", err)
	}

	status, found, err := repos.Runs().GetRunStatus(ctx, setup.RunID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}

	if !found || status != dal.RunStatusQueued {
		t.Fatalf("run should remain queued after duplicate finalization: found=%v status=%q", found, status)
	}

	assertExecutionAndSegmentStatus(t, db, setup.Dispatch.ExecutionID, setup.Dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 2)
	assertTaskExecutionStatuses(t, db, child, dal.TaskStatusPending, dal.SegmentStatusPending, dal.ExecutionStatusPending, 0)
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_RejectsStaleClaim(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "stale")

	if _, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", "stale-token", dal.ExecutionStatusSucceeded, "", ""); !dal.IsConflict(err) {
		t.Fatalf("expected stale execution claim conflict, got %v", err)
	}

	status, found, err := repos.Runs().GetRunStatus(ctx, setup.RunID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}

	if !found || status != dal.RunStatusRunning {
		t.Fatalf("run should remain running after stale finalization: found=%v status=%q", found, status)
	}

	assertExecutionAndSegmentStatus(t, db, setup.Dispatch.ExecutionID, setup.Dispatch.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertRootTaskAndAttemptStatus(t, db, setup.RunID, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)
	assertExecutionClaim(t, db, setup.Dispatch.ExecutionID, "worker-a", setup.LeaseUntil, setup.Token)
}

func TestRunsRepository_CompleteExecutionAndFinalizeRunByClaim_RecoversOrphanedRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()
	setup := setupClaimedExecutionFinalizationRun(t, ctx, repos, "orphaned")

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = ?, lease_until = ?
		WHERE run_id = ?
	`, dal.RunStatusOrphaned, time.Now().Add(-time.Minute).Unix(), setup.RunID); err != nil {
		t.Fatalf("mark run orphaned: %v", err)
	}

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, setup.Dispatch.ExecutionID, "worker-a", setup.Token, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("CompleteExecutionAndFinalizeRunByClaim orphaned success: %v", err)
	}

	if result.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded || !result.Summary.AllSucceeded() {
		t.Fatalf("orphaned success result mismatch: %+v", result)
	}

	status, found, err := repos.Runs().GetRunStatus(ctx, setup.RunID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}

	if !found || status != dal.RunStatusSucceeded {
		t.Fatalf("orphaned run should recover to succeeded: found=%v status=%q", found, status)
	}

	assertExecutionAndSegmentStatus(t, db, setup.Dispatch.ExecutionID, setup.Dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 2)
	assertExecutionClaimCleared(t, db, setup.Dispatch.ExecutionID)
}

func TestRunsRepository_DispatchAndTransitionsUseLinkedTaskAttempt(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-linked-task", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-linked-task"
	def := `{"id":"job-linked-task","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

	claimExecutionAccepted(t, ctx, repos.Runs(), rootDispatch.ExecutionID, "worker-root")

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

	claimExecutionAccepted(t, ctx, repos.Runs(), child.ExecutionID, "worker-child")
	assertExecutionAndSegmentStatus(t, db, child.ExecutionID, child.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertTaskAndAttemptStatus(t, db, child.TaskID, 1, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)
	assertRootTaskAndAttemptStatus(t, db, runID, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)
}

func TestRunCatalogUpdater_AppliesRunAndExecutionStatusUpdates(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-catalog-updates", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-catalog-updates"
	def := `{"id":"job-catalog-updates","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
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

func TestRunsRepository_ApplyTerminalExecutionSnapshotMaterializesFinalState(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-terminal-snapshot", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-terminal-snapshot"
	def := `{"id":"job-terminal-snapshot","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	root, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root pending execution: %v", err)
	}

	if err := repos.Runs().UpsertRunHotStateOwner(ctx, dal.RunHotStateOwnerUpdate{
		RunID:      runID,
		CellID:     "local",
		OwnerID:    "orchestrator:registry:local",
		OwnerEpoch: "worker-terminal-snapshot",
		LeaseUntil: time.Now().Add(time.Minute),
	}); err != nil {
		t.Fatalf("upsert hot-state owner: %v", err)
	}

	childTaskID := runID + ":child"
	childAttemptID := childTaskID + ":attempt:1"
	childExecutionID := childAttemptID + ":execution"
	acceptedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano()
	startedAt := time.Date(2026, 1, 2, 3, 4, 6, 0, time.UTC).UnixNano()
	finishedAt := time.Date(2026, 1, 2, 3, 4, 7, 0, time.UTC).UnixNano()

	err = repos.Runs().ApplyTerminalExecutionSnapshot(ctx, dal.TerminalExecutionSnapshotUpdate{
		RunID:   runID,
		Outcome: dal.ExecutionFinalizationOutcomeRunSucceeded,
		Executions: []dal.TaskExecutionSnapshot{
			{
				Record: dal.TaskExecutionRecord{
					RunID:         runID,
					TaskID:        root.TaskID,
					TaskKey:       dal.RootTaskKey,
					Name:          dal.RootTaskKey,
					TaskAttemptID: root.TaskAttemptID,
					SegmentID:     root.SegmentID,
					SegmentName:   root.SegmentName,
					ExecutionID:   root.ExecutionID,
					CellID:        root.CellID,
					Attempt:       root.Attempt,
				},
				Status:             dal.ExecutionStatusSucceeded,
				AcceptedAtUnixNano: acceptedAt,
				StartedAtUnixNano:  startedAt,
				FinishedAtUnixNano: finishedAt,
			},
			{
				Record: dal.TaskExecutionRecord{
					RunID:         runID,
					TaskID:        childTaskID,
					ParentTaskID:  root.TaskID,
					TaskKey:       "child",
					Name:          "child task",
					TaskAttemptID: childAttemptID,
					SegmentID:     childTaskID + ":segment",
					SegmentName:   "child task",
					ExecutionID:   childExecutionID,
					CellID:        "local",
					Attempt:       1,
				},
				Status:             dal.ExecutionStatusSucceeded,
				AcceptedAtUnixNano: acceptedAt,
				StartedAtUnixNano:  startedAt,
				FinishedAtUnixNano: finishedAt,
			},
		},
	})

	if err != nil {
		t.Fatalf("ApplyTerminalExecutionSnapshot: %v", err)
	}

	assertExecutionAndSegmentStatus(t, db, root.ExecutionID, root.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 1)
	assertTaskAndAttemptStatus(t, db, root.TaskID, 1, dal.TaskStatusSucceeded, dal.TaskStatusSucceeded, 1)

	tasks, _, err := repos.Runs().ListRunTasks(ctx, runID, 0, 10)
	if err != nil {
		t.Fatalf("list final task facts: %v", err)
	}

	var child *dal.TaskRecord
	for i := range tasks {
		if tasks[i].TaskID == childTaskID {
			child = &tasks[i]
			break
		}
	}

	if child == nil {
		t.Fatalf("child task missing from final facts: %+v", tasks)
		return
	}

	if child.Status != dal.TaskStatusSucceeded || len(child.Attempts) != 1 {
		t.Fatalf("unexpected child fact: %+v", child)
	}

	childAttempt := child.Attempts[0]
	if childAttempt.AttemptID != childAttemptID || childAttempt.ExecutionID != childExecutionID || childAttempt.ExecutionStatus != dal.ExecutionStatusSucceeded {
		t.Fatalf("unexpected child attempt fact: %+v", childAttempt)
	}

	if childAttempt.StartedAt == nil || *childAttempt.StartedAt != time.Unix(0, startedAt).UTC().Format(time.RFC3339Nano) {
		t.Fatalf("started_at fact: got %v", childAttempt.StartedAt)
	}

	summary, err := repos.Runs().GetRunTaskCompletion(ctx, runID)
	if err != nil {
		t.Fatalf("get final task completion: %v", err)
	}

	if summary.Total != 2 || summary.Succeeded != 2 || summary.Incomplete != 0 || summary.TerminalFailed != 0 {
		t.Fatalf("unexpected completion from final facts: %+v", summary)
	}

	run, err := repos.Runs().GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	if run.Status != dal.RunStatusSucceeded {
		t.Fatalf("run status: got %q, want %q", run.Status, dal.RunStatusSucceeded)
	}

	if owner, found, err := repos.Runs().GetRunHotStateOwner(ctx, runID); err != nil || found {
		t.Fatalf("hot-state owner after terminal snapshot: found=%v owner=%+v err=%v", found, owner, err)
	}
}

func TestRunsRepository_ApplyTerminalExecutionSnapshotRejectsConflictingTerminalRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-terminal-snapshot-conflict", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-terminal-snapshot-conflict"
	def := `{"id":"job-terminal-snapshot-conflict","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	root, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root pending execution: %v", err)
	}

	if err := repos.Runs().ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{
		RunID:  runID,
		Status: dal.RunStatusSucceeded,
	}); err != nil {
		t.Fatalf("apply succeeded run status: %v", err)
	}

	err = repos.Runs().ApplyTerminalExecutionSnapshot(ctx, dal.TerminalExecutionSnapshotUpdate{
		RunID:       runID,
		Outcome:     dal.ExecutionFinalizationOutcomeRunFailed,
		FailureCode: dal.FailureCodeExecution,
		Reason:      "late failed snapshot",
		Executions: []dal.TaskExecutionSnapshot{
			{
				Record: dal.TaskExecutionRecord{
					RunID:         runID,
					TaskID:        root.TaskID,
					TaskKey:       dal.RootTaskKey,
					Name:          dal.RootTaskKey,
					TaskAttemptID: root.TaskAttemptID,
					SegmentID:     root.SegmentID,
					SegmentName:   root.SegmentName,
					ExecutionID:   root.ExecutionID,
					CellID:        root.CellID,
					Attempt:       root.Attempt,
				},
				Status: dal.ExecutionStatusFailed,
			},
		},
	})

	if !dal.IsConflict(err) {
		t.Fatalf("expected conflicting terminal snapshot to return conflict, got %v", err)
	}

	run, err := repos.Runs().GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	if run.Status != dal.RunStatusSucceeded {
		t.Fatalf("run status = %q, want %q", run.Status, dal.RunStatusSucceeded)
	}

	var executionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, root.ExecutionID).Scan(&executionStatus); err != nil {
		t.Fatalf("query execution status: %v", err)
	}

	if executionStatus != dal.ExecutionStatusPending {
		t.Fatalf("execution status after rejected snapshot = %q, want %q", executionStatus, dal.ExecutionStatusPending)
	}
}

func TestRunsRepository_ApplyTerminalExecutionSnapshotClearsActiveExecutionClaims(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-terminal-snapshot-clears-claims", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-terminal-snapshot-clears-claims"
	def := `{"id":"job-terminal-snapshot-clears-claims","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	root, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root pending execution: %v", err)
	}

	sibling, created, err := repos.Runs().EnsurePendingTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: root.TaskID,
		TaskKey:      "sibling",
		SpecHash:     "sha256:sibling",
	})

	if err != nil {
		t.Fatalf("ensure sibling: %v", err)
	}

	if !created {
		t.Fatal("expected sibling task to be created")
	}

	claimExecutionAccepted(t, ctx, repos.Runs(), sibling.ExecutionID, "worker-sibling")

	err = repos.Runs().ApplyTerminalExecutionSnapshot(ctx, dal.TerminalExecutionSnapshotUpdate{
		RunID:       runID,
		Outcome:     dal.ExecutionFinalizationOutcomeRunFailed,
		FailureCode: dal.FailureCodeExecution,
		Reason:      "root failed while sibling was active",
		Executions: []dal.TaskExecutionSnapshot{
			{
				Record: dal.TaskExecutionRecord{
					RunID:         runID,
					TaskID:        root.TaskID,
					TaskKey:       dal.RootTaskKey,
					Name:          dal.RootTaskKey,
					TaskAttemptID: root.TaskAttemptID,
					SegmentID:     root.SegmentID,
					SegmentName:   root.SegmentName,
					ExecutionID:   root.ExecutionID,
					CellID:        root.CellID,
					Attempt:       root.Attempt,
				},
				Status: dal.ExecutionStatusFailed,
			},
			{
				Record: dal.TaskExecutionRecord{
					RunID:         runID,
					TaskID:        sibling.TaskID,
					ParentTaskID:  root.TaskID,
					TaskKey:       "sibling",
					Name:          "sibling",
					TaskAttemptID: sibling.TaskAttemptID,
					SegmentID:     sibling.SegmentID,
					SegmentName:   sibling.SegmentName,
					ExecutionID:   sibling.ExecutionID,
					CellID:        sibling.CellID,
					Attempt:       sibling.Attempt,
				},
				Status: dal.ExecutionStatusRunning,
			},
		},
	})

	if err != nil {
		t.Fatalf("ApplyTerminalExecutionSnapshot: %v", err)
	}

	run, err := repos.Runs().GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	if run.Status != dal.RunStatusFailed {
		t.Fatalf("run status = %q, want %q", run.Status, dal.RunStatusFailed)
	}

	assertExecutionAndSegmentStatus(t, db, root.ExecutionID, root.SegmentID, dal.ExecutionStatusFailed, dal.SegmentStatusFailed, 1)

	var activeClaims int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM segment_executions
		WHERE run_id = ?
			AND (lease_owner IS NOT NULL OR lease_until IS NOT NULL OR claim_token IS NOT NULL)
	`, runID).Scan(&activeClaims); err != nil {
		t.Fatalf("query active execution claims: %v", err)
	}

	if activeClaims != 0 {
		t.Fatalf("terminal snapshot should clear all active execution claims, found %d", activeClaims)
	}
}

func TestRunsRepository_RunHotStateOwnerUpsertGetClear(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	runID, _, err := repos.Runs().CreateRun(ctx, "job-hot-state-owner", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	firstLease := time.Now().Add(time.Minute).UTC()
	if err := repos.Runs().UpsertRunHotStateOwner(ctx, dal.RunHotStateOwnerUpdate{
		RunID:        runID,
		CellID:       "iad-a",
		OwnerID:      "orchestrator:pinned:127.0.0.1:8085",
		OwnerEpoch:   "epoch-1",
		LeaseUntil:   firstLease,
		LastSequence: 7,
	}); err != nil {
		t.Fatalf("upsert owner: %v", err)
	}

	owner, found, err := repos.Runs().GetRunHotStateOwner(ctx, runID)
	if err != nil {
		t.Fatalf("get owner: %v", err)
	}
	if !found {
		t.Fatal("expected hot-state owner")
	}
	if owner.RunID != runID || owner.CellID != "iad-a" || owner.OwnerID != "orchestrator:pinned:127.0.0.1:8085" ||
		owner.OwnerEpoch != "epoch-1" || owner.LastSequence != 7 || owner.LeaseUntil.Unix() != firstLease.Unix() {
		t.Fatalf("owner after first upsert: %+v", owner)
	}

	secondLease := time.Now().Add(2 * time.Minute).UTC()
	if err := repos.Runs().UpsertRunHotStateOwner(ctx, dal.RunHotStateOwnerUpdate{
		RunID:        runID,
		CellID:       "iad-b",
		OwnerID:      "orchestrator:registry:iad-b",
		OwnerEpoch:   "epoch-2",
		LeaseUntil:   secondLease,
		LastSequence: 9,
	}); err != nil {
		t.Fatalf("second upsert owner: %v", err)
	}

	owner, found, err = repos.Runs().GetRunHotStateOwner(ctx, runID)
	if err != nil {
		t.Fatalf("get second owner: %v", err)
	}
	if !found || owner.CellID != "iad-b" || owner.OwnerID != "orchestrator:registry:iad-b" ||
		owner.OwnerEpoch != "epoch-2" || owner.LastSequence != 9 || owner.LeaseUntil.Unix() != secondLease.Unix() {
		t.Fatalf("owner after second upsert: found=%v owner=%+v", found, owner)
	}

	if err := repos.Runs().ClearRunHotStateOwner(ctx, runID); err != nil {
		t.Fatalf("clear owner: %v", err)
	}

	if owner, found, err := repos.Runs().GetRunHotStateOwner(ctx, runID); err != nil || found {
		t.Fatalf("owner after clear: found=%v owner=%+v err=%v", found, owner, err)
	}
}

func TestRunCatalogUpdater_AppliesExecutionUpdatesFromPlannedRows(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-catalog-planned-updates", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-catalog-planned-updates"
	def := `{"id":"job-catalog-planned-updates","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	accepted, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: runID + ":" + dal.RootTaskKey,
		TaskKey:      "accepted-child",
		SpecHash:     "sha256:accepted-child",
	})
	if err != nil {
		t.Fatalf("ensure accepted child: %v", err)
	}

	running, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: runID + ":" + dal.RootTaskKey,
		TaskKey:      "running-child",
		SpecHash:     "sha256:running-child",
	})
	if err != nil {
		t.Fatalf("ensure running child: %v", err)
	}

	succeeded, _, err := repos.Runs().EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: runID + ":" + dal.RootTaskKey,
		TaskKey:      "succeeded-child",
		SpecHash:     "sha256:succeeded-child",
	})
	if err != nil {
		t.Fatalf("ensure succeeded child: %v", err)
	}

	var updater dal.RunCatalogUpdater = repos.Runs()
	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: accepted.ExecutionID,
		Status:      dal.ExecutionStatusAccepted,
	}); err != nil {
		t.Fatalf("apply planned accepted update: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, accepted.ExecutionID, accepted.SegmentID, dal.ExecutionStatusAccepted, dal.SegmentStatusAccepted, 1)
	assertTaskAndAttemptStatus(t, db, accepted.TaskID, 1, dal.TaskStatusAccepted, dal.TaskStatusAccepted, 1)

	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: running.ExecutionID,
		Status:      dal.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("apply planned running update: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, running.ExecutionID, running.SegmentID, dal.ExecutionStatusRunning, dal.SegmentStatusRunning, 1)
	assertTaskAndAttemptStatus(t, db, running.TaskID, 1, dal.TaskStatusRunning, dal.TaskStatusRunning, 1)

	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: succeeded.ExecutionID,
		Status:      dal.ExecutionStatusSucceeded,
	}); err != nil {
		t.Fatalf("apply planned succeeded update: %v", err)
	}
	assertExecutionAndSegmentStatus(t, db, succeeded.ExecutionID, succeeded.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 1)
	assertTaskAndAttemptStatus(t, db, succeeded.TaskID, 1, dal.TaskStatusSucceeded, dal.TaskStatusSucceeded, 1)
}

func TestRunCatalogUpdater_IgnoresStaleCatalogStatusUpdates(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	_, err := repos.Namespaces().Create(ctx, "team-catalog-stale-updates", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-catalog-stale-updates"
	def := `{"id":"job-catalog-stale-updates","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create definition snapshot: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	updater := repos.Runs()
	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: dispatch.ExecutionID,
		Status:      dal.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("apply running execution update: %v", err)
	}

	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusRunning, dal.SegmentStatusRunning, 1)
	assertTaskAndAttemptStatus(t, db, dispatch.TaskID, 1, dal.TaskStatusRunning, dal.TaskStatusRunning, 1)

	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: dispatch.ExecutionID,
		Status:      dal.ExecutionStatusAccepted,
	}); err != nil {
		t.Fatalf("apply stale accepted execution update: %v", err)
	}

	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusRunning, dal.SegmentStatusRunning, 1)
	assertTaskAndAttemptStatus(t, db, dispatch.TaskID, 1, dal.TaskStatusRunning, dal.TaskStatusRunning, 1)

	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: dispatch.ExecutionID,
		Status:      dal.ExecutionStatusSucceeded,
	}); err != nil {
		t.Fatalf("apply succeeded execution update: %v", err)
	}

	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 2)
	assertTaskAndAttemptStatus(t, db, dispatch.TaskID, 1, dal.TaskStatusSucceeded, dal.TaskStatusSucceeded, 2)

	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: dispatch.ExecutionID,
		Status:      dal.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("apply stale running execution update after terminal: %v", err)
	}

	assertExecutionAndSegmentStatus(t, db, dispatch.ExecutionID, dispatch.SegmentID, dal.ExecutionStatusSucceeded, dal.SegmentStatusSucceeded, 2)
	assertTaskAndAttemptStatus(t, db, dispatch.TaskID, 1, dal.TaskStatusSucceeded, dal.TaskStatusSucceeded, 2)

	if err := updater.ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{
		ExecutionID: dispatch.ExecutionID,
		Status:      dal.ExecutionStatusFailed,
	}); !dal.IsConflict(err) {
		t.Fatalf("apply conflicting terminal execution update error = %v, want conflict", err)
	}

	if err := updater.ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{
		RunID:  runID,
		Status: dal.RunStatusRunning,
	}); err != nil {
		t.Fatalf("apply running run update: %v", err)
	}

	if err := updater.ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{
		RunID:  runID,
		Status: dal.RunStatusSucceeded,
	}); err != nil {
		t.Fatalf("apply succeeded run update: %v", err)
	}

	if err := updater.ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{
		RunID:  runID,
		Status: dal.RunStatusRunning,
	}); err != nil {
		t.Fatalf("apply stale running run update after terminal: %v", err)
	}

	run, err := repos.Runs().GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}

	if run.Status != dal.RunStatusSucceeded {
		t.Fatalf("run status = %q, want %q", run.Status, dal.RunStatusSucceeded)
	}

	if err := updater.ApplyRunStatusUpdate(ctx, dal.RunStatusUpdate{
		RunID:       runID,
		Status:      dal.RunStatusFailed,
		FailureCode: dal.FailureCodeExecution,
		Reason:      "late failure",
	}); !dal.IsConflict(err) {
		t.Fatalf("apply conflicting terminal run update error = %v, want conflict", err)
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

	if err := repos.Runs().ApplyExecutionStatusUpdate(ctx, dal.ExecutionStatusUpdate{ExecutionID: "missing-execution", Status: dal.ExecutionStatusAccepted}); !dal.IsNotFound(err) {
		t.Fatalf("expected missing execution to return ErrNotFound, got %v", err)
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

	duplicate, created, err := events.Record(ctx, "iad-a", "event-1", "run.status", []byte(`{"run_id":"run-1","status":"running"}`))
	if err != nil {
		t.Fatalf("record duplicate event: %v", err)
	}

	if created {
		t.Fatal("expected duplicate record to be idempotent")
	}

	if duplicate.ID != first.ID || string(duplicate.Payload) != string(first.Payload) {
		t.Fatalf("duplicate should return original event, got %+v want %+v", duplicate, first)
	}

	if _, _, err := events.Record(ctx, "iad-a", "event-1", "run.status", []byte(`{"run_id":"run-1","status":"failed"}`)); !dal.IsConflict(err) {
		t.Fatalf("duplicate event with different payload should conflict, got %v", err)
	}

	if _, _, err := events.Record(ctx, "iad-a", "event-1", "execution.status", []byte(`{"execution_id":"execution-1","status":"accepted"}`)); !dal.IsConflict(err) {
		t.Fatalf("duplicate event with different type should conflict, got %v", err)
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

	if err := events.MarkRetryable(ctx, second.ID, "apply temporarily unavailable"); err != nil {
		t.Fatalf("mark retryable: %v", err)
	}

	pending, err = events.ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list pending after retryable mark: %v", err)
	}

	if len(pending) != 1 || pending[0].ID != second.ID || pending[0].Attempts != 1 || pending[0].LastError == nil || *pending[0].LastError != "apply temporarily unavailable" {
		t.Fatalf("expected retryable event to remain pending with last_error, got %+v", pending)
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

	if secondStatus != dal.CatalogEventStatusFailed || secondAttempts != 2 || !secondError.Valid || secondError.String != "apply failed" {
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

func assertExecutionClaim(t *testing.T, db *sql.DB, executionID, wantOwner string, wantLeaseUntil int64, wantToken string) {
	t.Helper()

	var owner, token sql.NullString
	var leaseUntil sql.NullInt64
	if err := db.QueryRow("SELECT lease_owner, lease_until, claim_token FROM segment_executions WHERE execution_id = ?", executionID).
		Scan(&owner, &leaseUntil, &token); err != nil {
		t.Fatalf("query execution claim: %v", err)
	}

	if !owner.Valid || owner.String != wantOwner || !leaseUntil.Valid || leaseUntil.Int64 != wantLeaseUntil || !token.Valid || token.String != wantToken {
		t.Fatalf("execution claim: got owner=%v lease_until=%v token=%v, want owner=%q lease_until=%d token=%q", owner, leaseUntil, token, wantOwner, wantLeaseUntil, wantToken)
	}
}

func assertRunExecutionOwner(t *testing.T, db *sql.DB, runID, wantStatus, wantOwner string, wantLeaseUntil int64) {
	t.Helper()

	var status string
	var owner sql.NullString
	var leaseUntil sql.NullInt64
	if err := db.QueryRow("SELECT status, lease_owner, lease_until FROM job_runs WHERE run_id = ?", runID).
		Scan(&status, &owner, &leaseUntil); err != nil {
		t.Fatalf("query run execution owner: %v", err)
	}

	if status != wantStatus || !owner.Valid || owner.String != wantOwner || !leaseUntil.Valid || leaseUntil.Int64 != wantLeaseUntil {
		t.Fatalf("run execution owner: got status=%q owner=%v lease_until=%v, want status=%q owner=%q lease_until=%d", status, owner, leaseUntil, wantStatus, wantOwner, wantLeaseUntil)
	}
}

func assertExecutionClaimCleared(t *testing.T, db *sql.DB, executionID string) {
	t.Helper()

	var owner, token sql.NullString
	var leaseUntil sql.NullInt64
	if err := db.QueryRow("SELECT lease_owner, lease_until, claim_token FROM segment_executions WHERE execution_id = ?", executionID).
		Scan(&owner, &leaseUntil, &token); err != nil {
		t.Fatalf("query execution claim: %v", err)
	}

	if owner.Valid || leaseUntil.Valid || token.Valid {
		t.Fatalf("execution claim should be cleared, got owner=%v lease_until=%v token=%v", owner, leaseUntil, token)
	}
}

func taskExecutionRecordsByKey(records []dal.TaskExecutionRecord) map[string]dal.TaskExecutionRecord {
	out := make(map[string]dal.TaskExecutionRecord, len(records))
	for _, rec := range records {
		out[rec.TaskKey] = rec
	}

	return out
}

func durableSequenceJob(jobID, runID string) *api.Job {
	rootID := "root"
	rootUses := "builtins/sequence"
	setupID := "setup"
	buildID := "build"
	compileID := "compile"
	testID := "test"
	deployID := "deploy"
	buildUses := "builtins/parallel"
	scriptUses := "builtins/script"

	return &api.Job{
		Id:    stringPtr(jobID),
		RunId: stringPtr(runID),
		Root: &api.Node{
			Id:   &rootID,
			Uses: &rootUses,
			Steps: []*api.Node{
				{
					Id:   &setupID,
					Uses: &scriptUses,
					With: map[string]string{"script": "echo setup"},
				},
				{
					Id:   &buildID,
					Uses: &buildUses,
					Steps: []*api.Node{
						{
							Id:   &compileID,
							Uses: &scriptUses,
							With: map[string]string{"script": "echo compile"},
						},
						{
							Id:   &testID,
							Uses: &scriptUses,
							With: map[string]string{"script": "echo test"},
						},
					},
				},
				{
					Id:   &deployID,
					Uses: &scriptUses,
					With: map[string]string{"script": "echo deploy"},
				},
			},
		},
	}
}

func stringPtr(s string) *string {
	return &s
}

func recordExecutionPayloadForJob(t *testing.T, ctx context.Context, repos *dal.SQLRepositories, runID string, j *api.Job) {
	t.Helper()

	payloadJSON, err := protojson.Marshal(&api.JobRequest{Job: j})
	if err != nil {
		t.Fatalf("marshal execution payload: %v", err)
	}

	if _, _, err := repos.Runs().RecordExecutionPayload(ctx, runID, string(payloadJSON), dal.DefinitionHash(string(payloadJSON))); err != nil {
		t.Fatalf("record execution payload: %v", err)
	}
}

func taskExecutionIDForKey(t *testing.T, tasks map[string]dal.TaskRecord, taskKey string) string {
	t.Helper()

	task, ok := tasks[taskKey]
	if !ok {
		t.Fatalf("task %q missing from %+v", taskKey, tasks)
	}

	if len(task.Attempts) == 0 || task.Attempts[0].ExecutionID == "" {
		t.Fatalf("task %q has no execution: %+v", taskKey, task)
	}

	return task.Attempts[0].ExecutionID
}

func assertTaskStatusByKey(t *testing.T, db *sql.DB, tasks map[string]dal.TaskRecord, taskKey, want string) {
	t.Helper()

	task, ok := tasks[taskKey]
	if !ok {
		t.Fatalf("task %q missing from %+v", taskKey, tasks)
	}

	var got string
	if err := db.QueryRow("SELECT status FROM run_tasks WHERE task_id = ?", task.TaskID).Scan(&got); err != nil {
		t.Fatalf("query task %s status: %v", taskKey, err)
	}

	if got != want {
		t.Fatalf("task %s status: got %q, want %q", taskKey, got, want)
	}
}

func assertActivatedTaskKeys(t *testing.T, records []dal.TaskExecutionRecord, want ...string) {
	t.Helper()

	if len(records) != len(want) {
		t.Fatalf("activated task count: got %d (%+v), want %d (%+v)", len(records), records, len(want), want)
	}

	for i, rec := range records {
		if rec.TaskKey != want[i] {
			t.Fatalf("activated task %d: got %q in %+v, want %q", i, rec.TaskKey, records, want[i])
		}
	}
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

func assertDispatchExpired(t *testing.T, db *sql.DB, runID, executionID, segmentID string) {
	t.Helper()

	var runStatus, failureCode string
	var failureReason sql.NullString
	if err := db.QueryRow(`SELECT status, failure_code, failure_reason FROM job_runs WHERE run_id = ?`, runID).
		Scan(&runStatus, &failureCode, &failureReason); err != nil {
		t.Fatalf("query expired run status: %v", err)
	}

	if runStatus != dal.RunStatusFailed || failureCode != dal.FailureCodeDispatchExpired || !failureReason.Valid {
		t.Fatalf("expired run status: status=%q failure_code=%q failure_reason=%v", runStatus, failureCode, failureReason)
	}

	var executionStatus string
	var finishedAt sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	if err := db.QueryRow(`SELECT status, finished_at, lease_owner, lease_until FROM segment_executions WHERE execution_id = ?`, executionID).
		Scan(&executionStatus, &finishedAt, &leaseOwner, &leaseUntil); err != nil {
		t.Fatalf("query expired execution status: %v", err)
	}

	if executionStatus != dal.ExecutionStatusFailed || !finishedAt.Valid || leaseOwner.Valid || leaseUntil.Valid {
		t.Fatalf("expired execution state: status=%q finished_at=%v lease_owner=%v lease_until=%v", executionStatus, finishedAt, leaseOwner, leaseUntil)
	}

	var segmentStatus string
	if err := db.QueryRow(`SELECT status FROM run_segments WHERE segment_id = ?`, segmentID).Scan(&segmentStatus); err != nil {
		t.Fatalf("query expired segment status: %v", err)
	}

	if segmentStatus != dal.SegmentStatusFailed {
		t.Fatalf("expired segment status: got %q want %q", segmentStatus, dal.SegmentStatusFailed)
	}

	var taskStatus string
	if err := db.QueryRow(`SELECT status FROM run_tasks WHERE task_id = ?`, runID+":"+dal.RootTaskKey).Scan(&taskStatus); err != nil {
		t.Fatalf("query expired task status: %v", err)
	}

	if taskStatus != dal.TaskStatusFailed {
		t.Fatalf("expired task status: got %q want %q", taskStatus, dal.TaskStatusFailed)
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

func insertSourceCronTriggerSpec(t *testing.T, ctx context.Context, db *sql.DB, jobID, repositoryID, cronSpec string, nextRun time.Time) int64 {
	t.Helper()

	result, err := db.ExecContext(ctx,
		"INSERT INTO job_triggers (job_id, trigger_type, source_repository_id) VALUES (?, ?, ?)",
		jobID,
		"cron",
		repositoryID,
	)

	if err != nil {
		t.Fatalf("insert source cron trigger: %v", err)
	}

	triggerID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("source cron trigger id: %v", err)
	}

	result, err = db.ExecContext(ctx,
		"INSERT INTO cron_trigger_specs (trigger_id, cron_spec, next_run_at) VALUES (?, ?, ?)",
		triggerID,
		cronSpec,
		nextRun.Format(time.RFC3339),
	)
	if err != nil {
		t.Fatalf("insert source cron trigger spec: %v", err)
	}

	specID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("source cron trigger spec id: %v", err)
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

func TestRunsRepository_CreateScheduledSourceDefinitionRunIdempotentByScheduleTick(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()
	ctx := context.Background()

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "source-repo",
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/tmp/source-repo",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	jobID := "source-cron"
	scheduledFor := time.Date(2026, 3, 21, 12, 10, 0, 0, time.UTC)
	scheduleID := insertSourceCronTriggerSpec(t, ctx, db, jobID, "source-repo", "* * * * *", scheduledFor)
	sourceRec := dal.JobDefinitionSourceRecord{
		RepositoryID:   "source-repo",
		RequestedRef:   "main",
		ResolvedCommit: "abc123",
		DefinitionPath: ".vectis/jobs/source-cron.json",
		BlobSHA:        "blob-1",
	}

	runID1, idx1, version1, created, err := runs.CreateScheduledSourceDefinitionRun(ctx, scheduleID, scheduledFor, jobID, `{"id":"source-cron"}`, sourceRec, dal.RunAuditMetadata{})
	if err != nil {
		t.Fatalf("create scheduled source run: %v", err)
	}

	if !created || version1 != 1 || idx1 != 1 {
		t.Fatalf("first scheduled source run mismatch: run=%s idx=%d version=%d created=%t", runID1, idx1, version1, created)
	}

	runID2, idx2, version2, created, err := runs.CreateScheduledSourceDefinitionRun(ctx, scheduleID, scheduledFor, jobID, `{"id":"source-cron","version":2}`, sourceRec, dal.RunAuditMetadata{})
	if err != nil {
		t.Fatalf("create duplicate scheduled source run: %v", err)
	}

	if created || runID2 != runID1 || idx2 != idx1 || version2 != version1 {
		t.Fatalf("duplicate scheduled source run mismatch: run=%s idx=%d version=%d created=%t", runID2, idx2, version2, created)
	}

	nextScheduledFor := scheduledFor.Add(time.Minute)
	sourceRec.ResolvedCommit = "def456"
	sourceRec.BlobSHA = "blob-2"
	runID3, idx3, version3, created, err := runs.CreateScheduledSourceDefinitionRun(ctx, scheduleID, nextScheduledFor, jobID, `{"id":"source-cron","version":2}`, sourceRec, dal.RunAuditMetadata{})
	if err != nil {
		t.Fatalf("create next scheduled source run: %v", err)
	}

	if !created || runID3 == runID1 || idx3 != 2 || version3 != 2 {
		t.Fatalf("next scheduled source run mismatch: run=%s idx=%d version=%d created=%t", runID3, idx3, version3, created)
	}

	var sourceCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_definition_sources WHERE job_id = ?", jobID).Scan(&sourceCount); err != nil {
		t.Fatalf("count job definition sources: %v", err)
	}

	if sourceCount != 2 {
		t.Fatalf("expected two source provenance rows, got %d", sourceCount)
	}

	gotSource, err := repos.Sources().GetDefinitionSource(ctx, jobID, 2)
	if err != nil {
		t.Fatalf("get second definition source: %v", err)
	}

	if gotSource.RepositoryID != "source-repo" || gotSource.ResolvedCommit != "def456" || gotSource.BlobSHA != "blob-2" {
		t.Fatalf("second definition source mismatch: %+v", gotSource)
	}

	var fireCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cron_schedule_fires WHERE schedule_id = ?", scheduleID).Scan(&fireCount); err != nil {
		t.Fatalf("count source schedule fires: %v", err)
	}

	if fireCount != 2 {
		t.Fatalf("expected two cron_schedule_fires rows, got %d", fireCount)
	}
}

func TestRunsRepository_ExecutionClaimRenewAndDispatchQueries(t *testing.T) {
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

	dispatchable, found, err := runs.GetQueuedRunForDispatch(ctx, runID)
	if err != nil {
		t.Fatalf("get queued run for dispatch: %v", err)
	}
	if !found || dispatchable.RunID != runID {
		t.Fatalf("expected queued run %s for direct dispatch, found=%v record=%+v", runID, found, dispatchable)
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

	dispatch, claim := claimPendingRunExecution(t, ctx, runs, runID, "worker-1", time.Now().Add(1*time.Minute))
	claimToken := claim.ClaimToken

	afterClaim, err := runs.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run after claim: %v", err)
	}

	if afterClaim.StartedAt == nil {
		t.Fatal("started_at should be set by the execution claim")
	}

	cancelRec, err := runs.GetRunForCancel(ctx, runID)
	if err != nil {
		t.Fatalf("get run for cancel: %v", err)
	}
	if cancelRec.CancelToken != claimToken {
		t.Fatalf("cancel token should match execution claim token, got cancel=%q claim=%q", cancelRec.CancelToken, claimToken)
	}

	duplicateClaim, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "worker-2", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim execution second: %v", err)
	}

	if duplicateClaim.Claimed {
		t.Fatal("expected second execution claim to fail")
	}

	if err := runs.RenewExecutionLease(ctx, dispatch.ExecutionID, "worker-1", claimToken, time.Now().Add(2*time.Minute)); err != nil {
		t.Fatalf("renew execution lease for owner: %v", err)
	}

	if err := runs.RenewExecutionLease(ctx, dispatch.ExecutionID, "worker-2", claimToken, time.Now().Add(2*time.Minute)); err == nil {
		t.Fatal("expected renew execution lease by non-owner to fail")
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

	dispatchable, found, err = runs.GetQueuedRunForDispatch(ctx, runID)
	if err != nil {
		t.Fatalf("get queued run for dispatch after touch: %v", err)
	}

	if found {
		t.Fatalf("expected touched run to be ineligible for direct dispatch, got %+v", dispatchable)
	}

	if _, err := db.ExecContext(ctx, `UPDATE job_runs SET status = 'orphaned' WHERE run_id = ?`, runID); err != nil {
		t.Fatalf("force orphaned status: %v", err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE segment_executions SET lease_until = ? WHERE execution_id = ?`, time.Now().Add(-1*time.Minute).Unix(), dispatch.ExecutionID); err != nil {
		t.Fatalf("force execution lease expiry: %v", err)
	}

	reclaim, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "worker-1", time.Now().Add(3*time.Minute))
	if err != nil {
		t.Fatalf("reclaim execution after orphaned run: %v", err)
	}

	if reclaim.Claimed {
		t.Fatalf("orphaned run should not be reclaimed automatically: %+v", reclaim)
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status: %v", err)
	}

	if status != "orphaned" {
		t.Fatalf("expected status orphaned after rejected reclaim, got %q", status)
	}
}

func TestRunsRepository_ListQueuedBeforeDispatchCutoffLimit(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runIDs := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		runIndex := i + 1
		runID, _, err := runs.CreateRun(ctx, "job-queued-limit", &runIndex, 1)
		if err != nil {
			t.Fatalf("create run %d: %v", runIndex, err)
		}
		runIDs = append(runIDs, runID)
	}

	queued, err := runs.ListQueuedBeforeDispatchCutoffLimit(ctx, time.Now().Unix()+60, 2)
	if err != nil {
		t.Fatalf("list queued limit: %v", err)
	}

	if len(queued) != 2 {
		t.Fatalf("expected 2 limited queued runs, got %+v", queued)
	}

	if queued[0].RunID != runIDs[0] || queued[1].RunID != runIDs[1] {
		t.Fatalf("expected first two queued runs in insertion order, got %+v", queued)
	}
}

func TestRunsRepository_ListQueuedBeforeDispatchCutoffSkipsActiveHotStateOwner(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	activeRun, _, err := runs.CreateRun(ctx, "job-queued-hot-active", nil, 1)
	if err != nil {
		t.Fatalf("create active run: %v", err)
	}

	expiredRun, _, err := runs.CreateRun(ctx, "job-queued-hot-expired", nil, 1)
	if err != nil {
		t.Fatalf("create expired run: %v", err)
	}

	plainRun, _, err := runs.CreateRun(ctx, "job-queued-hot-plain", nil, 1)
	if err != nil {
		t.Fatalf("create plain run: %v", err)
	}

	now := time.Now().UTC()
	if err := runs.UpsertRunHotStateOwner(ctx, dal.RunHotStateOwnerUpdate{
		RunID:      activeRun,
		CellID:     "local",
		OwnerID:    "orchestrator:registry:local",
		OwnerEpoch: "epoch-active",
		LeaseUntil: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("upsert active hot-state owner: %v", err)
	}

	if err := runs.UpsertRunHotStateOwner(ctx, dal.RunHotStateOwnerUpdate{
		RunID:      expiredRun,
		CellID:     "local",
		OwnerID:    "orchestrator:registry:local",
		OwnerEpoch: "epoch-expired",
		LeaseUntil: now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("upsert expired hot-state owner: %v", err)
	}

	queued, err := runs.ListQueuedBeforeDispatchCutoff(ctx, now.Unix())
	if err != nil {
		t.Fatalf("list queued before dispatch cutoff: %v", err)
	}

	if len(queued) != 2 {
		t.Fatalf("expected two queued runs, got %+v", queued)
	}

	if queued[0].RunID != expiredRun || queued[1].RunID != plainRun {
		t.Fatalf("expected expired and plain queued runs, got %+v", queued)
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

	_, claim := claimPendingRunExecution(t, ctx, runs, runID, "worker-cancel", time.Now().Add(time.Minute))
	token := claim.ClaimToken

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
		t.Fatalf("cancel token should match execution claim token, got cancel=%q claim=%q", rec.CancelToken, token)
	}

	if rec.CancelRequestedAt == nil || *rec.CancelRequestedAt <= 0 {
		t.Fatalf("expected cancel_requested_at to be set, got %v", rec.CancelRequestedAt)
	}
	if rec.CancelReason != dal.CancelReasonAPI {

		t.Fatalf("expected cancel reason %q, got %q", dal.CancelReasonAPI, rec.CancelReason)
	}

	requested, err := runs.RunCancelRequested(ctx, runID)
	if err != nil {
		t.Fatalf("run cancel requested: %v", err)
	}

	if !requested {
		t.Fatal("expected run-level cancel request to be visible")
	}

	if err := runs.MarkRunAborted(ctx, runID, dal.CancelReasonAPI); err != nil {
		t.Fatalf("mark run aborted: %v", err)
	}

	requested, err = runs.RunCancelRequested(ctx, runID)
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

	if err := dispatch.RecordDispatchSuccess(ctx, runID, dal.DispatchSourceAPI); err != nil {
		t.Fatalf("record dispatch success: %v", err)
	}

	events, err := dispatch.ListByRun(ctx, runID)
	if err != nil {
		t.Fatalf("list dispatch events: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %+v", events)
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

	if events[2].Source != dal.DispatchSourceAPI || events[2].EventType != dal.DispatchEventSuccess || events[2].Message != nil {
		t.Fatalf("unexpected success event: %+v", events[2])
	}

	var lastDispatchedAt sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT last_dispatched_at FROM job_runs WHERE run_id = ?", runID).Scan(&lastDispatchedAt); err != nil {
		t.Fatalf("query last_dispatched_at: %v", err)
	}
	if !lastDispatchedAt.Valid || lastDispatchedAt.Int64 == 0 {
		t.Fatalf("expected dispatch success to touch run, got %+v", lastDispatchedAt)
	}

	if events[0].CreatedAt == 0 || events[1].CreatedAt == 0 || events[2].CreatedAt == 0 {
		t.Fatalf("expected created_at values: %+v", events)
	}
}

func TestDispatchEventsRepository_RecordDispatchAttemptOutcome(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()
	dispatch := repos.DispatchEvents()
	ctx := context.Background()

	successRunID, _, err := runs.CreateRun(ctx, "job-dispatch-success", nil, 1)
	if err != nil {
		t.Fatalf("create success run: %v", err)
	}

	if err := dispatch.RecordDispatchAttemptOutcome(ctx, successRunID, dal.DispatchSourceAPI, dal.DispatchEventSuccess, nil); err != nil {
		t.Fatalf("record success outcome: %v", err)
	}

	successEvents, err := dispatch.ListByRun(ctx, successRunID)
	if err != nil {
		t.Fatalf("list success dispatch events: %v", err)
	}

	if len(successEvents) != 2 {
		t.Fatalf("expected 2 success events, got %+v", successEvents)
	}

	if successEvents[0].EventType != dal.DispatchEventAttempt || successEvents[0].Message != nil {
		t.Fatalf("unexpected success attempt event: %+v", successEvents[0])
	}

	if successEvents[1].EventType != dal.DispatchEventSuccess || successEvents[1].Message != nil {
		t.Fatalf("unexpected success event: %+v", successEvents[1])
	}

	var lastDispatchedAt sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT last_dispatched_at FROM job_runs WHERE run_id = ?", successRunID).Scan(&lastDispatchedAt); err != nil {
		t.Fatalf("query success last_dispatched_at: %v", err)
	}

	if !lastDispatchedAt.Valid || lastDispatchedAt.Int64 == 0 {
		t.Fatalf("expected success outcome to touch run, got %+v", lastDispatchedAt)
	}

	failureRunID, _, err := runs.CreateRun(ctx, "job-dispatch-failure", nil, 1)
	if err != nil {
		t.Fatalf("create failure run: %v", err)
	}

	msg := "queue unavailable"
	if err := dispatch.RecordDispatchAttemptOutcome(ctx, failureRunID, dal.DispatchSourceAPI, dal.DispatchEventFailure, &msg); err != nil {
		t.Fatalf("record failure outcome: %v", err)
	}

	failureEvents, err := dispatch.ListByRun(ctx, failureRunID)
	if err != nil {
		t.Fatalf("list failure dispatch events: %v", err)
	}

	if len(failureEvents) != 2 {
		t.Fatalf("expected 2 failure events, got %+v", failureEvents)
	}

	if failureEvents[0].EventType != dal.DispatchEventAttempt || failureEvents[0].Message != nil {
		t.Fatalf("unexpected failure attempt event: %+v", failureEvents[0])
	}

	if failureEvents[1].EventType != dal.DispatchEventFailure || failureEvents[1].Message == nil || *failureEvents[1].Message != msg {
		t.Fatalf("unexpected failure event: %+v", failureEvents[1])
	}

	lastDispatchedAt = sql.NullInt64{}
	if err := db.QueryRowContext(ctx, "SELECT last_dispatched_at FROM job_runs WHERE run_id = ?", failureRunID).Scan(&lastDispatchedAt); err != nil {
		t.Fatalf("query failure last_dispatched_at: %v", err)
	}

	if lastDispatchedAt.Valid {
		t.Fatalf("expected failure outcome not to touch run, got %+v", lastDispatchedAt)
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

	runC, _, err := runs.CreateRun(ctx, "job-orphan-hot-owner", nil, 1)
	if err != nil {
		t.Fatalf("create run C: %v", err)
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

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'running', lease_owner = 'worker-c', lease_until = ?
		WHERE run_id = ?
	`, leaseExpired, runC); err != nil {
		t.Fatalf("seed run C running expired lease: %v", err)
	}

	if err := runs.UpsertRunHotStateOwner(ctx, dal.RunHotStateOwnerUpdate{
		RunID:      runC,
		CellID:     "local",
		OwnerID:    "orchestrator:registry:local",
		OwnerEpoch: "epoch-c",
		LeaseUntil: time.Unix(leaseFuture, 0).UTC(),
	}); err != nil {
		t.Fatalf("upsert run C hot-state owner: %v", err)
	}

	orphaned, err := runs.MarkExpiredRunningAsOrphaned(ctx, time.Now().Unix())
	if err != nil {
		t.Fatalf("MarkExpiredRunningAsOrphaned: %v", err)
	}

	if len(orphaned) != 1 || orphaned[0] != runA {
		t.Fatalf("expected only runA orphaned, got %+v", orphaned)
	}

	var statusA, statusB, statusC string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runA).Scan(&statusA); err != nil {
		t.Fatalf("scan run A status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runB).Scan(&statusB); err != nil {
		t.Fatalf("scan run B status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runC).Scan(&statusC); err != nil {
		t.Fatalf("scan run C status: %v", err)
	}

	if statusA != "orphaned" {
		t.Fatalf("expected run A orphaned, got %q", statusA)
	}
	if statusB != "running" {
		t.Fatalf("expected run B running, got %q", statusB)
	}
	if statusC != "running" {
		t.Fatalf("expected run C running while hot-state owner is active, got %q", statusC)
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

	if err := runs.MarkRunSucceeded(ctx, runID); err != nil {
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

func TestRunsRepository_ExpiredExecutionClaimRejectsRenewFinalizeAndTakeover(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-fencing", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	claimA, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "worker-a", time.Now().Add(-1*time.Minute))
	if err != nil {
		t.Fatalf("try claim execution worker-a: %v", err)
	}

	if !claimA.Claimed || claimA.ClaimToken == "" {
		t.Fatalf("expected worker-a execution claim and token, got %+v", claimA)
	}
	tokenA := claimA.ClaimToken

	claimB, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "worker-b", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim execution worker-b: %v", err)
	}

	if claimB.Claimed || claimB.ClaimToken != "" {
		t.Fatalf("expected expired execution claim takeover to be rejected, got %+v", claimB)
	}

	if err := runs.RenewExecutionLease(ctx, dispatch.ExecutionID, "worker-b", tokenA, time.Now().Add(2*time.Minute)); err == nil {
		t.Fatal("expected stale token execution renew to fail")
	}

	if _, err := runs.CompleteExecutionAndFinalizeRunByClaim(ctx, dispatch.ExecutionID, "worker-a", tokenA, dal.ExecutionStatusSucceeded, "", ""); err == nil {
		t.Fatal("expected stale token execution finalization to fail")
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status after stale finalize: %v", err)
	}

	if status != "running" {
		t.Fatalf("expected status to remain running until orphan sweep, got %q", status)
	}
}

func TestRunsRepository_ExpiredExecutionClaimRejectsFailedAbortedFinalizeAndTakeover(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-fencing-stale-fail", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	claimA, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "worker-a", time.Now().Add(-1*time.Minute))
	if err != nil {
		t.Fatalf("try claim execution worker-a: %v", err)
	}

	if !claimA.Claimed || claimA.ClaimToken == "" {
		t.Fatalf("expected worker-a execution claim and token, got %+v", claimA)
	}
	tokenA := claimA.ClaimToken

	claimB, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "worker-b", time.Now().Add(1*time.Minute))
	if err != nil {
		t.Fatalf("try claim execution worker-b: %v", err)
	}

	if claimB.Claimed || claimB.ClaimToken != "" {
		t.Fatalf("expected expired execution claim takeover to be rejected, got %+v", claimB)
	}

	if _, err := runs.CompleteExecutionAndFinalizeRunByClaim(ctx, dispatch.ExecutionID, "worker-a", tokenA, dal.ExecutionStatusFailed, dal.FailureCodeExecution, "stale token fail"); err == nil {
		t.Fatal("expected stale token failed execution finalization to fail")
	}

	if _, err := runs.CompleteExecutionAndFinalizeRunByClaim(ctx, dispatch.ExecutionID, "worker-a", tokenA, dal.ExecutionStatusAborted, "", dal.CancelReasonAPI); err == nil {
		t.Fatal("expected stale token aborted execution finalization to fail")
	}

	var status string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("scan status after stale transitions: %v", err)
	}

	if status != "running" {
		t.Fatalf("expected status running until orphan sweep, got %q", status)
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

	claimPendingRunExecution(t, ctx, runs, runID, "worker-a", time.Now().Add(time.Minute))

	if err := runs.MarkRunAborted(ctx, runID, dal.CancelReasonAPI); err != nil {
		t.Fatalf("mark run aborted: %v", err)
	}

	var status string
	var failureCode string
	var failure sql.NullString
	var finishedAt sql.NullString
	var cancelToken sql.NullString
	var cancelRequestedAt sql.NullInt64
	var cancelReason sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64

	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason, CAST(finished_at AS TEXT), cancel_token, cancel_requested_at, cancel_reason, lease_owner, lease_until
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&status, &failureCode, &failure, &finishedAt, &cancelToken, &cancelRequestedAt, &cancelReason, &leaseOwner, &leaseUntil); err != nil {
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

	if cancelToken.Valid || cancelRequestedAt.Valid || cancelReason.Valid || leaseOwner.Valid || leaseUntil.Valid {
		t.Fatalf("expected abort to clear runtime fields; got cancel=%v cancel_at=%v cancel_reason=%v owner=%v lease_until=%v",
			cancelToken, cancelRequestedAt, cancelReason, leaseOwner, leaseUntil)
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

func TestRunsRepository_RequeueRunForRetry_ClearsRuntimeFields(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-requeue-retry", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimedDispatch, _ := claimPendingRunExecution(t, ctx, runs, runID, "worker-a", time.Now().Add(time.Minute))

	if err := runs.MarkRunFailed(ctx, runID, dal.FailureCodeExecution, "test failure"); err != nil {
		t.Fatalf("mark run failed: %v", err)
	}

	if err := runs.RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatalf("RequeueRunForRetry: %v", err)
	}

	var status string
	var failureCode string
	var failure sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	var lastDispatched sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason, lease_owner, lease_until, last_dispatched_at
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&status, &failureCode, &failure, &leaseOwner, &leaseUntil, &lastDispatched); err != nil {
		t.Fatalf("query requeued run: %v", err)
	}

	if status != "queued" {
		t.Fatalf("expected queued status, got %q", status)
	}

	if failureCode != "" || failure.Valid || leaseOwner.Valid || leaseUntil.Valid || lastDispatched.Valid {
		t.Fatalf("expected queue retry to clear runtime fields; got failure_code=%q failure=%v owner=%v lease_until=%v dispatched=%v",
			failureCode, failure, leaseOwner, leaseUntil, lastDispatched)
	}

	retryDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("expected retry to restore a pending execution: %v", err)
	}
	if retryDispatch.ExecutionID == claimedDispatch.ExecutionID {
		t.Fatalf("expected retry to create a fresh execution attempt, got original execution %s", retryDispatch.ExecutionID)
	}
	if retryDispatch.Attempt != 2 {
		t.Fatalf("expected retry attempt 2, got %d", retryDispatch.Attempt)
	}
	if retryDispatch.StartDeadlineUnixNano != 0 {
		t.Fatalf("expected retry dispatch to clear stale start deadline, got %d", retryDispatch.StartDeadlineUnixNano)
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
	claimPendingRunExecution(t, ctx, runs, runningRunID, "worker-a", time.Now().Add(time.Minute))
	if err := runs.RepairMarkRunAbandoned(ctx, runningRunID, "worker deleted"); !dal.IsConflict(err) {
		t.Fatalf("expected running run conflict, got %v", err)
	}

	orphanRunID, _, err := runs.CreateRun(ctx, "job-repair-orphan", nil, 1)
	if err != nil {
		t.Fatalf("create orphan run: %v", err)
	}
	claimPendingRunExecution(t, ctx, runs, orphanRunID, "worker-a", time.Now().Add(time.Minute))
	if err := runs.MarkRunOrphaned(ctx, orphanRunID, dal.OrphanReasonLeaseExpired); err != nil {
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

	claimPendingRunExecution(t, ctx, runs, runID, "worker-a", time.Now().Add(time.Minute))

	err = runs.RequeueRunForRetry(ctx, runID)
	if !dal.IsConflict(err) {
		t.Fatalf("expected conflict requeueing running run, got %v", err)
	}
}

func TestRunsRepository_MarkRunOrphaned_ClearsExecutionOwner(t *testing.T) {
	db := dbtest.NewTestDB(t)
	runs := dal.NewSQLRepositories(db).Runs()
	ctx := context.Background()

	runID, _, err := runs.CreateRun(ctx, "job-mark-orphaned", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	claimPendingRunExecution(t, ctx, runs, runID, "worker-a", time.Now().Add(time.Minute))

	if err := runs.MarkRunOrphaned(ctx, runID, dal.OrphanReasonAckUncertain); err != nil {
		t.Fatalf("MarkRunOrphaned: %v", err)
	}

	var status string
	var reason sql.NullString
	var orphanReason sql.NullString
	var leaseOwner sql.NullString
	var leaseUntil sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_reason, orphan_reason, lease_owner, lease_until
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&status, &reason, &orphanReason, &leaseOwner, &leaseUntil); err != nil {
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

	if leaseOwner.Valid || leaseUntil.Valid {
		t.Fatalf("expected run owner cleared, got owner=%v lease=%v", leaseOwner, leaseUntil)
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
	def := `{"id":"ephemeral-job-id","root":{"uses":"builtins/script","with":{"script":"echo x"}}}`
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

func TestSQLRepositories_SourceRunDispatchUsesRepositoryNamespace(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	ns, err := repos.Namespaces().Create(ctx, "team-source-dispatch", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "source-repo",
		NamespaceID:  ns.ID,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/tmp/source-repo",
		DefaultRef:   "main",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	runID, _, _, err := repos.CreateSourceDefinitionAndRunInCellWithAudit(ctx, "source-job", `{"id":"source-job","root":{"uses":"builtins/script"}}`, dal.JobDefinitionSourceRecord{
		RepositoryID:   "source-repo",
		RequestedRef:   "main",
		ResolvedCommit: "abc123",
		DefinitionPath: ".vectis/jobs/source-job.json",
		BlobSHA:        "blob123",
	}, "", dal.RunAuditMetadata{})
	if err != nil {
		t.Fatalf("CreateSourceDefinitionAndRunInCellWithAudit: %v", err)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("GetPendingExecution: %v", err)
	}

	if dispatch.NamespacePath != "/team-source-dispatch" {
		t.Fatalf("pending dispatch namespace path: got %q, want /team-source-dispatch", dispatch.NamespacePath)
	}

	byExecutionID, err := repos.Runs().GetExecutionDispatch(ctx, dispatch.ExecutionID)
	if err != nil {
		t.Fatalf("GetExecutionDispatch: %v", err)
	}

	if byExecutionID.NamespacePath != "/team-source-dispatch" {
		t.Fatalf("execution dispatch namespace path: got %q, want /team-source-dispatch", byExecutionID.NamespacePath)
	}

	claimed, _ := claimPendingRunExecution(t, ctx, repos.Runs(), runID, "worker-a", time.Now().Add(time.Minute))
	active, err := repos.Runs().GetActiveExecutionDispatch(ctx, runID, claimed.ExecutionID)
	if err != nil {
		t.Fatalf("GetActiveExecutionDispatch: %v", err)
	}

	if active.NamespacePath != "/team-source-dispatch" {
		t.Fatalf("active dispatch namespace path: got %q, want /team-source-dispatch", active.NamespacePath)
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

	if err := idempotency.AttachResource(ctx, "scope-a", "key-a", "trigger_invocation", "invocation-a"); err != nil {
		t.Fatalf("attach resource: %v", err)
	}

	rec, created, err = idempotency.Reserve(ctx, "scope-a", "key-a", "hash-a")
	if err != nil {
		t.Fatalf("reserve with resource: %v", err)
	}
	if created {
		t.Fatal("expected resource reserve to read existing record")
	}
	if rec.ResourceType != "trigger_invocation" || rec.ResourceID != "invocation-a" {
		t.Fatalf("expected attached resource, got type=%q id=%q", rec.ResourceType, rec.ResourceID)
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
	if rec.ResourceType != "trigger_invocation" || rec.ResourceID != "invocation-a" {
		t.Fatalf("expected resource to survive completion, got type=%q id=%q", rec.ResourceType, rec.ResourceID)
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

	if err := jobs.CreateDefinitionSnapshot(ctx, "cron-job", `{"id":"cron-job"}`); err != nil {
		t.Fatalf("create definition snapshot: %v", err)
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

func TestSchedulesRepository_CronScheduleSummary(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	jobs := repos.Jobs()
	schedules := repos.Schedules()
	ctx := context.Background()

	if err := jobs.CreateDefinitionSnapshot(ctx, "cron-summary-job", `{"id":"cron-summary-job"}`); err != nil {
		t.Fatalf("create definition snapshot: %v", err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	activeClaimDue := now.Add(-10 * time.Minute)
	activeClaimID := insertCronTriggerSpec(t, ctx, db, "cron-summary-job", "* * * * *", activeClaimDue)
	expiredClaimID := insertCronTriggerSpec(t, ctx, db, "cron-summary-job", "*/2 * * * *", now.Add(-5*time.Minute))
	insertCronTriggerSpec(t, ctx, db, "cron-summary-job", "*/3 * * * *", now.Add(-3*time.Minute))
	insertCronTriggerSpec(t, ctx, db, "cron-summary-job", "*/5 * * * *", now.Add(5*time.Minute))
	disabledID := insertCronTriggerSpec(t, ctx, db, "cron-summary-job", "*/7 * * * *", now.Add(-20*time.Minute))

	claimCronTriggerSpec(t, ctx, db, activeClaimID, "active-claim", now.Add(5*time.Minute))
	claimCronTriggerSpec(t, ctx, db, expiredClaimID, "expired-claim", now.Add(-time.Minute))
	if _, err := db.ExecContext(ctx, "UPDATE job_triggers SET enabled = false WHERE id = (SELECT trigger_id FROM cron_trigger_specs WHERE id = ?)", disabledID); err != nil {
		t.Fatalf("disable cron trigger: %v", err)
	}

	summary, err := schedules.CronScheduleSummary(ctx, now)
	if err != nil {
		t.Fatalf("cron schedule summary: %v", err)
	}

	if summary.ScheduleCount != 4 || summary.DueCount != 2 || summary.ClaimedCount != 1 {
		t.Fatalf("unexpected cron schedule summary: %+v", summary)
	}
	if summary.OldestDueAt == nil || !summary.OldestDueAt.Equal(activeClaimDue) {
		t.Fatalf("oldest due: got %+v, want %s", summary.OldestDueAt, activeClaimDue.Format(time.RFC3339))
	}
}

func claimCronTriggerSpec(t *testing.T, ctx context.Context, db *sql.DB, specID int64, claimToken string, claimedUntil time.Time) {
	t.Helper()

	if _, err := db.ExecContext(ctx, "UPDATE cron_trigger_specs SET claim_token = ?, claimed_until = ? WHERE id = ?", claimToken, claimedUntil.Format(time.RFC3339), specID); err != nil {
		t.Fatalf("claim cron trigger spec: %v", err)
	}
}

func TestSchedulesRepository_ListSourceCronSchedules(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	childNS, err := repos.Namespaces().Create(ctx, "team-a", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "repo-a",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/repo-a",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create repo-a: %v", err)
	}
	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "repo-b",
		NamespaceID:  1,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/repo-b",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create repo-b: %v", err)
	}
	if _, err := repos.Sources().CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID: "repo-c",
		NamespaceID:  childNS.ID,
		SourceKind:   dal.SourceKindLocalCheckout,
		CheckoutPath: "/work/repo-c",
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create repo-c: %v", err)
	}

	nextRun := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "nightly-a",
		JobID:              "build",
		CronSpec:           "0 * * * *",
		NextRunAt:          nextRun,
		SourceRepositoryID: "repo-a",
		SourceRef:          "main",
		Enabled:            true,
	}); err != nil {
		t.Fatalf("create source schedule repo-a: %v", err)
	}
	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "nightly-b",
		JobID:              "deploy",
		CronSpec:           "30 * * * *",
		NextRunAt:          nextRun,
		SourceRepositoryID: "repo-b",
		SourcePath:         ".vectis/jobs/deploy.json",
		Enabled:            false,
	}); err != nil {
		t.Fatalf("create source schedule repo-b: %v", err)
	}
	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "nightly-c",
		JobID:              "test",
		CronSpec:           "15 * * * *",
		NextRunAt:          nextRun,
		SourceRepositoryID: "repo-c",
		Enabled:            true,
	}); err != nil {
		t.Fatalf("create source schedule repo-c: %v", err)
	}
	insertCronTriggerSpec(t, ctx, db, "non-source-schedule", "* * * * *", nextRun)

	rootSchedules, err := repos.Schedules().ListSourceCronSchedules(ctx, 1, "")
	if err != nil {
		t.Fatalf("list root source schedules: %v", err)
	}
	if len(rootSchedules) != 2 {
		t.Fatalf("root source schedules len=%d, want 2: %+v", len(rootSchedules), rootSchedules)
	}
	if rootSchedules[0].ScheduleID != "nightly-a" || rootSchedules[1].ScheduleID != "nightly-b" {
		t.Fatalf("root schedules order/content mismatch: %+v", rootSchedules)
	}
	if rootSchedules[1].Enabled {
		t.Fatalf("expected disabled schedule to be listed with enabled=false: %+v", rootSchedules[1])
	}

	overridden, err := repos.Schedules().SetSourceCronScheduleOverride(ctx, "nightly-a", dal.SourceScheduleOverride{
		Ref:           "hotfix/build",
		Path:          ".vectis/jobs/build-hotfix.json",
		Reason:        "verify hotfix",
		CreatedAtUnix: 1770000000,
	})

	if err != nil {
		t.Fatalf("set source schedule override: %v", err)
	}

	if overridden.SourceOverrideRef != "hotfix/build" ||
		overridden.SourceOverridePath != ".vectis/jobs/build-hotfix.json" ||
		overridden.SourceOverrideReason != "verify hotfix" ||
		overridden.SourceOverrideCreatedAtUnix != 1770000000 {
		t.Fatalf("override mismatch: %+v", overridden)
	}

	updated, err := repos.Schedules().UpdateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "nightly-a",
		JobID:              "build",
		CronSpec:           "15 * * * *",
		SourceRepositoryID: "repo-a",
		SourceRef:          "main",
		Enabled:            true,
	})

	if err != nil {
		t.Fatalf("update source schedule with active override: %v", err)
	}

	if updated.SourceOverrideRef != "hotfix/build" || updated.SourceOverridePath != ".vectis/jobs/build-hotfix.json" {
		t.Fatalf("expected update to preserve source schedule override: %+v", updated)
	}

	counts, err := repos.Schedules().CountSourceCronSchedules(ctx, []string{"nightly-a", "nightly-c", "nightly-a", " ", "nightly-b' OR 1=1 --"})
	if err != nil {
		t.Fatalf("count source cron schedules: %v", err)
	}

	if counts.Total != 3 ||
		counts.Enabled != 2 ||
		counts.Disabled != 1 ||
		counts.Declared != 2 ||
		counts.StaleEnabled != 0 ||
		counts.StaleDisabled != 1 ||
		counts.ActiveOverrides != 1 {
		t.Fatalf("unexpected source schedule counts: %+v", counts)
	}

	cleared, err := repos.Schedules().ClearSourceCronScheduleOverride(ctx, "nightly-a")
	if err != nil {
		t.Fatalf("clear source schedule override: %v", err)
	}

	if cleared.SourceOverrideRef != "" ||
		cleared.SourceOverridePath != "" ||
		cleared.SourceOverrideReason != "" ||
		cleared.SourceOverrideCreatedAtUnix != 0 {
		t.Fatalf("expected override to be cleared: %+v", cleared)
	}

	repoSchedules, err := repos.Schedules().ListSourceCronSchedules(ctx, 1, "repo-a")
	if err != nil {
		t.Fatalf("list repo-a source schedules: %v", err)
	}
	if len(repoSchedules) != 1 || repoSchedules[0].SourceRepositoryID != "repo-a" {
		t.Fatalf("repo-a schedules mismatch: %+v", repoSchedules)
	}

	childSchedules, err := repos.Schedules().ListSourceCronSchedules(ctx, childNS.ID, "")
	if err != nil {
		t.Fatalf("list child source schedules: %v", err)
	}
	if len(childSchedules) != 1 || childSchedules[0].SourceRepositoryID != "repo-c" {
		t.Fatalf("child schedules mismatch: %+v", childSchedules)
	}
}

func TestSchedulesRepository_DeleteSourceCronSchedule(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	ctx := context.Background()

	nextRun := time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC)
	sourceRec, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID:         "stale-source",
		JobID:              "build",
		CronSpec:           "0 * * * *",
		NextRunAt:          nextRun,
		SourceRepositoryID: "repo-a",
		SourceRef:          "main",
		Enabled:            false,
	})

	if err != nil {
		t.Fatalf("create source schedule: %v", err)
	}

	if _, err := repos.Schedules().CreateCronSchedule(ctx, dal.CronScheduleRecord{
		ScheduleID: "non-source-schedule",
		JobID:      "non-source",
		CronSpec:   "30 * * * *",
		NextRunAt:  nextRun,
		Enabled:    true,
	}); err != nil {
		t.Fatalf("create non-source delete guard fixture: %v", err)
	}

	if err := repos.Schedules().DeleteSourceCronSchedule(ctx, "stale-source"); err != nil {
		t.Fatalf("delete source schedule: %v", err)
	}

	if _, err := repos.Schedules().GetCronScheduleByScheduleID(ctx, "stale-source"); !dal.IsNotFound(err) {
		t.Fatalf("deleted source schedule lookup should be not found, got %v", err)
	}

	var triggerRows int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_triggers WHERE id = ?", sourceRec.TriggerID).Scan(&triggerRows); err != nil {
		t.Fatalf("count source trigger rows: %v", err)
	}

	if triggerRows != 0 {
		t.Fatalf("source trigger rows=%d, want 0", triggerRows)
	}

	if _, err := repos.Schedules().GetCronScheduleByScheduleID(ctx, "non-source-schedule"); err != nil {
		t.Fatalf("non-source delete guard fixture should remain: %v", err)
	}

	if err := repos.Schedules().DeleteSourceCronSchedule(ctx, "non-source-schedule"); !dal.IsNotFound(err) {
		t.Fatalf("deleting non-source delete guard fixture through source delete should be not found, got %v", err)
	}

	if err := repos.Schedules().DeleteSourceCronSchedule(ctx, "missing"); !dal.IsNotFound(err) {
		t.Fatalf("deleting missing source schedule should be not found, got %v", err)
	}
}

func TestSchedulesRepository_ClaimDueCompleteAndRelease(t *testing.T) {
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositories(db)
	jobs := repos.Jobs()
	schedules := repos.Schedules()
	ctx := context.Background()

	if err := jobs.CreateDefinitionSnapshot(ctx, "cron-job", `{"id":"cron-job"}`); err != nil {
		t.Fatalf("create definition snapshot: %v", err)
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
