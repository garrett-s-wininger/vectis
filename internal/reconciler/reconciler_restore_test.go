package reconciler

import (
	"context"
	"os"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	queuepkg "vectis/internal/queue"
	"vectis/internal/testutil/dbtest"
)

func TestService_Process_RestoreSkewRepopulatesLostQueueFromFrozenSQLPayload(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	jobID := "job-restore-skew"
	runID := seedStoredJobAndRun(t, db, jobID)

	queueDir := t.TempDir()
	clock := mocks.NewMockClock()
	now := time.Now().UTC()
	clock.SetNow(now)

	repos := dal.NewSQLRepositories(db)
	firstQueue := newPersistedQueueForRestoreTest(t, queueDir)
	firstSvc := NewServiceWithRepositories(interfaces.NewLogger("test"), repos.Jobs(), repos.Runs(), firstQueue, clock)
	firstSvc.SetServiceLeases(nil)
	firstSvc.SetMinDispatchGap(time.Second)

	if err := firstSvc.Process(ctx); err != nil {
		t.Fatalf("first Process: %v", err)
	}

	firstPayload, err := repos.Runs().GetExecutionPayloadForRun(ctx, runID)
	if err != nil {
		t.Fatalf("get first execution payload: %v", err)
	}

	firstEnvelope := executionEnvelopeMetadataForRestoreTest(t, firstPayload.PayloadJSON)
	if firstEnvelope == "" {
		t.Fatal("expected first dispatch payload to include execution envelope metadata")
	}

	pending, inflight, dlq := queuepkg.MetricsSnapshot(firstQueue)
	if pending != 1 || inflight != 0 || dlq != 0 {
		t.Fatalf("expected first queue to hold one pending job, got pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	closeQueueServiceForRestoreTest(t, firstQueue)
	if err := os.RemoveAll(queueDir); err != nil {
		t.Fatalf("remove queue persistence dir: %v", err)
	}

	restoredQueue := newPersistedQueueForRestoreTest(t, queueDir)
	defer closeQueueServiceForRestoreTest(t, restoredQueue)

	pending, inflight, dlq = queuepkg.MetricsSnapshot(restoredQueue)
	if pending != 0 || inflight != 0 || dlq != 0 {
		t.Fatalf("expected restored queue to start empty, got pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	clock.SetNow(now.Add(2 * time.Second))
	restoredSvc := NewServiceWithRepositories(interfaces.NewLogger("test"), repos.Jobs(), repos.Runs(), restoredQueue, clock)
	restoredSvc.SetServiceLeases(nil)
	restoredSvc.SetMinDispatchGap(time.Second)

	if err := restoredSvc.Process(ctx); err != nil {
		t.Fatalf("second Process after queue state loss: %v", err)
	}

	pending, inflight, dlq = queuepkg.MetricsSnapshot(restoredQueue)
	if pending != 1 || inflight != 0 || dlq != 0 {
		t.Fatalf("expected reconciler to repopulate one pending job, got pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	got, err := restoredQueue.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("try dequeue redispatched job: %v", err)
	}

	if got == nil {
		t.Fatal("expected redispatched job, got nil")
	}

	if got.GetJob().GetId() != jobID || got.GetJob().GetRunId() != runID {
		t.Fatalf("redispatched job identity mismatch: job=%q run=%q", got.GetJob().GetId(), got.GetJob().GetRunId())
	}

	secondEnvelope := got.GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if secondEnvelope != firstEnvelope {
		t.Fatalf("redispatched envelope changed after queue restore skew:\nfirst:  %s\nsecond: %s", firstEnvelope, secondEnvelope)
	}

	secondPayload, err := repos.Runs().GetExecutionPayloadForRun(ctx, runID)
	if err != nil {
		t.Fatalf("get second execution payload: %v", err)
	}

	if secondPayload.PayloadHash != firstPayload.PayloadHash || secondPayload.PayloadJSON != firstPayload.PayloadJSON {
		t.Fatalf("execution payload changed after redispatch:\nfirst hash:  %s\nsecond hash: %s", firstPayload.PayloadHash, secondPayload.PayloadHash)
	}

	var runCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_runs WHERE job_id = ?", jobID).Scan(&runCount); err != nil {
		t.Fatalf("count job runs: %v", err)
	}

	if runCount != 1 {
		t.Fatalf("restore skew should not create another run, got %d runs for %s", runCount, jobID)
	}
}

func TestService_Process_RestoreSkewRequeuesLostInflightDeliveryFromFrozenSQLPayload(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	jobID := "job-restore-skew-inflight"
	runID := seedStoredJobAndRun(t, db, jobID)

	queueDir := t.TempDir()
	clock := mocks.NewMockClock()
	now := time.Now().UTC()
	clock.SetNow(now)

	repos := dal.NewSQLRepositories(db)
	firstQueue := newPersistedQueueForRestoreTest(t, queueDir)
	firstSvc := NewServiceWithRepositories(interfaces.NewLogger("test"), repos.Jobs(), repos.Runs(), firstQueue, clock)
	firstSvc.SetServiceLeases(nil)
	firstSvc.SetMinDispatchGap(time.Second)

	if err := firstSvc.Process(ctx); err != nil {
		t.Fatalf("first Process: %v", err)
	}

	delivered, err := firstQueue.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("dequeue first delivery: %v", err)
	}

	if delivered == nil {
		t.Fatal("expected first delivery, got nil")
	}

	if delivered.GetJob().GetId() != jobID || delivered.GetJob().GetRunId() != runID {
		t.Fatalf("first delivery identity mismatch: job=%q run=%q", delivered.GetJob().GetId(), delivered.GetJob().GetRunId())
	}

	firstPayload, err := repos.Runs().GetExecutionPayloadForRun(ctx, runID)
	if err != nil {
		t.Fatalf("get first execution payload: %v", err)
	}

	firstEnvelope := delivered.GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if firstEnvelope == "" {
		t.Fatal("expected first delivery to include execution envelope metadata")
	}

	if payloadEnvelope := executionEnvelopeMetadataForRestoreTest(t, firstPayload.PayloadJSON); payloadEnvelope != firstEnvelope {
		t.Fatalf("delivered envelope differs from frozen payload:\npayload:  %s\ndelivery: %s", payloadEnvelope, firstEnvelope)
	}

	pending, inflight, dlq := queuepkg.MetricsSnapshot(firstQueue)
	if pending != 0 || inflight != 1 || dlq != 0 {
		t.Fatalf("expected first queue to hold one in-flight delivery, got pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	closeQueueServiceForRestoreTest(t, firstQueue)
	if err := os.RemoveAll(queueDir); err != nil {
		t.Fatalf("remove queue persistence dir: %v", err)
	}

	restoredQueue := newPersistedQueueForRestoreTest(t, queueDir)
	defer closeQueueServiceForRestoreTest(t, restoredQueue)

	pending, inflight, dlq = queuepkg.MetricsSnapshot(restoredQueue)
	if pending != 0 || inflight != 0 || dlq != 0 {
		t.Fatalf("expected restored queue to start empty, got pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	clock.SetNow(now.Add(2 * time.Second))
	restoredSvc := NewServiceWithRepositories(interfaces.NewLogger("test"), repos.Jobs(), repos.Runs(), restoredQueue, clock)
	restoredSvc.SetServiceLeases(nil)
	restoredSvc.SetMinDispatchGap(time.Second)

	if err := restoredSvc.Process(ctx); err != nil {
		t.Fatalf("second Process after in-flight queue state loss: %v", err)
	}

	pending, inflight, dlq = queuepkg.MetricsSnapshot(restoredQueue)
	if pending != 1 || inflight != 0 || dlq != 0 {
		t.Fatalf("expected reconciler to repopulate one pending job, got pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	redispatched, err := restoredQueue.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("dequeue redispatched delivery: %v", err)
	}

	if redispatched == nil {
		t.Fatal("expected redispatched delivery, got nil")
	}

	if redispatched.GetJob().GetId() != jobID || redispatched.GetJob().GetRunId() != runID {
		t.Fatalf("redispatched identity mismatch: job=%q run=%q", redispatched.GetJob().GetId(), redispatched.GetJob().GetRunId())
	}

	secondEnvelope := redispatched.GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
	if secondEnvelope != firstEnvelope {
		t.Fatalf("redispatched envelope changed after lost in-flight delivery:\nfirst:  %s\nsecond: %s", firstEnvelope, secondEnvelope)
	}

	secondPayload, err := repos.Runs().GetExecutionPayloadForRun(ctx, runID)
	if err != nil {
		t.Fatalf("get second execution payload: %v", err)
	}

	if secondPayload.PayloadHash != firstPayload.PayloadHash || secondPayload.PayloadJSON != firstPayload.PayloadJSON {
		t.Fatalf("execution payload changed after in-flight redispatch:\nfirst hash:  %s\nsecond hash: %s", firstPayload.PayloadHash, secondPayload.PayloadHash)
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution after redispatch: %v", err)
	}

	claim1, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-a", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim redispatched execution: %v", err)
	}

	if !claim1.Claimed || claim1.ClaimToken == "" {
		t.Fatalf("expected redispatched execution to be claimable, claim=%+v", claim1)
	}

	claim2, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-b", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim duplicate redispatched execution: %v", err)
	}

	if claim2.Claimed || claim2.ClaimToken != "" {
		t.Fatalf("expected duplicate redispatched claim to lose, claim=%+v", claim2)
	}

	var runCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM job_runs WHERE job_id = ?", jobID).Scan(&runCount); err != nil {
		t.Fatalf("count job runs: %v", err)
	}

	if runCount != 1 {
		t.Fatalf("lost in-flight delivery should not create another run, got %d runs for %s", runCount, jobID)
	}
}

func TestService_Process_RestoreSkewActiveDurableClaimSuppressesRedispatchUntilReclaim(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	jobID := "job-restore-skew-claimed"
	runID := seedStoredJobAndRun(t, db, jobID)

	queueDir := t.TempDir()
	clock := mocks.NewMockClock()
	now := time.Now().UTC()
	clock.SetNow(now)

	repos := dal.NewSQLRepositories(db)
	firstQueue := newPersistedQueueForRestoreTest(t, queueDir)
	firstSvc := NewServiceWithRepositories(interfaces.NewLogger("test"), repos.Jobs(), repos.Runs(), firstQueue, clock)
	firstSvc.SetServiceLeases(nil)
	firstSvc.SetMinDispatchGap(time.Second)

	if err := firstSvc.Process(ctx); err != nil {
		t.Fatalf("first Process: %v", err)
	}

	delivered, err := firstQueue.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		t.Fatalf("dequeue first delivery: %v", err)
	}

	if delivered == nil {
		t.Fatal("expected first delivery, got nil")
	}

	if delivered.GetJob().GetId() != jobID || delivered.GetJob().GetRunId() != runID {
		t.Fatalf("first delivery identity mismatch: job=%q run=%q", delivered.GetJob().GetId(), delivered.GetJob().GetRunId())
	}

	dispatch, err := repos.Runs().GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	firstClaimUntil := time.Now().Add(time.Minute)
	firstClaim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-a", firstClaimUntil)
	if err != nil {
		t.Fatalf("claim first delivery: %v", err)
	}

	if !firstClaim.Claimed || firstClaim.ClaimToken == "" {
		t.Fatalf("expected first delivery to claim execution, claim=%+v", firstClaim)
	}

	closeQueueServiceForRestoreTest(t, firstQueue)
	if err := os.RemoveAll(queueDir); err != nil {
		t.Fatalf("remove queue persistence dir: %v", err)
	}

	restoredQueue := newPersistedQueueForRestoreTest(t, queueDir)
	defer closeQueueServiceForRestoreTest(t, restoredQueue)

	clock.SetNow(now.Add(2 * time.Second))
	restoredSvc := NewServiceWithRepositories(interfaces.NewLogger("test"), repos.Jobs(), repos.Runs(), restoredQueue, clock)
	restoredSvc.SetServiceLeases(nil)
	restoredSvc.SetMinDispatchGap(time.Second)

	if err := restoredSvc.Process(ctx); err != nil {
		t.Fatalf("Process with active durable claim after queue state loss: %v", err)
	}

	pending, inflight, dlq := queuepkg.MetricsSnapshot(restoredQueue)
	if pending != 0 || inflight != 0 || dlq != 0 {
		t.Fatalf("active durable claim should suppress redispatch, got pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	duplicateClaim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-b", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("duplicate claim with active owner: %v", err)
	}

	if duplicateClaim.Claimed || duplicateClaim.ClaimToken != "" {
		t.Fatalf("active durable claim should fence duplicate owner, claim=%+v", duplicateClaim)
	}

	expiredLease := now.Add(-time.Minute).Unix()
	if _, err := db.ExecContext(ctx, "UPDATE segment_executions SET lease_until = ? WHERE execution_id = ?", expiredLease, dispatch.ExecutionID); err != nil {
		t.Fatalf("expire execution lease: %v", err)
	}

	if _, err := db.ExecContext(ctx, "UPDATE job_runs SET lease_until = ? WHERE run_id = ?", expiredLease, runID); err != nil {
		t.Fatalf("expire run lease: %v", err)
	}

	clock.SetNow(now.Add(4 * time.Second))
	if err := restoredSvc.Process(ctx); err != nil {
		t.Fatalf("Process after durable claim expiry: %v", err)
	}

	pending, inflight, dlq = queuepkg.MetricsSnapshot(restoredQueue)
	if pending != 0 || inflight != 0 || dlq != 0 {
		t.Fatalf("expired durable claim should orphan without queue redispatch, got pending=%d inflight=%d dlq=%d", pending, inflight, dlq)
	}

	var status string
	if err := db.QueryRowContext(ctx, "SELECT status FROM job_runs WHERE run_id = ?", runID).Scan(&status); err != nil {
		t.Fatalf("query run status after lease expiry: %v", err)
	}

	if status != dal.RunStatusOrphaned {
		t.Fatalf("expired durable claim should orphan run, got %q", status)
	}

	replacementClaim, err := repos.Runs().TryClaimExecution(ctx, dispatch.ExecutionID, "worker-b", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("replacement claim after orphaning: %v", err)
	}

	if !replacementClaim.Claimed || replacementClaim.ClaimToken == "" || replacementClaim.ClaimToken == firstClaim.ClaimToken {
		t.Fatalf("expected replacement claim with new token, first=%+v replacement=%+v", firstClaim, replacementClaim)
	}

	if err := repos.Runs().RenewExecutionLease(ctx, dispatch.ExecutionID, "worker-a", firstClaim.ClaimToken, time.Now().Add(time.Minute)); !dal.IsConflict(err) {
		t.Fatalf("expected stale owner renew conflict, got %v", err)
	}

	if _, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, dispatch.ExecutionID, "worker-a", firstClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", ""); !dal.IsConflict(err) {
		t.Fatalf("expected stale owner completion conflict, got %v", err)
	}

	result, err := repos.Runs().CompleteExecutionAndFinalizeRunByClaim(ctx, dispatch.ExecutionID, "worker-b", replacementClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	if err != nil {
		t.Fatalf("complete replacement claim: %v", err)
	}

	if result.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		t.Fatalf("replacement finalization outcome: %+v", result)
	}
}

func newPersistedQueueForRestoreTest(t *testing.T, dir string) api.QueueServiceServer {
	t.Helper()

	svc, err := queuepkg.NewQueueServiceWithOptions(mocks.NopLogger{}, queuepkg.QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  2,
	}, nil)
	if err != nil {
		t.Fatalf("create persisted queue: %v", err)
	}

	return svc
}

func closeQueueServiceForRestoreTest(t *testing.T, svc api.QueueServiceServer) {
	t.Helper()

	closer, ok := svc.(interface{ Close() error })
	if !ok {
		return
	}

	if err := closer.Close(); err != nil {
		t.Fatalf("close queue service: %v", err)
	}
}

func executionEnvelopeMetadataForRestoreTest(t *testing.T, payloadJSON string) string {
	t.Helper()

	var req api.JobRequest
	if err := protojson.Unmarshal([]byte(payloadJSON), &req); err != nil {
		t.Fatalf("unmarshal execution payload: %v", err)
	}

	return req.GetMetadata()[cell.ExecutionEnvelopeMetadataKey]
}
