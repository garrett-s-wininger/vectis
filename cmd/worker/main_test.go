package main

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/testutil/dbtest"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLeaseRenewalLoop_ReclaimsOrphanedRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-reclaim", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-1"
	claimToken := "claim-test-1"
	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'orphaned', lease_owner = ?, claim_token = ?, lease_until = ?
		WHERE run_id = ?
	`, workerID, claimToken, time.Now().Add(-1*time.Minute).Unix(), runID); err != nil {
		t.Fatalf("seed orphaned run: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		store:         runs,
		renewInterval: 5 * time.Millisecond,
	}

	execCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopRenew := make(chan struct{})
	doneRenew := make(chan struct{})
	go w.leaseRenewalLoop(execCtx, runID, claimToken, stopRenew, doneRenew)

	time.Sleep(30 * time.Millisecond)
	close(stopRenew)
	<-doneRenew

	var status string
	var leaseUntil int64
	if err := db.QueryRowContext(ctx, `SELECT status, lease_until FROM job_runs WHERE run_id = ?`, runID).Scan(&status, &leaseUntil); err != nil {
		t.Fatalf("query run state: %v", err)
	}

	if status != "running" {
		t.Fatalf("expected run status running after renew, got %q", status)
	}

	if leaseUntil <= time.Now().Unix() {
		t.Fatalf("expected lease_until to be renewed into the future, got %d", leaseUntil)
	}
}

func TestWorkerRunClaimedJob_CompletesWhileOrphaned_MarksSucceeded(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-finish-orphaned", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-2"
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         runs,
	}

	jobID := "job-worker-finish-orphaned"
	deliveryID := "delivery-orphaned-finish"
	commandNodeID := "node-1"
	command := "sleep 0.08"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	done := make(chan struct{})
	go func() {
		w.runClaimedJob(j, jobID, runID, deliveryID)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		var status string
		if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
			t.Fatalf("query run status: %v", err)
		}

		if status == "running" {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for run to reach running, last status=%q", status)
		}

		time.Sleep(5 * time.Millisecond)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE job_runs
		SET status = 'orphaned', lease_owner = ?, lease_until = ?
		WHERE run_id = ?
	`, workerID, time.Now().Add(-1*time.Minute).Unix(), runID); err != nil {
		t.Fatalf("mark run orphaned during execution: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for worker runClaimedJob")
	}

	var status string
	var leaseOwner any
	var leaseUntil any
	if err := db.QueryRowContext(ctx, `
		SELECT status, lease_owner, lease_until
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&status, &leaseOwner, &leaseUntil); err != nil {
		t.Fatalf("query final run state: %v", err)
	}

	if status != "succeeded" {
		t.Fatalf("expected run to succeed after orphaned mid-flight, got %q", status)
	}

	if leaseOwner != nil || leaseUntil != nil {
		t.Fatalf("expected lease fields cleared on success, got lease_owner=%v lease_until=%v", leaseOwner, leaseUntil)
	}
}

type scriptedAckQueue struct {
	ackErrors []error
	ackCalls  int
}

func (q *scriptedAckQueue) Enqueue(context.Context, *api.Job) error {
	return errors.New("not implemented")
}
func (q *scriptedAckQueue) Dequeue(context.Context) (*api.Job, error) {
	return nil, errors.New("not implemented")
}
func (q *scriptedAckQueue) TryDequeue(context.Context) (*api.Job, error) {
	return nil, errors.New("not implemented")
}
func (q *scriptedAckQueue) Close() error { return nil }
func (q *scriptedAckQueue) Ack(context.Context, string) error {
	err := error(nil)
	if q.ackCalls < len(q.ackErrors) {
		err = q.ackErrors[q.ackCalls]
	}
	q.ackCalls++
	return err
}

func TestWorkerRunClaimedJob_AckTransientThenSuccess_Completes(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-ack-retry", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-ack-retry"
	clock := mocks.NewMockClock()
	queue := &scriptedAckQueue{
		ackErrors: []error{
			status.Error(codes.Unavailable, "queue temporarily unavailable"),
			status.Error(codes.Unavailable, "queue temporarily unavailable"),
			nil,
		},
	}

	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         runs,
	}

	jobID := "job-worker-ack-retry"
	deliveryID := "delivery-ack-retry"
	commandNodeID := "node-1"
	command := "echo ack-retry"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	w.runClaimedJob(j, jobID, runID, deliveryID)

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query final status: %v", err)
	}

	if statusVal != "succeeded" {
		t.Fatalf("expected succeeded after ack retries recover, got %q", statusVal)
	}

	if queue.ackCalls != 3 {
		t.Fatalf("expected 3 ack attempts, got %d", queue.ackCalls)
	}

	sleeps := clock.GetSleeps()
	if len(sleeps) != 2 {
		t.Fatalf("expected 2 backoff sleeps for transient ack errors, got %d", len(sleeps))
	}
}

func TestWorkerRunClaimedJob_AckPersistentFailure_OrphansRunWithoutExecution(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-ack-persistent", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-ack-persistent"
	clock := mocks.NewMockClock()
	queue := &scriptedAckQueue{
		ackErrors: []error{
			status.Error(codes.Unavailable, "queue unavailable"),
			status.Error(codes.Unavailable, "queue unavailable"),
			status.Error(codes.Unavailable, "queue unavailable"),
			status.Error(codes.Unavailable, "queue unavailable"),
		},
	}

	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		executor:      job.NewExecutor(),
		store:         runs,
	}

	jobID := "job-worker-ack-persistent"
	deliveryID := "delivery-ack-persistent"
	commandNodeID := "node-1"
	command := "echo should-not-run"
	action := "builtins/shell"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"command": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	w.runClaimedJob(j, jobID, runID, deliveryID)

	var statusVal string
	var reason sql.NullString
	var orphanReason sql.NullString
	var claimToken sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_reason, orphan_reason, claim_token FROM job_runs WHERE run_id = ?`, runID).
		Scan(&statusVal, &reason, &orphanReason, &claimToken); err != nil {
		t.Fatalf("query final status: %v", err)
	}

	if statusVal != "orphaned" {
		t.Fatalf("expected orphaned on persistent ack failure, got %q", statusVal)
	}

	if !reason.Valid || reason.String != dal.OrphanReasonAckUncertain {
		t.Fatalf("expected failure_reason %q, got %v", dal.OrphanReasonAckUncertain, reason)
	}

	if !orphanReason.Valid || orphanReason.String != dal.OrphanReasonAckUncertain {
		t.Fatalf("expected orphan_reason %q, got %v", dal.OrphanReasonAckUncertain, orphanReason)
	}

	if claimToken.Valid {
		t.Fatalf("expected claim_token cleared after orphaning, got %v", claimToken)
	}

	if queue.ackCalls != ackMaxAttempts {
		t.Fatalf("expected %d ack attempts, got %d", ackMaxAttempts, queue.ackCalls)
	}

	if logClient.GetStreamCount() != 0 {
		t.Fatalf("expected job execution to not start after persistent ack failure, got %d log streams", logClient.GetStreamCount())
	}
}
