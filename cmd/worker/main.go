package main

import (
	"context"
	"os"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	api "vectis/api/gen/go"
	"vectis/internal/backoff"
	"vectis/internal/cli"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/interfaces"
	"vectis/internal/job"
	"vectis/internal/multidial"
	"vectis/internal/queueclient"

	_ "github.com/mattn/go-sqlite3"
)

const (
	maxFailureReasonLen = 4096
	dequeueBackoffBase  = 500 * time.Millisecond
	dequeueBackoffMax   = 30 * time.Second
	longPollTimeout     = 30 * time.Second
)

func runWorker(cmd *cobra.Command, args []string) {
	ctx := context.Background()
	logger := interfaces.NewLogger("worker")
	cli.SetLogLevel(logger)

	workerID := uuid.New().String()
	logger.Info("Worker ID: %s", workerID)

	dbPath := database.GetDBPath()
	logger.Info("Using database: %s", dbPath)
	db, err := database.OpenDB(dbPath)
	if err != nil {
		logger.Fatal("Failed to open database: %v", err)
	}
	defer db.Close()

	dial := func(ctx context.Context) (interfaces.QueueClient, interfaces.LogClient, func(), error) {
		q, l, cleanup, err := multidial.DialQueueAndLog(ctx, logger)
		return q, l, cleanup, err
	}

	clients, err := queueclient.NewManagingWorkerDial(ctx, logger, dial)
	if err != nil {
		logger.Fatal("Failed to connect to queue or log service: %v", err)
	}
	defer func() { _ = clients.Close() }()

	w := &worker{
		ctx:       ctx,
		logger:    logger,
		workerID:  workerID,
		clock:     interfaces.SystemClock{},
		queue:     clients,
		logClient: clients,
		executor:  job.NewExecutor(),
		store:     dal.NewSQLRepositories(db).Runs(),
	}
	w.run()
}

type worker struct {
	ctx                context.Context
	logger             interfaces.Logger
	workerID           string
	clock              interfaces.Clock
	queue              interfaces.QueueClient
	logClient          interfaces.LogClient
	executor           *job.Executor
	store              dal.RunsRepository
	dequeueFailAttempt int
}

func (w *worker) run() {
	for {
		job, keepGoing := w.dequeueNext()
		if !keepGoing {
			return
		}

		if job == nil {
			continue
		}

		w.handleJob(job)
	}
}

func (w *worker) dequeueNext() (*api.Job, bool) {
	w.logger.Debug("Initiating long poll from queue...")
	pollCtx, cancelPoll := context.WithTimeout(w.ctx, longPollTimeout)
	job, err := w.queue.Dequeue(pollCtx)
	cancelPoll()

	if err != nil {
		return w.handleDequeueError(err)
	}

	w.dequeueFailAttempt = 0
	if job == nil {
		w.logger.Debug("Dequeue returned nil job, skipping")
		return nil, true
	}

	return job, true
}

func (w *worker) handleDequeueError(err error) (*api.Job, bool) {
	st, ok := status.FromError(err)
	if ok && st.Code() == codes.DeadlineExceeded {
		w.logger.Debug("Long poll timed out. Retrying...")
		w.dequeueFailAttempt = 0
		return nil, true
	}

	delay := backoff.ExponentialDelay(dequeueBackoffBase, w.dequeueFailAttempt, dequeueBackoffMax)
	w.logger.Warn("Failed to dequeue job: %v; retrying in %v", err, delay)
	if sleepErr := w.clock.Sleep(w.ctx, delay); sleepErr != nil {
		w.logger.Info("Stopping worker dequeue loop: %v", sleepErr)
		return nil, false
	}

	w.dequeueFailAttempt++
	return nil, true
}

func (w *worker) handleJob(job *api.Job) {
	jobID := job.GetId()
	runID := job.GetRunId()
	deliveryID := job.GetDeliveryId()
	w.logger.Info("Dequeued job: %s (run %s)", jobID, runID)

	if runID != "" {
		w.runClaimedJob(job, jobID, runID, deliveryID)
		return
	}

	if err := w.ackDelivery(deliveryID); err != nil {
		w.logger.Error("Ack delivery %s failed for job %s: %v", deliveryID, jobID, err)
		return
	}

	if err := w.executor.ExecuteJob(w.ctx, job, w.logClient, w.logger); err != nil {
		w.logger.Error("Job %s failed: %v", jobID, err)
		return
	}

	w.logger.Info("Job completed successfully: %s", jobID)
}

func (w *worker) runClaimedJob(job *api.Job, jobID, runID, deliveryID string) {
	leaseUntil := time.Now().Add(dal.DefaultLeaseTTL)
	claimed, claimErr := w.store.TryClaim(w.ctx, runID, w.workerID, leaseUntil)
	if claimErr != nil {
		w.logger.Error("TryClaim %s: %v", runID, claimErr)
		return
	}

	if !claimed {
		w.logger.Debug("Run %s not claimed (other worker or not queued); dropping message", runID)
		if err := w.ackDelivery(deliveryID); err != nil {
			w.logger.Warn("Ack delivery %s for unclaimed run %s failed: %v", deliveryID, runID, err)
		}

		return
	}

	if err := w.ackDelivery(deliveryID); err != nil {
		w.logger.Error("Ack delivery %s failed for claimed run %s: %v", deliveryID, runID, err)
		if markErr := w.store.MarkRunFailed(w.ctx, runID, "queue ack failed"); markErr != nil {
			w.logger.Error("Failed to mark run %s failed after ack error: %v", runID, markErr)
		}

		return
	}

	renewFailed, execErr := w.executeWithLeaseRenewal(runID, job)
	if renewFailed {
		w.logger.Error("Run %s: lease renewal failed", runID)
		if err := w.store.MarkRunFailed(w.ctx, runID, "lease renewal failed"); err != nil {
			w.logger.Error("Failed to mark run %s failed: %v", runID, err)
		}

		return
	}

	if execErr != nil {
		w.logger.Error("Job %s failed: %v", jobID, execErr)
		reason := truncateFailureReason(execErr.Error())
		if err := w.store.MarkRunFailed(w.ctx, runID, reason); err != nil {
			w.logger.Error("Failed to mark run %s failed: %v", runID, err)
		}

		return
	}

	if err := w.store.MarkRunSucceeded(w.ctx, runID); err != nil {
		w.logger.Error("Failed to mark run %s succeeded: %v", runID, err)
	}

	w.logger.Info("Job completed successfully: %s", jobID)
}

func (w *worker) ackDelivery(deliveryID string) error {
	if deliveryID == "" {
		return nil
	}

	return w.queue.Ack(w.ctx, deliveryID)
}

func (w *worker) executeWithLeaseRenewal(runID string, job *api.Job) (renewFailed bool, err error) {
	execCtx, execCancel := context.WithCancel(w.ctx)
	defer execCancel()

	stopRenew := make(chan struct{})
	doneRenew := make(chan struct{})
	var renewFailedAtom atomic.Bool

	go w.leaseRenewalLoop(execCtx, execCancel, runID, stopRenew, doneRenew, &renewFailedAtom)

	err = w.executor.ExecuteJob(execCtx, job, w.logClient, w.logger)
	close(stopRenew)
	<-doneRenew

	return renewFailedAtom.Load(), err
}

func (w *worker) leaseRenewalLoop(
	execCtx context.Context,
	execCancel context.CancelFunc,
	runID string,
	stopRenew <-chan struct{},
	doneRenew chan<- struct{},
	renewFailed *atomic.Bool,
) {
	defer close(doneRenew)

	ticker := time.NewTicker(dal.DefaultRenewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stopRenew:
			return
		case <-execCtx.Done():
			return
		case <-ticker.C:
			next := time.Now().Add(dal.DefaultLeaseTTL)
			if err := w.store.RenewLease(w.ctx, runID, w.workerID, next); err != nil {
				renewFailed.Store(true)
				execCancel()
				return
			}
		}
	}
}

func truncateFailureReason(reason string) string {
	if len(reason) <= maxFailureReasonLen {
		return reason
	}

	return reason[:maxFailureReasonLen] + "..."
}

var rootCmd = &cobra.Command{
	Use:   "vectis-worker",
	Short: "Vectis Worker",
	Long:  `The Vectis Worker executes jobs from the queue using the action system.`,
	Run:   runWorker,
}

func init() {
	viper.SetEnvPrefix("VECTIS_WORKER")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
