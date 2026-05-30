//go:build integration

package multicell_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	apiserver "vectis/internal/api"
	"vectis/internal/catalog"
	"vectis/internal/cell"
	"vectis/internal/cellingress"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/logserver"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/grpcservices"

	"github.com/spf13/viper"
)

func TestIntegrationMultiCell_RoutedExecutionCatalogFanInAndBackfill(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	ctx := context.Background()
	logger := mocks.NewMockLogger()
	cellID := "pdx-b"
	jobID := "integration-multicell-job"
	definition := `{"id":"integration-multicell-job","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo hello-from-pdx"}}}`

	globalDB := dbtest.NewTestDB(t)
	cellDB := dbtest.NewTestDB(t)
	globalRepos := dal.NewSQLRepositoriesWithCellID(globalDB, dal.DefaultCellID)
	cellRepos := dal.NewSQLRepositoriesWithCellID(cellDB, cellID)

	if err := globalRepos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		t.Fatalf("create global job: %v", err)
	}

	_, cellQueueClient, cellQueueService := grpcservices.StartQueueServer(t, logger)
	cellIngress := cellingress.NewQueueServer(cellID, cellQueueService, logger)
	cellIngress.SetAcceptanceStore(cellRepos.CellExecutionAcceptances())
	cellIngressHTTP := httptest.NewServer(cellIngress.Handler())
	t.Cleanup(cellIngressHTTP.Close)

	logStore, err := logserver.NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("create log store: %v", err)
	}

	_, logClient := grpcservices.StartLogServer(t, logger, logStore)

	api := apiserver.NewAPIServer(logger, globalDB)
	api.SetExecutionIngress(cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{
		cellID: cell.NewHTTPExecutionIngress(cellIngressHTTP.URL, cellIngressHTTP.Client(), logger),
	}))

	runID := triggerJobInCell(t, api, jobID, cellID)

	worker := &integrationWorker{
		runCtx:                context.Background(),
		workerID:              "integration-worker-pdx-b",
		logger:                logger,
		queue:                 cellQueueClient,
		logClient:             logClient,
		executor:              job.NewExecutor(),
		store:                 cellRepos.Runs(),
		catalog:               cell.NewCatalogEventPublisher(cellID, cellRepos.CatalogEvents()),
		recordTerminalCatalog: false,
	}

	if err := worker.runOne(ctx); err != nil {
		t.Fatalf("worker run: %v", err)
	}

	assertCellRunStatus(t, ctx, cellRepos, runID, dal.RunStatusSucceeded)
	assertSourceMissingTerminalCatalogEvent(t, ctx, cellRepos, runID)

	fanIn := catalog.NewFanInProcessor(globalRepos.CatalogEvents(), []catalog.FanInSource{
		{
			CellID: cellID,
			Events: cellRepos.CatalogEvents(),
			Backfill: catalog.NewBackfillProcessor(
				cellID,
				cellRepos.CatalogStatusBackfill(),
				cell.NewCatalogEventPublisher(cellID, cellRepos.CatalogEvents()),
			),
		},
	})

	fanInResult, err := fanIn.IngestPending(ctx, 100)
	if err != nil {
		t.Fatalf("catalog fan-in: %v", err)
	}

	if fanInResult.Backfilled == 0 {
		t.Fatalf("expected fan-in backfill to repair missing terminal events, got %+v", fanInResult)
	}

	if fanInResult.Copied == 0 {
		t.Fatalf("expected fan-in to copy catalog events, got %+v", fanInResult)
	}

	inbox := cell.NewCatalogInboxProcessor(globalRepos.CatalogEvents(), globalRepos.Runs())
	processResult, err := inbox.ProcessPending(ctx, 100)
	if err != nil {
		t.Fatalf("process global catalog inbox: %v", err)
	}

	if processResult.Failed != 0 || processResult.Applied == 0 {
		t.Fatalf("unexpected catalog inbox result: %+v", processResult)
	}

	assertGlobalAPIRunStatus(t, api, jobID, runID, cellID, dal.RunStatusSucceeded)
}

type integrationWorker struct {
	runCtx                context.Context
	workerID              string
	logger                interfaces.Logger
	queue                 interfaces.QueueClient
	logClient             interfaces.LogClient
	executor              *job.Executor
	store                 dal.RunsRepository
	catalog               cell.CatalogEventPublisher
	recordTerminalCatalog bool
}

func (w *integrationWorker) runOne(ctx context.Context) error {
	dequeueCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	req, err := w.queue.Dequeue(dequeueCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("dequeue: %w", err)
	}

	jobReq := req.GetJob()
	if jobReq == nil {
		return fmt.Errorf("dequeued empty job request")
	}

	env, ok, err := cell.ExecutionEnvelopeFromRequest(req)
	if err != nil {
		return fmt.Errorf("decode execution envelope: %w", err)
	}

	if !ok {
		return fmt.Errorf("missing execution envelope")
	}

	runID := jobReq.GetRunId()
	claimed, claimToken, err := w.store.TryClaim(w.runCtx, runID, w.workerID, time.Now().Add(dal.DefaultLeaseTTL))
	if err != nil {
		return fmt.Errorf("claim run: %w", err)
	}

	if !claimed {
		return fmt.Errorf("run %s was not claimable", runID)
	}

	if err := w.catalog.RecordRunStatus(w.runCtx, dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusRunning}); err != nil {
		return fmt.Errorf("record running catalog event: %w", err)
	}

	if err := w.queue.Ack(w.runCtx, jobReq.GetDeliveryId()); err != nil {
		return fmt.Errorf("ack delivery: %w", err)
	}

	if err := w.store.MarkExecutionAccepted(w.runCtx, env.ExecutionID); err != nil {
		return fmt.Errorf("mark execution accepted: %w", err)
	}

	if err := w.catalog.RecordExecutionStatus(w.runCtx, dal.ExecutionStatusUpdate{ExecutionID: env.ExecutionID, Status: dal.ExecutionStatusAccepted}); err != nil {
		return fmt.Errorf("record accepted catalog event: %w", err)
	}

	if err := w.store.MarkExecutionStarted(w.runCtx, env.ExecutionID); err != nil {
		return fmt.Errorf("mark execution running: %w", err)
	}

	if err := w.catalog.RecordExecutionStatus(w.runCtx, dal.ExecutionStatusUpdate{ExecutionID: env.ExecutionID, Status: dal.ExecutionStatusRunning}); err != nil {
		return fmt.Errorf("record running execution catalog event: %w", err)
	}

	if err := w.executor.ExecuteJob(w.runCtx, jobReq, w.logClient, w.logger); err != nil {
		reason := err.Error()
		_ = w.store.MarkRunFailed(w.runCtx, runID, claimToken, dal.FailureCodeExecution, reason)
		_ = w.catalog.RecordRunStatus(w.runCtx, dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusFailed, FailureCode: dal.FailureCodeExecution, Reason: reason})
		_ = w.store.MarkExecutionTerminal(w.runCtx, env.ExecutionID, dal.ExecutionStatusFailed)
		_ = w.catalog.RecordExecutionStatus(w.runCtx, dal.ExecutionStatusUpdate{ExecutionID: env.ExecutionID, Status: dal.ExecutionStatusFailed})
		return fmt.Errorf("execute job: %w", err)
	}

	if err := w.store.MarkRunSucceeded(w.runCtx, runID, claimToken); err != nil {
		return fmt.Errorf("mark run succeeded: %w", err)
	}

	if err := w.store.MarkExecutionTerminal(w.runCtx, env.ExecutionID, dal.ExecutionStatusSucceeded); err != nil {
		return fmt.Errorf("mark execution succeeded: %w", err)
	}

	if w.recordTerminalCatalog {
		if err := w.catalog.RecordRunStatus(w.runCtx, dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusSucceeded}); err != nil {
			return fmt.Errorf("record succeeded catalog event: %w", err)
		}

		if err := w.catalog.RecordExecutionStatus(w.runCtx, dal.ExecutionStatusUpdate{ExecutionID: env.ExecutionID, Status: dal.ExecutionStatusSucceeded}); err != nil {
			return fmt.Errorf("record succeeded execution catalog event: %w", err)
		}
	}

	return nil
}

func triggerJobInCell(t *testing.T, api *apiserver.APIServer, jobID, cellID string) string {
	t.Helper()

	body := bytes.NewBufferString(fmt.Sprintf(`{"cell_id":%q}`, cellID))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID, body)
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", jobID)
	rec := httptest.NewRecorder()

	api.TriggerJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("trigger status: got %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	var resp struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode trigger response: %v", err)
	}

	if resp.RunID == "" {
		t.Fatalf("trigger response did not include run_id: %s", rec.Body.String())
	}

	return resp.RunID
}

func assertCellRunStatus(t *testing.T, ctx context.Context, repos *dal.SQLRepositories, runID, want string) {
	t.Helper()

	got, found, err := repos.Runs().GetRunStatus(ctx, runID)
	if err != nil {
		t.Fatalf("get cell run status: %v", err)
	}

	if !found {
		t.Fatalf("cell run %s not found", runID)
	}

	if got != want {
		t.Fatalf("cell run status: got %q, want %q", got, want)
	}
}

func assertSourceMissingTerminalCatalogEvent(t *testing.T, ctx context.Context, repos *dal.SQLRepositories, runID string) {
	t.Helper()

	pending, err := repos.CatalogEvents().ListPending(ctx, 100)
	if err != nil {
		t.Fatalf("list source catalog events: %v", err)
	}

	terminalKey := cell.CatalogRunStatusEventKey(runID, dal.RunStatusSucceeded)
	for _, event := range pending {
		if event.EventKey == terminalKey {
			t.Fatalf("expected terminal run event to be missing before backfill, got %+v", pending)
		}
	}
}

func assertGlobalAPIRunStatus(t *testing.T, api *apiserver.APIServer, jobID, runID, cellID, wantStatus string) {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+jobID+"/runs", nil)
	req.SetPathValue("id", jobID)
	rec := httptest.NewRecorder()

	api.GetJobRuns(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("list runs status: got %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp struct {
		Data []struct {
			RunID      string `json:"run_id"`
			Status     string `json:"status"`
			OwningCell string `json:"owning_cell"`
		} `json:"data"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode runs response: %v", err)
	}

	for _, run := range resp.Data {
		if run.RunID != runID {
			continue
		}

		if run.Status != wantStatus {
			t.Fatalf("global API run status: got %q, want %q", run.Status, wantStatus)
		}

		if run.OwningCell != cellID {
			t.Fatalf("global API owning cell: got %q, want %q", run.OwningCell, cellID)
		}

		return
	}

	t.Fatalf("global API response did not include run %s: %+v", runID, resp.Data)
}
