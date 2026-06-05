//go:build integration

package multicell_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
	"vectis/internal/reconciler"
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

func TestIntegrationMultiCell_FanOutRunsAcrossCellsAndFanInIsIdempotent(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	ctx := context.Background()
	logger := mocks.NewMockLogger()
	jobID := "integration-multicell-fanout"
	definition := `{"id":"integration-multicell-fanout","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo fanout"}}}`

	globalDB := dbtest.NewTestDB(t)
	globalRepos := dal.NewSQLRepositoriesWithCellID(globalDB, dal.DefaultCellID)
	if err := globalRepos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		t.Fatalf("create global job: %v", err)
	}

	logStore, err := logserver.NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("create log store: %v", err)
	}

	_, logClient := grpcservices.StartLogServer(t, logger, logStore)
	cells := []*integrationCell{
		startIntegrationCell(t, "iad-a", logger, logClient),
		startIntegrationCell(t, "pdx-b", logger, logClient),
	}

	routes := make(map[string]cell.ExecutionIngress, len(cells))
	for _, c := range cells {
		routes[c.id] = c.ingress
	}

	api := apiserver.NewAPIServer(logger, globalDB)
	api.SetExecutionIngress(cell.NewStaticExecutionRouter(routes))

	triggered := triggerJobInCells(t, api, jobID, "iad-a", "pdx-b")
	if len(triggered) != len(cells) {
		t.Fatalf("triggered runs: got %d, want %d", len(triggered), len(cells))
	}

	runByCell := map[string]string{}
	for _, run := range triggered {
		runByCell[run.CellID] = run.RunID
	}

	for _, c := range cells {
		runID := runByCell[c.id]
		if runID == "" {
			t.Fatalf("trigger response did not include cell %q: %+v", c.id, triggered)
		}

		if err := c.worker.runOne(ctx); err != nil {
			t.Fatalf("worker run for cell %s: %v", c.id, err)
		}

		assertCellRunStatus(t, ctx, c.repos, runID, dal.RunStatusSucceeded)
	}

	fanInResult, processResult := fanInAndProcess(t, ctx, globalRepos, cells)
	if fanInResult.Sources != len(cells) || fanInResult.Copied == 0 || fanInResult.Read == 0 {
		t.Fatalf("unexpected fan-in result: %+v", fanInResult)
	}

	if processResult.Failed != 0 || processResult.Applied == 0 {
		t.Fatalf("unexpected catalog inbox result: %+v", processResult)
	}

	wantRuns := map[string]apiRunExpectation{}
	for _, c := range cells {
		wantRuns[runByCell[c.id]] = apiRunExpectation{
			status:     dal.RunStatusSucceeded,
			owningCell: c.id,
		}
	}

	assertGlobalAPIRuns(t, api, jobID, wantRuns)

	secondFanInResult, secondProcessResult := fanInAndProcess(t, ctx, globalRepos, cells)
	if secondFanInResult.Backfilled != 0 || secondFanInResult.Read != 0 || secondFanInResult.Copied != 0 {
		t.Fatalf("second fan-in should be idempotent, got %+v", secondFanInResult)
	}

	if secondProcessResult.Read != 0 || secondProcessResult.Applied != 0 || secondProcessResult.Failed != 0 {
		t.Fatalf("second inbox process should be idempotent, got %+v", secondProcessResult)
	}
}

func TestIntegrationMultiCell_ReplayRunRoutesToNamedCellWithSourceSnapshot(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	ctx := context.Background()
	logger := mocks.NewMockLogger()
	jobID := "integration-multicell-replay"
	definitionV1 := `{"id":"integration-multicell-replay","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo replay-source"}}}`
	definitionV2 := `{"id":"integration-multicell-replay","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo replay-current"}}}`

	globalDB := dbtest.NewTestDB(t)
	globalRepos := dal.NewSQLRepositoriesWithCellID(globalDB, dal.DefaultCellID)
	if err := globalRepos.Jobs().Create(ctx, jobID, definitionV1, 1); err != nil {
		t.Fatalf("create global job: %v", err)
	}

	logStore, err := logserver.NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("create log store: %v", err)
	}

	_, logClient := grpcservices.StartLogServer(t, logger, logStore)
	c := startIntegrationCell(t, "pdx-b", logger, logClient)

	api := apiserver.NewAPIServer(logger, globalDB)
	api.SetExecutionIngress(cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{
		c.id: c.ingress,
	}))

	sourceRunID := triggerJobInCell(t, api, jobID, c.id)
	if err := c.worker.runOne(ctx); err != nil {
		t.Fatalf("worker run for source: %v", err)
	}

	assertCellRunStatus(t, ctx, c.repos, sourceRunID, dal.RunStatusSucceeded)
	fanInAndProcess(t, ctx, globalRepos, []*integrationCell{c})
	assertGlobalAPIRunStatus(t, api, jobID, sourceRunID, c.id, dal.RunStatusSucceeded)

	if _, err := globalRepos.Jobs().UpdateDefinition(ctx, jobID, definitionV2); err != nil {
		t.Fatalf("update job definition: %v", err)
	}

	replayed := replayRunInCell(t, api, sourceRunID, c.id)
	if replayed.RunID == sourceRunID {
		t.Fatal("replay should create a new run id")
	}

	if replayed.JobID != jobID || replayed.CellID != c.id || replayed.ReplayOfRunID != sourceRunID {
		t.Fatalf("unexpected replay response: %+v", replayed)
	}

	events := waitForDispatchEvents(t, ctx, globalRepos, replayed.RunID, 2)
	assertDispatchSuccess(t, events)

	replayRun, err := globalRepos.Runs().GetRun(ctx, replayed.RunID)
	if err != nil {
		t.Fatalf("get replay run: %v", err)
	}

	if replayRun.DefinitionVersion != 1 || replayRun.ReplayOfRunID == nil || *replayRun.ReplayOfRunID != sourceRunID {
		t.Fatalf("unexpected replay audit metadata: %+v", replayRun)
	}

	payload, err := globalRepos.Runs().GetExecutionPayloadForRun(ctx, replayed.RunID)
	if err != nil {
		t.Fatalf("get replay execution payload: %v", err)
	}

	if !strings.Contains(payload.PayloadJSON, "replay-source") || strings.Contains(payload.PayloadJSON, "replay-current") {
		t.Fatalf("replay payload did not preserve the source definition snapshot: %s", payload.PayloadJSON)
	}

	if err := c.worker.runOne(ctx); err != nil {
		t.Fatalf("worker run for replay: %v", err)
	}

	assertCellRunStatus(t, ctx, c.repos, replayed.RunID, dal.RunStatusSucceeded)

	fanInResult, processResult := fanInAndProcess(t, ctx, globalRepos, []*integrationCell{c})
	if fanInResult.Copied == 0 || fanInResult.Read == 0 {
		t.Fatalf("unexpected replay fan-in result: %+v", fanInResult)
	}

	if processResult.Failed != 0 || processResult.Applied == 0 {
		t.Fatalf("unexpected replay catalog inbox result: %+v", processResult)
	}

	assertGlobalAPIRunStatus(t, api, jobID, replayed.RunID, c.id, dal.RunStatusSucceeded)
}

func TestIntegrationMultiCell_CellsStatusCombinesRoutesRunsAndCatalogSources(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	ctx := context.Background()
	logger := mocks.NewMockLogger()
	jobID := "integration-multicell-cells-status"
	definition := `{"id":"integration-multicell-cells-status","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo cell-status"}}}`

	globalDB := dbtest.NewTestDB(t)
	globalRepos := dal.NewSQLRepositoriesWithCellID(globalDB, dal.DefaultCellID)
	if err := globalRepos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		t.Fatalf("create global job: %v", err)
	}

	logStore, err := logserver.NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("create log store: %v", err)
	}

	_, logClient := grpcservices.StartLogServer(t, logger, logStore)
	readyCell := startIntegrationCell(t, "iad-a", logger, logClient)
	missingCellID := "pdx-b"

	viper.Set("cell_ingress_endpoints", []string{
		readyCell.id + "=" + readyCell.ingressURL,
	})

	api := apiserver.NewAPIServer(logger, globalDB)
	api.SetExecutionIngress(cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{
		readyCell.id: readyCell.ingress,
	}))

	triggered := triggerJobInCells(t, api, jobID, readyCell.id, missingCellID)
	runByCell := map[string]string{}
	for _, run := range triggered {
		runByCell[run.CellID] = run.RunID
	}

	if runByCell[readyCell.id] == "" || runByCell[missingCellID] == "" {
		t.Fatalf("trigger response missing expected cells: %+v", triggered)
	}

	readyEvents := waitForDispatchEvents(t, ctx, globalRepos, runByCell[readyCell.id], 2)
	assertDispatchSuccess(t, readyEvents)

	missingEvents := waitForDispatchEvents(t, ctx, globalRepos, runByCell[missingCellID], 2)
	assertDispatchFailure(t, missingEvents, missingCellID, "cell not routable")

	status := getCellsStatus(t, api)
	readyStatus := status[readyCell.id]
	if readyStatus.CellID == "" {
		t.Fatalf("cells status missing ready cell %q: %+v", readyCell.id, status)
	}

	if !readyStatus.IngressConfigured || !readyStatus.IngressReachable || readyStatus.Status != "ready" {
		t.Fatalf("unexpected ready cell status: %+v", readyStatus)
	}

	if readyStatus.Queued != 1 || readyStatus.Stuck != 0 {
		t.Fatalf("ready cell run counts: got queued=%d stuck=%d, want queued=1 stuck=0 (%+v)", readyStatus.Queued, readyStatus.Stuck, readyStatus)
	}

	missingStatus := status[missingCellID]
	if missingStatus.CellID == "" {
		t.Fatalf("cells status missing unroutable cell %q: %+v", missingCellID, status)
	}

	if !missingStatus.IngressRequired || missingStatus.IngressConfigured || missingStatus.IngressReachable || missingStatus.Status != "missing_route" {
		t.Fatalf("unexpected missing-route cell status: %+v", missingStatus)
	}

	if missingStatus.Queued != 1 || missingStatus.Stuck != 1 {
		t.Fatalf("missing route run counts: got queued=%d stuck=%d, want queued=1 stuck=1 (%+v)", missingStatus.Queued, missingStatus.Stuck, missingStatus)
	}

	if err := readyCell.worker.runOne(ctx); err != nil {
		t.Fatalf("worker run for ready cell: %v", err)
	}

	fanIn := catalog.NewFanInProcessor(globalRepos.CatalogEvents(), []catalog.FanInSource{
		{
			CellID: readyCell.id,
			Events: readyCell.repos.CatalogEvents(),
			Backfill: catalog.NewBackfillProcessor(
				readyCell.id,
				readyCell.repos.CatalogStatusBackfill(),
				cell.NewCatalogEventPublisher(readyCell.id, readyCell.repos.CatalogEvents()),
			),
		},
	})

	fanInResult, err := fanIn.IngestPending(ctx, 100)
	if err != nil {
		t.Fatalf("catalog fan-in: %v", err)
	}
	if fanInResult.Copied == 0 || fanInResult.Read == 0 {
		t.Fatalf("expected cell catalog events to be copied, got %+v", fanInResult)
	}

	status = getCellsStatus(t, api)
	readyStatus = status[readyCell.id]
	if readyStatus.CatalogPending == 0 || readyStatus.CatalogTotal == 0 {
		t.Fatalf("expected ready cell catalog source counts after fan-in, got %+v", readyStatus)
	}
}

func TestIntegrationMultiCell_FailedExecutionPropagatesThroughCatalog(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	ctx := context.Background()
	logger := mocks.NewMockLogger()
	jobID := "integration-multicell-failure"
	definition := `{"id":"integration-multicell-failure","root":{"id":"root","uses":"builtins/shell","with":{"command":"exit 42"}}}`

	globalDB := dbtest.NewTestDB(t)
	globalRepos := dal.NewSQLRepositoriesWithCellID(globalDB, dal.DefaultCellID)
	if err := globalRepos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		t.Fatalf("create global job: %v", err)
	}

	logStore, err := logserver.NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("create log store: %v", err)
	}

	_, logClient := grpcservices.StartLogServer(t, logger, logStore)
	c := startIntegrationCell(t, "ord-c", logger, logClient)

	api := apiserver.NewAPIServer(logger, globalDB)
	api.SetExecutionIngress(cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{
		c.id: c.ingress,
	}))

	runID := triggerJobInCell(t, api, jobID, c.id)
	if err := c.worker.runOne(ctx); err == nil {
		t.Fatalf("expected worker execution to fail")
	}

	assertCellRunStatus(t, ctx, c.repos, runID, dal.RunStatusFailed)

	fanInResult, processResult := fanInAndProcess(t, ctx, globalRepos, []*integrationCell{c})
	if fanInResult.Copied == 0 || fanInResult.Read == 0 {
		t.Fatalf("unexpected fan-in result: %+v", fanInResult)
	}

	if processResult.Failed != 0 || processResult.Applied == 0 {
		t.Fatalf("unexpected catalog inbox result: %+v", processResult)
	}

	assertGlobalAPIRunStatus(t, api, jobID, runID, c.id, dal.RunStatusFailed)
}

func TestIntegrationMultiCell_UnroutableCellRecordsDispatchFailure(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	ctx := context.Background()
	logger := mocks.NewMockLogger()
	cellID := "missing-cell"
	jobID := "integration-multicell-unroutable"
	definition := `{"id":"integration-multicell-unroutable","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo unroutable"}}}`

	globalDB := dbtest.NewTestDB(t)
	globalRepos := dal.NewSQLRepositoriesWithCellID(globalDB, dal.DefaultCellID)
	if err := globalRepos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		t.Fatalf("create global job: %v", err)
	}

	api := apiserver.NewAPIServer(logger, globalDB)
	api.SetExecutionIngress(cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{}))

	runID := triggerJobInCell(t, api, jobID, cellID)
	events := waitForDispatchEvents(t, ctx, globalRepos, runID, 2)
	assertDispatchFailure(t, events, cellID, "cell not routable")
	assertGlobalAPIRunStatus(t, api, jobID, runID, cellID, dal.RunStatusQueued)
}

func TestIntegrationMultiCell_UnavailableCellIngressRecordsDispatchFailure(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	ctx := context.Background()
	logger := mocks.NewMockLogger()
	cellID := "lax-a"
	jobID := "integration-multicell-ingress-down"
	definition := `{"id":"integration-multicell-ingress-down","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo ingress-down"}}}`

	globalDB := dbtest.NewTestDB(t)
	globalRepos := dal.NewSQLRepositoriesWithCellID(globalDB, dal.DefaultCellID)
	if err := globalRepos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		t.Fatalf("create global job: %v", err)
	}

	closedIngress := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("closed ingress unexpectedly received %s %s", r.Method, r.URL.Path)
	}))
	ingressURL := closedIngress.URL
	ingressClient := closedIngress.Client()
	closedIngress.Close()

	api := apiserver.NewAPIServer(logger, globalDB)
	api.SetExecutionIngress(cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{
		cellID: cell.NewHTTPExecutionIngress(ingressURL, ingressClient, logger),
	}))

	runID := triggerJobInCell(t, api, jobID, cellID)
	events := waitForDispatchEvents(t, ctx, globalRepos, runID, 2)
	assertDispatchFailure(t, events, "submit execution to cell ingress")
	assertGlobalAPIRunStatus(t, api, jobID, runID, cellID, dal.RunStatusQueued)
}

func TestIntegrationMultiCell_ReconcilerRepairsFailedDispatchThroughCellIngress(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	ctx := context.Background()
	logger := mocks.NewMockLogger()
	cellID := "den-a"
	jobID := "integration-multicell-repair"
	definition := `{"id":"integration-multicell-repair","root":{"id":"root","uses":"builtins/shell","with":{"command":"echo repaired"}}}`

	globalDB := dbtest.NewTestDB(t)
	globalRepos := dal.NewSQLRepositoriesWithCellID(globalDB, dal.DefaultCellID)
	if err := globalRepos.Jobs().Create(ctx, jobID, definition, 1); err != nil {
		t.Fatalf("create global job: %v", err)
	}

	api := apiserver.NewAPIServer(logger, globalDB)
	api.SetExecutionIngress(cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{}))

	runID := triggerJobInCell(t, api, jobID, cellID)
	events := waitForDispatchEvents(t, ctx, globalRepos, runID, 2)
	assertDispatchFailure(t, events, cellID, "cell not routable")
	assertGlobalAPIRunStatus(t, api, jobID, runID, cellID, dal.RunStatusQueued)

	logStore, err := logserver.NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("create log store: %v", err)
	}

	_, logClient := grpcservices.StartLogServer(t, logger, logStore)
	c := startIntegrationCell(t, cellID, logger, logClient)

	rec := reconciler.NewService(logger, globalDB, mocks.NewMockQueueService(), interfaces.SystemClock{})
	rec.SetMinDispatchGap(1 * time.Millisecond)
	rec.SetExecutionIngress(cell.NewStaticExecutionRouter(map[string]cell.ExecutionIngress{
		cellID: c.ingress,
	}))

	if err := rec.Process(ctx); err != nil {
		t.Fatalf("reconciler process: %v", err)
	}

	events = waitForDispatchEvents(t, ctx, globalRepos, runID, 4)
	assertDispatchSuccessAfterFailure(t, events)

	if err := c.worker.runOne(ctx); err != nil {
		t.Fatalf("worker run after repair: %v", err)
	}

	assertCellRunStatus(t, ctx, c.repos, runID, dal.RunStatusSucceeded)

	fanInResult, processResult := fanInAndProcess(t, ctx, globalRepos, []*integrationCell{c})
	if fanInResult.Copied == 0 || fanInResult.Read == 0 {
		t.Fatalf("unexpected fan-in result: %+v", fanInResult)
	}
	if processResult.Failed != 0 || processResult.Applied == 0 {
		t.Fatalf("unexpected catalog inbox result: %+v", processResult)
	}

	assertGlobalAPIRunStatus(t, api, jobID, runID, cellID, dal.RunStatusSucceeded)
}

type integrationCell struct {
	id         string
	repos      *dal.SQLRepositories
	ingressURL string
	ingress    cell.ExecutionIngress
	worker     *integrationWorker
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

type triggeredRun struct {
	RunID    string `json:"run_id"`
	RunIndex int    `json:"run_index"`
	CellID   string `json:"cell_id"`
}

type replayedRun struct {
	JobID         string `json:"job_id"`
	RunID         string `json:"run_id"`
	RunIndex      int    `json:"run_index"`
	CellID        string `json:"cell_id"`
	ReplayOfRunID string `json:"replay_of_run_id"`
}

type apiRunExpectation struct {
	status     string
	owningCell string
}

type apiRunSnapshot struct {
	RunID      string
	Status     string
	OwningCell string
}

func startIntegrationCell(t *testing.T, cellID string, logger interfaces.Logger, logClient interfaces.LogClient) *integrationCell {
	t.Helper()

	cellDB := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(cellDB, cellID)

	_, queueClient, queueService := grpcservices.StartQueueServer(t, logger)
	cellIngress := cellingress.NewQueueServer(cellID, queueService, logger)
	cellIngress.SetAcceptanceStore(repos.CellExecutionAcceptances())
	cellIngressHTTP := httptest.NewServer(cellIngress.Handler())
	t.Cleanup(cellIngressHTTP.Close)

	return &integrationCell{
		id:         cellID,
		repos:      repos,
		ingressURL: cellIngressHTTP.URL,
		ingress:    cell.NewHTTPExecutionIngress(cellIngressHTTP.URL, cellIngressHTTP.Client(), logger),
		worker: &integrationWorker{
			runCtx:                context.Background(),
			workerID:              "integration-worker-" + cellID,
			logger:                logger,
			queue:                 queueClient,
			logClient:             logClient,
			executor:              job.NewExecutor(),
			store:                 repos.Runs(),
			catalog:               cell.NewCatalogEventPublisher(cellID, repos.CatalogEvents()),
			recordTerminalCatalog: true,
		},
	}
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

	completer := job.NewTaskCompletionService(w.store)
	if err := w.executor.ExecuteTask(w.runCtx, jobReq, env.TaskKey, w.logClient, w.logger); err != nil {
		reason := err.Error()
		_, _ = completer.CompleteTaskExecution(w.runCtx, env.ExecutionID, dal.ExecutionStatusFailed)
		_ = w.store.MarkRunFailed(w.runCtx, runID, claimToken, dal.FailureCodeExecution, reason)
		_ = w.catalog.RecordRunStatus(w.runCtx, dal.RunStatusUpdate{RunID: runID, Status: dal.RunStatusFailed, FailureCode: dal.FailureCodeExecution, Reason: reason})
		_ = w.catalog.RecordExecutionStatus(w.runCtx, dal.ExecutionStatusUpdate{ExecutionID: env.ExecutionID, Status: dal.ExecutionStatusFailed})
		return fmt.Errorf("execute task: %w", err)
	}

	if _, err := completer.CompleteTaskExecution(w.runCtx, env.ExecutionID, dal.ExecutionStatusSucceeded); err != nil {
		return fmt.Errorf("complete task execution succeeded: %w", err)
	}

	if err := w.store.MarkRunSucceeded(w.runCtx, runID, claimToken); err != nil {
		return fmt.Errorf("mark run succeeded: %w", err)
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

func triggerJobInCells(t *testing.T, api *apiserver.APIServer, jobID string, cellIDs ...string) []triggeredRun {
	t.Helper()

	bodyBytes, err := json.Marshal(map[string][]string{"cell_ids": cellIDs})
	if err != nil {
		t.Fatalf("encode trigger request: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", jobID)
	rec := httptest.NewRecorder()

	api.TriggerJob(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("trigger status: got %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	var resp struct {
		Runs []triggeredRun `json:"runs"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode trigger response: %v", err)
	}

	return resp.Runs
}

func replayRunInCell(t *testing.T, api *apiserver.APIServer, sourceRunID, cellID string) replayedRun {
	t.Helper()

	body := bytes.NewBufferString(fmt.Sprintf(`{"cell_id":%q}`, cellID))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs/"+sourceRunID+"/replay", body)
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("id", sourceRunID)
	rec := httptest.NewRecorder()

	api.ReplayRun(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("replay status: got %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}

	var resp replayedRun
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode replay response: %v", err)
	}

	if resp.RunID == "" {
		t.Fatalf("replay response did not include run_id: %s", rec.Body.String())
	}

	return resp
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

	assertGlobalAPIRuns(t, api, jobID, map[string]apiRunExpectation{
		runID: {
			status:     wantStatus,
			owningCell: cellID,
		},
	})
}

func assertGlobalAPIRuns(t *testing.T, api *apiserver.APIServer, jobID string, want map[string]apiRunExpectation) {
	t.Helper()

	got := listGlobalAPIRuns(t, api, jobID)
	for runID, expectation := range want {
		run, ok := got[runID]
		if !ok {
			t.Fatalf("global API response did not include run %s: %+v", runID, got)
		}

		if run.Status != expectation.status {
			t.Fatalf("global API run %s status: got %q, want %q", runID, run.Status, expectation.status)
		}

		if run.OwningCell != expectation.owningCell {
			t.Fatalf("global API run %s owning cell: got %q, want %q", runID, run.OwningCell, expectation.owningCell)
		}
	}
}

func waitForDispatchEvents(t *testing.T, ctx context.Context, repos *dal.SQLRepositories, runID string, want int) []dal.DispatchEvent {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var events []dal.DispatchEvent
	for {
		var err error
		events, err = repos.DispatchEvents().ListByRun(ctx, runID)
		if err != nil {
			t.Fatalf("list dispatch events: %v", err)
		}

		if len(events) >= want {
			return events
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d dispatch events for run %s, got %+v", want, runID, events)
		}

		time.Sleep(5 * time.Millisecond)
	}
}

func assertDispatchFailure(t *testing.T, events []dal.DispatchEvent, messageParts ...string) {
	t.Helper()

	if len(events) < 2 {
		t.Fatalf("expected at least dispatch attempt and failure, got %+v", events)
	}

	attempt := events[0]
	if attempt.Source != dal.DispatchSourceAPI || attempt.EventType != dal.DispatchEventAttempt {
		t.Fatalf("unexpected dispatch attempt event: %+v", attempt)
	}

	for _, event := range events {
		if event.EventType == dal.DispatchEventSuccess {
			t.Fatalf("dispatch should not have succeeded, got events %+v", events)
		}
	}

	failure := events[len(events)-1]
	if failure.Source != dal.DispatchSourceAPI || failure.EventType != dal.DispatchEventFailure {
		t.Fatalf("unexpected dispatch failure event: %+v", failure)
	}

	if failure.Message == nil {
		t.Fatalf("dispatch failure did not include a message: %+v", failure)
	}

	for _, part := range messageParts {
		if !strings.Contains(*failure.Message, part) {
			t.Fatalf("dispatch failure message %q does not contain %q", *failure.Message, part)
		}
	}
}

func assertDispatchSuccess(t *testing.T, events []dal.DispatchEvent) {
	t.Helper()

	if len(events) < 2 {
		t.Fatalf("expected at least dispatch attempt and success, got %+v", events)
	}

	attempt := events[0]
	if attempt.Source != dal.DispatchSourceAPI || attempt.EventType != dal.DispatchEventAttempt {
		t.Fatalf("unexpected dispatch attempt event: %+v", attempt)
	}

	success := events[len(events)-1]
	if success.Source != dal.DispatchSourceAPI || success.EventType != dal.DispatchEventSuccess {
		t.Fatalf("unexpected dispatch success event: %+v", success)
	}
}

func assertDispatchSuccessAfterFailure(t *testing.T, events []dal.DispatchEvent) {
	t.Helper()

	if len(events) < 4 {
		t.Fatalf("expected API failure and reconciler success dispatch events, got %+v", events)
	}

	hasAPIFailure := false
	hasReconcilerAttempt := false
	hasReconcilerSuccess := false
	for _, event := range events {
		switch {
		case event.Source == dal.DispatchSourceAPI && event.EventType == dal.DispatchEventFailure:
			hasAPIFailure = true
		case event.Source == dal.DispatchSourceReconciler && event.EventType == dal.DispatchEventAttempt:
			hasReconcilerAttempt = true
		case event.Source == dal.DispatchSourceReconciler && event.EventType == dal.DispatchEventSuccess:
			hasReconcilerSuccess = true
		}
	}

	if !hasAPIFailure || !hasReconcilerAttempt || !hasReconcilerSuccess {
		t.Fatalf("dispatch events do not show API failure followed by reconciler success: %+v", events)
	}
}

func listGlobalAPIRuns(t *testing.T, api *apiserver.APIServer, jobID string) map[string]apiRunSnapshot {
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

	out := make(map[string]apiRunSnapshot, len(resp.Data))
	for _, run := range resp.Data {
		out[run.RunID] = apiRunSnapshot{
			RunID:      run.RunID,
			Status:     run.Status,
			OwningCell: run.OwningCell,
		}
	}

	return out
}

type cellStatusSnapshot struct {
	CellID            string `json:"cell_id"`
	IngressRequired   bool   `json:"ingress_required"`
	IngressConfigured bool   `json:"ingress_configured"`
	IngressReachable  bool   `json:"ingress_reachable"`
	Status            string `json:"status"`
	Queued            int64  `json:"queued"`
	Stuck             int64  `json:"stuck"`
	CatalogPending    int64  `json:"catalog_pending"`
	CatalogFailed     int64  `json:"catalog_failed"`
	CatalogTotal      int64  `json:"catalog_total"`
}

func getCellsStatus(t *testing.T, api *apiserver.APIServer) map[string]cellStatusSnapshot {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/cells/status", nil)
	rec := httptest.NewRecorder()

	api.GetCellsStatus(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cells status: got %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp struct {
		Cells []cellStatusSnapshot `json:"cells"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode cells status response: %v", err)
	}

	out := make(map[string]cellStatusSnapshot, len(resp.Cells))
	for _, cell := range resp.Cells {
		out[cell.CellID] = cell
	}

	return out
}

func fanInAndProcess(t *testing.T, ctx context.Context, globalRepos *dal.SQLRepositories, cells []*integrationCell) (catalog.FanInResult, cell.CatalogInboxProcessResult) {
	t.Helper()

	sources := make([]catalog.FanInSource, 0, len(cells))
	for _, c := range cells {
		sources = append(sources, catalog.FanInSource{
			CellID: c.id,
			Events: c.repos.CatalogEvents(),
			Backfill: catalog.NewBackfillProcessor(
				c.id,
				c.repos.CatalogStatusBackfill(),
				cell.NewCatalogEventPublisher(c.id, c.repos.CatalogEvents()),
			),
		})
	}

	fanIn := catalog.NewFanInProcessor(globalRepos.CatalogEvents(), sources)
	fanInResult, err := fanIn.IngestPending(ctx, 100)
	if err != nil {
		t.Fatalf("catalog fan-in: %v", err)
	}

	inbox := cell.NewCatalogInboxProcessor(globalRepos.CatalogEvents(), globalRepos.Runs())
	processResult, err := inbox.ProcessPending(ctx, 100)
	if err != nil {
		t.Fatalf("process global catalog inbox: %v", err)
	}

	return fanInResult, processResult
}
