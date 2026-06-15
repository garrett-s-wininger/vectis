package perf_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apipb "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/action/actionregistry"
	"vectis/internal/action/builtins"
	"vectis/internal/api"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/logserver"
	"vectis/internal/migrations"
	"vectis/internal/observability"
	"vectis/internal/orchestrator"
	"vectis/internal/queue"
	"vectis/internal/taskgraph"

	_ "vectis/internal/dbdrivers"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMacroTriggerClients = 4
	defaultMacroWorkers        = 4
	macroOrchestratorBufSize   = 1024 * 1024

	envMacroDatabaseDriver       = "VECTIS_PERF_DATABASE_DRIVER"
	envMacroDatabaseDSN          = "VECTIS_PERF_DATABASE_DSN"
	envMacroDatabaseMaxOpenConns = "VECTIS_PERF_DATABASE_MAX_OPEN_CONNS"
	envMacroDatabaseMaxIdleConns = "VECTIS_PERF_DATABASE_MAX_IDLE_CONNS"
	envMacroFanoutWidths         = "VECTIS_PERF_FANOUT_WIDTHS"
	envMacroPGStatStatements     = "VECTIS_PERF_PG_STAT_STATEMENTS"
	envMacroPGStatStatementsOut  = "VECTIS_PERF_PG_STAT_STATEMENTS_OUTPUT"
	envMacroStatusReadClients    = "VECTIS_PERF_STATUS_READ_CLIENTS"
	envMacroStatusReadsPerRun    = "VECTIS_PERF_STATUS_READS_PER_RUN"

	macroPGStatStatementsTopLimit = 12
	macroPGStatQueryMaxLen        = 240
)

var macroJobSequence atomic.Uint64

type noopLogClient struct{}

func (noopLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return noopLogStream{}, nil
}

func (noopLogClient) Close() error {
	return nil
}

type noopLogStream struct{}

func (noopLogStream) Send(*apipb.LogChunk) error {
	return nil
}

func (noopLogStream) CloseSend() error {
	return nil
}

type macroBenchEnv struct {
	handler           http.Handler
	queue             macroWorkerQueue
	apiQueue          interfaces.QueueService
	runs              dal.RunsRepository
	choreo            macroChoreography
	log               interfaces.Logger
	db                *sql.DB
	dbDriver          string
	artifactPublisher action.ArtifactPublisher
	actionResolver    actionregistry.Resolver
	actionLocks       []actionregistry.ActionLock
}

type macroWorkerQueue interface {
	TryDequeue(context.Context, *apipb.DequeueRequest) (*apipb.JobRequest, error)
	Ack(context.Context, *apipb.AckRequest) (*apipb.Empty, error)
}

type macroChoreography interface {
	LoadRun(context.Context, *apipb.JobRequest) error
	ClaimAndStartExecution(context.Context, string, string, string, time.Time) (dal.ExecutionClaimResult, error)
	CompleteExecution(context.Context, string, string, string, string, string, string, string) (dal.ExecutionFinalizationResult, error)
	DBBacked() bool
}

type macroSQLChoreography struct {
	runs dal.RunsRepository
}

func (c macroSQLChoreography) LoadRun(context.Context, *apipb.JobRequest) error {
	return nil
}

func (c macroSQLChoreography) ClaimAndStartExecution(ctx context.Context, runID, executionID, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	claim, err := c.runs.TryClaimExecution(ctx, executionID, owner, leaseUntil)
	if err != nil || !claim.Claimed {
		return claim, err
	}

	if claim.TransitionedToAccepted {
		if err := c.runs.MarkExecutionStarted(ctx, executionID); err != nil {
			return dal.ExecutionClaimResult{}, err
		}
		claim.ExecutionStarted = true
	}

	return claim, nil
}

func (c macroSQLChoreography) CompleteExecution(ctx context.Context, runID, executionID, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	return c.runs.CompleteExecutionAndFinalizeRunByClaim(ctx, executionID, owner, claimToken, status, failureCode, reason)
}

func (c macroSQLChoreography) DBBacked() bool {
	return true
}

type macroInProcessOrchestratorChoreography struct {
	service *orchestrator.Service
}

func (c macroInProcessOrchestratorChoreography) LoadRun(ctx context.Context, req *apipb.JobRequest) error {
	spec, err := orchestrator.RunSpecFromJobRequest(req)
	if err != nil {
		return err
	}

	_, err = c.service.LoadRun(ctx, spec)
	return err
}

func (c macroInProcessOrchestratorChoreography) ClaimAndStartExecution(ctx context.Context, runID, executionID, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	return c.service.ClaimExecution(ctx, runID, executionID, owner, leaseUntil)
}

func (c macroInProcessOrchestratorChoreography) CompleteExecution(ctx context.Context, runID, executionID, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	return c.service.CompleteExecutionByClaim(ctx, runID, executionID, owner, claimToken, status, failureCode, reason)
}

func (c macroInProcessOrchestratorChoreography) DBBacked() bool {
	return false
}

type macroGRPCOrchestratorChoreography struct {
	client apipb.OrchestratorServiceClient
}

func (c macroGRPCOrchestratorChoreography) LoadRun(ctx context.Context, req *apipb.JobRequest) error {
	spec, err := orchestrator.RunSpecFromJobRequest(req)
	if err != nil {
		return err
	}

	tasks := make([]*apipb.OrchestratorTaskSpec, 0, len(spec.Tasks))
	for _, task := range spec.Tasks {
		tasks = append(tasks, &apipb.OrchestratorTaskSpec{
			TaskKey:       macroString(task.TaskKey),
			ParentTaskKey: macroString(task.ParentTaskKey),
			Name:          macroString(task.Name),
			CellId:        macroString(task.CellID),
			ChildTaskKeys: append([]string(nil), task.ChildTaskKeys...),
		})
	}

	_, err = c.client.LoadRun(ctx, &apipb.LoadRunRequest{
		RunId:  macroString(spec.RunID),
		Root:   macroTaskExecutionToProto(spec.Root),
		CellId: macroString(spec.CellID),
		Tasks:  tasks,
	})
	return err
}

func (c macroGRPCOrchestratorChoreography) ClaimAndStartExecution(ctx context.Context, runID, executionID, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	claim, err := c.client.ClaimExecution(ctx, &apipb.ClaimExecutionRequest{
		RunId:              macroString(runID),
		ExecutionId:        macroString(executionID),
		Owner:              macroString(owner),
		LeaseUntilUnixNano: macroInt64(leaseUntil.UnixNano()),
	})
	if err != nil {
		return dal.ExecutionClaimResult{}, err
	}

	return dal.ExecutionClaimResult{
		Claimed:                claim.GetClaimed(),
		ClaimToken:             claim.GetClaimToken(),
		TransitionedToAccepted: claim.GetTransitionedToAccepted(),
		ExecutionStarted:       claim.GetExecutionStarted(),
	}, nil
}

func (c macroGRPCOrchestratorChoreography) CompleteExecution(ctx context.Context, runID, executionID, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	result, err := c.client.CompleteExecution(ctx, &apipb.CompleteExecutionRequest{
		RunId:       macroString(runID),
		ExecutionId: macroString(executionID),
		Owner:       macroString(owner),
		ClaimToken:  macroString(claimToken),
		Status:      macroString(status),
		FailureCode: macroString(failureCode),
		Reason:      macroString(reason),
	})
	if err != nil {
		return dal.ExecutionFinalizationResult{}, err
	}

	return dal.ExecutionFinalizationResult{
		ExecutionID: result.GetExecutionId(),
		RunID:       result.GetRunId(),
		Outcome:     dal.ExecutionFinalizationOutcome(result.GetOutcome()),
		Summary:     macroRunTaskCompletionFromProto(result.GetSummary()),
		Children:    macroTaskExecutionsFromProto(result.GetChildren()),
		Activated:   int(result.GetActivated()),
	}, nil
}

func (c macroGRPCOrchestratorChoreography) DBBacked() bool {
	return false
}

type macroLogBenchEnv struct {
	macroBenchEnv
	store   *logserver.LocalRunLogStore
	logSink storeLogClient
	job     macroJobSpec
}

type macroBenchEnvFactory func(*testing.B, []macroJobSpec) macroBenchEnv

type macroRunTimings struct {
	runID                       string
	httpAcceptedToQueueAccepted int64
	queueAcceptedToDequeued     int64
	dequeuedToClaimed           int64
	claimedToTerminal           int64
	acceptedToTerminal          int64
	triggerToTerminal           int64
	logFlush                    int64
	db                          macroDBTimings
}

type macroDBTimings struct {
	createRun                 int64
	attachEnvelope            int64
	touchDispatched           int64
	tryClaimExecution         int64
	markExecutionStarted      int64
	finalizeExecution         int64
	hotStateOwner             int64
	choreographyLoadRun       int64
	choreographyClaimAndStart int64
	choreographyFinalize      int64
}

type macroTriggerInfo struct {
	runID          string
	triggerStart   time.Time
	httpAcceptedAt time.Time
}

type macroStatusReadMode string

const (
	macroStatusReadAPI           macroStatusReadMode = "api"
	macroStatusReadHotOwnerProbe macroStatusReadMode = "hot_owner_probe"
)

type macroStatusReadJob struct {
	runID string
}

type macroStatusReadResult struct {
	latency      int64
	ownerChecked bool
	ownerFound   bool
	err          error
}

func macroBenchmarkEnvInt(b *testing.B, name string, defaultValue int) int {
	b.Helper()

	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		b.Fatalf("%s must be a positive integer, got %q", name, raw)
	}

	return value
}

func macroBenchmarkTriggerClients(b *testing.B) int {
	return macroBenchmarkEnvInt(b, "VECTIS_PERF_TRIGGER_CLIENTS", defaultMacroTriggerClients)
}

func macroBenchmarkWorkers(b *testing.B) int {
	return macroBenchmarkEnvInt(b, "VECTIS_PERF_WORKERS", defaultMacroWorkers)
}

func macroBenchmarkStatusReadClients(b *testing.B) int {
	return macroBenchmarkOptionalEnvInt(b, envMacroStatusReadClients, defaultMacroTriggerClients)
}

func macroBenchmarkStatusReadsPerRun(b *testing.B) int {
	return macroBenchmarkOptionalEnvInt(b, envMacroStatusReadsPerRun, 4)
}

func macroBenchmarkFanoutWidths(b *testing.B) []int {
	b.Helper()

	raw := os.Getenv(envMacroFanoutWidths)
	if strings.TrimSpace(raw) == "" {
		return []int{1, 10, 100}
	}

	parts := strings.Split(raw, ",")
	widths := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		width, err := strconv.Atoi(part)
		if err != nil || width <= 0 {
			b.Fatalf("%s must be a comma-separated list of positive integers, got %q", envMacroFanoutWidths, raw)
		}

		widths = append(widths, width)
	}

	if len(widths) == 0 {
		b.Fatalf("%s must include at least one positive integer, got %q", envMacroFanoutWidths, raw)
	}

	return widths
}

type macroDatabaseConfig struct {
	driver       string
	dsn          string
	maxOpenConns int
	maxIdleConns int
}

func macroDatabaseConfigFromEnv(b *testing.B) macroDatabaseConfig {
	b.Helper()

	driver := os.Getenv(envMacroDatabaseDriver)
	if driver == "" {
		driver = "sqlite3"
	}

	switch driver {
	case "sqlite3":
		dsn := os.Getenv(envMacroDatabaseDSN)
		if dsn == "" {
			dsn = ":memory:"
		}

		maxOpen := macroBenchmarkOptionalEnvInt(b, envMacroDatabaseMaxOpenConns, 1)
		maxIdle := macroBenchmarkOptionalEnvInt(b, envMacroDatabaseMaxIdleConns, 1)
		return macroDatabaseConfig{
			driver:       driver,
			dsn:          dsn,
			maxOpenConns: maxOpen,
			maxIdleConns: min(maxIdle, maxOpen),
		}
	case "pgx":
		dsn := os.Getenv(envMacroDatabaseDSN)
		if dsn == "" {
			b.Fatalf("%s is required when %s=pgx", envMacroDatabaseDSN, envMacroDatabaseDriver)
		}

		maxOpen := macroBenchmarkOptionalEnvInt(b, envMacroDatabaseMaxOpenConns, 32)
		maxIdle := macroBenchmarkOptionalEnvInt(b, envMacroDatabaseMaxIdleConns, min(16, maxOpen))
		return macroDatabaseConfig{
			driver:       driver,
			dsn:          dsn,
			maxOpenConns: maxOpen,
			maxIdleConns: min(maxIdle, maxOpen),
		}
	default:
		b.Fatalf("%s must be sqlite3 or pgx, got %q", envMacroDatabaseDriver, driver)
		return macroDatabaseConfig{}
	}
}

func macroBenchmarkOptionalEnvInt(b *testing.B, name string, defaultValue int) int {
	b.Helper()

	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		b.Fatalf("%s must be a positive integer, got %q", name, raw)
	}

	return value
}

func resetMacroDBStats(b *testing.B, env macroBenchEnv) bool {
	b.Helper()

	if !macroPGStatStatementsEnabled(env) {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := env.db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"); err != nil {
		emitMacroPGStatLine(b, "# pg_stat_statements benchmark=%s iterations=%d unavailable create_extension=%q", b.Name(), b.N, err)
		return false
	}

	if _, err := env.db.ExecContext(ctx, "SELECT pg_stat_statements_reset()"); err != nil {
		emitMacroPGStatLine(b, "# pg_stat_statements benchmark=%s iterations=%d unavailable reset=%q", b.Name(), b.N, err)
		return false
	}

	return true
}

func reportMacroDBStats(b *testing.B, env macroBenchEnv, enabled bool) {
	b.Helper()

	if !enabled {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := env.db.QueryContext(ctx, `
		SELECT query, calls, total_exec_time, mean_exec_time, rows
		FROM pg_stat_statements
		WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
		  AND query NOT LIKE '%pg_stat_statements%'
		ORDER BY total_exec_time DESC
		LIMIT $1
	`, macroPGStatStatementsTopLimit)

	if err != nil {
		emitMacroPGStatLine(b, "# pg_stat_statements benchmark=%s iterations=%d unavailable query=%q", b.Name(), b.N, err)
		return
	}
	defer rows.Close()

	rank := 0
	for rows.Next() {
		var query string
		var calls int64
		var totalMS float64
		var meanMS float64
		var returnedRows int64
		if err := rows.Scan(&query, &calls, &totalMS, &meanMS, &returnedRows); err != nil {
			emitMacroPGStatLine(b, "# pg_stat_statements benchmark=%s iterations=%d unavailable scan=%q", b.Name(), b.N, err)
			return
		}

		rank++
		emitMacroPGStatLine(
			b,
			"# pg_stat_statements benchmark=%s iterations=%d rank=%d calls=%d total_ms=%.3f mean_ms=%.3f rows=%d query=%q",
			b.Name(),
			b.N,
			rank,
			calls,
			totalMS,
			meanMS,
			returnedRows,
			compactMacroSQL(query),
		)
	}

	if err := rows.Err(); err != nil {
		emitMacroPGStatLine(b, "# pg_stat_statements benchmark=%s iterations=%d unavailable rows=%q", b.Name(), b.N, err)
	}
}

func emitMacroPGStatLine(b *testing.B, format string, args ...any) {
	b.Helper()

	line := fmt.Sprintf(format, args...)
	path := strings.TrimSpace(os.Getenv(envMacroPGStatStatementsOut))
	if path == "" {
		b.Log(line)
		return
	}

	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			b.Logf("prepare pg_stat_statements output %s: %v", path, err)
			b.Log(line)
			return
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		b.Logf("write pg_stat_statements output %s: %v", path, err)
		b.Log(line)
		return
	}
	defer file.Close()

	if _, err := fmt.Fprintln(file, line); err != nil {
		b.Logf("write pg_stat_statements output %s: %v", path, err)
		b.Log(line)
	}
}

func macroPGStatStatementsEnabled(env macroBenchEnv) bool {
	if env.db == nil || env.dbDriver != "pgx" {
		return false
	}

	switch strings.ToLower(strings.TrimSpace(os.Getenv(envMacroPGStatStatements))) {
	case "", "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func compactMacroSQL(query string) string {
	query = strings.Join(strings.Fields(query), " ")
	if len(query) <= macroPGStatQueryMaxLen {
		return query
	}

	return query[:macroPGStatQueryMaxLen-3] + "..."
}

func BenchmarkMacro_APIQueueWorker_TriggerToTerminal(b *testing.B) {
	benchmarkMacroAPIQueueWorkerTriggerToTerminalWithJobAndEnv(b, noopMacroJob(), newMacroBenchEnv)
}

func BenchmarkMacro_OrchestratorAPIQueueWorker_TriggerToTerminal(b *testing.B) {
	benchmarkMacroAPIQueueWorkerTriggerToTerminalWithJobAndEnv(b, noopMacroJob(), newMacroInProcessOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCAPIQueueWorker_TriggerToTerminal(b *testing.B) {
	benchmarkMacroAPIQueueWorkerTriggerToTerminalWithJobAndEnv(b, noopMacroJob(), newMacroGRPCOrchestratorBenchEnv)
}

func BenchmarkMacro_ResultActionAPIQueueWorker_TriggerToTerminal(b *testing.B) {
	benchmarkMacroAPIQueueWorkerTriggerToTerminalWithJobAndEnv(b, resultMacroJob(), newMacroBenchEnv)
}

func BenchmarkMacro_OrchestratorResultActionAPIQueueWorker_TriggerToTerminal(b *testing.B) {
	benchmarkMacroAPIQueueWorkerTriggerToTerminalWithJobAndEnv(b, resultMacroJob(), newMacroInProcessOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCResultActionAPIQueueWorker_TriggerToTerminal(b *testing.B) {
	benchmarkMacroAPIQueueWorkerTriggerToTerminalWithJobAndEnv(b, resultMacroJob(), newMacroGRPCOrchestratorBenchEnv)
}

func benchmarkMacroAPIQueueWorkerTriggerToTerminalWithJobAndEnv(b *testing.B, macroJob macroJobSpec, newEnv macroBenchEnvFactory) {
	b.Helper()

	ctx := context.Background()
	macroJob = uniqueMacroJob(macroJob)
	env := newEnv(b, []macroJobSpec{macroJob})
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()

	acceptedToQueueSamples := make([]int64, 0, b.N)
	queueToDequeuedSamples := make([]int64, 0, b.N)
	dequeuedToClaimedSamples := make([]int64, 0, b.N)
	claimedToTerminalSamples := make([]int64, 0, b.N)
	acceptedToTerminalSamples := make([]int64, 0, b.N)
	triggerToTerminalSamples := make([]int64, 0, b.N)
	dbTimingSamples := make([]macroDBTimings, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		timings := runMacroTriggerToTerminal(b, ctx, env, macroJob.id, noopLogClient{}, i)
		acceptedToQueueSamples = append(acceptedToQueueSamples, timings.httpAcceptedToQueueAccepted)
		queueToDequeuedSamples = append(queueToDequeuedSamples, timings.queueAcceptedToDequeued)
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToTerminalSamples = append(claimedToTerminalSamples, timings.claimedToTerminal)
		acceptedToTerminalSamples = append(acceptedToTerminalSamples, timings.acceptedToTerminal)
		triggerToTerminalSamples = append(triggerToTerminalSamples, timings.triggerToTerminal)
		dbTimingSamples = append(dbTimingSamples, timings.db)
	}

	b.StopTimer()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	reportLatencyMetrics(b, "accepted_to_queue", acceptedToQueueSamples)
	reportLatencyMetrics(b, "queue_to_dequeued", queueToDequeuedSamples)
	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_terminal", claimedToTerminalSamples)
	reportLatencyMetrics(b, "accepted_to_terminal", acceptedToTerminalSamples)
	reportLatencyMetrics(b, "trigger_to_terminal", triggerToTerminalSamples)
	reportMacroDBTimingMetrics(b, dbTimingSamples)

	if total := sumNanoseconds(triggerToTerminalSamples); total > 0 {
		b.ReportMetric(float64(b.N)/(float64(total)/float64(time.Second)), "terminal_runs/s")
	}
}

func BenchmarkMacro_ConcurrentNoop_TriggerToTerminal(b *testing.B) {
	runMacroConcurrentTriggerToTerminalBenchmarkWithJobAndEnv(b, noopMacroJob(), newMacroBenchEnv)
}

func BenchmarkMacro_OrchestratorConcurrentNoop_TriggerToTerminal(b *testing.B) {
	runMacroConcurrentTriggerToTerminalBenchmarkWithJobAndEnv(b, noopMacroJob(), newMacroInProcessOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCConcurrentNoop_TriggerToTerminal(b *testing.B) {
	runMacroConcurrentTriggerToTerminalBenchmarkWithJobAndEnv(b, noopMacroJob(), newMacroGRPCOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCConcurrentNoop_TriggerToTerminalWithAPIStatusReads(b *testing.B) {
	runMacroConcurrentTriggerToTerminalBenchmarkWithStatusReads(b, noopMacroJob(), newMacroGRPCOrchestratorBenchEnv, macroStatusReadAPI)
}

func BenchmarkMacro_OrchestratorGRPCConcurrentNoop_TriggerToTerminalWithHotOwnerStatusReads(b *testing.B) {
	runMacroConcurrentTriggerToTerminalBenchmarkWithStatusReads(b, noopMacroJob(), newMacroGRPCOrchestratorBenchEnv, macroStatusReadHotOwnerProbe)
}

func BenchmarkMacro_ResultActionConcurrent_TriggerToTerminal(b *testing.B) {
	runMacroConcurrentTriggerToTerminalBenchmarkWithJobAndEnv(b, resultMacroJob(), newMacroBenchEnv)
}

func BenchmarkMacro_OrchestratorResultActionConcurrent_TriggerToTerminal(b *testing.B) {
	runMacroConcurrentTriggerToTerminalBenchmarkWithJobAndEnv(b, resultMacroJob(), newMacroInProcessOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCResultActionConcurrent_TriggerToTerminal(b *testing.B) {
	runMacroConcurrentTriggerToTerminalBenchmarkWithJobAndEnv(b, resultMacroJob(), newMacroGRPCOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCConcurrentE2ECanonicalLocal_TriggerToTerminal(b *testing.B) {
	runMacroConcurrentTriggerToTerminalBenchmarkWithJobAndEnv(b, e2eCanonicalLocalMacroJob(b), newMacroGRPCOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCConcurrentE2ECanonicalDistributed_TriggerToTerminal(b *testing.B) {
	runMacroDistributedTriggerToTerminalBenchmarkWithJobAndEnv(b, e2eCanonicalDistributedMacroJob(b), newMacroGRPCOrchestratorBenchEnv)
}

func runMacroConcurrentTriggerToTerminalBenchmarkWithJobAndEnv(b *testing.B, macroJob macroJobSpec, newEnv macroBenchEnvFactory) {
	b.Helper()

	triggerClients := macroBenchmarkTriggerClients(b)
	workerCount := macroBenchmarkWorkers(b)

	ctx := context.Background()
	macroJob = uniqueMacroJob(macroJob)
	env := newEnv(b, []macroJobSpec{macroJob})
	totalRuns := b.N
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()

	b.ReportAllocs()
	b.ResetTimer()

	triggerRegistry := newMacroTriggerRegistry(totalRuns)
	workCtx, cancel := context.WithCancel(ctx)
	resultCh := make(chan macroWorkerResult, totalRuns)
	waitWorkers := startMacroWorkers(workCtx, env, triggerRegistry, workerCount, resultCh)
	defer func() {
		cancel()
		waitWorkers()
	}()

	workStart := time.Now()
	_, triggerDuration := triggerMacroBurst(b, ctx, env.handler, macroJob.id, triggerClients, totalRuns, triggerRegistry.add)
	triggerDone := time.Now()
	results := collectMacroWorkerResults(b, resultCh, totalRuns)
	terminalDone := time.Now()

	b.StopTimer()
	cancel()
	waitWorkers()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), totalRuns)

	acceptedToQueueSamples := make([]int64, 0, len(results))
	queueToDequeuedSamples := make([]int64, 0, len(results))
	dequeuedToClaimedSamples := make([]int64, 0, len(results))
	claimedToTerminalSamples := make([]int64, 0, len(results))
	acceptedToTerminalSamples := make([]int64, 0, len(results))
	triggerToTerminalSamples := make([]int64, 0, len(results))
	logFlushSamples := make([]int64, 0, len(results))
	dbTimingSamples := make([]macroDBTimings, 0, len(results))

	for _, timings := range results {
		acceptedToQueueSamples = append(acceptedToQueueSamples, timings.httpAcceptedToQueueAccepted)
		queueToDequeuedSamples = append(queueToDequeuedSamples, timings.queueAcceptedToDequeued)
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToTerminalSamples = append(claimedToTerminalSamples, timings.claimedToTerminal)
		acceptedToTerminalSamples = append(acceptedToTerminalSamples, timings.acceptedToTerminal)
		triggerToTerminalSamples = append(triggerToTerminalSamples, timings.triggerToTerminal)
		logFlushSamples = append(logFlushSamples, timings.logFlush)
		dbTimingSamples = append(dbTimingSamples, timings.db)
	}

	reportLatencyMetrics(b, "accepted_to_queue", acceptedToQueueSamples)
	reportLatencyMetrics(b, "queue_to_dequeued", queueToDequeuedSamples)
	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_terminal", claimedToTerminalSamples)
	reportLatencyMetrics(b, "accepted_to_terminal", acceptedToTerminalSamples)
	reportLatencyMetrics(b, "trigger_to_terminal", triggerToTerminalSamples)
	reportLatencyMetrics(b, "log_flush", logFlushSamples)
	reportMacroDBTimingMetrics(b, dbTimingSamples)
	reportMacroArtifactMetrics(b, env.artifactPublisher, totalRuns)

	if triggerDuration > 0 {
		b.ReportMetric(float64(totalRuns)/triggerDuration.Seconds(), "accepted_requests/s")
	}

	if terminalDuration := terminalDone.Sub(workStart); terminalDuration > 0 {
		b.ReportMetric(float64(totalRuns)/terminalDuration.Seconds(), "terminal_runs/s")
	}

	b.ReportMetric(float64(triggerDone.Sub(workStart))/float64(time.Millisecond), "trigger_burst_ms")
	b.ReportMetric(float64(terminalDone.Sub(triggerDone))/float64(time.Millisecond), "terminal_drain_ms")
	b.ReportMetric(float64(triggerClients), "trigger_clients")
	b.ReportMetric(float64(workerCount), "worker_count")
	b.ReportMetric(float64(totalRuns), "total_runs")
}

func runMacroDistributedTriggerToTerminalBenchmarkWithJobAndEnv(b *testing.B, macroJob macroJobSpec, newEnv macroBenchEnvFactory) {
	b.Helper()

	triggerClients := macroBenchmarkTriggerClients(b)
	workerCount := macroBenchmarkWorkers(b)

	ctx := context.Background()
	macroJob = uniqueMacroJob(macroJob)
	env := newEnv(b, []macroJobSpec{macroJob})
	totalRuns := b.N
	expectedTasksPerRun := macroJob.expectedTasks
	if expectedTasksPerRun <= 0 {
		expectedTasksPerRun = 1
	}
	totalTasks := totalRuns * expectedTasksPerRun
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()

	b.ReportAllocs()
	b.ResetTimer()

	triggerRegistry := newMacroTriggerRegistry(totalRuns)
	workCtx, cancel := context.WithCancel(ctx)
	taskCh := make(chan macroDistributedTaskResult, totalTasks+workerCount)
	terminalCh := make(chan macroDistributedTerminalResult, totalRuns)
	waitWorkers := startMacroDistributedWorkers(workCtx, env, triggerRegistry, workerCount, taskCh, terminalCh)
	defer func() {
		cancel()
		waitWorkers()
	}()

	workStart := time.Now()
	_, triggerDuration := triggerMacroBurst(b, ctx, env.handler, macroJob.id, triggerClients, totalRuns, triggerRegistry.add)
	triggerDone := time.Now()
	terminalResults := collectMacroDistributedTerminalResults(b, terminalCh, totalRuns)
	taskResults := collectMacroDistributedTaskResults(b, taskCh, totalTasks)
	workDone := time.Now()

	b.StopTimer()
	cancel()
	waitWorkers()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), totalRuns)

	acceptedToTerminalSamples := make([]int64, 0, len(terminalResults))
	triggerToTerminalSamples := make([]int64, 0, len(terminalResults))
	taskQueueToDequeuedSamples := make([]int64, 0, len(taskResults))
	taskDequeuedToClaimedSamples := make([]int64, 0, len(taskResults))
	taskClaimedToFinalizedSamples := make([]int64, 0, len(taskResults))
	taskLogFlushSamples := make([]int64, 0, len(taskResults))
	dbTimingSamples := make([]macroDBTimings, 0, len(taskResults))
	taskChildEnqueueSamples := make([]int64, 0, len(taskResults))
	childEnqueues := 0
	activatedChildren := 0

	for _, timings := range terminalResults {
		acceptedToTerminalSamples = append(acceptedToTerminalSamples, timings.acceptedToTerminal)
		triggerToTerminalSamples = append(triggerToTerminalSamples, timings.triggerToTerminal)
	}

	for _, result := range taskResults {
		taskQueueToDequeuedSamples = append(taskQueueToDequeuedSamples, result.timings.queueAcceptedToDequeued)
		taskDequeuedToClaimedSamples = append(taskDequeuedToClaimedSamples, result.timings.dequeuedToClaimed)
		taskClaimedToFinalizedSamples = append(taskClaimedToFinalizedSamples, result.timings.claimedToTerminal)
		taskLogFlushSamples = append(taskLogFlushSamples, result.timings.logFlush)
		dbTimingSamples = append(dbTimingSamples, result.timings.db)
		taskChildEnqueueSamples = append(taskChildEnqueueSamples, int64(result.childEnqueue))
		childEnqueues += result.childEnqueue
		activatedChildren += result.activated
	}

	reportLatencyMetrics(b, "accepted_to_terminal", acceptedToTerminalSamples)
	reportLatencyMetrics(b, "trigger_to_terminal", triggerToTerminalSamples)
	reportLatencyMetrics(b, "task_queue_to_dequeued", taskQueueToDequeuedSamples)
	reportLatencyMetrics(b, "task_dequeued_to_claimed", taskDequeuedToClaimedSamples)
	reportLatencyMetrics(b, "task_claimed_to_finalized", taskClaimedToFinalizedSamples)
	reportLatencyMetrics(b, "task_log_flush", taskLogFlushSamples)
	reportCountMetrics(b, "task_child_enqueues", taskChildEnqueueSamples)
	reportMacroDBTimingMetrics(b, dbTimingSamples)
	reportMacroArtifactMetrics(b, env.artifactPublisher, totalRuns)

	if triggerDuration > 0 {
		b.ReportMetric(float64(totalRuns)/triggerDuration.Seconds(), "accepted_requests/s")
	}

	if duration := workDone.Sub(workStart); duration > 0 {
		b.ReportMetric(float64(totalRuns)/duration.Seconds(), "terminal_runs/s")
		b.ReportMetric(float64(len(taskResults))/duration.Seconds(), "task_executions/s")
	}

	b.ReportMetric(float64(triggerDone.Sub(workStart))/float64(time.Millisecond), "trigger_burst_ms")
	b.ReportMetric(float64(workDone.Sub(triggerDone))/float64(time.Millisecond), "terminal_drain_ms")
	b.ReportMetric(float64(triggerClients), "trigger_clients")
	b.ReportMetric(float64(workerCount), "worker_count")
	b.ReportMetric(float64(totalRuns), "total_runs")
	b.ReportMetric(float64(len(taskResults)), "total_task_executions")
	b.ReportMetric(float64(expectedTasksPerRun), "expected_task_executions/run")
	if totalRuns > 0 {
		b.ReportMetric(float64(len(taskResults))/float64(totalRuns), "task_executions/run")
		b.ReportMetric(float64(childEnqueues)/float64(totalRuns), "child_enqueues/run")
		b.ReportMetric(float64(activatedChildren)/float64(totalRuns), "activated_children/run")
	}
}

func runMacroConcurrentTriggerToTerminalBenchmarkWithStatusReads(
	b *testing.B,
	macroJob macroJobSpec,
	newEnv macroBenchEnvFactory,
	readMode macroStatusReadMode,
) {
	b.Helper()

	triggerClients := macroBenchmarkTriggerClients(b)
	workerCount := macroBenchmarkWorkers(b)
	statusReadClients := macroBenchmarkStatusReadClients(b)
	statusReadsPerRun := macroBenchmarkStatusReadsPerRun(b)

	ctx := context.Background()
	macroJob = uniqueMacroJob(macroJob)
	env := newEnv(b, []macroJobSpec{macroJob})
	totalRuns := b.N
	totalStatusReads := totalRuns * statusReadsPerRun
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()

	b.ReportAllocs()
	b.ResetTimer()

	triggerRegistry := newMacroTriggerRegistry(totalRuns)
	statusReadJobs := make(chan macroStatusReadJob, totalStatusReads)
	statusReadResults := make(chan macroStatusReadResult, totalStatusReads)
	var closeStatusReadJobs sync.Once
	closeStatusJobs := func() {
		closeStatusReadJobs.Do(func() { close(statusReadJobs) })
	}
	workCtx, cancel := context.WithCancel(ctx)
	waitReaders := startMacroStatusReaders(workCtx, env, readMode, statusReadClients, statusReadJobs, statusReadResults)
	resultCh := make(chan macroWorkerResult, totalRuns)
	waitWorkers := startMacroWorkers(workCtx, env, triggerRegistry, workerCount, resultCh)
	defer func() {
		cancel()
		closeStatusJobs()
		waitReaders()
		waitWorkers()
	}()

	workStart := time.Now()
	_, triggerDuration := triggerMacroBurst(b, ctx, env.handler, macroJob.id, triggerClients, totalRuns, func(info macroTriggerInfo) {
		triggerRegistry.add(info)
		for i := 0; i < statusReadsPerRun; i++ {
			statusReadJobs <- macroStatusReadJob{runID: info.runID}
		}
	})
	closeStatusJobs()
	triggerDone := time.Now()
	results := collectMacroWorkerResults(b, resultCh, totalRuns)
	terminalDone := time.Now()
	statusReadSamples, ownerHits, ownerMisses := collectMacroStatusReadResults(b, statusReadResults, totalStatusReads)
	statusReadsDone := time.Now()

	b.StopTimer()
	cancel()
	waitReaders()
	waitWorkers()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), totalRuns)

	acceptedToQueueSamples := make([]int64, 0, len(results))
	queueToDequeuedSamples := make([]int64, 0, len(results))
	dequeuedToClaimedSamples := make([]int64, 0, len(results))
	claimedToTerminalSamples := make([]int64, 0, len(results))
	acceptedToTerminalSamples := make([]int64, 0, len(results))
	triggerToTerminalSamples := make([]int64, 0, len(results))
	logFlushSamples := make([]int64, 0, len(results))
	dbTimingSamples := make([]macroDBTimings, 0, len(results))

	for _, timings := range results {
		acceptedToQueueSamples = append(acceptedToQueueSamples, timings.httpAcceptedToQueueAccepted)
		queueToDequeuedSamples = append(queueToDequeuedSamples, timings.queueAcceptedToDequeued)
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToTerminalSamples = append(claimedToTerminalSamples, timings.claimedToTerminal)
		acceptedToTerminalSamples = append(acceptedToTerminalSamples, timings.acceptedToTerminal)
		triggerToTerminalSamples = append(triggerToTerminalSamples, timings.triggerToTerminal)
		logFlushSamples = append(logFlushSamples, timings.logFlush)
		dbTimingSamples = append(dbTimingSamples, timings.db)
	}

	reportLatencyMetrics(b, "accepted_to_queue", acceptedToQueueSamples)
	reportLatencyMetrics(b, "queue_to_dequeued", queueToDequeuedSamples)
	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_terminal", claimedToTerminalSamples)
	reportLatencyMetrics(b, "accepted_to_terminal", acceptedToTerminalSamples)
	reportLatencyMetrics(b, "trigger_to_terminal", triggerToTerminalSamples)
	reportLatencyMetrics(b, "log_flush", logFlushSamples)
	reportLatencyMetrics(b, "status_read", statusReadSamples)
	reportMacroDBTimingMetrics(b, dbTimingSamples)

	if triggerDuration > 0 {
		b.ReportMetric(float64(totalRuns)/triggerDuration.Seconds(), "accepted_requests/s")
	}
	if terminalDuration := terminalDone.Sub(workStart); terminalDuration > 0 {
		b.ReportMetric(float64(totalRuns)/terminalDuration.Seconds(), "terminal_runs/s")
	}
	if statusReadDuration := statusReadsDone.Sub(workStart); statusReadDuration > 0 {
		b.ReportMetric(float64(totalStatusReads)/statusReadDuration.Seconds(), "status_reads/s")
	}

	b.ReportMetric(float64(triggerDone.Sub(workStart))/float64(time.Millisecond), "trigger_burst_ms")
	b.ReportMetric(float64(terminalDone.Sub(triggerDone))/float64(time.Millisecond), "terminal_drain_ms")
	b.ReportMetric(float64(statusReadsDone.Sub(workStart))/float64(time.Millisecond), "mixed_workload_ms")
	b.ReportMetric(float64(triggerClients), "trigger_clients")
	b.ReportMetric(float64(workerCount), "worker_count")
	b.ReportMetric(float64(statusReadClients), "status_read_clients")
	b.ReportMetric(float64(statusReadsPerRun), "status_reads/run")
	b.ReportMetric(float64(totalStatusReads), "total_status_reads")
	b.ReportMetric(float64(ownerHits), "status_hot_owner_hits")
	b.ReportMetric(float64(ownerMisses), "status_hot_owner_misses")
	b.ReportMetric(float64(totalRuns), "total_runs")
}

func BenchmarkMacro_APITriggerToQueued(b *testing.B) {
	runMacroAPITriggerToQueuedBenchmark(b)
}

func BenchmarkMacro_DB_CreateAttachTouchQueuedRun(b *testing.B) {
	benchmarkMacroDBCreateAttachTouchQueuedRunWithEnv(b, newMacroBenchEnv)
}

func BenchmarkMacro_DB_EnsurePlannedFanoutTasks(b *testing.B) {
	for _, width := range macroBenchmarkFanoutWidths(b) {
		b.Run(fmt.Sprintf("children_%03d", width), func(b *testing.B) {
			benchmarkMacroDBEnsurePlannedFanoutTasksWithEnv(b, width, newMacroBenchEnv)
		})
	}
}

func BenchmarkMacro_DB_MarkExecutionStarted(b *testing.B) {
	benchmarkMacroDBMarkExecutionStartedWithEnv(b, newMacroBenchEnv)
}

func BenchmarkMacro_DB_GetRun(b *testing.B) {
	benchmarkMacroDBGetRunWithEnv(b, newMacroBenchEnv)
}

func BenchmarkMacro_DB_GetRunHotStateOwner(b *testing.B) {
	benchmarkMacroDBGetRunHotStateOwnerWithEnv(b, true, newMacroBenchEnv)
}

func BenchmarkMacro_DB_GetRunHotStateOwnerMissing(b *testing.B) {
	benchmarkMacroDBGetRunHotStateOwnerWithEnv(b, false, newMacroBenchEnv)
}

func BenchmarkMacro_DB_GetRunWithHotStateOwnerLookup(b *testing.B) {
	benchmarkMacroDBGetRunWithHotStateOwnerLookupWithEnv(b, true, newMacroBenchEnv)
}

func BenchmarkMacro_DB_GetRunAfterMissingHotStateOwnerLookup(b *testing.B) {
	benchmarkMacroDBGetRunWithHotStateOwnerLookupWithEnv(b, false, newMacroBenchEnv)
}

func BenchmarkMacro_DB_CompleteExecutionAndFinalizeRoot(b *testing.B) {
	benchmarkMacroDBCompleteExecutionAndFinalizeRootWithEnv(b, newMacroBenchEnv)
}

func BenchmarkMacro_DB_CompleteExecutionAndActivateFanout(b *testing.B) {
	for _, width := range macroBenchmarkFanoutWidths(b) {
		b.Run(fmt.Sprintf("children_%03d", width), func(b *testing.B) {
			benchmarkMacroDBCompleteExecutionAndActivateFanoutWithEnv(b, width, newMacroBenchEnv)
		})
	}
}

func BenchmarkMacro_OrchestratorDB_CreateAttachLoadTouchQueuedRun(b *testing.B) {
	benchmarkMacroDBCreateAttachTouchQueuedRunWithEnv(b, newMacroInProcessOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCDB_CreateAttachLoadTouchQueuedRun(b *testing.B) {
	benchmarkMacroDBCreateAttachTouchQueuedRunWithEnv(b, newMacroGRPCOrchestratorBenchEnv)
}

func benchmarkMacroDBCreateAttachTouchQueuedRunWithEnv(b *testing.B, newEnv macroBenchEnvFactory) {
	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newEnv(b, []macroJobSpec{macroJob})
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()

	dbTimingSamples := make([]macroDBTimings, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		dbTimingSamples = append(dbTimingSamples, preseedMacroQueuedRunMeasured(b, ctx, env, macroJob, i+1))
	}

	elapsed := time.Since(start)
	b.StopTimer()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	reportMacroDBTimingMetrics(b, dbTimingSamples)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "queued_runs/s")
	}

	b.ReportMetric(float64(b.N), "total_runs")
}

func benchmarkMacroDBEnsurePlannedFanoutTasksWithEnv(b *testing.B, width int, newEnv macroBenchEnvFactory) {
	if width <= 0 {
		b.Fatal("fanout width must be positive")
	}

	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newEnv(b, []macroJobSpec{macroJob})
	runIDs := make([]string, b.N)
	for i := 0; i < b.N; i++ {
		runIDs[i] = createMacroDBBenchmarkRun(b, ctx, env, macroJob.id, i+1)
	}

	b.ReportAllocs()
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()
	b.ResetTimer()
	start := time.Now()

	createdTasks := 0
	for i := 0; i < b.N; i++ {
		for child := 0; child < width; child++ {
			if _, created, err := ensureMacroBenchmarkPlannedChild(ctx, env.runs, runIDs[i], child); err != nil {
				b.Fatalf("ensure planned child %d for run %s: %v", child, runIDs[i], err)
			} else if created {
				createdTasks++
			}
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "runs/s")
		b.ReportMetric(float64(createdTasks)/elapsed.Seconds(), "planned_tasks/s")
	}

	b.ReportMetric(float64(width), "fanout_width")
	b.ReportMetric(float64(createdTasks), "planned_tasks")
}

func benchmarkMacroDBMarkExecutionStartedWithEnv(b *testing.B, newEnv macroBenchEnvFactory) {
	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newEnv(b, []macroJobSpec{macroJob})
	claimed := make([]macroDBClaimedExecution, b.N)
	for i := 0; i < b.N; i++ {
		claimed[i] = prepareMacroDBClaimedRootExecution(b, ctx, env, macroJob.id, i+1)
	}

	b.ReportAllocs()
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		if err := env.runs.MarkExecutionStarted(ctx, claimed[i].executionID); err != nil {
			b.Fatalf("mark execution started %s: %v", claimed[i].executionID, err)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "started_executions/s")
	}

	b.ReportMetric(float64(b.N), "total_executions")
}

func benchmarkMacroDBGetRunWithEnv(b *testing.B, newEnv macroBenchEnvFactory) {
	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newEnv(b, []macroJobSpec{macroJob})
	runID := createMacroDBBenchmarkRun(b, ctx, env, macroJob.id, 1)
	getRunSamples := make([]int64, 0, b.N)

	b.ReportAllocs()
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		getRunStarted := time.Now()
		rec, err := env.runs.GetRun(ctx, runID)
		getRunSamples = append(getRunSamples, time.Since(getRunStarted).Nanoseconds())
		if err != nil {
			b.Fatalf("get run %s: %v", runID, err)
		}
		if rec.RunID != runID {
			b.Fatalf("run id=%q, want %q", rec.RunID, runID)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)
	reportLatencyMetrics(b, "db_get_run", getRunSamples)

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "reads/s")
	}
	b.ReportMetric(float64(b.N), "total_reads")
}

func benchmarkMacroDBGetRunHotStateOwnerWithEnv(b *testing.B, present bool, newEnv macroBenchEnvFactory) {
	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newEnv(b, []macroJobSpec{macroJob})
	runID := createMacroDBBenchmarkRun(b, ctx, env, macroJob.id, 1)
	if present {
		seedMacroRunHotStateOwner(b, ctx, env.runs, runID)
	}
	ownerLookupSamples := make([]int64, 0, b.N)

	b.ReportAllocs()
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		lookupStarted := time.Now()
		owner, found, err := env.runs.GetRunHotStateOwner(ctx, runID)
		ownerLookupSamples = append(ownerLookupSamples, time.Since(lookupStarted).Nanoseconds())
		if err != nil {
			b.Fatalf("get hot-state owner for run %s: %v", runID, err)
		}
		if found != present {
			b.Fatalf("hot-state owner found=%t, want %t", found, present)
		}
		if present && owner.RunID != runID {
			b.Fatalf("owner run id=%q, want %q", owner.RunID, runID)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)
	reportLatencyMetrics(b, "db_hot_owner_lookup", ownerLookupSamples)

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "lookups/s")
	}
	b.ReportMetric(float64(b.N), "total_lookups")
}

func benchmarkMacroDBGetRunWithHotStateOwnerLookupWithEnv(b *testing.B, present bool, newEnv macroBenchEnvFactory) {
	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newEnv(b, []macroJobSpec{macroJob})
	runID := createMacroDBBenchmarkRun(b, ctx, env, macroJob.id, 1)
	if present {
		seedMacroRunHotStateOwner(b, ctx, env.runs, runID)
	}
	ownerLookupSamples := make([]int64, 0, b.N)
	getRunSamples := make([]int64, 0, b.N)
	totalSamples := make([]int64, 0, b.N)

	b.ReportAllocs()
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		totalStarted := time.Now()
		lookupStarted := time.Now()
		_, found, err := env.runs.GetRunHotStateOwner(ctx, runID)
		ownerLookupSamples = append(ownerLookupSamples, time.Since(lookupStarted).Nanoseconds())
		if err != nil {
			b.Fatalf("get hot-state owner for run %s: %v", runID, err)
		}
		if found != present {
			b.Fatalf("hot-state owner found=%t, want %t", found, present)
		}

		getRunStarted := time.Now()
		rec, err := env.runs.GetRun(ctx, runID)
		getRunSamples = append(getRunSamples, time.Since(getRunStarted).Nanoseconds())
		if err != nil {
			b.Fatalf("get run %s: %v", runID, err)
		}
		if rec.RunID != runID {
			b.Fatalf("run id=%q, want %q", rec.RunID, runID)
		}
		totalSamples = append(totalSamples, time.Since(totalStarted).Nanoseconds())
	}

	elapsed := time.Since(start)
	b.StopTimer()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)
	reportLatencyMetrics(b, "db_hot_owner_lookup", ownerLookupSamples)
	reportLatencyMetrics(b, "db_get_run", getRunSamples)
	reportLatencyMetrics(b, "db_read_total", totalSamples)

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "reads/s")
	}
	b.ReportMetric(float64(b.N), "total_reads")
}

func benchmarkMacroDBCompleteExecutionAndFinalizeRootWithEnv(b *testing.B, newEnv macroBenchEnvFactory) {
	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newEnv(b, []macroJobSpec{macroJob})
	claimed := make([]macroDBClaimedExecution, b.N)
	for i := 0; i < b.N; i++ {
		claimed[i] = prepareMacroDBClaimedRootExecution(b, ctx, env, macroJob.id, i+1)
	}

	b.ReportAllocs()
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		result, err := env.runs.CompleteExecutionAndFinalizeRunByClaim(ctx, claimed[i].executionID, claimed[i].owner, claimed[i].claimToken, dal.ExecutionStatusSucceeded, "", "")
		if err != nil {
			b.Fatalf("complete root execution %s: %v", claimed[i].executionID, err)
		}

		if result.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
			b.Fatalf("complete root execution %s outcome %q", claimed[i].executionID, result.Outcome)
		}
	}

	elapsed := time.Since(start)
	b.StopTimer()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "finalized_runs/s")
	}
	b.ReportMetric(float64(b.N), "total_runs")
}

func benchmarkMacroDBCompleteExecutionAndActivateFanoutWithEnv(b *testing.B, width int, newEnv macroBenchEnvFactory) {
	if width <= 0 {
		b.Fatal("fanout width must be positive")
	}

	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newEnv(b, []macroJobSpec{macroJob})
	claimed := make([]macroDBClaimedExecution, b.N)
	for i := 0; i < b.N; i++ {
		claimed[i] = prepareMacroDBClaimedRootExecution(b, ctx, env, macroJob.id, i+1)
		for child := 0; child < width; child++ {
			if _, _, err := ensureMacroBenchmarkPlannedChild(ctx, env.runs, claimed[i].runID, child); err != nil {
				b.Fatalf("ensure planned child %d for run %s: %v", child, claimed[i].runID, err)
			}
		}
	}

	b.ReportAllocs()
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()
	b.ResetTimer()
	start := time.Now()

	activatedTasks := 0
	for i := 0; i < b.N; i++ {
		result, err := env.runs.CompleteExecutionAndFinalizeRunByClaim(ctx, claimed[i].executionID, claimed[i].owner, claimed[i].claimToken, dal.ExecutionStatusSucceeded, "", "")
		if err != nil {
			b.Fatalf("complete fanout root execution %s: %v", claimed[i].executionID, err)
		}
		if result.Outcome != dal.ExecutionFinalizationOutcomeContinued || result.Activated != width || len(result.Children) != width {
			b.Fatalf("fanout root result: outcome=%q activated=%d children=%d width=%d", result.Outcome, result.Activated, len(result.Children), width)
		}
		activatedTasks += result.Activated
	}

	elapsed := time.Since(start)
	b.StopTimer()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "parent_finalizations/s")
		b.ReportMetric(float64(activatedTasks)/elapsed.Seconds(), "activated_tasks/s")
	}
	b.ReportMetric(float64(width), "fanout_width")
	b.ReportMetric(float64(activatedTasks), "activated_tasks")
}

func BenchmarkMacro_WorkerClaimAck(b *testing.B) {
	runMacroWorkerClaimAckBenchmark(b)
}

func BenchmarkMacro_OrchestratorWorkerClaimAck(b *testing.B) {
	runMacroWorkerClaimAckBenchmarkWithWorkersAndEnv(b, macroBenchmarkWorkers(b), newMacroInProcessOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCWorkerClaimAck(b *testing.B) {
	runMacroWorkerClaimAckBenchmarkWithWorkersAndEnv(b, macroBenchmarkWorkers(b), newMacroGRPCOrchestratorBenchEnv)
}

func BenchmarkMacro_WorkerClaimAckComplete(b *testing.B) {
	runMacroWorkerClaimAckCompleteBenchmark(b)
}

func BenchmarkMacro_OrchestratorWorkerClaimAckComplete(b *testing.B) {
	runMacroWorkerClaimAckCompleteBenchmarkWithWorkersJobAndEnv(b, macroBenchmarkWorkers(b), noopMacroJob(), newMacroInProcessOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCWorkerClaimAckComplete(b *testing.B) {
	runMacroWorkerClaimAckCompleteBenchmarkWithWorkersJobAndEnv(b, macroBenchmarkWorkers(b), noopMacroJob(), newMacroGRPCOrchestratorBenchEnv)
}

func BenchmarkMacro_ResultActionWorkerClaimAckComplete(b *testing.B) {
	runMacroWorkerClaimAckCompleteBenchmarkWithWorkersAndJob(b, macroBenchmarkWorkers(b), resultMacroJob())
}

func BenchmarkMacro_OrchestratorResultActionWorkerClaimAckComplete(b *testing.B) {
	runMacroWorkerClaimAckCompleteBenchmarkWithWorkersJobAndEnv(b, macroBenchmarkWorkers(b), resultMacroJob(), newMacroInProcessOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCResultActionWorkerClaimAckComplete(b *testing.B) {
	runMacroWorkerClaimAckCompleteBenchmarkWithWorkersJobAndEnv(b, macroBenchmarkWorkers(b), resultMacroJob(), newMacroGRPCOrchestratorBenchEnv)
}

func BenchmarkMacro_WorkerClaimAckFinalize(b *testing.B) {
	runMacroWorkerClaimAckFinalizeBenchmark(b)
}

func BenchmarkMacro_OrchestratorWorkerClaimAckFinalize(b *testing.B) {
	runMacroWorkerClaimAckFinalizeBenchmarkWithWorkersAndEnv(b, macroBenchmarkWorkers(b), newMacroInProcessOrchestratorBenchEnv)
}

func BenchmarkMacro_OrchestratorGRPCWorkerClaimAckFinalize(b *testing.B) {
	runMacroWorkerClaimAckFinalizeBenchmarkWithWorkersAndEnv(b, macroBenchmarkWorkers(b), newMacroGRPCOrchestratorBenchEnv)
}

func BenchmarkMacro_WorkerScale_ClaimAckComplete(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers_%02d", workers), func(b *testing.B) {
			runMacroWorkerClaimAckCompleteBenchmarkWithWorkers(b, workers)
		})
	}
}

func BenchmarkMacro_OrchestratorWorkerScale_ClaimAckComplete(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers_%02d", workers), func(b *testing.B) {
			runMacroWorkerClaimAckCompleteBenchmarkWithWorkersJobAndEnv(b, workers, noopMacroJob(), newMacroInProcessOrchestratorBenchEnv)
		})
	}
}

func BenchmarkMacro_OrchestratorGRPCWorkerScale_ClaimAckComplete(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers_%02d", workers), func(b *testing.B) {
			runMacroWorkerClaimAckCompleteBenchmarkWithWorkersJobAndEnv(b, workers, noopMacroJob(), newMacroGRPCOrchestratorBenchEnv)
		})
	}
}

func BenchmarkMacro_WorkerScale_ResultActionClaimAckComplete(b *testing.B) {
	for _, workers := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("workers_%02d", workers), func(b *testing.B) {
			runMacroWorkerClaimAckCompleteBenchmarkWithWorkersAndJob(b, workers, resultMacroJob())
		})
	}
}

func BenchmarkMacro_LogHeavy_TriggerToTerminalReplay(b *testing.B) {
	ctx := context.Background()
	env := newMacroLogBenchEnv(b, 200)
	statsEnabled := resetMacroDBStats(b, env.macroBenchEnv)
	dbStatsStart := env.db.Stats()

	acceptedToQueueSamples := make([]int64, 0, b.N)
	queueToDequeuedSamples := make([]int64, 0, b.N)
	dequeuedToClaimedSamples := make([]int64, 0, b.N)
	claimedToTerminalSamples := make([]int64, 0, b.N)
	acceptedToTerminalSamples := make([]int64, 0, b.N)
	triggerToTerminalSamples := make([]int64, 0, b.N)
	logFlushSamples := make([]int64, 0, b.N)
	logReplaySamples := make([]int64, 0, b.N)
	logChunkSamples := make([]int64, 0, b.N)
	dbTimingSamples := make([]macroDBTimings, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		timings, logTimings := runMacroLogHeavyTriggerToTerminal(b, ctx, env, i)
		acceptedToQueueSamples = append(acceptedToQueueSamples, timings.httpAcceptedToQueueAccepted)
		queueToDequeuedSamples = append(queueToDequeuedSamples, timings.queueAcceptedToDequeued)
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToTerminalSamples = append(claimedToTerminalSamples, timings.claimedToTerminal)
		acceptedToTerminalSamples = append(acceptedToTerminalSamples, timings.acceptedToTerminal)
		triggerToTerminalSamples = append(triggerToTerminalSamples, timings.triggerToTerminal)
		logFlushSamples = append(logFlushSamples, logTimings.flush)
		logReplaySamples = append(logReplaySamples, logTimings.replay)
		logChunkSamples = append(logChunkSamples, int64(logTimings.chunks))
		dbTimingSamples = append(dbTimingSamples, timings.db)
	}

	b.StopTimer()
	reportMacroDBStats(b, env.macroBenchEnv, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	reportLatencyMetrics(b, "accepted_to_queue", acceptedToQueueSamples)
	reportLatencyMetrics(b, "queue_to_dequeued", queueToDequeuedSamples)
	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_terminal", claimedToTerminalSamples)
	reportLatencyMetrics(b, "accepted_to_terminal", acceptedToTerminalSamples)
	reportLatencyMetrics(b, "trigger_to_terminal", triggerToTerminalSamples)
	reportLatencyMetrics(b, "log_flush", logFlushSamples)
	reportLatencyMetrics(b, "log_replay", logReplaySamples)
	reportCountMetrics(b, "log_chunks", logChunkSamples)
	reportMacroDBTimingMetrics(b, dbTimingSamples)

	if total := sumNanoseconds(triggerToTerminalSamples); total > 0 {
		b.ReportMetric(float64(b.N)/(float64(total)/float64(time.Second)), "terminal_runs/s")
	}
}

type macroJobSpec struct {
	id                string
	uses              string
	with              map[string]string
	command           string
	definitionJSON    string
	expectedTasks     int
	artifactPublisher action.ArtifactPublisher
	actionResolver    actionregistry.Resolver
	actionLocks       []actionregistry.ActionLock
}

func noopMacroJob() macroJobSpec {
	return macroJobSpec{id: "macro-noop", uses: "builtins/shell", with: map[string]string{"command": "true"}, command: "true"}
}

func resultMacroJob() macroJobSpec {
	return macroJobSpec{id: "macro-result", uses: "builtins/result", with: map[string]string{"success": "true"}}
}

func e2eCanonicalLocalMacroJob(b *testing.B) macroJobSpec {
	return e2eCanonicalMacroJob(b, true)
}

func e2eCanonicalDistributedMacroJob(b *testing.B) macroJobSpec {
	return e2eCanonicalMacroJob(b, false)
}

func e2eCanonicalMacroJob(b *testing.B, localFanout bool) macroJobSpec {
	b.Helper()

	data, err := os.ReadFile(macroRepoPath(b, "examples", "e2e-canonical.json"))
	if err != nil {
		b.Fatalf("read e2e canonical example: %v", err)
	}

	var def apipb.Job
	if err := job.DecodeDefinitionJSON(data, &def); err != nil {
		b.Fatalf("decode e2e canonical example: %v", err)
	}

	adaptE2ECanonicalForPerf(b, &def, localFanout)
	localSource, err := actionregistry.NewLocalManifestSource(macroRepoPath(b, "examples", "actions"))
	if err != nil {
		b.Fatalf("create local example action source: %v", err)
	}

	resolver := actionregistry.NewCompositeResolver(builtins.NewRegistry(), localSource)
	locks, err := actionregistry.ResolveJobActions(&def, resolver)
	if err != nil {
		b.Fatalf("resolve e2e canonical actions: %v", err)
	}

	plan, err := taskgraph.PlanTaskBoundaries(&def, dal.RootTaskKey)
	if err != nil {
		b.Fatalf("plan e2e canonical task boundaries: %v", err)
	}

	body, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(&def)
	if err != nil {
		b.Fatalf("marshal e2e canonical job definition: %v", err)
	}

	return macroJobSpec{
		id:                def.GetId(),
		definitionJSON:    string(body),
		expectedTasks:     1 + len(plan.Entries),
		artifactPublisher: &macroArtifactPublisher{},
		actionResolver:    resolver,
		actionLocks:       locks,
	}
}

func adaptE2ECanonicalForPerf(b *testing.B, def *apipb.Job, localFanout bool) {
	b.Helper()

	def.Secrets = nil
	visitMacroJobNodes(def.GetRoot(), func(node *apipb.Node) {
		switch node.GetId() {
		case "setup-control":
			setMacroNodeWithValue(node, "command", "echo canonical-control-start")
		case "fanout-control":
			if localFanout {
				setMacroNodeWithValue(node, "execution", "local")
			} else {
				setMacroNodeWithValue(node, "execution", "distributed")
			}
		case "secret-lane":
			setMacroNodeWithValue(node, "command", "echo canonical-secret-ok")
		}
	})
}

func visitMacroJobNodes(node *apipb.Node, visit func(*apipb.Node)) {
	if node == nil {
		return
	}

	visit(node)
	for _, child := range node.GetSteps() {
		visitMacroJobNodes(child, visit)
	}

	for _, port := range node.GetPorts() {
		for _, child := range port.GetNodes() {
			visitMacroJobNodes(child, visit)
		}
	}
}

func setMacroNodeWithValue(node *apipb.Node, key, value string) {
	if node.With == nil {
		node.With = map[string]string{}
	}

	node.With[key] = value
}

func macroRepoPath(b *testing.B, elem ...string) string {
	b.Helper()

	wd, err := os.Getwd()
	if err != nil {
		b.Fatalf("get working directory: %v", err)
	}

	for {
		candidate := filepath.Join(append([]string{wd}, elem...)...)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		} else if !os.IsNotExist(err) {
			b.Fatalf("stat %s: %v", candidate, err)
		}

		parent := filepath.Dir(wd)
		if parent == wd {
			b.Fatalf("could not find repo path %s", filepath.Join(elem...))
		}

		wd = parent
	}
}

type macroArtifactPublisher struct {
	artifacts atomic.Int64
	bytes     atomic.Int64
}

func (p *macroArtifactPublisher) PublishArtifact(_ context.Context, req action.ArtifactPublishRequest) (action.ArtifactPublishResult, error) {
	if req.Reader == nil {
		return action.ArtifactPublishResult{}, fmt.Errorf("artifact reader is required")
	}

	hasher := sha256.New()
	size, err := io.Copy(hasher, req.Reader)
	if err != nil {
		return action.ArtifactPublishResult{}, err
	}

	if req.RequireSize && size != req.ExpectedSize {
		return action.ArtifactPublishResult{}, fmt.Errorf("artifact size=%d, want %d", size, req.ExpectedSize)
	}

	digest := hex.EncodeToString(hasher.Sum(nil))
	contentType := req.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	p.artifacts.Add(1)
	p.bytes.Add(size)
	return action.ArtifactPublishResult{
		Name:            req.Name,
		Path:            req.Path,
		ContentType:     contentType,
		BlobKey:         "sha256:" + digest,
		BlobAlgorithm:   "sha256",
		BlobDigest:      digest,
		SizeBytes:       size,
		ArtifactShardID: "macro-artifact",
	}, nil
}

func logHeavyMacroJob(lines int) macroJobSpec {
	command := fmt.Sprintf(`i=0; while [ "$i" -lt %d ]; do printf 'line-%%04d\n' "$i"; i=$((i + 1)); done`, lines)
	return macroJobSpec{
		id:      "macro-log-heavy",
		uses:    "builtins/shell",
		with:    map[string]string{"command": command},
		command: command,
	}
}

func uniqueMacroJob(job macroJobSpec) macroJobSpec {
	job.id = fmt.Sprintf("%s-%d", job.id, macroJobSequence.Add(1))
	return job
}

func newMacroBenchEnv(b *testing.B, jobs []macroJobSpec) macroBenchEnv {
	b.Helper()

	b.Setenv("VECTIS_API_ALLOWED_HOSTS", "example.com")

	dbConfig := macroDatabaseConfigFromEnv(b)
	db, err := sql.Open(dbConfig.driver, dbConfig.dsn)
	if err != nil {
		b.Fatalf("open benchmark db: %v", err)
	}

	db.SetMaxOpenConns(dbConfig.maxOpenConns)
	db.SetMaxIdleConns(dbConfig.maxIdleConns)

	if err := migrations.Run(db, dbConfig.driver); err != nil {
		_ = db.Close()
		b.Fatalf("run migrations: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	logger := mocks.NopLogger{}
	server := api.NewAPIServer(logger, db)
	queueService := queue.NewQueueService(logger)
	server.SetQueueClient(queueService)
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	var artifactPublisher action.ArtifactPublisher
	var actionResolver actionregistry.Resolver
	var actionLocks []actionregistry.ActionLock
	for _, j := range jobs {
		seedMacroJobSnapshot(b, repos.Jobs(), j)
		if j.artifactPublisher != nil {
			artifactPublisher = j.artifactPublisher
		}
		if j.actionResolver != nil {
			actionResolver = j.actionResolver
			actionLocks = actionregistry.CloneActionLocks(j.actionLocks)
		}
	}
	if actionResolver != nil {
		server.SetActionDescriptorResolver(actionResolver)
	}
	handler := server.Handler()

	job.SetLogSpoolDirForTest(b.TempDir())

	return macroBenchEnv{
		handler:           handler,
		queue:             queueService,
		apiQueue:          queueService,
		runs:              runs,
		choreo:            macroSQLChoreography{runs: runs},
		log:               logger,
		db:                db,
		dbDriver:          dbConfig.driver,
		artifactPublisher: artifactPublisher,
		actionResolver:    actionResolver,
		actionLocks:       actionLocks,
	}
}

func newMacroInProcessOrchestratorBenchEnv(b *testing.B, jobs []macroJobSpec) macroBenchEnv {
	b.Helper()

	env := newMacroBenchEnv(b, jobs)
	service := orchestrator.New(0)
	b.Cleanup(service.Close)
	env.choreo = macroInProcessOrchestratorChoreography{service: service}
	return env
}

func newMacroGRPCOrchestratorBenchEnv(b *testing.B, jobs []macroJobSpec) macroBenchEnv {
	b.Helper()

	env := newMacroBenchEnv(b, jobs)
	service := orchestrator.New(0)
	b.Cleanup(service.Close)

	listener := bufconn.Listen(macroOrchestratorBufSize)
	server := grpc.NewServer()
	orchestrator.RegisterOrchestratorService(server, service, mocks.NopLogger{})
	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		"passthrough:///macro-orchestrator",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		b.Fatalf("dial macro orchestrator: %v", err)
	}

	b.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})

	env.choreo = macroGRPCOrchestratorChoreography{client: apipb.NewOrchestratorServiceClient(conn)}
	return env
}

func runMacroAPITriggerToQueuedBenchmark(b *testing.B) {
	b.Helper()

	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newMacroBenchEnv(b, []macroJobSpec{macroJob})
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()

	acceptedToQueueSamples := make([]int64, 0, b.N)
	triggerToQueueSamples := make([]int64, 0, b.N)
	queueToDequeuedSamples := make([]int64, 0, b.N)
	triggerRequestSamples := make([]int64, 0, b.N)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()

	for i := 0; i < b.N; i++ {
		info, err := triggerMacroJob(ctx, env.handler, macroJob.id, fmt.Sprintf("%s-queued-%d", macroJob.id, i))
		if err != nil {
			b.Fatal(err)
		}

		jobReq, dequeuedAt := waitForDequeuedJob(b, ctx, env.queue)
		queuedJob := jobReq.GetJob()
		if queuedJob.GetRunId() != info.runID {
			b.Fatalf("dequeued run_id=%q, want %q", queuedJob.GetRunId(), info.runID)
		}

		queueAcceptedAt := macroQueueAcceptedAt(jobReq, info.httpAcceptedAt)
		deliveryID := queuedJob.GetDeliveryId()
		if _, err := env.queue.Ack(ctx, &apipb.AckRequest{DeliveryId: &deliveryID}); err != nil {
			b.Fatalf("ack queued delivery %s: %v", deliveryID, err)
		}

		acceptedToQueueSamples = append(acceptedToQueueSamples, max(queueAcceptedAt.Sub(info.httpAcceptedAt).Nanoseconds(), 0))
		triggerToQueueSamples = append(triggerToQueueSamples, max(queueAcceptedAt.Sub(info.triggerStart).Nanoseconds(), 0))
		queueToDequeuedSamples = append(queueToDequeuedSamples, max(dequeuedAt.Sub(queueAcceptedAt).Nanoseconds(), 0))
		triggerRequestSamples = append(triggerRequestSamples, max(info.httpAcceptedAt.Sub(info.triggerStart).Nanoseconds(), 0))
	}

	elapsed := time.Since(start)
	b.StopTimer()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	reportLatencyMetrics(b, "trigger_request", triggerRequestSamples)
	reportLatencyMetrics(b, "accepted_to_queue", acceptedToQueueSamples)
	reportLatencyMetrics(b, "trigger_to_queue", triggerToQueueSamples)
	reportLatencyMetrics(b, "queue_to_dequeued", queueToDequeuedSamples)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "queued_runs/s")
	}

	b.ReportMetric(float64(b.N), "total_runs")
}

func runMacroWorkerClaimAckBenchmark(b *testing.B) {
	b.Helper()

	runMacroWorkerClaimAckBenchmarkWithWorkers(b, macroBenchmarkWorkers(b))
}

func runMacroWorkerClaimAckBenchmarkWithWorkers(b *testing.B, workerCount int) {
	b.Helper()

	runMacroWorkerClaimAckBenchmarkWithWorkersAndEnv(b, workerCount, newMacroBenchEnv)
}

func runMacroWorkerClaimAckBenchmarkWithWorkersAndEnv(b *testing.B, workerCount int, newEnv macroBenchEnvFactory) {
	b.Helper()

	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newEnv(b, []macroJobSpec{macroJob})
	preseedMacroQueuedRuns(b, ctx, env, macroJob, b.N)
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()

	resultCh := make(chan macroClaimAckResult, b.N)
	workCtx, cancel := context.WithCancel(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	waitWorkers := startMacroClaimAckWorkers(workCtx, env, workerCount, resultCh)
	defer func() {
		cancel()
		waitWorkers()
	}()

	results := collectMacroClaimAckResults(b, resultCh, b.N)
	elapsed := time.Since(start)
	b.StopTimer()

	cancel()
	waitWorkers()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	reportMacroClaimAckMetrics(b, results)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "dispatches/s")
	}

	b.ReportMetric(float64(workerCount), "worker_count")
	b.ReportMetric(float64(b.N), "total_runs")
}

func runMacroWorkerClaimAckCompleteBenchmark(b *testing.B) {
	b.Helper()

	runMacroWorkerClaimAckCompleteBenchmarkWithWorkers(b, macroBenchmarkWorkers(b))
}

func runMacroWorkerClaimAckCompleteBenchmarkWithWorkers(b *testing.B, workerCount int) {
	b.Helper()

	runMacroWorkerClaimAckCompleteBenchmarkWithWorkersAndJob(b, workerCount, noopMacroJob())
}

func runMacroWorkerClaimAckCompleteBenchmarkWithWorkersAndJob(
	b *testing.B,
	workerCount int,
	macroJob macroJobSpec,
) {
	b.Helper()

	runMacroWorkerClaimAckCompleteBenchmarkWithWorkersJobAndEnv(b, workerCount, macroJob, newMacroBenchEnv)
}

func runMacroWorkerClaimAckCompleteBenchmarkWithWorkersJobAndEnv(
	b *testing.B,
	workerCount int,
	macroJob macroJobSpec,
	newEnv macroBenchEnvFactory,
) {
	b.Helper()

	ctx := context.Background()
	macroJob = uniqueMacroJob(macroJob)
	env := newEnv(b, []macroJobSpec{macroJob})
	preseedMacroQueuedRuns(b, ctx, env, macroJob, b.N)
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()

	resultCh := make(chan macroWorkerResult, b.N)
	workCtx, cancel := context.WithCancel(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	waitWorkers := startMacroCompleteWorkers(workCtx, env, workerCount, resultCh)
	defer func() {
		cancel()
		waitWorkers()
	}()

	results := collectMacroWorkerResults(b, resultCh, b.N)
	elapsed := time.Since(start)
	b.StopTimer()

	cancel()
	waitWorkers()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	reportMacroRunTimingMetrics(b, results)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "terminal_runs/s")
	}

	b.ReportMetric(float64(workerCount), "worker_count")
	b.ReportMetric(float64(b.N), "total_runs")
}

func runMacroWorkerClaimAckFinalizeBenchmark(b *testing.B) {
	b.Helper()

	runMacroWorkerClaimAckFinalizeBenchmarkWithWorkers(b, macroBenchmarkWorkers(b))
}

func runMacroWorkerClaimAckFinalizeBenchmarkWithWorkers(b *testing.B, workerCount int) {
	b.Helper()

	runMacroWorkerClaimAckFinalizeBenchmarkWithWorkersAndEnv(b, workerCount, newMacroBenchEnv)
}

func runMacroWorkerClaimAckFinalizeBenchmarkWithWorkersAndEnv(b *testing.B, workerCount int, newEnv macroBenchEnvFactory) {
	b.Helper()

	ctx := context.Background()
	macroJob := uniqueMacroJob(noopMacroJob())
	env := newEnv(b, []macroJobSpec{macroJob})
	preseedMacroQueuedRuns(b, ctx, env, macroJob, b.N)
	statsEnabled := resetMacroDBStats(b, env)
	dbStatsStart := env.db.Stats()

	resultCh := make(chan macroClaimAckFinalizeResult, b.N)
	workCtx, cancel := context.WithCancel(ctx)

	b.ReportAllocs()
	b.ResetTimer()
	start := time.Now()
	waitWorkers := startMacroClaimAckFinalizeWorkers(workCtx, env, workerCount, resultCh)
	defer func() {
		cancel()
		waitWorkers()
	}()

	results := collectMacroClaimAckFinalizeResults(b, resultCh, b.N)
	elapsed := time.Since(start)
	b.StopTimer()

	cancel()
	waitWorkers()
	reportMacroDBStats(b, env, statsEnabled)
	reportMacroDBPoolMetrics(b, dbStatsStart, env.db.Stats(), b.N)

	reportMacroClaimAckFinalizeMetrics(b, results)
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "finalized_runs/s")
	}

	b.ReportMetric(float64(workerCount), "worker_count")
	b.ReportMetric(float64(b.N), "total_runs")
}

func preseedMacroQueuedRuns(b *testing.B, ctx context.Context, env macroBenchEnv, job macroJobSpec, total int) {
	b.Helper()

	for i := 0; i < total; i++ {
		preseedMacroQueuedRunMeasured(b, ctx, env, job, i+1)
	}
}

func preseedMacroQueuedRunMeasured(
	b *testing.B,
	ctx context.Context,
	env macroBenchEnv,
	job macroJobSpec,
	runIndex int,
) macroDBTimings {
	b.Helper()

	var dbTimings macroDBTimings

	createStarted := time.Now()
	runID, _, err := env.runs.CreateRun(ctx, job.id, &runIndex, 1)
	dbTimings.createRun = time.Since(createStarted).Nanoseconds()
	if err != nil {
		b.Fatalf("create queued run %d: %v", runIndex, err)
	}

	req := macroJobRequest(b, job, runID)
	attachStarted := time.Now()
	if _, err := cell.AttachPendingExecutionEnvelopeWithActions(ctx, env.runs, req, runID, time.Now().UnixNano(), env.actionResolver); err != nil {
		b.Fatalf("attach execution envelope for queued run %s: %v", runID, err)
	}

	dbTimings.attachEnvelope = time.Since(attachStarted).Nanoseconds()

	loadStarted := time.Now()
	if err := env.choreo.LoadRun(ctx, req); err != nil {
		b.Fatalf("load choreography for queued run %s: %v", runID, err)
	}
	if !env.choreo.DBBacked() {
		dbTimings.choreographyLoadRun = time.Since(loadStarted).Nanoseconds()
	}

	if _, err := env.apiQueue.Enqueue(ctx, req); err != nil {
		b.Fatalf("enqueue queued run %s: %v", runID, err)
	}

	touchStarted := time.Now()
	if err := env.runs.TouchDispatched(ctx, runID); err != nil {
		b.Fatalf("touch dispatched queued run %s: %v", runID, err)
	}

	dbTimings.touchDispatched = time.Since(touchStarted).Nanoseconds()
	return dbTimings
}

type macroDBClaimedExecution struct {
	runID       string
	executionID string
	owner       string
	claimToken  string
}

func createMacroDBBenchmarkRun(b *testing.B, ctx context.Context, env macroBenchEnv, jobID string, runIndex int) string {
	b.Helper()

	runID, _, err := env.runs.CreateRun(ctx, jobID, &runIndex, 1)
	if err != nil {
		b.Fatalf("create DB benchmark run %d: %v", runIndex, err)
	}

	return runID
}

func seedMacroRunHotStateOwner(b *testing.B, ctx context.Context, runs dal.RunsRepository, runID string) {
	b.Helper()

	if err := runs.UpsertRunHotStateOwner(ctx, dal.RunHotStateOwnerUpdate{
		RunID:        runID,
		CellID:       dal.DefaultCellID,
		OwnerID:      "orchestrator:macro-read-benchmark",
		OwnerEpoch:   "macro-read-benchmark",
		LeaseUntil:   time.Now().Add(dal.DefaultLeaseTTL),
		LastSequence: 42,
	}); err != nil {
		b.Fatalf("seed hot-state owner for run %s: %v", runID, err)
	}
}

func prepareMacroDBClaimedRootExecution(b *testing.B, ctx context.Context, env macroBenchEnv, jobID string, runIndex int) macroDBClaimedExecution {
	b.Helper()

	runID := createMacroDBBenchmarkRun(b, ctx, env, jobID, runIndex)
	dispatch, err := env.runs.GetPendingExecution(ctx, runID)
	if err != nil {
		b.Fatalf("get pending execution for run %s: %v", runID, err)
	}

	owner := "macro-db-worker"
	claim, err := env.runs.TryClaimExecution(ctx, dispatch.ExecutionID, owner, time.Now().Add(dal.DefaultLeaseTTL))
	if err != nil {
		b.Fatalf("claim execution %s: %v", dispatch.ExecutionID, err)
	}

	if !claim.Claimed {
		b.Fatalf("execution %s was not claimed", dispatch.ExecutionID)
	}

	return macroDBClaimedExecution{
		runID:       runID,
		executionID: dispatch.ExecutionID,
		owner:       owner,
		claimToken:  claim.ClaimToken,
	}
}

func ensureMacroBenchmarkPlannedChild(ctx context.Context, runs dal.RunsRepository, runID string, child int) (dal.TaskExecutionRecord, bool, error) {
	taskKey := fmt.Sprintf("child-%05d", child)
	return runs.EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: runID + ":" + dal.RootTaskKey,
		TaskKey:      taskKey,
		Name:         taskKey,
		SpecHash:     "sha256:macro-fanout-benchmark",
		TargetCellID: dal.DefaultCellID,
	})
}

func macroJobRequest(b *testing.B, macroJob macroJobSpec, runID string) *apipb.JobRequest {
	b.Helper()

	if macroJob.definitionJSON != "" {
		def, err := decodeMacroJobDefinition(macroJob.definitionJSON, macroJob.id, runID)
		if err != nil {
			b.Fatalf("decode benchmark job %s: %v", macroJob.id, err)
		}

		return &apipb.JobRequest{Job: def}
	}

	rootID := "root"
	uses := macroJob.uses
	if uses == "" {
		uses = "builtins/shell"
	}

	with := make(map[string]string, len(macroJob.with))
	for k, v := range macroJob.with {
		with[k] = v
	}

	if len(with) == 0 && macroJob.command != "" {
		with["command"] = macroJob.command
	}

	return &apipb.JobRequest{
		Job: &apipb.Job{
			Id:    &macroJob.id,
			RunId: &runID,
			Root: &apipb.Node{
				Id:   &rootID,
				Uses: &uses,
				With: with,
			},
		},
	}
}

func macroTaskExecutionToProto(in dal.TaskExecutionRecord) *apipb.OrchestratorTaskExecution {
	if in.ExecutionID == "" {
		return nil
	}

	return &apipb.OrchestratorTaskExecution{
		RunId:         macroString(in.RunID),
		TaskId:        macroString(in.TaskID),
		ParentTaskId:  macroString(in.ParentTaskID),
		TaskKey:       macroString(in.TaskKey),
		Name:          macroString(in.Name),
		TaskAttemptId: macroString(in.TaskAttemptID),
		SegmentId:     macroString(in.SegmentID),
		SegmentName:   macroString(in.SegmentName),
		ExecutionId:   macroString(in.ExecutionID),
		CellId:        macroString(in.CellID),
		Attempt:       macroInt32(int32(in.Attempt)),
	}
}

func macroRunTaskCompletionFromProto(in *apipb.OrchestratorRunTaskCompletion) dal.RunTaskCompletion {
	if in == nil {
		return dal.RunTaskCompletion{}
	}

	return dal.RunTaskCompletion{
		RunID:          in.GetRunId(),
		Total:          int(in.GetTotal()),
		Succeeded:      int(in.GetSucceeded()),
		TerminalFailed: int(in.GetTerminalFailed()),
		Incomplete:     int(in.GetIncomplete()),
	}
}

func macroTaskExecutionsFromProto(in []*apipb.OrchestratorTaskExecution) []dal.TaskExecutionRecord {
	out := make([]dal.TaskExecutionRecord, 0, len(in))
	for _, record := range in {
		if record == nil {
			continue
		}

		out = append(out, dal.TaskExecutionRecord{
			RunID:         record.GetRunId(),
			TaskID:        record.GetTaskId(),
			ParentTaskID:  record.GetParentTaskId(),
			TaskKey:       record.GetTaskKey(),
			Name:          record.GetName(),
			TaskAttemptID: record.GetTaskAttemptId(),
			SegmentID:     record.GetSegmentId(),
			SegmentName:   record.GetSegmentName(),
			ExecutionID:   record.GetExecutionId(),
			CellID:        record.GetCellId(),
			Attempt:       int(record.GetAttempt()),
		})
	}

	return out
}

func macroString(v string) *string {
	return &v
}

func macroInt64(v int64) *int64 {
	return &v
}

func macroInt32(v int32) *int32 {
	return &v
}

type macroClaimAckTimings struct {
	queueAcceptedToDequeued int64
	dequeuedToClaimed       int64
	claimedToAcked          int64
	dequeuedToAcked         int64
	db                      macroDBTimings
}

type macroClaimAckResult struct {
	timings macroClaimAckTimings
	err     error
}

type macroClaimAckFinalizeTimings struct {
	dequeuedToClaimed   int64
	claimedToAcked      int64
	ackedToFinalized    int64
	claimedToFinalized  int64
	dequeuedToFinalized int64
	db                  macroDBTimings
}

type macroClaimAckFinalizeResult struct {
	timings macroClaimAckFinalizeTimings
	err     error
}

func startMacroClaimAckWorkers(
	ctx context.Context,
	env macroBenchEnv,
	workers int,
	resultCh chan<- macroClaimAckResult,
) func() {
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("macro-claim-worker-%d", i)
		wg.Add(1)

		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				jobReq, err := env.queue.TryDequeue(ctx, &apipb.DequeueRequest{})
				if err != nil {
					sendMacroClaimAckResult(ctx, resultCh, macroClaimAckResult{err: fmt.Errorf("try dequeue: %w", err)})
					return
				}

				if jobReq == nil {
					time.Sleep(10 * time.Microsecond)
					continue
				}

				dequeuedAt := time.Now()
				timings, err := finishDequeuedMacroClaimAck(ctx, env, jobReq, dequeuedAt, workerID)
				sendMacroClaimAckResult(ctx, resultCh, macroClaimAckResult{timings: timings, err: err})
				if err != nil {
					return
				}
			}
		}()
	}

	return wg.Wait
}

func startMacroClaimAckFinalizeWorkers(
	ctx context.Context,
	env macroBenchEnv,
	workers int,
	resultCh chan<- macroClaimAckFinalizeResult,
) func() {
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("macro-finalize-worker-%d", i)
		wg.Add(1)

		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				jobReq, err := env.queue.TryDequeue(ctx, &apipb.DequeueRequest{})
				if err != nil {
					sendMacroClaimAckFinalizeResult(ctx, resultCh, macroClaimAckFinalizeResult{err: fmt.Errorf("try dequeue: %w", err)})
					return
				}

				if jobReq == nil {
					time.Sleep(10 * time.Microsecond)
					continue
				}

				dequeuedAt := time.Now()
				timings, err := finishDequeuedMacroClaimAckFinalize(ctx, env, jobReq, dequeuedAt, workerID)
				sendMacroClaimAckFinalizeResult(ctx, resultCh, macroClaimAckFinalizeResult{timings: timings, err: err})

				if err != nil {
					return
				}
			}
		}()
	}

	return wg.Wait
}

func startMacroCompleteWorkers(
	ctx context.Context,
	env macroBenchEnv,
	workers int,
	resultCh chan<- macroWorkerResult,
) func() {
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("macro-complete-worker-%d", i)
		wg.Add(1)

		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				jobReq, err := env.queue.TryDequeue(ctx, &apipb.DequeueRequest{})
				if err != nil {
					sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{err: fmt.Errorf("try dequeue: %w", err)})
					return
				}

				if jobReq == nil {
					time.Sleep(10 * time.Microsecond)
					continue
				}

				dequeuedAt := time.Now()
				runID := jobReq.GetJob().GetRunId()
				queueAcceptedAt := macroQueueAcceptedAt(jobReq, dequeuedAt)
				info := macroTriggerInfo{
					runID:          runID,
					triggerStart:   queueAcceptedAt,
					httpAcceptedAt: queueAcceptedAt,
				}

				timings, err := finishDequeuedMacroJob(ctx, env, jobReq, info, dequeuedAt, workerID, noopLogClient{})
				sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{timings: timings, err: err})
				if err != nil {
					return
				}
			}
		}()
	}

	return wg.Wait
}

func finishDequeuedMacroClaimAck(
	ctx context.Context,
	env macroBenchEnv,
	jobReq *apipb.JobRequest,
	dequeuedAt time.Time,
	workerID string,
) (macroClaimAckTimings, error) {
	var dbTimings macroDBTimings
	queuedJob := jobReq.GetJob()
	runID := queuedJob.GetRunId()
	if runID == "" {
		return macroClaimAckTimings{}, fmt.Errorf("dequeued job missing run_id")
	}

	executionEnvelope, ok, err := cell.ExecutionEnvelopeFromRequest(jobReq)
	if err != nil {
		return macroClaimAckTimings{}, fmt.Errorf("decode execution envelope: %w", err)
	}

	if !ok {
		return macroClaimAckTimings{}, fmt.Errorf("missing execution envelope")
	}

	queueAcceptedAt := macroQueueAcceptedAt(jobReq, dequeuedAt)
	claimStarted := time.Now()
	executionClaim, err := env.choreo.ClaimAndStartExecution(ctx, runID, executionEnvelope.ExecutionID, workerID, time.Now().Add(dal.DefaultLeaseTTL))
	claimedAt := time.Now()
	dbTimings.choreographyClaimAndStart = claimedAt.Sub(claimStarted).Nanoseconds()
	if env.choreo.DBBacked() {
		dbTimings.tryClaimExecution = dbTimings.choreographyClaimAndStart
	}
	if err != nil {
		return macroClaimAckTimings{}, fmt.Errorf("try claim execution %s: %w", executionEnvelope.ExecutionID, err)
	}
	if !executionClaim.Claimed {
		return macroClaimAckTimings{}, fmt.Errorf("execution %s was not claimed", executionEnvelope.ExecutionID)
	}

	deliveryID := queuedJob.GetDeliveryId()
	if deliveryID == "" {
		return macroClaimAckTimings{}, fmt.Errorf("run %s missing delivery id", runID)
	}

	if _, err := env.queue.Ack(ctx, &apipb.AckRequest{DeliveryId: &deliveryID}); err != nil {
		return macroClaimAckTimings{}, fmt.Errorf("ack delivery %s: %w", deliveryID, err)
	}

	ackedAt := time.Now()

	return macroClaimAckTimings{
		queueAcceptedToDequeued: max(dequeuedAt.Sub(queueAcceptedAt).Nanoseconds(), 0),
		dequeuedToClaimed:       max(claimedAt.Sub(dequeuedAt).Nanoseconds(), 0),
		claimedToAcked:          max(ackedAt.Sub(claimedAt).Nanoseconds(), 0),
		dequeuedToAcked:         max(ackedAt.Sub(dequeuedAt).Nanoseconds(), 0),
		db:                      dbTimings,
	}, nil
}

func finishDequeuedMacroClaimAckFinalize(
	ctx context.Context,
	env macroBenchEnv,
	jobReq *apipb.JobRequest,
	dequeuedAt time.Time,
	workerID string,
) (macroClaimAckFinalizeTimings, error) {
	var dbTimings macroDBTimings
	queuedJob := jobReq.GetJob()
	runID := queuedJob.GetRunId()
	if runID == "" {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("dequeued job missing run_id")
	}

	executionEnvelope, ok, err := cell.ExecutionEnvelopeFromRequest(jobReq)
	if err != nil {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("decode execution envelope: %w", err)
	}
	if !ok {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("missing execution envelope")
	}

	claimStarted := time.Now()
	executionClaim, err := env.choreo.ClaimAndStartExecution(ctx, runID, executionEnvelope.ExecutionID, workerID, time.Now().Add(dal.DefaultLeaseTTL))
	claimedAt := time.Now()
	dbTimings.choreographyClaimAndStart = claimedAt.Sub(claimStarted).Nanoseconds()
	if env.choreo.DBBacked() {
		dbTimings.tryClaimExecution = dbTimings.choreographyClaimAndStart
	}
	if err != nil {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("try claim execution %s: %w", executionEnvelope.ExecutionID, err)
	}
	if !executionClaim.Claimed {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("execution %s was not claimed", executionEnvelope.ExecutionID)
	}

	deliveryID := queuedJob.GetDeliveryId()
	if deliveryID == "" {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("run %s missing delivery id", runID)
	}

	if _, err := env.queue.Ack(ctx, &apipb.AckRequest{DeliveryId: &deliveryID}); err != nil {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("ack delivery %s: %w", deliveryID, err)
	}

	ackedAt := time.Now()

	finalizeStarted := time.Now()
	finalized, err := env.choreo.CompleteExecution(ctx, runID, executionEnvelope.ExecutionID, workerID, executionClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	finalizedAt := time.Now()
	dbTimings.choreographyFinalize = finalizedAt.Sub(finalizeStarted).Nanoseconds()
	if env.choreo.DBBacked() {
		dbTimings.finalizeExecution = dbTimings.choreographyFinalize
	}
	if err != nil {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("finalize execution %s: %w", executionEnvelope.ExecutionID, err)
	}
	if finalized.Outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		return macroClaimAckFinalizeTimings{}, fmt.Errorf("finalize execution %s outcome %q", executionEnvelope.ExecutionID, finalized.Outcome)
	}

	return macroClaimAckFinalizeTimings{
		dequeuedToClaimed:   max(claimedAt.Sub(dequeuedAt).Nanoseconds(), 0),
		claimedToAcked:      max(ackedAt.Sub(claimedAt).Nanoseconds(), 0),
		ackedToFinalized:    max(finalizedAt.Sub(ackedAt).Nanoseconds(), 0),
		claimedToFinalized:  max(finalizedAt.Sub(claimedAt).Nanoseconds(), 0),
		dequeuedToFinalized: max(finalizedAt.Sub(dequeuedAt).Nanoseconds(), 0),
		db:                  dbTimings,
	}, nil
}

func collectMacroClaimAckResults(
	b *testing.B,
	resultCh <-chan macroClaimAckResult,
	total int,
) []macroClaimAckTimings {
	b.Helper()

	results := make([]macroClaimAckTimings, 0, total)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for len(results) < total {
		select {
		case result := <-resultCh:
			if result.err != nil {
				b.Fatalf("drain claim/ack queue: %v", result.err)
			}
			results = append(results, result.timings)
		case <-timeout.C:
			b.Fatalf("timed out draining claim/ack queue: got %d of %d results", len(results), total)
		}
	}

	return results
}

func collectMacroClaimAckFinalizeResults(
	b *testing.B,
	resultCh <-chan macroClaimAckFinalizeResult,
	total int,
) []macroClaimAckFinalizeTimings {
	b.Helper()

	results := make([]macroClaimAckFinalizeTimings, 0, total)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for len(results) < total {
		select {
		case result := <-resultCh:
			if result.err != nil {
				b.Fatalf("drain claim/ack/finalize queue: %v", result.err)
			}
			results = append(results, result.timings)
		case <-timeout.C:
			b.Fatalf("timed out draining claim/ack/finalize queue: got %d of %d results", len(results), total)
		}
	}

	return results
}

func sendMacroClaimAckResult(ctx context.Context, ch chan<- macroClaimAckResult, result macroClaimAckResult) {
	select {
	case ch <- result:
	case <-ctx.Done():
	}
}

func sendMacroClaimAckFinalizeResult(ctx context.Context, ch chan<- macroClaimAckFinalizeResult, result macroClaimAckFinalizeResult) {
	select {
	case ch <- result:
	case <-ctx.Done():
	}
}

func reportMacroClaimAckMetrics(b *testing.B, results []macroClaimAckTimings) {
	b.Helper()

	dequeuedToClaimedSamples := make([]int64, 0, len(results))
	claimedToAckedSamples := make([]int64, 0, len(results))
	dequeuedToAckedSamples := make([]int64, 0, len(results))
	dbTimingSamples := make([]macroDBTimings, 0, len(results))

	for _, timings := range results {
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToAckedSamples = append(claimedToAckedSamples, timings.claimedToAcked)
		dequeuedToAckedSamples = append(dequeuedToAckedSamples, timings.dequeuedToAcked)
		dbTimingSamples = append(dbTimingSamples, timings.db)
	}

	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_acked", claimedToAckedSamples)
	reportLatencyMetrics(b, "dequeued_to_acked", dequeuedToAckedSamples)
	reportMacroDBTimingMetrics(b, dbTimingSamples)
}

func reportMacroClaimAckFinalizeMetrics(b *testing.B, results []macroClaimAckFinalizeTimings) {
	b.Helper()

	dequeuedToClaimedSamples := make([]int64, 0, len(results))
	claimedToAckedSamples := make([]int64, 0, len(results))
	ackedToFinalizedSamples := make([]int64, 0, len(results))
	claimedToFinalizedSamples := make([]int64, 0, len(results))
	dequeuedToFinalizedSamples := make([]int64, 0, len(results))
	dbTimingSamples := make([]macroDBTimings, 0, len(results))

	for _, timings := range results {
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToAckedSamples = append(claimedToAckedSamples, timings.claimedToAcked)
		ackedToFinalizedSamples = append(ackedToFinalizedSamples, timings.ackedToFinalized)
		claimedToFinalizedSamples = append(claimedToFinalizedSamples, timings.claimedToFinalized)
		dequeuedToFinalizedSamples = append(dequeuedToFinalizedSamples, timings.dequeuedToFinalized)
		dbTimingSamples = append(dbTimingSamples, timings.db)
	}

	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_acked", claimedToAckedSamples)
	reportLatencyMetrics(b, "acked_to_finalized", ackedToFinalizedSamples)
	reportLatencyMetrics(b, "claimed_to_finalized", claimedToFinalizedSamples)
	reportLatencyMetrics(b, "dequeued_to_finalized", dequeuedToFinalizedSamples)
	reportMacroDBTimingMetrics(b, dbTimingSamples)
}

func reportMacroRunTimingMetrics(b *testing.B, results []macroRunTimings) {
	b.Helper()

	dequeuedToClaimedSamples := make([]int64, 0, len(results))
	claimedToTerminalSamples := make([]int64, 0, len(results))
	dequeuedToTerminalSamples := make([]int64, 0, len(results))
	logFlushSamples := make([]int64, 0, len(results))
	dbTimingSamples := make([]macroDBTimings, 0, len(results))

	for _, timings := range results {
		dequeuedToClaimedSamples = append(dequeuedToClaimedSamples, timings.dequeuedToClaimed)
		claimedToTerminalSamples = append(claimedToTerminalSamples, timings.claimedToTerminal)
		dequeuedToTerminalSamples = append(dequeuedToTerminalSamples, timings.dequeuedToClaimed+timings.claimedToTerminal)
		logFlushSamples = append(logFlushSamples, timings.logFlush)
		dbTimingSamples = append(dbTimingSamples, timings.db)
	}

	reportLatencyMetrics(b, "dequeued_to_claimed", dequeuedToClaimedSamples)
	reportLatencyMetrics(b, "claimed_to_terminal", claimedToTerminalSamples)
	reportLatencyMetrics(b, "dequeued_to_terminal", dequeuedToTerminalSamples)
	reportLatencyMetrics(b, "log_flush", logFlushSamples)
	reportMacroDBTimingMetrics(b, dbTimingSamples)
}

func macroQueueAcceptedAt(req *apipb.JobRequest, fallback time.Time) time.Time {
	if raw := req.GetMetadata()[observability.JobEnqueueAcceptedUnixNanoKey]; raw != "" {
		if ns, err := parseUnixNano(raw); err == nil && ns > 0 {
			return time.Unix(0, ns)
		}
	}

	if raw := req.GetMetadata()[observability.JobEnqueuedAtUnixNanoKey]; raw != "" {
		if ns, err := parseUnixNano(raw); err == nil && ns > 0 {
			return time.Unix(0, ns)
		}
	}

	return fallback
}

func newMacroLogBenchEnv(b *testing.B, lines int) macroLogBenchEnv {
	b.Helper()

	macroJob := uniqueMacroJob(logHeavyMacroJob(lines))
	env := newMacroBenchEnv(b, []macroJobSpec{macroJob})
	store, err := logserver.NewLocalRunLogStore(b.TempDir())
	if err != nil {
		b.Fatalf("create log store: %v", err)
	}

	return macroLogBenchEnv{
		macroBenchEnv: env,
		store:         store,
		logSink:       storeLogClient{store: store, stats: newStoreLogStats()},
		job:           macroJob,
	}
}

func seedMacroJobSnapshot(b *testing.B, jobs dal.JobsRepository, job macroJobSpec) {
	b.Helper()

	definition, err := macroJobDefinitionJSON(job)
	if err != nil {
		b.Fatalf("marshal benchmark job: %v", err)
	}

	if err := jobs.CreateDefinitionSnapshot(context.Background(), job.id, definition); err != nil {
		b.Fatalf("create benchmark job %s: %v", job.id, err)
	}
}

func macroJobDefinitionJSON(macroJob macroJobSpec) (string, error) {
	if macroJob.definitionJSON != "" {
		def, err := decodeMacroJobDefinition(macroJob.definitionJSON, macroJob.id, "")
		if err != nil {
			return "", err
		}

		body, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(def)
		if err != nil {
			return "", err
		}

		return string(body), nil
	}

	uses := macroJob.uses
	if uses == "" {
		uses = "builtins/shell"
	}

	with := make(map[string]string, len(macroJob.with))
	for k, v := range macroJob.with {
		with[k] = v
	}

	if len(with) == 0 && macroJob.command != "" {
		with["command"] = macroJob.command
	}

	body, err := json.Marshal(map[string]any{
		"id": macroJob.id,
		"root": map[string]any{
			"id":   "root",
			"uses": uses,
			"with": with,
		},
	})
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func decodeMacroJobDefinition(definitionJSON, jobID, runID string) (*apipb.Job, error) {
	var def apipb.Job
	if err := job.DecodeDefinitionJSON([]byte(definitionJSON), &def); err != nil {
		return nil, err
	}

	def.Id = &jobID
	if runID == "" {
		def.RunId = nil
	} else {
		def.RunId = &runID
	}

	return &def, nil
}

func triggerMacroBurst(
	b *testing.B,
	ctx context.Context,
	handler http.Handler,
	jobID string,
	clients int,
	total int,
	onInfo func(macroTriggerInfo),
) ([]macroTriggerInfo, time.Duration) {
	b.Helper()

	if clients <= 0 {
		clients = 1
	}

	infos := make([]macroTriggerInfo, total)
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var firstErr error
	var errMu sync.Mutex
	setErr := func(err error) {
		if err == nil {
			return
		}

		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	start := time.Now()
	var wg sync.WaitGroup
	for client := 0; client < clients; client++ {
		client := client

		wg.Go(func() {
			for i := client; i < total; i += clients {
				if workCtx.Err() != nil {
					return
				}

				info, err := triggerMacroJob(workCtx, handler, jobID, fmt.Sprintf("%s-concurrent-%d", jobID, i))
				if err != nil {
					setErr(err)
					return
				}

				infos[i] = info
				if onInfo != nil {
					onInfo(info)
				}
			}
		})
	}

	wg.Wait()
	duration := time.Since(start)

	errMu.Lock()
	err := firstErr
	errMu.Unlock()
	if err != nil {
		b.Fatalf("trigger burst: %v", err)
	}

	return infos, duration
}

func triggerMacroJob(ctx context.Context, handler http.Handler, jobID, idempotencyKey string) (macroTriggerInfo, error) {
	triggerStart := time.Now()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/"+jobID, nil).WithContext(ctx)
	req.Header.Set("Idempotency-Key", idempotencyKey)

	handler.ServeHTTP(rec, req)
	httpAcceptedAt := time.Now()
	if rec.Code != http.StatusAccepted {
		return macroTriggerInfo{}, fmt.Errorf("trigger job: status=%d body=%s", rec.Code, rec.Body.String())
	}

	var resp struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		return macroTriggerInfo{}, fmt.Errorf("decode trigger response: %w", err)
	}

	if resp.RunID == "" {
		return macroTriggerInfo{}, fmt.Errorf("trigger response missing run_id: %s", rec.Body.String())
	}

	return macroTriggerInfo{
		runID:          resp.RunID,
		triggerStart:   triggerStart,
		httpAcceptedAt: httpAcceptedAt,
	}, nil
}

type macroTriggerRegistry struct {
	mu      sync.Mutex
	infos   map[string]macroTriggerInfo
	timeout time.Duration
}

func newMacroTriggerRegistry(capacity int) *macroTriggerRegistry {
	return &macroTriggerRegistry{
		infos:   make(map[string]macroTriggerInfo, capacity),
		timeout: 5 * time.Second,
	}
}

func (r *macroTriggerRegistry) add(info macroTriggerInfo) {
	r.mu.Lock()
	r.infos[info.runID] = info
	r.mu.Unlock()
}

func (r *macroTriggerRegistry) wait(ctx context.Context, runID string) (macroTriggerInfo, error) {
	deadline := time.NewTimer(r.timeout)
	defer deadline.Stop()

	ticker := time.NewTicker(10 * time.Microsecond)
	defer ticker.Stop()

	for {
		r.mu.Lock()
		info, ok := r.infos[runID]
		r.mu.Unlock()

		if ok {
			return info, nil
		}

		select {
		case <-ctx.Done():
			return macroTriggerInfo{}, ctx.Err()
		case <-deadline.C:
			return macroTriggerInfo{}, fmt.Errorf("timed out waiting for trigger metadata for run %q", runID)
		case <-ticker.C:
		}
	}
}

func startMacroWorkers(
	ctx context.Context,
	env macroBenchEnv,
	triggerRegistry *macroTriggerRegistry,
	workers int,
	resultCh chan<- macroWorkerResult,
) func() {
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("macro-worker-%d", i)

		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				jobReq, err := env.queue.TryDequeue(ctx, &apipb.DequeueRequest{})
				if err != nil {
					sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{err: fmt.Errorf("try dequeue: %w", err)})
					return
				}

				if jobReq == nil {
					time.Sleep(10 * time.Microsecond)
					continue
				}

				dequeuedAt := time.Now()
				runID := jobReq.GetJob().GetRunId()
				info, err := triggerRegistry.wait(ctx, runID)
				if err != nil {
					sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{err: err})
					return
				}

				dbTimings, err := loadDequeuedMacroRun(ctx, env, jobReq)
				if err != nil {
					sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{err: err})
					return
				}

				timings, err := finishDequeuedMacroJob(ctx, env, jobReq, info, dequeuedAt, workerID, noopLogClient{})
				timings.db.choreographyLoadRun = dbTimings.choreographyLoadRun
				sendMacroWorkerResult(ctx, resultCh, macroWorkerResult{timings: timings, err: err})

				if err != nil {
					return
				}
			}
		})
	}

	return wg.Wait
}

func startMacroDistributedWorkers(
	ctx context.Context,
	env macroBenchEnv,
	triggerRegistry *macroTriggerRegistry,
	workers int,
	taskCh chan<- macroDistributedTaskResult,
	terminalCh chan<- macroDistributedTerminalResult,
) func() {
	if workers <= 0 {
		workers = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		workerID := fmt.Sprintf("macro-distributed-worker-%d", i)

		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				jobReq, err := env.queue.TryDequeue(ctx, &apipb.DequeueRequest{})
				if err != nil {
					sendMacroDistributedTaskResult(ctx, taskCh, macroDistributedTaskResult{err: fmt.Errorf("try dequeue: %w", err)})
					return
				}

				if jobReq == nil {
					time.Sleep(10 * time.Microsecond)
					continue
				}

				dequeuedAt := time.Now()
				runID := jobReq.GetJob().GetRunId()
				info, err := triggerRegistry.wait(ctx, runID)
				if err != nil {
					sendMacroDistributedTaskResult(ctx, taskCh, macroDistributedTaskResult{err: err})
					return
				}

				dbTimings, err := loadDequeuedMacroRun(ctx, env, jobReq)
				if err != nil {
					sendMacroDistributedTaskResult(ctx, taskCh, macroDistributedTaskResult{err: err})
					return
				}

				execution, err := finishDequeuedMacroExecution(ctx, env, jobReq, info, dequeuedAt, workerID, noopLogClient{})
				execution.timings.db.choreographyLoadRun = dbTimings.choreographyLoadRun
				if err != nil {
					sendMacroDistributedTaskResult(ctx, taskCh, macroDistributedTaskResult{execution: execution, err: err})
					return
				}

				if execution.outcome == dal.ExecutionFinalizationOutcomeContinued || execution.outcome == dal.ExecutionFinalizationOutcomeWaiting {
					enqueued, err := enqueueMacroContinuationExecutions(ctx, env, jobReq, execution)
					execution.childEnqueue = enqueued
					if err != nil {
						sendMacroDistributedTaskResult(ctx, taskCh, macroDistributedTaskResult{execution: execution, err: err})
						return
					}

					if execution.outcome == dal.ExecutionFinalizationOutcomeContinued && enqueued == 0 {
						sendMacroDistributedTaskResult(ctx, taskCh, macroDistributedTaskResult{
							execution: execution,
							err:       fmt.Errorf("continued execution %s without dispatchable children", execution.taskKey),
						})

						return
					}
				}

				sendMacroDistributedTaskResult(ctx, taskCh, macroDistributedTaskResult{execution: execution})

				switch execution.outcome {
				case dal.ExecutionFinalizationOutcomeRunSucceeded:
					sendMacroDistributedTerminalResult(ctx, terminalCh, macroDistributedTerminalResult{timings: execution.timings})
				case dal.ExecutionFinalizationOutcomeRunFailed:
					sendMacroDistributedTerminalResult(ctx, terminalCh, macroDistributedTerminalResult{
						timings: execution.timings,
						err:     fmt.Errorf("distributed run %s failed at task %s", runID, execution.taskKey),
					})

					return
				case dal.ExecutionFinalizationOutcomeContinued, dal.ExecutionFinalizationOutcomeWaiting:
				default:
					sendMacroDistributedTaskResult(ctx, taskCh, macroDistributedTaskResult{
						execution: execution,
						err:       fmt.Errorf("unsupported execution outcome %q for task %s", execution.outcome, execution.taskKey),
					})

					return
				}
			}
		})
	}

	return wg.Wait
}

func startMacroStatusReaders(
	ctx context.Context,
	env macroBenchEnv,
	mode macroStatusReadMode,
	clients int,
	jobs <-chan macroStatusReadJob,
	results chan<- macroStatusReadResult,
) func() {
	if clients <= 0 {
		clients = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}

					started := time.Now()
					ownerChecked, ownerFound, err := runMacroStatusRead(ctx, env, mode, job.runID)
					sendMacroStatusReadResult(ctx, results, macroStatusReadResult{
						latency:      time.Since(started).Nanoseconds(),
						ownerChecked: ownerChecked,
						ownerFound:   ownerFound,
						err:          err,
					})
					if err != nil {
						return
					}
				}
			}
		})
	}

	return wg.Wait
}

func runMacroStatusRead(ctx context.Context, env macroBenchEnv, mode macroStatusReadMode, runID string) (bool, bool, error) {
	switch mode {
	case macroStatusReadAPI:
		return false, false, runMacroAPIStatusRead(ctx, env, runID)
	case macroStatusReadHotOwnerProbe:
		return runMacroHotOwnerProbeStatusRead(ctx, env, runID)
	default:
		return false, false, fmt.Errorf("unknown macro status read mode %q", mode)
	}
}

func runMacroAPIStatusRead(ctx context.Context, env macroBenchEnv, runID string) error {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/runs/"+runID, nil).WithContext(ctx)
	env.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		return fmt.Errorf("get run status=%d: %s", rec.Code, rec.Body.String())
	}

	return nil
}

func runMacroHotOwnerProbeStatusRead(ctx context.Context, env macroBenchEnv, runID string) (bool, bool, error) {
	_, ownerFound, err := env.runs.GetRunHotStateOwner(ctx, runID)
	if err != nil {
		return true, false, fmt.Errorf("get hot-state owner for run %s: %w", runID, err)
	}

	rec, err := env.runs.GetRun(ctx, runID)
	if err != nil {
		return true, ownerFound, fmt.Errorf("get run %s: %w", runID, err)
	}
	if rec.RunID != runID {
		return true, ownerFound, fmt.Errorf("run id=%q, want %q", rec.RunID, runID)
	}

	return true, ownerFound, nil
}

func enqueueMacroContinuationExecutions(
	ctx context.Context,
	env macroBenchEnv,
	sourceReq *apipb.JobRequest,
	result macroExecutionResult,
) (int, error) {
	source, ok, err := cell.ExecutionEnvelopeFromRequest(sourceReq)
	if err != nil {
		return 0, fmt.Errorf("decode source execution envelope: %w", err)
	}

	if !ok {
		return 0, fmt.Errorf("missing source execution envelope")
	}

	jobDef := sourceReq.GetJob()
	if jobDef == nil {
		return 0, fmt.Errorf("source job is required")
	}

	enqueued := 0
	for _, child := range result.children {
		if child.ExecutionID == "" {
			continue
		}

		req := &apipb.JobRequest{
			Job:      macroCloneJob(jobDef),
			Metadata: macroCloneStringMap(source.Metadata),
		}

		dispatch := macroExecutionDispatchRecordFromTaskExecution(jobDef, source, child)
		if _, err := cell.AttachExecutionEnvelopeWithActions(req, dispatch, time.Now().UnixNano(), env.actionResolver); err != nil {
			return enqueued, fmt.Errorf("attach child execution envelope %s: %w", child.ExecutionID, err)
		}

		if _, err := env.apiQueue.Enqueue(ctx, req); err != nil {
			return enqueued, fmt.Errorf("enqueue child execution %s: %w", child.ExecutionID, err)
		}

		enqueued++
	}

	return enqueued, nil
}

func macroExecutionDispatchRecordFromTaskExecution(j *apipb.Job, source *cell.ExecutionEnvelope, rec dal.TaskExecutionRecord) dal.ExecutionDispatchRecord {
	return dal.ExecutionDispatchRecord{
		RunID:             rec.RunID,
		JobID:             j.GetId(),
		RunIndex:          source.RunIndex,
		TaskID:            rec.TaskID,
		TaskKey:           rec.TaskKey,
		TaskName:          rec.Name,
		TaskAttemptID:     rec.TaskAttemptID,
		SegmentID:         rec.SegmentID,
		SegmentName:       rec.SegmentName,
		SegmentStatus:     dal.SegmentStatusPending,
		ExecutionID:       rec.ExecutionID,
		ExecutionStatus:   dal.ExecutionStatusPending,
		CellID:            rec.CellID,
		Attempt:           rec.Attempt,
		DefinitionVersion: source.DefinitionVersion,
		DefinitionHash:    source.DefinitionHash,
		OwningCell:        source.CellID,
	}
}

func macroCloneJob(j *apipb.Job) *apipb.Job {
	if j == nil {
		return nil
	}

	cloned, ok := proto.Clone(j).(*apipb.Job)
	if !ok {
		return j
	}

	return cloned
}

func macroCloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}

	return out
}

func collectMacroWorkerResults(
	b *testing.B,
	resultCh <-chan macroWorkerResult,
	total int,
) []macroRunTimings {
	b.Helper()

	results := make([]macroRunTimings, 0, total)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for len(results) < total {
		select {
		case result := <-resultCh:
			if result.err != nil {
				b.Fatalf("drain macro queue: %v", result.err)
			}

			results = append(results, result.timings)
		case <-timeout.C:
			b.Fatalf("timed out draining macro queue: got %d of %d results", len(results), total)
		}
	}

	return results
}

func collectMacroDistributedTaskResults(
	b *testing.B,
	resultCh <-chan macroDistributedTaskResult,
	total int,
) []macroExecutionResult {
	b.Helper()

	results := make([]macroExecutionResult, 0, total)
	timeout := time.NewTimer(60 * time.Second)
	defer timeout.Stop()

	for len(results) < total {
		select {
		case result := <-resultCh:
			if result.err != nil {
				b.Fatalf("distributed macro task: %v", result.err)
			}

			results = append(results, result.execution)
		case <-timeout.C:
			b.Fatalf("timed out draining distributed macro tasks: got %d of %d results", len(results), total)
		}
	}

	return results
}

func collectMacroDistributedTerminalResults(
	b *testing.B,
	resultCh <-chan macroDistributedTerminalResult,
	total int,
) []macroRunTimings {
	b.Helper()

	results := make([]macroRunTimings, 0, total)
	timeout := time.NewTimer(60 * time.Second)
	defer timeout.Stop()

	for len(results) < total {
		select {
		case result := <-resultCh:
			if result.err != nil {
				b.Fatalf("distributed macro terminal run: %v", result.err)
			}

			results = append(results, result.timings)
		case <-timeout.C:
			b.Fatalf("timed out draining distributed macro terminal runs: got %d of %d results", len(results), total)
		}
	}

	return results
}

func collectMacroStatusReadResults(
	b *testing.B,
	resultCh <-chan macroStatusReadResult,
	total int,
) ([]int64, int, int) {
	b.Helper()

	samples := make([]int64, 0, total)
	ownerHits := 0
	ownerMisses := 0
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for len(samples) < total {
		select {
		case result := <-resultCh:
			if result.err != nil {
				b.Fatalf("status read: %v", result.err)
			}

			samples = append(samples, result.latency)
			if result.ownerChecked {
				if result.ownerFound {
					ownerHits++
				} else {
					ownerMisses++
				}
			}
		case <-timeout.C:
			b.Fatalf("timed out collecting status reads: got %d of %d results", len(samples), total)
		}
	}

	return samples, ownerHits, ownerMisses
}

type macroWorkerResult struct {
	timings macroRunTimings
	err     error
}

type macroExecutionResult struct {
	timings      macroRunTimings
	taskKey      string
	outcome      dal.ExecutionFinalizationOutcome
	children     []dal.TaskExecutionRecord
	activated    int
	childEnqueue int
}

type macroDistributedTaskResult struct {
	execution macroExecutionResult
	err       error
}

type macroDistributedTerminalResult struct {
	timings macroRunTimings
	err     error
}

func sendMacroStatusReadResult(ctx context.Context, ch chan<- macroStatusReadResult, result macroStatusReadResult) {
	select {
	case ch <- result:
	case <-ctx.Done():
	}
}

func sendMacroWorkerResult(ctx context.Context, ch chan<- macroWorkerResult, result macroWorkerResult) {
	select {
	case ch <- result:
	case <-ctx.Done():
	}
}

func sendMacroDistributedTaskResult(ctx context.Context, ch chan<- macroDistributedTaskResult, result macroDistributedTaskResult) {
	select {
	case ch <- result:
	case <-ctx.Done():
	}
}

func sendMacroDistributedTerminalResult(ctx context.Context, ch chan<- macroDistributedTerminalResult, result macroDistributedTerminalResult) {
	select {
	case ch <- result:
	case <-ctx.Done():
	}
}

func runMacroTriggerToTerminal(
	b *testing.B,
	ctx context.Context,
	env macroBenchEnv,
	jobID string,
	logSink interfaces.LogClient,
	i int,
) macroRunTimings {
	b.Helper()

	info, err := triggerMacroJob(ctx, env.handler, jobID, fmt.Sprintf("%s-%d", jobID, i))
	if err != nil {
		b.Fatal(err)
	}

	jobReq, dequeuedAt := waitForDequeuedJob(b, ctx, env.queue)
	dbTimings, err := loadDequeuedMacroRun(ctx, env, jobReq)
	if err != nil {
		b.Fatal(err)
	}

	timings, err := finishDequeuedMacroJob(ctx, env, jobReq, info, dequeuedAt, "macro-worker", logSink)
	if err != nil {
		b.Fatal(err)
	}

	timings.db.choreographyLoadRun = dbTimings.choreographyLoadRun
	return timings
}

func loadDequeuedMacroRun(ctx context.Context, env macroBenchEnv, jobReq *apipb.JobRequest) (macroDBTimings, error) {
	started := time.Now()
	if err := env.choreo.LoadRun(ctx, jobReq); err != nil {
		runID := ""
		if jobReq != nil && jobReq.GetJob() != nil {
			runID = jobReq.GetJob().GetRunId()
		}

		return macroDBTimings{}, fmt.Errorf("load choreography for dequeued run %s: %w", runID, err)
	}

	if env.choreo.DBBacked() {
		return macroDBTimings{}, nil
	}

	return macroDBTimings{choreographyLoadRun: time.Since(started).Nanoseconds()}, nil
}

func publishMacroHotStateOwner(ctx context.Context, env macroBenchEnv, runID string, leaseUntil time.Time) error {
	if env.choreo.DBBacked() {
		return nil
	}

	ownerID := "orchestrator:macro"
	if _, ok := env.choreo.(macroGRPCOrchestratorChoreography); ok {
		ownerID = "orchestrator:macro-grpc"
	}

	return env.runs.UpsertRunHotStateOwner(ctx, dal.RunHotStateOwnerUpdate{
		RunID:      runID,
		CellID:     dal.DefaultCellID,
		OwnerID:    ownerID,
		OwnerEpoch: "macro",
		LeaseUntil: leaseUntil,
	})
}

func finishDequeuedMacroExecution(
	ctx context.Context,
	env macroBenchEnv,
	jobReq *apipb.JobRequest,
	info macroTriggerInfo,
	dequeuedAt time.Time,
	workerID string,
	logSink interfaces.LogClient,
) (macroExecutionResult, error) {
	var dbTimings macroDBTimings
	queuedJob := jobReq.GetJob()
	if queuedJob.GetRunId() != info.runID {
		return macroExecutionResult{}, fmt.Errorf("dequeued run_id=%q, want %q", queuedJob.GetRunId(), info.runID)
	}

	executionEnvelope, ok, err := cell.ExecutionEnvelopeFromRequest(jobReq)
	if err != nil {
		return macroExecutionResult{}, fmt.Errorf("decode execution envelope: %w", err)
	}

	if !ok {
		return macroExecutionResult{}, fmt.Errorf("missing execution envelope")
	}

	queueAcceptedAt := macroQueueAcceptedAt(jobReq, info.httpAcceptedAt)

	deliveryID := queuedJob.GetDeliveryId()
	runID := queuedJob.GetRunId()
	if _, err := env.queue.Ack(ctx, &apipb.AckRequest{DeliveryId: &deliveryID}); err != nil {
		return macroExecutionResult{}, fmt.Errorf("ack delivery %s: %w", deliveryID, err)
	}

	claimStarted := time.Now()
	executionClaim, err := env.choreo.ClaimAndStartExecution(ctx, runID, executionEnvelope.ExecutionID, workerID, time.Now().Add(dal.DefaultLeaseTTL))
	claimedAt := time.Now()
	dbTimings.choreographyClaimAndStart = claimedAt.Sub(claimStarted).Nanoseconds()
	if env.choreo.DBBacked() {
		dbTimings.tryClaimExecution = dbTimings.choreographyClaimAndStart
	}

	if err != nil {
		return macroExecutionResult{}, fmt.Errorf("try claim execution %s: %w", executionEnvelope.ExecutionID, err)
	}

	if !executionClaim.Claimed {
		return macroExecutionResult{}, fmt.Errorf("execution %s was not claimed", executionEnvelope.ExecutionID)
	}

	if !env.choreo.DBBacked() {
		ownerStarted := time.Now()
		if err := publishMacroHotStateOwner(ctx, env, runID, time.Now().Add(dal.DefaultLeaseTTL)); err != nil {
			return macroExecutionResult{}, fmt.Errorf("publish hot-state owner for run %s: %w", runID, err)
		}

		dbTimings.hotStateOwner = time.Since(ownerStarted).Nanoseconds()
	}

	logDone := make(chan job.LogStreamWaiter, 1)
	exec := job.NewExecutor()
	exec.TestLogStreamHook = logDone

	if env.choreo.DBBacked() {
		markStartedAt := time.Now()
		if err := env.runs.MarkExecutionStarted(ctx, executionEnvelope.ExecutionID); err != nil {
			return macroExecutionResult{}, fmt.Errorf("mark execution started %s: %w", executionEnvelope.ExecutionID, err)
		}

		dbTimings.markExecutionStarted = time.Since(markStartedAt).Nanoseconds()
	}

	execOptions := job.ExecuteOptions{
		ArtifactPublisher: env.artifactPublisher,
		ActionResolver:    env.actionResolver,
		ActionLocks:       actionregistry.CloneActionLocks(env.actionLocks),
	}

	if err := exec.ExecuteTaskWithOptions(ctx, queuedJob, executionEnvelope.TaskKey, logSink, env.log, execOptions); err != nil {
		_, _ = env.choreo.CompleteExecution(ctx, runID, executionEnvelope.ExecutionID, workerID, executionClaim.ClaimToken, dal.ExecutionStatusFailed, dal.FailureCodeExecution, err.Error())
		return macroExecutionResult{}, fmt.Errorf("execute task %s: %w", queuedJob.GetId(), err)
	}

	finalizeStarted := time.Now()
	finalized, err := env.choreo.CompleteExecution(ctx, runID, executionEnvelope.ExecutionID, workerID, executionClaim.ClaimToken, dal.ExecutionStatusSucceeded, "", "")
	terminalAt := time.Now()
	dbTimings.choreographyFinalize = terminalAt.Sub(finalizeStarted).Nanoseconds()
	if env.choreo.DBBacked() {
		dbTimings.finalizeExecution = dbTimings.choreographyFinalize
	}
	if err != nil {
		return macroExecutionResult{}, fmt.Errorf("finalize execution %s: %w", executionEnvelope.ExecutionID, err)
	}

	flushStarted := time.Now()
	if err := waitForLogFlushErr(logDone); err != nil {
		return macroExecutionResult{}, err
	}

	logFlush := time.Since(flushStarted).Nanoseconds()

	return macroExecutionResult{
		timings: macroRunTimings{
			runID:                       info.runID,
			httpAcceptedToQueueAccepted: max(queueAcceptedAt.Sub(info.httpAcceptedAt).Nanoseconds(), 0),
			queueAcceptedToDequeued:     max(dequeuedAt.Sub(queueAcceptedAt).Nanoseconds(), 0),
			dequeuedToClaimed:           max(claimedAt.Sub(dequeuedAt).Nanoseconds(), 0),
			claimedToTerminal:           max(terminalAt.Sub(claimedAt).Nanoseconds(), 0),
			acceptedToTerminal:          max(terminalAt.Sub(info.httpAcceptedAt).Nanoseconds(), 0),
			triggerToTerminal:           max(terminalAt.Sub(info.triggerStart).Nanoseconds(), 0),
			logFlush:                    max(logFlush, 0),
			db:                          dbTimings,
		},
		taskKey:   executionEnvelope.TaskKey,
		outcome:   finalized.Outcome,
		children:  finalized.Children,
		activated: finalized.Activated,
	}, nil
}

func finishDequeuedMacroJob(
	ctx context.Context,
	env macroBenchEnv,
	jobReq *apipb.JobRequest,
	info macroTriggerInfo,
	dequeuedAt time.Time,
	workerID string,
	logSink interfaces.LogClient,
) (macroRunTimings, error) {
	result, err := finishDequeuedMacroExecution(ctx, env, jobReq, info, dequeuedAt, workerID, logSink)
	if err != nil {
		return macroRunTimings{}, err
	}

	if result.outcome != dal.ExecutionFinalizationOutcomeRunSucceeded {
		return macroRunTimings{}, fmt.Errorf("finalize execution %s outcome %q", result.taskKey, result.outcome)
	}

	return result.timings, nil
}

type macroLogTimings struct {
	flush  int64
	replay int64
	chunks int
}

func runMacroLogHeavyTriggerToTerminal(
	b *testing.B,
	ctx context.Context,
	env macroLogBenchEnv,
	i int,
) (macroRunTimings, macroLogTimings) {
	b.Helper()

	timings := runMacroTriggerToTerminal(b, ctx, env.macroBenchEnv, env.job.id, env.logSink, i)
	runID := timings.runID
	if runID == "" {
		b.Fatal("log-heavy run did not return a run id")
	}

	entries := env.logSink.entryCount(runID)
	if entries == 0 {
		b.Fatalf("log-heavy run %s did not store log entries", runID)
	}

	replayStarted := time.Now()
	replayed, err := env.store.List(runID)
	replayDone := time.Now()
	if err != nil {
		b.Fatalf("replay logs for run %s: %v", runID, err)
	}

	if len(replayed) != entries {
		b.Fatalf("replayed entries=%d, stored entries=%d", len(replayed), entries)
	}

	return timings, macroLogTimings{
		flush:  timings.logFlush,
		replay: replayDone.Sub(replayStarted).Nanoseconds(),
		chunks: len(replayed),
	}
}

type storeLogClient struct {
	store *logserver.LocalRunLogStore
	stats *storeLogStats
}

type storeLogStats struct {
	mu     sync.Mutex
	counts map[string]int
}

func newStoreLogStats() *storeLogStats {
	return &storeLogStats{counts: make(map[string]int)}
}

func (c storeLogClient) StreamLogs(context.Context) (interfaces.LogStream, error) {
	return &storeLogStream{store: c.store, stats: c.stats}, nil
}

func (c storeLogClient) Close() error {
	return nil
}

func (c storeLogClient) entryCount(runID string) int {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	return c.stats.counts[runID]
}

type storeLogStream struct {
	store *logserver.LocalRunLogStore
	stats *storeLogStats
}

func (s *storeLogStream) Send(chunk *apipb.LogChunk) error {
	runID := chunk.GetRunId()
	entry := logserver.LogEntry{
		Timestamp: time.Now(),
		Stream:    chunk.GetStream(),
		Sequence:  chunk.GetSequence(),
		Data:      append([]byte(nil), chunk.GetData()...),
		Completed: chunk.GetCompleted(),
	}

	if ts := chunk.GetTimestamp(); ts != nil {
		entry.Timestamp = ts.AsTime()
	}

	if err := s.store.Append(runID, entry); err != nil {
		return err
	}

	s.stats.mu.Lock()
	s.stats.counts[runID]++
	s.stats.mu.Unlock()
	return nil
}

func (s *storeLogStream) CloseSend() error {
	return nil
}

func waitForDequeuedJob(b *testing.B, ctx context.Context, queueService macroWorkerQueue) (*apipb.JobRequest, time.Time) {
	b.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		jobReq, err := queueService.TryDequeue(ctx, &apipb.DequeueRequest{})
		if err != nil {
			b.Fatalf("try dequeue: %v", err)
		}

		if jobReq != nil {
			return jobReq, time.Now()
		}

		time.Sleep(10 * time.Microsecond)
	}

	b.Fatal("timed out waiting for async enqueue")
	return nil, time.Time{}
}

func waitForLogFlush(b *testing.B, ch <-chan job.LogStreamWaiter) {
	b.Helper()

	if err := waitForLogFlushErr(ch); err != nil {
		b.Fatal(err)
	}
}

func waitForLogFlushErr(ch <-chan job.LogStreamWaiter) error {
	select {
	case waiter := <-ch:
		if err := waiter.WaitForDone(2 * time.Second); err != nil {
			return fmt.Errorf("wait for log flush: %w", err)
		}

		return nil
	default:
		return fmt.Errorf("executor did not expose log stream waiter")
	}
}

func parseUnixNano(raw string) (int64, error) {
	return strconv.ParseInt(raw, 10, 64)
}

func reportLatencyMetrics(b *testing.B, prefix string, values []int64) {
	b.Helper()
	if len(values) == 0 {
		return
	}

	slices.Sort(values)
	b.ReportMetric(float64(quantile(values, 0.50))/float64(time.Millisecond), prefix+"_p50_ms")
	b.ReportMetric(float64(quantile(values, 0.95))/float64(time.Millisecond), prefix+"_p95_ms")
	b.ReportMetric(float64(quantile(values, 0.99))/float64(time.Millisecond), prefix+"_p99_ms")
}

func reportMacroDBTimingMetrics(b *testing.B, values []macroDBTimings) {
	b.Helper()
	if len(values) == 0 {
		return
	}

	reportMacroDBTimingMetric(b, "db_create_run", values, func(v macroDBTimings) int64 {
		return v.createRun
	})

	reportMacroDBTimingMetric(b, "db_attach_envelope", values, func(v macroDBTimings) int64 {
		return v.attachEnvelope
	})

	reportMacroDBTimingMetric(b, "db_touch_dispatched", values, func(v macroDBTimings) int64 {
		return v.touchDispatched
	})

	reportMacroDBTimingMetric(b, "db_try_claim_execution", values, func(v macroDBTimings) int64 {
		return v.tryClaimExecution
	})

	reportMacroDBTimingMetric(b, "db_mark_execution_started", values, func(v macroDBTimings) int64 {
		return v.markExecutionStarted
	})

	reportMacroDBTimingMetric(b, "db_finalize_execution", values, func(v macroDBTimings) int64 {
		return v.finalizeExecution
	})

	reportMacroDBTimingMetric(b, "db_hot_state_owner", values, func(v macroDBTimings) int64 {
		return v.hotStateOwner
	})

	reportMacroDBTimingMetric(b, "db_total", values, macroDBTimingTotal)

	reportMacroDBTimingMetric(b, "choreography_load_run", values, func(v macroDBTimings) int64 {
		return v.choreographyLoadRun
	})

	reportMacroDBTimingMetric(b, "choreography_claim_and_start", values, func(v macroDBTimings) int64 {
		return v.choreographyClaimAndStart
	})

	reportMacroDBTimingMetric(b, "choreography_finalize", values, func(v macroDBTimings) int64 {
		return v.choreographyFinalize
	})

	reportMacroDBTimingMetric(b, "choreography_total", values, macroChoreographyTimingTotal)
}

func reportMacroArtifactMetrics(b *testing.B, publisher action.ArtifactPublisher, runs int) {
	b.Helper()

	stats, ok := publisher.(*macroArtifactPublisher)
	if !ok || stats == nil {
		return
	}

	artifacts := stats.artifacts.Load()
	bytes := stats.bytes.Load()
	b.ReportMetric(float64(artifacts), "artifacts")
	b.ReportMetric(float64(bytes), "artifact_bytes")
	if runs > 0 {
		b.ReportMetric(float64(artifacts)/float64(runs), "artifacts/run")
		b.ReportMetric(float64(bytes)/float64(runs), "artifact_bytes/run")
	}
}

func reportMacroDBTimingMetric(
	b *testing.B,
	prefix string,
	values []macroDBTimings,
	extract func(macroDBTimings) int64,
) {
	b.Helper()

	samples := make([]int64, 0, len(values))
	for _, value := range values {
		if sample := extract(value); sample > 0 {
			samples = append(samples, sample)
		}
	}

	reportLatencyMetrics(b, prefix, samples)
}

func macroDBTimingTotal(value macroDBTimings) int64 {
	return value.createRun +
		value.attachEnvelope +
		value.touchDispatched +
		value.tryClaimExecution +
		value.markExecutionStarted +
		value.finalizeExecution +
		value.hotStateOwner
}

func macroChoreographyTimingTotal(value macroDBTimings) int64 {
	return value.choreographyLoadRun +
		value.choreographyClaimAndStart +
		value.choreographyFinalize
}

func reportMacroDBPoolMetrics(b *testing.B, before, after sql.DBStats, totalRuns int) {
	b.Helper()

	waitCount := after.WaitCount - before.WaitCount
	if waitCount < 0 {
		waitCount = 0
	}

	waitDuration := after.WaitDuration - before.WaitDuration
	if waitDuration < 0 {
		waitDuration = 0
	}

	b.ReportMetric(float64(after.MaxOpenConnections), "db_pool_max_open_conns")
	b.ReportMetric(float64(waitCount), "db_pool_wait_count")
	b.ReportMetric(float64(waitDuration)/float64(time.Millisecond), "db_pool_wait_ms")

	if totalRuns > 0 {
		b.ReportMetric(float64(waitCount)/float64(totalRuns), "db_pool_waits/run")
	}
}

func reportCountMetrics(b *testing.B, prefix string, values []int64) {
	b.Helper()
	if len(values) == 0 {
		return
	}

	slices.Sort(values)
	b.ReportMetric(float64(quantile(values, 0.50)), prefix+"_p50")
	b.ReportMetric(float64(quantile(values, 0.95)), prefix+"_p95")
	b.ReportMetric(float64(quantile(values, 0.99)), prefix+"_p99")
}

func sumNanoseconds(values []int64) int64 {
	var total int64
	for _, value := range values {
		total += value
	}

	return total
}

func quantile(values []int64, q float64) int64 {
	if len(values) == 0 {
		return 0
	}

	if q <= 0 {
		return values[0]
	}

	if q >= 1 {
		return values[len(values)-1]
	}

	return values[int(float64(len(values)-1)*q)]
}
