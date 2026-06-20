package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action/actionregistry"
	"vectis/internal/action/builtins"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/dispatchmeta"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/observability"
	"vectis/internal/orchestrator"
	"vectis/internal/secrets"
	sourcepkg "vectis/internal/source"
	"vectis/internal/spire"
	"vectis/internal/taskfinalize"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/runfixture"
	"vectis/internal/workercore"
	"vectis/internal/workloadidentity"
	workersdk "vectis/sdk/workercore"

	"github.com/spf13/viper"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func workerStrp(s string) *string { return &s }

type countingExecutionChoreographer struct {
	inner executionChoreographer
	mu    sync.Mutex
	loads int
}

func (c *countingExecutionChoreographer) LoadRun(ctx context.Context, j *api.Job, env *cell.ExecutionEnvelope, snapshots []orchestrator.TaskExecutionSnapshot) error {
	c.mu.Lock()
	c.loads++
	c.mu.Unlock()
	return c.inner.LoadRun(ctx, j, env, snapshots)
}

func (c *countingExecutionChoreographer) ClaimAndStartExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	return c.inner.ClaimAndStartExecution(ctx, env, owner, leaseUntil)
}

func (c *countingExecutionChoreographer) RenewExecutionLease(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken string, leaseUntil time.Time) error {
	return c.inner.RenewExecutionLease(ctx, env, owner, claimToken, leaseUntil)
}

func (c *countingExecutionChoreographer) CompleteExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	return c.inner.CompleteExecution(ctx, env, owner, claimToken, status, failureCode, reason)
}

func (c *countingExecutionChoreographer) RequiresDurableTaskRows() bool {
	return c.inner.RequiresDurableTaskRows()
}

func (c *countingExecutionChoreographer) LoadCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.loads
}

type workerDescriptorResolver map[string]actionregistry.Descriptor

func (r workerDescriptorResolver) ResolveDescriptor(uses string) (actionregistry.Descriptor, error) {
	descriptor, ok := r[uses]
	if !ok {
		return actionregistry.Descriptor{}, fmt.Errorf("unknown action: %s", uses)
	}

	return descriptor, nil
}

func attachPendingExecutionEnvelopeForTest(t *testing.T, runs dal.RunsRepository, j *api.Job, runID string) *cell.ExecutionEnvelope {
	t.Helper()

	dispatch, err := runs.GetPendingExecution(context.Background(), runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if dispatch.DefinitionHash == "" {
		dispatch.DefinitionHash = "test-definition-hash"
	}

	env, err := cell.AttachExecutionEnvelope(&api.JobRequest{Job: j}, dispatch, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	return env
}

func testWorkerCore(executor *job.Executor) workercore.Core {
	return workercore.NewExecutorCore(executor)
}

func TestWorkerCheckoutCacheRemoteURLsUsesPersistentSources(t *testing.T) {
	db := dbtest.NewTestDB(t)
	sources := dal.NewSQLRepositories(db).Sources()
	ctx := context.Background()

	for _, rec := range []dal.SourceRepositoryRecord{
		{
			RepositoryID:       "persistent",
			NamespaceID:        1,
			SourceKind:         dal.SourceKindLocalCheckout,
			CheckoutPath:       "/work/persistent",
			WorkerCacheMode:    dal.SourceWorkerCacheModePersistent,
			CanonicalURL:       "https://mirror.invalid/persistent.git",
			FallbackRemoteURLs: []string{"https://tier1.invalid/persistent.git", "https://mirror.invalid/persistent.git"},
			Enabled:            true,
		},
		{
			RepositoryID:    "ephemeral",
			NamespaceID:     1,
			SourceKind:      dal.SourceKindLocalCheckout,
			CheckoutPath:    "/work/ephemeral",
			WorkerCacheMode: dal.SourceWorkerCacheModeEphemeral,
			CanonicalURL:    "https://mirror.invalid/ephemeral.git",
			Enabled:         true,
		},
		{
			RepositoryID:    "disabled-persistent",
			NamespaceID:     1,
			SourceKind:      dal.SourceKindLocalCheckout,
			CheckoutPath:    "/work/disabled-persistent",
			WorkerCacheMode: dal.SourceWorkerCacheModePersistent,
			CanonicalURL:    "https://mirror.invalid/disabled.git",
			Enabled:         false,
		},
	} {
		if _, err := sources.CreateRepository(ctx, rec); err != nil {
			t.Fatalf("create source repository %s: %v", rec.RepositoryID, err)
		}
	}

	w := &worker{
		sourceRepositories: sources,
		logger:             mocks.NewMockLogger(),
	}

	got := w.checkoutCacheRemoteURLs(ctx)
	want := []string{
		"https://mirror.invalid/persistent.git",
		"https://tier1.invalid/persistent.git",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("checkout cache remotes = %+v, want %+v", got, want)
	}

	gotStructured := w.checkoutCacheRemotes(ctx)
	wantStructured := []workercore.CheckoutCacheRemote{
		{
			RemoteURL:          "https://mirror.invalid/persistent.git",
			FallbackRemoteURLs: []string{"https://tier1.invalid/persistent.git"},
		},
	}

	if !reflect.DeepEqual(gotStructured, wantStructured) {
		t.Fatalf("structured checkout cache remotes = %+v, want %+v", gotStructured, wantStructured)
	}
}

func TestWorkerCheckoutCacheRemotesResolveCredentials(t *testing.T) {
	db := dbtest.NewTestDB(t)
	sources := dal.NewSQLRepositories(db).Sources()
	ctx := context.Background()

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID:    "private",
		NamespaceID:     1,
		SourceKind:      dal.SourceKindLocalCheckout,
		CheckoutPath:    "/work/private",
		WorkerCacheMode: dal.SourceWorkerCacheModePersistent,
		CanonicalURL:    "https://mirror.invalid/private.git",
		CredentialRef:   "secret://git/private",
		Enabled:         true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	var resolvedRepositoryID string
	w := &worker{
		sourceRepositories: sources,
		logger:             mocks.NewMockLogger(),
		sourceCredentialResolver: func(_ context.Context, rec dal.SourceRepositoryRecord) (sourcepkg.GitCredentials, error) {
			resolvedRepositoryID = rec.RepositoryID
			return sourcepkg.GitCredentials{Username: "oauth2", Password: "token"}, nil
		},
	}

	remotes, failures := w.checkoutCacheRemotesWithFailures(ctx)
	if len(failures) != 0 {
		t.Fatalf("credential failures = %+v, want none", failures)
	}
	if resolvedRepositoryID != "private" {
		t.Fatalf("resolved repository id = %q, want private", resolvedRepositoryID)
	}
	if len(remotes) != 1 ||
		remotes[0].RemoteURL != "https://mirror.invalid/private.git" ||
		remotes[0].Credentials.Username != "oauth2" ||
		remotes[0].Credentials.Password != "token" {
		t.Fatalf("checkout cache remotes = %+v, want credentialed private remote", remotes)
	}
}

func TestWorkerCheckoutCacheRemotesReportsCredentialFailure(t *testing.T) {
	db := dbtest.NewTestDB(t)
	sources := dal.NewSQLRepositories(db).Sources()
	ctx := context.Background()

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID:    "private",
		NamespaceID:     1,
		SourceKind:      dal.SourceKindLocalCheckout,
		CheckoutPath:    "/work/private",
		WorkerCacheMode: dal.SourceWorkerCacheModePersistent,
		CanonicalURL:    "https://mirror.invalid/private.git",
		CredentialRef:   "secret://git/private",
		Enabled:         true,
	}); err != nil {
		t.Fatalf("create source repository: %v", err)
	}

	w := &worker{
		sourceRepositories: sources,
		logger:             mocks.NewMockLogger(),
		sourceCredentialResolver: func(context.Context, dal.SourceRepositoryRecord) (sourcepkg.GitCredentials, error) {
			return sourcepkg.GitCredentials{}, fmt.Errorf("secret missing")
		},
	}

	remotes, failures := w.checkoutCacheRemotesWithFailures(ctx)
	if len(remotes) != 0 {
		t.Fatalf("checkout cache remotes = %+v, want none", remotes)
	}
	if len(failures) != 1 ||
		failures[0].RemoteURL != "https://mirror.invalid/private.git" ||
		!strings.Contains(failures[0].Message, "secret missing") {
		t.Fatalf("credential failures = %+v, want private remote failure", failures)
	}
}

func TestWorkerWarmCheckoutCacheUsesPersistentSources(t *testing.T) {
	db := dbtest.NewTestDB(t)
	sources := dal.NewSQLRepositories(db).Sources()
	ctx := context.Background()

	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID:       "persistent",
		NamespaceID:        1,
		SourceKind:         dal.SourceKindLocalCheckout,
		CheckoutPath:       "/work/persistent",
		WorkerCacheMode:    dal.SourceWorkerCacheModePersistent,
		CanonicalURL:       "https://mirror.invalid/persistent.git",
		FallbackRemoteURLs: []string{"https://tier1.invalid/persistent.git"},
		Enabled:            true,
	}); err != nil {
		t.Fatalf("create persistent source repository: %v", err)
	}

	warmer := &recordingCheckoutCacheWarmer{}
	w := &worker{
		sourceRepositories: sources,
		logger:             mocks.NewMockLogger(),
	}

	w.warmCheckoutCache(ctx, warmer)

	want := []string{
		"https://mirror.invalid/persistent.git",
		"https://tier1.invalid/persistent.git",
	}
	wantStructured := []workercore.CheckoutCacheRemote{
		{
			RemoteURL:          "https://mirror.invalid/persistent.git",
			FallbackRemoteURLs: []string{"https://tier1.invalid/persistent.git"},
		},
	}
	if len(warmer.requests) != 1 ||
		!reflect.DeepEqual(warmer.requests[0].RemoteURLs, want) ||
		!reflect.DeepEqual(warmer.requests[0].Remotes, wantStructured) {
		t.Fatalf("warm requests = %+v, want one request with %+v", warmer.requests, want)
	}
}

func TestWorkerWarmCheckoutCacheAppliesTimeout(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.execution.checkout_cache_warm_timeout", 10*time.Millisecond)

	db := dbtest.NewTestDB(t)
	sources := dal.NewSQLRepositories(db).Sources()
	ctx := context.Background()
	if _, err := sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID:    "persistent",
		NamespaceID:     1,
		SourceKind:      dal.SourceKindLocalCheckout,
		CheckoutPath:    "/work/persistent",
		WorkerCacheMode: dal.SourceWorkerCacheModePersistent,
		CanonicalURL:    "https://mirror.invalid/persistent.git",
		Enabled:         true,
	}); err != nil {
		t.Fatalf("create persistent source repository: %v", err)
	}

	warmer := &blockingCheckoutCacheWarmer{}
	w := &worker{
		sourceRepositories: sources,
		logger:             mocks.NewMockLogger(),
	}

	w.warmCheckoutCache(ctx, warmer)

	if !errors.Is(warmer.err, context.DeadlineExceeded) {
		t.Fatalf("warm error = %v, want deadline exceeded", warmer.err)
	}
}

func TestCheckoutCacheWarmJitterDelays(t *testing.T) {
	interval := time.Minute
	if got := checkoutCacheWarmInitialDelay(interval, 0, "worker-a"); got != 0 {
		t.Fatalf("initial delay without jitter = %v, want 0", got)
	}

	initial := checkoutCacheWarmInitialDelay(interval, 0.2, "worker-a")
	if initial < 0 || initial > 12*time.Second {
		t.Fatalf("initial delay = %v, want within 20%% of interval", initial)
	}

	loopDelay := checkoutCacheWarmLoopDelay(interval, 0.2, "worker-a")
	if loopDelay < interval || loopDelay > interval+12*time.Second {
		t.Fatalf("loop delay = %v, want interval plus bounded jitter", loopDelay)
	}

	if again := checkoutCacheWarmLoopDelay(interval, 0.2, "worker-a"); again != loopDelay {
		t.Fatalf("loop delay is not stable: got %v then %v", loopDelay, again)
	}
}

type recordingCheckoutCacheWarmer struct {
	requests []workercore.WarmCheckoutCacheRequest
	result   workercore.WarmCheckoutCacheResult
	err      error
}

func (w *recordingCheckoutCacheWarmer) WarmCheckoutCache(_ context.Context, req workercore.WarmCheckoutCacheRequest) (workercore.WarmCheckoutCacheResult, error) {
	w.requests = append(w.requests, req)
	return w.result, w.err
}

type blockingCheckoutCacheWarmer struct {
	err error
}

func (w *blockingCheckoutCacheWarmer) WarmCheckoutCache(ctx context.Context, _ workercore.WarmCheckoutCacheRequest) (workercore.WarmCheckoutCacheResult, error) {
	<-ctx.Done()
	w.err = ctx.Err()
	return workercore.WarmCheckoutCacheResult{}, w.err
}

func TestWorkerMarkExecutionStarted_DefersDurableStartForOrchestratorRuns(t *testing.T) {
	ctx := context.Background()
	env := &cell.ExecutionEnvelope{
		RunID:       "run-root",
		TaskID:      "run-root:root",
		TaskKey:     dal.RootTaskKey,
		ExecutionID: "execution-root",
	}

	store := &mocks.MockRunsRepository{}
	w := &worker{
		runCtx:        context.Background(),
		choreographer: newLocalOrchestratorChoreographer(t),
		store:         store,
		catalog:       cell.NewCatalogEventPublisher("local", nil),
	}

	w.markExecutionStarted(ctx, env)

	if len(store.ExecutionTransitions) != 0 {
		t.Fatalf("orchestrator root start durable transitions: got %+v, want none", store.ExecutionTransitions)
	}
}

func TestWorkerMarkExecutionStarted_PersistsDurableStartForSQLRuns(t *testing.T) {
	ctx := context.Background()
	env := &cell.ExecutionEnvelope{
		RunID:       "run-root",
		TaskID:      "run-root:root",
		TaskKey:     dal.RootTaskKey,
		ExecutionID: "execution-root",
	}

	store := &mocks.MockRunsRepository{}
	w := &worker{
		runCtx:        context.Background(),
		choreographer: sqlExecutionChoreographer{runs: store},
		store:         store,
		catalog:       cell.NewCatalogEventPublisher("local", nil),
	}

	w.markExecutionStarted(ctx, env)

	want := []string{"execution-root:" + dal.ExecutionStatusRunning}
	if fmt.Sprint(store.ExecutionTransitions) != fmt.Sprint(want) {
		t.Fatalf("SQL durable start transitions: got %+v, want %+v", store.ExecutionTransitions, want)
	}
}

func TestWorkerPublishRunHotStateOwner_OnlyForOrchestratorRuns(t *testing.T) {
	ctx := context.Background()
	leaseUntil := time.Now().Add(dal.DefaultLeaseTTL).UTC()
	env := &cell.ExecutionEnvelope{
		RunID:       "run-hot",
		TaskID:      "run-hot:root",
		TaskKey:     dal.RootTaskKey,
		ExecutionID: "execution-hot",
		CellID:      "iad-a",
	}

	store := &mocks.MockRunsRepository{}
	orchestratorWorker := &worker{
		runCtx:             context.Background(),
		workerID:           "worker-hot",
		cellID:             "iad-a",
		choreographer:      newLocalOrchestratorChoreographer(t),
		store:              store,
		hotStateOwnerID:    "orchestrator:registry:iad-a",
		hotStateOwnerEpoch: "epoch-hot",
	}

	if err := orchestratorWorker.publishRunHotStateOwner(ctx, env, leaseUntil); err != nil {
		t.Fatalf("publish orchestrator owner: %v", err)
	}

	if store.LastHotStateOwner.RunID != env.RunID ||
		store.LastHotStateOwner.CellID != "iad-a" ||
		store.LastHotStateOwner.OwnerID != "orchestrator:registry:iad-a" ||
		store.LastHotStateOwner.OwnerEpoch != "epoch-hot" ||
		!store.LastHotStateOwner.LeaseUntil.Equal(leaseUntil) {
		t.Fatalf("hot-state owner update: %+v", store.LastHotStateOwner)
	}

	childEnv := *env
	childEnv.TaskID = "run-hot:child"
	childEnv.TaskKey = "child"
	childEnv.ExecutionID = "execution-child"
	store.LastHotStateOwner = dal.RunHotStateOwnerUpdate{}
	if err := orchestratorWorker.publishRunHotStateOwner(ctx, &childEnv, leaseUntil); err != nil {
		t.Fatalf("publish fresh child owner: %v", err)
	}

	if store.LastHotStateOwner.RunID != "" {
		t.Fatalf("fresh child owner should not republish, got %+v", store.LastHotStateOwner)
	}

	store.HotStateOwner.LeaseUntil = time.Now().Add(time.Minute).UTC()
	if err := orchestratorWorker.publishRunHotStateOwner(ctx, &childEnv, leaseUntil); err != nil {
		t.Fatalf("publish stale child owner: %v", err)
	}

	if store.LastHotStateOwner.RunID != "" {
		t.Fatalf("child start should not renew stale owner, got %+v", store.LastHotStateOwner)
	}

	if err := orchestratorWorker.renewRunHotStateOwner(ctx, &childEnv, leaseUntil); err != nil {
		t.Fatalf("renew child owner: %v", err)
	}

	if store.LastHotStateOwner.RunID != env.RunID {
		t.Fatalf("child lease renewal should refresh owner, got %+v", store.LastHotStateOwner)
	}

	sqlStore := &mocks.MockRunsRepository{}
	sqlWorker := &worker{
		runCtx:        context.Background(),
		workerID:      "worker-sql",
		cellID:        "iad-a",
		choreographer: sqlExecutionChoreographer{runs: sqlStore},
		store:         sqlStore,
	}

	if err := sqlWorker.publishRunHotStateOwner(ctx, env, leaseUntil); err != nil {
		t.Fatalf("publish SQL owner: %v", err)
	}

	if sqlStore.LastHotStateOwner.RunID != "" {
		t.Fatalf("SQL-backed worker should not publish hot-state owner, got %+v", sqlStore.LastHotStateOwner)
	}
}

func TestWorkerMirrorsHotStateSecretExecutionClaim(t *testing.T) {
	ctx := context.Background()
	leaseUntil := time.Now().Add(time.Minute).UTC()
	fileType := api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE
	secretJob := &api.Job{
		Secrets: []*api.SecretReference{{
			Id:       workerStrp("token"),
			Ref:      workerStrp("encryptedfs://team/token"),
			TaskKeys: []string{"secret-lane"},
			Delivery: &api.SecretDelivery{
				Type: &fileType,
				Path: workerStrp("token"),
			},
		}},
	}

	secretEnv := &cell.ExecutionEnvelope{
		RunID:       "run-secret",
		TaskID:      "run-secret:secret-lane",
		TaskKey:     "secret-lane",
		ExecutionID: "execution-secret",
		CellID:      "local",
	}

	secretStore := &mocks.MockRunsRepository{}
	secretWorker := &worker{
		runCtx:        context.Background(),
		workerID:      "worker-secret",
		choreographer: newLocalOrchestratorChoreographer(t),
		store:         secretStore,
	}

	if err := secretWorker.mirrorExecutionClaim(ctx, secretJob, secretEnv, "claim-secret", leaseUntil); err != nil {
		t.Fatalf("mirror secret execution claim: %v", err)
	}

	if secretStore.LastMirroredExecID != "execution-secret" || secretStore.LastMirroredToken != "claim-secret" {
		t.Fatalf("secret execution mirror = id %q token %q", secretStore.LastMirroredExecID, secretStore.LastMirroredToken)
	}

	if err := secretWorker.renewMirroredExecutionClaim(ctx, secretJob, secretEnv, "claim-secret", leaseUntil.Add(time.Minute)); err != nil {
		t.Fatalf("renew secret mirrored claim: %v", err)
	}

	if secretStore.LastExecutionRenewID != "execution-secret" {
		t.Fatalf("secret execution renew id = %q, want execution-secret", secretStore.LastExecutionRenewID)
	}

	plainStore := &mocks.MockRunsRepository{}
	plainWorker := &worker{
		runCtx:        context.Background(),
		workerID:      "worker-plain",
		choreographer: newLocalOrchestratorChoreographer(t),
		store:         plainStore,
	}

	plainEnv := &cell.ExecutionEnvelope{
		RunID:       "run-plain",
		TaskID:      "run-plain:plain-lane",
		TaskKey:     "plain-lane",
		ExecutionID: "execution-plain",
		CellID:      "local",
	}

	if err := plainWorker.mirrorExecutionClaim(ctx, &api.Job{}, plainEnv, "claim-plain", leaseUntil); err != nil {
		t.Fatalf("mirror plain execution claim: %v", err)
	}

	if plainStore.LastMirroredExecID != "" {
		t.Fatalf("plain hot-state execution should not mirror, got %q", plainStore.LastMirroredExecID)
	}

	if err := plainWorker.renewMirroredExecutionClaim(ctx, &api.Job{}, plainEnv, "claim-plain", leaseUntil.Add(time.Minute)); err != nil {
		t.Fatalf("renew plain mirrored claim: %v", err)
	}

	if plainStore.LastExecutionRenewID != "" {
		t.Fatalf("plain hot-state execution should not renew mirror, got %q", plainStore.LastExecutionRenewID)
	}

	rootStore := &mocks.MockRunsRepository{}
	rootWorker := &worker{
		runCtx:        context.Background(),
		workerID:      "worker-root",
		choreographer: newLocalOrchestratorChoreographer(t),
		store:         rootStore,
	}

	rootEnv := &cell.ExecutionEnvelope{
		RunID:       "run-root",
		TaskID:      "run-root:root",
		TaskKey:     dal.RootTaskKey,
		ExecutionID: "execution-root",
		CellID:      "local",
	}

	if err := rootWorker.mirrorExecutionClaim(ctx, &api.Job{}, rootEnv, "claim-root", leaseUntil); err != nil {
		t.Fatalf("mirror root hot-state execution claim: %v", err)
	}

	if rootStore.LastMirroredExecID != "" {
		t.Fatalf("root hot-state execution should not mirror, got %q", rootStore.LastMirroredExecID)
	}

	if err := rootWorker.renewMirroredExecutionClaim(ctx, &api.Job{}, rootEnv, "claim-root", leaseUntil.Add(time.Minute)); err != nil {
		t.Fatalf("renew root hot-state mirrored claim: %v", err)
	}

	if rootStore.LastExecutionRenewID != "" {
		t.Fatalf("root hot-state execution should not renew mirror, got %q", rootStore.LastExecutionRenewID)
	}

	sqlStore := &mocks.MockRunsRepository{}
	sqlWorker := &worker{
		runCtx:        context.Background(),
		workerID:      "worker-sql",
		choreographer: sqlExecutionChoreographer{runs: sqlStore},
		store:         sqlStore,
	}

	if err := sqlWorker.mirrorExecutionClaim(ctx, &api.Job{}, rootEnv, "claim-sql", leaseUntil); err != nil {
		t.Fatalf("mirror SQL execution claim: %v", err)
	}

	if sqlStore.LastMirroredExecID != "execution-root" || sqlStore.LastMirroredToken != "claim-sql" {
		t.Fatalf("SQL execution mirror = id %q token %q", sqlStore.LastMirroredExecID, sqlStore.LastMirroredToken)
	}

	if err := sqlWorker.renewMirroredExecutionClaim(ctx, &api.Job{}, rootEnv, "claim-sql", leaseUntil.Add(time.Minute)); err != nil {
		t.Fatalf("renew SQL mirrored claim: %v", err)
	}

	if sqlStore.LastExecutionRenewID != "execution-root" {
		t.Fatalf("SQL execution renew id = %q, want execution-root", sqlStore.LastExecutionRenewID)
	}
}

func TestWorkerPrepareRunForExecutionMaterializesHotStateSecretPath(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	jobID := "job-worker-secret-path"
	def := `{"id":"job-worker-secret-path","root":{"uses":"builtins/script"}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job definition: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	sequence := "builtins/sequence"
	parallel := "builtins/parallel"
	script := "builtins/script"
	fileType := api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE
	j := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Secrets: []*api.SecretReference{{
			Id:       workerStrp("token"),
			Ref:      workerStrp("encryptedfs://team/token"),
			TaskKeys: []string{"secret-lane"},
			Delivery: &api.SecretDelivery{
				Type: &fileType,
				Path: workerStrp("token"),
			},
		}},
		Root: &api.Node{
			Id:   workerStrp("root-control"),
			Uses: &sequence,
			Ports: map[string]*api.NodePort{
				"steps": {
					Nodes: []*api.Node{{
						Id:   workerStrp("fanout-control"),
						Uses: &parallel,
						Ports: map[string]*api.NodePort{
							"branches": {
								Nodes: []*api.Node{
									{Id: workerStrp("secret-lane"), Uses: &script, With: map[string]string{"script": "echo secret"}},
									{Id: workerStrp("plain-lane"), Uses: &script, With: map[string]string{"script": "echo plain"}},
								},
							},
						},
					}},
				},
			},
		},
	}

	env := &cell.ExecutionEnvelope{
		RunID:       runID,
		TaskID:      runID + ":secret-lane",
		TaskKey:     "secret-lane",
		ExecutionID: runID + ":secret-lane:attempt:1:execution",
		CellID:      "local",
	}
	w := &worker{
		runCtx:        context.Background(),
		workerID:      "worker-secret-path",
		store:         runs,
		choreographer: newLocalOrchestratorChoreographer(t),
	}

	if err := w.prepareRunForExecution(ctx, j, env, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("prepare secret execution: %v", err)
	}

	for _, taskKey := range []string{"fanout-control", "secret-lane"} {
		var rows int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_tasks WHERE run_id = ? AND task_key = ?`, runID, taskKey).Scan(&rows); err != nil {
			t.Fatalf("count task %s: %v", taskKey, err)
		}

		if rows != 1 {
			t.Fatalf("task %s rows = %d, want 1", taskKey, rows)
		}
	}

	var plainRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_tasks WHERE run_id = ? AND task_key = ?`, runID, "plain-lane").Scan(&plainRows); err != nil {
		t.Fatalf("count plain task: %v", err)
	}

	if plainRows != 0 {
		t.Fatalf("plain hot-state sibling rows = %d, want 0", plainRows)
	}

	if err := runs.MirrorExecutionClaim(ctx, env.ExecutionID, "worker-secret-path", "claim-secret", time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("mirror prepared secret claim: %v", err)
	}
}

type errorWorkerCore struct {
	err error
}

func (c errorWorkerCore) ExecuteTask(context.Context, workercore.ExecuteTaskRequest) error {
	return c.err
}

type successAfterContextCancelCore struct {
	started chan struct{}
}

func (c *successAfterContextCancelCore) ExecuteTask(ctx context.Context, _ workercore.ExecuteTaskRequest) error {
	close(c.started)
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(time.Second):
		return errors.New("timed out waiting for execution context cancellation")
	}
}

type recordingSecretsResolver struct {
	req    secrets.ResolveRequest
	bundle secrets.Bundle
	err    error
}

func (r *recordingSecretsResolver) Resolve(_ context.Context, req secrets.ResolveRequest) (secrets.Bundle, error) {
	r.req = req
	if r.err != nil {
		return secrets.Bundle{}, r.err
	}

	return r.bundle, nil
}

func spanAttributeString(attrs []attribute.KeyValue, key string) string {
	for _, attr := range attrs {
		if string(attr.Key) == key {
			return attr.Value.AsString()
		}
	}

	return ""
}

func TestNewSecretsResolverFactoryDisabledAddress(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.secrets.address", "disabled")

	factory, err := newSecretsResolverFactory(mocks.NopLogger{})
	if err != nil {
		t.Fatalf("newSecretsResolverFactory: %v", err)
	}
	if factory != nil {
		t.Fatal("newSecretsResolverFactory returned resolver for disabled address")
	}
}

func TestWorkerResolveExecutionSecretsSendsTaskScopedRequest(t *testing.T) {
	fileType := api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE
	resolver := &recordingSecretsResolver{
		bundle: secrets.Bundle{Files: []secrets.FileMaterial{{
			ID:   "npm-token",
			Path: "npm/token",
			Data: []byte("secret-value"),
			Mode: secrets.DefaultFileMode,
		}}},
	}

	store := mocks.NewMockRunsRepository()
	w := &worker{secretResolver: resolver, store: store}
	workload := &workloadidentity.Identity{SPIFFEID: "spiffe://vectis.internal/execution/run-1"}

	files, err := w.resolveExecutionSecrets(context.Background(), &api.Job{
		Secrets: []*api.SecretReference{
			{
				Id:  workerStrp("global"),
				Ref: workerStrp("encryptedfs://team/global"),
				Delivery: &api.SecretDelivery{
					Type: &fileType,
					Path: workerStrp("global"),
				},
			},
			{
				Id:       workerStrp("npm-token"),
				Ref:      workerStrp("encryptedfs://team/npm-token"),
				TaskKeys: []string{"build"},
				Delivery: &api.SecretDelivery{
					Type: &fileType,
					Path: workerStrp("npm/token"),
				},
			},
			{
				Id:       workerStrp("deploy-token"),
				Ref:      workerStrp("encryptedfs://team/deploy-token"),
				TaskKeys: []string{"deploy"},
				Delivery: &api.SecretDelivery{
					Type: &fileType,
					Path: workerStrp("deploy/token"),
				},
			},
		},
	}, &cell.ExecutionEnvelope{
		RunID:         "run-1",
		TaskID:        "run-1:build",
		TaskKey:       "build",
		TaskAttemptID: "run-1:build:attempt:1",
		ExecutionID:   "execution-1",
	}, "claim-1", workload)

	if err != nil {
		t.Fatalf("resolveExecutionSecrets: %v", err)
	}

	if len(files) != 1 || files[0].ID != "npm-token" {
		t.Fatalf("files = %+v", files)
	}

	if resolver.req.RunID != "run-1" || resolver.req.ExecutionID != "execution-1" || resolver.req.ExecutionClaimToken != "claim-1" {
		t.Fatalf("request identity = %+v", resolver.req)
	}

	if resolver.req.Workload != workload {
		t.Fatalf("request workload = %+v, want original workload", resolver.req.Workload)
	}

	if len(resolver.req.Secrets) != 2 || resolver.req.Secrets[0].ID != "global" || resolver.req.Secrets[1].ID != "npm-token" {
		t.Fatalf("request secrets = %+v", resolver.req.Secrets)
	}

	events := store.SnapshotExecutionSecurityEvents()
	if len(events) != 1 {
		t.Fatalf("security events: got %d want 1: %+v", len(events), events)
	}
	event := events[0]
	if event.RunID != "run-1" || event.TaskID != "run-1:build" || event.TaskAttemptID != "run-1:build:attempt:1" || event.ExecutionID != "execution-1" {
		t.Fatalf("security event identity = %+v", event)
	}
	if event.EventType != dal.ExecutionSecurityEventSecretResolution || event.Outcome != observability.SecretsResolveOutcomeSuccess || event.Reason != observability.SecretsResolveReasonOK || event.Provider != "encryptedfs" {
		t.Fatalf("security event result = %+v", event)
	}
	if event.SecretCount == nil || *event.SecretCount != 2 || event.FileCount == nil || *event.FileCount != 1 {
		t.Fatalf("security event counts = %+v", event)
	}
}

func TestWorkerResolveExecutionSecretsUsesWorkloadResolverFactory(t *testing.T) {
	fileType := api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE
	resolver := &recordingSecretsResolver{
		bundle: secrets.Bundle{Files: []secrets.FileMaterial{{
			ID:   "npm-token",
			Path: "npm/token",
			Data: []byte("secret-value"),
			Mode: secrets.DefaultFileMode,
		}}},
	}

	workload := &workloadidentity.Identity{SPIFFEID: "spiffe://vectis.internal/execution/run-1"}
	cleanupCalled := false
	factoryCalled := false
	w := &worker{
		secretResolver: &recordingSecretsResolver{err: errors.New("static resolver should not be used")},
		secretResolverForWorkload: func(got *workloadidentity.Identity) (secrets.Resolver, func(), error) {
			factoryCalled = true
			if got != workload {
				t.Fatalf("factory workload = %+v, want original workload", got)
			}

			return resolver, func() { cleanupCalled = true }, nil
		},
	}

	files, err := w.resolveExecutionSecrets(context.Background(), &api.Job{
		Secrets: []*api.SecretReference{{
			Id:  workerStrp("npm-token"),
			Ref: workerStrp("encryptedfs://team/npm-token"),
			Delivery: &api.SecretDelivery{
				Type: &fileType,
				Path: workerStrp("npm/token"),
			},
		}},
	}, &cell.ExecutionEnvelope{
		RunID:       "run-1",
		TaskKey:     "root",
		ExecutionID: "execution-1",
	}, "claim-1", workload)

	if err != nil {
		t.Fatalf("resolveExecutionSecrets: %v", err)
	}

	if !factoryCalled {
		t.Fatal("workload resolver factory was not called")
	}

	if !cleanupCalled {
		t.Fatal("workload resolver cleanup was not called")
	}

	if len(files) != 1 || files[0].ID != "npm-token" {
		t.Fatalf("files = %+v", files)
	}

	if resolver.req.Workload != workload || resolver.req.ExecutionClaimToken != "claim-1" {
		t.Fatalf("resolver request = %+v", resolver.req)
	}
}

func TestWorkerResolveExecutionSecretsRequiresResolverForDeclaredSecrets(t *testing.T) {
	fileType := api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE
	w := &worker{}

	_, err := w.resolveExecutionSecrets(context.Background(), &api.Job{
		Secrets: []*api.SecretReference{{
			Id:  workerStrp("npm-token"),
			Ref: workerStrp("encryptedfs://team/npm-token"),
			Delivery: &api.SecretDelivery{
				Type: &fileType,
				Path: workerStrp("npm/token"),
			},
		}},
	}, &cell.ExecutionEnvelope{RunID: "run-1", TaskKey: "root", ExecutionID: "execution-1"}, "claim-1", nil)

	if err == nil {
		t.Fatalf("resolveExecutionSecrets succeeded without resolver")
	}
}

func TestWorkerResolveExecutionSecretsRejectsMismatchedWorkloadIdentity(t *testing.T) {
	fileType := api.SecretDeliveryType_SECRET_DELIVERY_TYPE_FILE
	resolver := &recordingSecretsResolver{}
	w := &worker{secretResolver: resolver}

	_, err := w.resolveExecutionSecrets(context.Background(), &api.Job{
		Secrets: []*api.SecretReference{{
			Id:  workerStrp("npm-token"),
			Ref: workerStrp("encryptedfs://team/npm-token"),
			Delivery: &api.SecretDelivery{
				Type: &fileType,
				Path: workerStrp("npm/token"),
			},
		}},
	}, &cell.ExecutionEnvelope{
		RunID:       "run-1",
		TaskKey:     "root",
		ExecutionID: "execution-1",
	}, "claim-1", &workloadidentity.Identity{
		SPIFFEID:    "spiffe://vectis.internal/cell/local/job/job-1/run/other-run/execution/execution-1",
		RunID:       "other-run",
		ExecutionID: "execution-1",
	})

	if !errors.Is(err, secrets.ErrInvalidResolveIdentity) {
		t.Fatalf("resolveExecutionSecrets error = %v, want ErrInvalidResolveIdentity", err)
	}

	if resolver.req.RunID != "" {
		t.Fatalf("resolver was called with request %+v", resolver.req)
	}
}

func assertTaskFinalizeOutcome(t *testing.T, recorder *tracetest.SpanRecorder, want taskfinalize.Outcome) {
	t.Helper()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans: got %d, want 1", len(spans))
	}

	for _, event := range spans[0].Events() {
		if event.Name != "task.finalize" {
			continue
		}

		if got := spanAttributeString(event.Attributes, "vectis.task.finalize.outcome"); got != string(want) {
			t.Fatalf("task finalize outcome: got %q, want %q", got, want)
		}

		return
	}

	t.Fatalf("task.finalize event missing: %+v", spans[0].Events())
}

type flakyFinalizeRunsStore struct {
	dal.RunsRepository

	mu                  sync.Mutex
	renewFailuresLeft   int
	succeedFailuresLeft int
	failedFailuresLeft  int
	orphanFailuresLeft  int
}

type recordingExecutionClaimStore struct {
	dal.RunsRepository

	mu                   sync.Mutex
	claimedExecutions    []string
	renewedExecutions    []string
	executionClaimTokens []string
}

func (s *recordingExecutionClaimStore) TryClaimExecution(ctx context.Context, executionID, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	claim, err := s.RunsRepository.TryClaimExecution(ctx, executionID, owner, leaseUntil)

	s.mu.Lock()
	s.claimedExecutions = append(s.claimedExecutions, executionID)
	if claim.ClaimToken != "" {
		s.executionClaimTokens = append(s.executionClaimTokens, claim.ClaimToken)
	}
	s.mu.Unlock()

	return claim, err
}

func (s *recordingExecutionClaimStore) RenewExecutionLease(ctx context.Context, executionID, owner, claimToken string, leaseUntil time.Time) error {
	err := s.RunsRepository.RenewExecutionLease(ctx, executionID, owner, claimToken, leaseUntil)
	s.mu.Lock()
	s.renewedExecutions = append(s.renewedExecutions, executionID)
	s.mu.Unlock()

	return err
}

func (s *recordingExecutionClaimStore) executionClaimCounts() (claimed, renewed int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.claimedExecutions), len(s.renewedExecutions)
}

type recordedCatalogEvent struct {
	sourceCell string
	eventKey   string
	eventType  string
	payload    []byte
}

type recordingCatalogEventsRepository struct {
	mu     sync.Mutex
	events []recordedCatalogEvent
}

func (r *recordingCatalogEventsRepository) Record(ctx context.Context, sourceCell, eventKey, eventType string, payload []byte) (dal.CatalogEventRecord, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, recordedCatalogEvent{
		sourceCell: sourceCell,
		eventKey:   eventKey,
		eventType:  eventType,
		payload:    append([]byte(nil), payload...),
	})

	now := time.Now().UnixNano()
	return dal.CatalogEventRecord{
		ID:         int64(len(r.events)),
		SourceCell: sourceCell,
		EventKey:   eventKey,
		EventType:  eventType,
		Payload:    append([]byte(nil), payload...),
		Status:     dal.CatalogEventStatusPending,
		ReceivedAt: now,
		UpdatedAt:  now,
	}, true, nil
}

func (r *recordingCatalogEventsRepository) ListPending(ctx context.Context, limit int) ([]dal.CatalogEventRecord, error) {
	return nil, nil
}

func (r *recordingCatalogEventsRepository) MarkApplied(ctx context.Context, id int64) error {
	return nil
}

func (r *recordingCatalogEventsRepository) MarkFailed(ctx context.Context, id int64, message string) error {
	return nil
}

func (r *recordingCatalogEventsRepository) MarkRetryable(ctx context.Context, id int64, message string) error {
	return nil
}

func (r *recordingCatalogEventsRepository) Summary(ctx context.Context) (dal.CatalogEventSummary, error) {
	return dal.CatalogEventSummary{}, nil
}

func (r *recordingCatalogEventsRepository) SummaryBySource(ctx context.Context) ([]dal.CatalogEventSourceSummary, error) {
	return nil, nil
}

func (r *recordingCatalogEventsRepository) countByKey(eventKey string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := 0
	for _, event := range r.events {
		if event.eventKey == eventKey {
			count++
		}
	}

	return count
}

func (s *flakyFinalizeRunsStore) RenewExecutionLease(ctx context.Context, executionID, owner, claimToken string, leaseUntil time.Time) error {
	s.mu.Lock()
	if s.renewFailuresLeft > 0 {
		s.renewFailuresLeft--
		s.mu.Unlock()
		return fmt.Errorf("renew execution: %w", sql.ErrConnDone)
	}
	s.mu.Unlock()

	return s.RunsRepository.RenewExecutionLease(ctx, executionID, owner, claimToken, leaseUntil)
}

func (s *flakyFinalizeRunsStore) MarkRunFailed(ctx context.Context, runID, failureCode, reason string) error {
	s.mu.Lock()
	if s.failedFailuresLeft > 0 {
		s.failedFailuresLeft--
		s.mu.Unlock()
		return fmt.Errorf("finalize failed: %w", sql.ErrConnDone)
	}
	s.mu.Unlock()

	return s.RunsRepository.MarkRunFailed(ctx, runID, failureCode, reason)
}

func (s *flakyFinalizeRunsStore) CompleteExecutionAndFinalizeRunByClaim(ctx context.Context, executionID, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	s.mu.Lock()
	switch status {
	case dal.ExecutionStatusSucceeded:
		if s.succeedFailuresLeft > 0 {
			s.succeedFailuresLeft--
			s.mu.Unlock()
			return dal.ExecutionFinalizationResult{}, fmt.Errorf("finalize success: %w", sql.ErrConnDone)
		}
	case dal.ExecutionStatusFailed:
		if s.failedFailuresLeft > 0 {
			s.failedFailuresLeft--
			s.mu.Unlock()
			return dal.ExecutionFinalizationResult{}, fmt.Errorf("finalize failed: %w", sql.ErrConnDone)
		}
	}
	s.mu.Unlock()

	return s.RunsRepository.CompleteExecutionAndFinalizeRunByClaim(ctx, executionID, owner, claimToken, status, failureCode, reason)
}

func (s *flakyFinalizeRunsStore) ApplyTerminalExecutionSnapshot(ctx context.Context, update dal.TerminalExecutionSnapshotUpdate) error {
	s.mu.Lock()
	switch update.Outcome {
	case dal.ExecutionFinalizationOutcomeRunSucceeded:
		if s.succeedFailuresLeft > 0 {
			s.succeedFailuresLeft--
			s.mu.Unlock()
			return fmt.Errorf("finalize success: %w", sql.ErrConnDone)
		}
	case dal.ExecutionFinalizationOutcomeRunFailed:
		if s.failedFailuresLeft > 0 {
			s.failedFailuresLeft--
			s.mu.Unlock()
			return fmt.Errorf("finalize failed: %w", sql.ErrConnDone)
		}
	case dal.ExecutionFinalizationOutcomeContinued, dal.ExecutionFinalizationOutcomeWaiting, dal.ExecutionFinalizationOutcomeRunCancelled:
	}
	s.mu.Unlock()

	return s.RunsRepository.ApplyTerminalExecutionSnapshot(ctx, update)
}

func (s *flakyFinalizeRunsStore) MarkRunOrphaned(ctx context.Context, runID, reason string) error {
	s.mu.Lock()
	if s.orphanFailuresLeft > 0 {
		s.orphanFailuresLeft--
		s.mu.Unlock()
		return fmt.Errorf("finalize orphan: %w", sql.ErrConnDone)
	}
	s.mu.Unlock()

	return s.RunsRepository.MarkRunOrphaned(ctx, runID, reason)
}

type blockingSuccessStore struct {
	dal.RunsRepository

	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingSuccessStore) CompleteExecutionAndFinalizeRunByClaim(ctx context.Context, executionID, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	if status == dal.ExecutionStatusSucceeded {
		s.once.Do(func() { close(s.entered) })
		<-s.release
	}

	return s.RunsRepository.CompleteExecutionAndFinalizeRunByClaim(ctx, executionID, owner, claimToken, status, failureCode, reason)
}

type restartOnCompleteChoreographer struct {
	mu                 sync.Mutex
	service            *orchestrator.Service
	restarted          bool
	completeTokens     []string
	loadSnapshotCounts []int
}

func newRestartOnCompleteChoreographer(t cleanupTestingT) *restartOnCompleteChoreographer {
	t.Helper()

	c := &restartOnCompleteChoreographer{service: orchestrator.New(1)}
	t.Cleanup(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.service.Close()
	})

	return c
}

func (c *restartOnCompleteChoreographer) LoadRun(ctx context.Context, j *api.Job, env *cell.ExecutionEnvelope, snapshots []orchestrator.TaskExecutionSnapshot) error {
	spec, err := orchestrator.RunSpecFromJobAndEnvelope(j, env)
	if err != nil {
		return err
	}

	spec.Executions = append([]orchestrator.TaskExecutionSnapshot(nil), snapshots...)

	c.mu.Lock()
	c.loadSnapshotCounts = append(c.loadSnapshotCounts, len(snapshots))
	service := c.service
	c.mu.Unlock()

	_, err = service.LoadRun(ctx, spec)
	return err
}

func (c *restartOnCompleteChoreographer) ClaimAndStartExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner string, leaseUntil time.Time) (dal.ExecutionClaimResult, error) {
	c.mu.Lock()
	service := c.service
	c.mu.Unlock()
	return service.ClaimExecution(ctx, env.RunID, env.ExecutionID, owner, leaseUntil)
}

func (c *restartOnCompleteChoreographer) RenewExecutionLease(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken string, leaseUntil time.Time) error {
	c.mu.Lock()
	service := c.service
	c.mu.Unlock()
	return service.RenewExecutionLease(ctx, env.RunID, env.ExecutionID, owner, claimToken, leaseUntil)
}

func (c *restartOnCompleteChoreographer) CompleteExecution(ctx context.Context, env *cell.ExecutionEnvelope, owner, claimToken, status, failureCode, reason string) (dal.ExecutionFinalizationResult, error) {
	c.mu.Lock()
	c.completeTokens = append(c.completeTokens, claimToken)
	if !c.restarted {
		c.service.Close()
		c.service = orchestrator.New(1)
		c.restarted = true
		c.mu.Unlock()
		return dal.ExecutionFinalizationResult{}, statusErrorNotFound("orchestrator restarted")
	}
	service := c.service
	c.mu.Unlock()

	return service.CompleteExecutionByClaim(ctx, env.RunID, env.ExecutionID, owner, claimToken, status, failureCode, reason)
}

func (c *restartOnCompleteChoreographer) RequiresDurableTaskRows() bool {
	return false
}

func (c *restartOnCompleteChoreographer) completeClaimTokens() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.completeTokens...)
}

func (c *restartOnCompleteChoreographer) snapshotLoadCounts() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.loadSnapshotCounts...)
}

func statusErrorNotFound(message string) error {
	return status.Error(codes.NotFound, message)
}

func TestWorkerExecutionLeaseTTLConfiguresDeadlineAndRenewal(t *testing.T) {
	clock := mocks.NewMockClock()
	now := time.Now().Add(2 * time.Second).UTC()
	clock.SetNow(now)

	w := &worker{
		clock:    clock,
		leaseTTL: 30 * time.Second,
	}

	deadline := w.leaseDeadline()
	if got, want := deadline.Sub(now), 30*time.Second; got != want {
		t.Fatalf("lease deadline delta = %v, want %v", got, want)
	}

	if got, want := workerRenewInterval(30*time.Second), 10*time.Second; got != want {
		t.Fatalf("renew interval = %v, want %v", got, want)
	}

	if got := workerRenewInterval(30 * time.Minute); got != dal.DefaultRenewInterval {
		t.Fatalf("long lease renew interval = %v, want default %v", got, dal.DefaultRenewInterval)
	}
}

func TestLeaseRenewalLoop_RenewsExecutionLease(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-execution-renew", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	workerID := "worker-test-1"
	claim, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, workerID, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim execution: %v", err)
	}

	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected execution claim, got %+v", claim)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		renewInterval: 5 * time.Millisecond,
	}

	execCtx := t.Context()

	stopRenew := make(chan struct{})
	doneRenew := make(chan struct{})
	env := &cell.ExecutionEnvelope{ExecutionID: dispatch.ExecutionID}
	go w.leaseRenewalLoop(execCtx, runID, nil, env, newExecutionClaimState(claim.ClaimToken), nil, stopRenew, doneRenew)

	time.Sleep(30 * time.Millisecond)
	close(stopRenew)
	<-doneRenew

	var status string
	var leaseUntil int64
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&status); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if status != "running" {
		t.Fatalf("expected run status running after execution claim, got %q", status)
	}

	if err := db.QueryRowContext(ctx, `SELECT lease_until FROM segment_executions WHERE execution_id = ?`, dispatch.ExecutionID).Scan(&leaseUntil); err != nil {
		t.Fatalf("query execution lease: %v", err)
	}
	if leaseUntil <= time.Now().Unix() {
		t.Fatalf("expected execution lease_until to be renewed into the future, got %d", leaseUntil)
	}
}

func TestWorkerDBUnavailableSignals_LogOutageAndRecoveryOnce(t *testing.T) {
	logger := mocks.NewMockLogger()
	workerMetrics, err := observability.NewWorkerMetrics()
	if err != nil {
		t.Fatalf("worker metrics: %v", err)
	}
	w := &worker{
		ctx:     context.Background(),
		logger:  logger,
		metrics: workerMetrics,
	}

	w.noteDBError(errors.New("database is closed"))
	w.noteDBError(errors.New("database is closed"))

	warns := logger.GetWarnCalls()
	if len(warns) != 1 {
		t.Fatalf("expected a single outage warning, got %d (%v)", len(warns), warns)
	}

	if !workerMetrics.DBUnavailable() {
		t.Fatal("expected worker metrics to report database unavailable")
	}

	w.noteDBRecovered()
	if workerMetrics.DBUnavailable() {
		t.Fatal("expected worker metrics to clear database unavailable after recovery")
	}

	infos := logger.GetInfoCalls()
	foundRecovery := false
	for _, msg := range infos {
		if strings.Contains(msg, "Database connectivity recovered; DB-backed run transitions resumed") {
			foundRecovery = true
			break
		}
	}

	if !foundRecovery {
		t.Fatalf("expected recovery info log, got %v", infos)
	}
}

func TestWorkerRunTaskExecution_CompletesWhileOrphaned_MarksSucceeded(t *testing.T) {
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
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		cellID:        "local",
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	jobID := "job-worker-finish-orphaned"
	deliveryID := "delivery-orphaned-finish"
	commandNodeID := "node-1"
	command := "sleep 0.08"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}
	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)

	done := make(chan struct{})
	go func() {
		w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
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
		t.Fatal("timed out waiting for worker runTaskExecution")
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

func TestWorkerRunTaskExecution_UploadArtifactFailureFinalizesRunFailedAndClearsClaim(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-artifact-failure-finalize", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-artifact-failure-finalize"
	w := &worker{
		ctx:               context.Background(),
		runCtx:            context.Background(),
		logger:            interfaces.NewLogger("worker-test"),
		workerID:          workerID,
		cellID:            "local",
		renewInterval:     time.Hour,
		queue:             mocks.NewMockQueueClient(),
		logClient:         mocks.NewMockLogClient(),
		core:              testWorkerCore(job.NewExecutor()),
		store:             runs,
		choreographer:     sqlExecutionChoreographer{runs: runs},
		catalog:           cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
		artifactManifests: repos.Artifacts(),
	}

	jobID := "job-worker-artifact-failure-finalize"
	deliveryID := "delivery-artifact-failure-finalize"
	action := "builtins/upload-artifact"
	root := &api.Node{
		Id:   workerStrp("upload"),
		Uses: &action,
		With: map[string]string{
			"name": "coverage",
			"path": "missing/out.txt",
		},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}
	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)

	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	if outcome != observability.WorkerOutcomeFailed {
		t.Fatalf("worker outcome: got %q, want %q", outcome, observability.WorkerOutcomeFailed)
	}

	var runStatus string
	var failureReason sql.NullString
	var runLeaseOwner sql.NullString
	var runLeaseUntil sql.NullInt64
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_reason, lease_owner, lease_until
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&runStatus, &failureReason, &runLeaseOwner, &runLeaseUntil); err != nil {
		t.Fatalf("query run after artifact failure: %v", err)
	}

	if runStatus != dal.RunStatusFailed {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusFailed)
	}

	if !failureReason.Valid || !strings.Contains(failureReason.String, "artifact") {
		t.Fatalf("expected artifact failure reason, got %+v", failureReason)
	}

	if runLeaseOwner.Valid || runLeaseUntil.Valid {
		t.Fatalf("expected run lease cleared after artifact failure, got owner=%v until=%v", runLeaseOwner, runLeaseUntil)
	}

	var executionStatus string
	var executionLeaseOwner sql.NullString
	var executionLeaseUntil sql.NullInt64
	var claimToken sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, lease_owner, lease_until, claim_token
		FROM segment_executions
		WHERE execution_id = ?
	`, env.ExecutionID).Scan(&executionStatus, &executionLeaseOwner, &executionLeaseUntil, &claimToken); err != nil {
		t.Fatalf("query execution after artifact failure: %v", err)
	}

	if executionStatus != dal.ExecutionStatusFailed {
		t.Fatalf("execution status: got %q, want %q", executionStatus, dal.ExecutionStatusFailed)
	}

	if executionLeaseOwner.Valid || executionLeaseUntil.Valid || claimToken.Valid {
		t.Fatalf("expected execution claim cleared after artifact failure, got owner=%v until=%v token=%v", executionLeaseOwner, executionLeaseUntil, claimToken)
	}

	if _, err := repos.Artifacts().GetByRunAndName(ctx, runID, "coverage"); !dal.IsNotFound(err) {
		t.Fatalf("expected no artifact manifest after failed upload action, got %v", err)
	}
}

func TestWorkerRunTaskExecution_WithExecutionEnvelope_TransitionsExecution(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-envelope", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-envelope"
	def := `{"id":"job-worker-envelope","root":{"uses":"builtins/script","with":{"script":"echo envelope"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	workerID := "worker-test-envelope"
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		cellID:        "local",
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-envelope"
	commandNodeID := "node-1"
	command := "echo envelope"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	req := &api.JobRequest{Job: j}
	env, err := cell.AttachExecutionEnvelope(req, dispatch, 1)
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}
	w.handleJob(req)

	var executionStatus string
	var segmentStatus string
	var eventSequence int64
	var acceptedAt, startedAt, finishedAt sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT se.status, rs.status, se.accepted_at, se.started_at, se.finished_at, se.event_sequence
		FROM segment_executions se
		JOIN run_segments rs ON rs.segment_id = se.segment_id
		WHERE se.execution_id = ?
	`, env.ExecutionID).Scan(&executionStatus, &segmentStatus, &acceptedAt, &startedAt, &finishedAt, &eventSequence); err != nil {
		t.Fatalf("query execution state: %v", err)
	}

	if executionStatus != dal.ExecutionStatusSucceeded {
		t.Fatalf("execution status: got %q, want %q", executionStatus, dal.ExecutionStatusSucceeded)
	}

	if segmentStatus != dal.SegmentStatusSucceeded {
		t.Fatalf("segment status: got %q, want %q", segmentStatus, dal.SegmentStatusSucceeded)
	}

	if eventSequence != 3 {
		t.Fatalf("event sequence: got %d, want 3", eventSequence)
	}

	if !acceptedAt.Valid || !startedAt.Valid || !finishedAt.Valid {
		t.Fatalf("expected accepted_at, started_at, and finished_at to be set; got accepted=%v started=%v finished=%v", acceptedAt, startedAt, finishedAt)
	}

	events, err := repos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list catalog events: %v", err)
	}

	wantEvents := []struct {
		key       string
		eventType string
	}{
		{key: cell.CatalogExecutionStatusEventKey(env.ExecutionID, dal.ExecutionStatusAccepted), eventType: cell.CatalogEventTypeExecutionStatus},
		{key: cell.CatalogRunStatusEventKey(runID, dal.RunStatusRunning), eventType: cell.CatalogEventTypeRunStatus},
		{key: cell.CatalogExecutionStatusEventKey(env.ExecutionID, dal.ExecutionStatusRunning), eventType: cell.CatalogEventTypeExecutionStatus},
		{key: cell.CatalogExecutionStatusEventKey(env.ExecutionID, dal.ExecutionStatusSucceeded), eventType: cell.CatalogEventTypeExecutionStatus},
		{key: cell.CatalogRunStatusEventKey(runID, dal.RunStatusSucceeded), eventType: cell.CatalogEventTypeRunStatus},
	}

	if len(events) != len(wantEvents) {
		t.Fatalf("catalog events: got %d, want %d (%+v)", len(events), len(wantEvents), events)
	}

	for i, want := range wantEvents {
		if events[i].SourceCell != "local" || events[i].EventKey != want.key || events[i].EventType != want.eventType {
			t.Fatalf("catalog event %d: got source=%q key=%q type=%q, want source=local key=%q type=%q",
				i, events[i].SourceCell, events[i].EventKey, events[i].EventType, want.key, want.eventType)
		}
	}
}

func TestWorkerTryClaimExecution_RejectsExpiredReclaimAfterInitialClaim(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-execution-reclaim-catalog", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-execution-reclaim-catalog"
	def := `{"id":"job-worker-execution-reclaim-catalog","root":{"id":"root","uses":"builtins/script","with":{"script":"echo claim"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	rootID := "root"
	action := "builtins/script"
	j := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"script": "echo claim"},
		},
	}

	env, err := cell.NewExecutionEnvelope(dispatch, j, nil, 1)
	if err != nil {
		t.Fatalf("build execution envelope: %v", err)
	}

	catalogEvents := &recordingCatalogEventsRepository{}
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-execution-reclaim-catalog",
		cellID:        "local",
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		catalog:       cell.NewCatalogEventPublisher("local", catalogEvents),
	}

	firstToken, claimed, _, err := w.tryClaimExecution(ctx, j, env, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("first claim execution: %v", err)
	}

	if !claimed || firstToken == "" {
		t.Fatalf("expected first execution claim, claimed=%v token=%q", claimed, firstToken)
	}

	secondToken, claimed, _, err := w.tryClaimExecution(ctx, j, env, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("second claim execution: %v", err)
	}

	if claimed || secondToken != "" {
		t.Fatalf("expected expired execution reclaim to be rejected, claimed=%v first=%q second=%q", claimed, firstToken, secondToken)
	}

	acceptedKey := cell.CatalogExecutionStatusEventKey(env.ExecutionID, dal.ExecutionStatusAccepted)
	if got := catalogEvents.countByKey(acceptedKey); got != 1 {
		t.Fatalf("accepted catalog events: got %d, want 1", got)
	}
}

func TestWorkerRunClaimedJob_SPIFFEEnabledRejectsMissingSVIDBeforeAction(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")
	viper.Set("worker.spiffe.enabled", true)
	viper.Set("worker.spiffe.workload_api_address", "unix:///tmp/spiffe-workload.sock")

	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-spiffe-gate", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-spiffe-gate"
	def := `{"id":"job-worker-spiffe-gate","root":{"uses":"builtins/script","with":{"script":"echo spiffe"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	marker := t.TempDir() + "/action-ran"
	command := fmt.Sprintf("printf ok > %q", marker)

	workerID := "worker-test-spiffe-gate"
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	registrar := &recordingSPIFFERegistrar{}
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		cellID:        "local",
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
		spiffeSVIDSource: fakeWorkerSVIDSource{svids: []spire.X509SVID{
			{SPIFFEID: "spiffe://prod.example/cell/other/namespace/other/job/other/run/other/execution/other"},
		}},
		spiffeRegistrar:            registrar,
		spiffeRegistrationParentID: "spiffe://prod.example/vectis-spiffe/agent/worker-test-spiffe-gate",
		spiffeRegistrationSelectors: []spire.Selector{
			{Type: "unix", Value: "uid:1000"},
		},
	}

	deliveryID := "delivery-spiffe-gate"
	commandNodeID := "node-1"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	req := &api.JobRequest{Job: j}
	env, err := cell.AttachExecutionEnvelope(req, dispatch, 1)
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	w.handleJob(req)

	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("action marker stat error = %v, want file not created", err)
	}

	if intents := registrar.Intents(); len(intents) != 1 {
		t.Fatalf("SPIFFE registration intents = %d, want 1", len(intents))
	}
	if releases := registrar.Releases(); len(releases) != 1 {
		t.Fatalf("SPIFFE registration releases = %d, want 1", len(releases))
	}

	var runStatus string
	var executionStatus string
	var segmentStatus string
	var eventSequence int64
	var acceptedAt, startedAt, finishedAt sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT jr.status, se.status, rs.status, se.accepted_at, se.started_at, se.finished_at, se.event_sequence
		FROM job_runs jr
		JOIN segment_executions se ON se.run_id = jr.run_id
		JOIN run_segments rs ON rs.segment_id = se.segment_id
		WHERE jr.run_id = ? AND se.execution_id = ?
	`, runID, env.ExecutionID).Scan(
		&runStatus,
		&executionStatus,
		&segmentStatus,
		&acceptedAt,
		&startedAt,
		&finishedAt,
		&eventSequence,
	); err != nil {
		t.Fatalf("query execution state: %v", err)
	}

	if runStatus != dal.RunStatusFailed {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusFailed)
	}

	if executionStatus != dal.ExecutionStatusFailed {
		t.Fatalf("execution status: got %q, want %q", executionStatus, dal.ExecutionStatusFailed)
	}

	if segmentStatus != dal.SegmentStatusFailed {
		t.Fatalf("segment status: got %q, want %q", segmentStatus, dal.SegmentStatusFailed)
	}

	if !acceptedAt.Valid || !startedAt.Valid || !finishedAt.Valid {
		t.Fatalf("expected accepted, started, and finished timestamps; got accepted=%v started=%v finished=%v",
			acceptedAt, startedAt, finishedAt)
	}

	if eventSequence != 3 {
		t.Fatalf("event sequence: got %d, want 3", eventSequence)
	}
}

func TestWorkerHydrateJobRequest_CachedPayloadKeepsDeliveryIdentityIsolated(t *testing.T) {
	payloadHash := "sha256:payload"
	jobID := "job-cached-payload"
	rootID := "root"
	runOne := "run-one"
	runTwo := "run-two"
	deliveryOne := "delivery-one"
	deliveryTwo := "delivery-two"

	w := &worker{
		ctx:    context.Background(),
		runCtx: context.Background(),
		payloadJobs: map[string]*api.Job{
			payloadHash: {
				Id:   &jobID,
				Root: &api.Node{Id: &rootID},
			},
		},
	}

	first, err := w.hydrateJobRequest(context.Background(), &api.JobRequest{
		Job: &api.Job{
			Id:         &jobID,
			RunId:      &runOne,
			DeliveryId: &deliveryOne,
		},
		Metadata: map[string]string{cell.ExecutionPayloadHashMetadataKey: payloadHash},
	})
	if err != nil {
		t.Fatalf("hydrate first delivery: %v", err)
	}

	second, err := w.hydrateJobRequest(context.Background(), &api.JobRequest{
		Job: &api.Job{
			Id:         &jobID,
			RunId:      &runTwo,
			DeliveryId: &deliveryTwo,
		},
		Metadata: map[string]string{cell.ExecutionPayloadHashMetadataKey: payloadHash},
	})
	if err != nil {
		t.Fatalf("hydrate second delivery: %v", err)
	}

	if got := first.GetJob().GetRunId(); got != runOne {
		t.Fatalf("first hydrated run_id: got %q, want %q", got, runOne)
	}

	if got := first.GetJob().GetDeliveryId(); got != deliveryOne {
		t.Fatalf("first hydrated delivery_id: got %q, want %q", got, deliveryOne)
	}

	if got := second.GetJob().GetRunId(); got != runTwo {
		t.Fatalf("second hydrated run_id: got %q, want %q", got, runTwo)
	}

	if got := second.GetJob().GetDeliveryId(); got != deliveryTwo {
		t.Fatalf("second hydrated delivery_id: got %q, want %q", got, deliveryTwo)
	}

	cached := w.payloadJobs[payloadHash]
	if got := cached.GetRunId(); got != "" {
		t.Fatalf("cached payload run_id mutated: got %q", got)
	}

	if got := cached.GetDeliveryId(); got != "" {
		t.Fatalf("cached payload delivery_id mutated: got %q", got)
	}
}

func TestWorkerRunTaskExecution_TaskFanoutQueuesContinuation(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-task-fanout", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-task-fanout"
	def := `{"id":"job-worker-task-fanout","root":{"id":"root","uses":"builtins/parallel","steps":[{"id":"child","uses":"builtins/script","with":{"script":"echo child"}}]}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	queue := mocks.NewMockQueueClient()
	choreographer := &countingExecutionChoreographer{inner: newLocalOrchestratorChoreographer(t)}
	clock := mocks.NewMockClock()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-task-fanout",
		cellID:        "local",
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: choreographer,
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-task-fanout"
	rootID := "root"
	childID := "child"
	rootAction := "builtins/parallel"
	childAction := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &rootAction,
			Steps: []*api.Node{{
				Id:   &childID,
				Uses: &childAction,
				With: map[string]string{"script": "echo child"},
			}},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-a"}}
	if _, err := cell.AttachExecutionEnvelope(req, rootDispatch, 1); err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	payloadJSON, err := protojson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal root payload: %v", err)
	}

	if _, _, err := runs.RecordExecutionPayload(ctx, runID, string(payloadJSON), dal.DefinitionHash(def)); err != nil {
		t.Fatalf("record execution payload: %v", err)
	}

	w.handleJob(req)
	if got := choreographer.LoadCount(); got != 1 {
		t.Fatalf("root execution LoadRun count: got %d, want 1", got)
	}

	var runStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusQueued {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusQueued)
	}

	owner, found, err := runs.GetRunHotStateOwner(ctx, runID)
	if err != nil {
		t.Fatalf("get run hot-state owner: %v", err)
	}

	if !found {
		t.Fatal("expected run hot-state owner after root fanout")
	}

	if owner.RunID != runID || owner.OwnerID == "" || owner.OwnerEpoch == "" || !owner.LeaseUntil.After(time.Now()) {
		t.Fatalf("run hot-state owner: %+v", owner)
	}

	reqs := queue.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("queued continuation requests: got %d, want 1", len(reqs))
	}

	env, ok, err := cell.ExecutionEnvelopeFromRequest(reqs[0])
	if err != nil {
		t.Fatalf("queued child envelope: %v", err)
	}

	if !ok {
		t.Fatal("queued continuation missing execution envelope")
	}

	if env.ExecutionID == "" || env.TaskID == "" || env.TaskKey != "child" {
		t.Fatalf("queued child envelope mismatch: got %+v", env)
	}

	if got := reqs[0].GetMetadata()[cell.ExecutionPayloadHashMetadataKey]; got == "" {
		t.Fatal("queued child missing execution payload hash")
	}

	if got := reqs[0].GetJob().GetRoot().GetId(); got != childID {
		t.Fatalf("queued child should carry compact task root %q, got %q", childID, got)
	}

	if env.Metadata["traceparent"] != "trace-a" {
		t.Fatalf("queued child trace metadata: got %q, want trace-a", env.Metadata["traceparent"])
	}

	w.handleJob(reqs[0])
	if got := choreographer.LoadCount(); got != 1 {
		t.Fatalf("hot-state child should claim without LoadRun; count got %d, want 1", got)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status after child: %v", err)
	}

	if runStatus != dal.RunStatusSucceeded {
		t.Fatalf("run status after compact child: got %q, want %q", runStatus, dal.RunStatusSucceeded)
	}

	var taskRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_tasks WHERE run_id = ?`, runID).Scan(&taskRows); err != nil {
		t.Fatalf("count run tasks: %v", err)
	}

	if taskRows != 1 {
		t.Fatalf("run task rows after compact orchestrator fanout: got %d, want root row only", taskRows)
	}
}

func TestWorkerRunTaskExecution_TaskFanoutPersistsContinuationBeforeEnqueueFailure(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-task-fanout-repair", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-task-fanout-repair"
	def := `{"id":"job-worker-task-fanout-repair","root":{"id":"root","uses":"builtins/parallel","steps":[{"id":"child","uses":"builtins/script","with":{"script":"echo child"}}]}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create definition snapshot: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	queue := mocks.NewMockQueueClient()
	queue.SetEnqueueError(errors.New("queue unavailable"))
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-task-fanout-repair",
		cellID:        "local",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: newLocalOrchestratorChoreographer(t),
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-task-fanout-repair"
	rootID := "root"
	childID := "child"
	rootAction := "builtins/parallel"
	childAction := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &rootAction,
			Steps: []*api.Node{{
				Id:   &childID,
				Uses: &childAction,
				With: map[string]string{"script": "echo child"},
			}},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-repair"}}
	if _, err := cell.AttachExecutionEnvelope(req, rootDispatch, 1); err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	payloadJSON, err := protojson.Marshal(req)
	if err != nil {
		t.Fatalf("marshal root payload: %v", err)
	}

	if _, _, err := runs.RecordExecutionPayload(ctx, runID, string(payloadJSON), dal.DefinitionHash(def)); err != nil {
		t.Fatalf("record execution payload: %v", err)
	}

	w.handleJob(req)

	if got := len(queue.GetJobRequests()); got != 0 {
		t.Fatalf("queue should reject direct continuation request, got %d", got)
	}

	var runStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusQueued {
		t.Fatalf("run status after failed direct fan-out: got %q, want %q", runStatus, dal.RunStatusQueued)
	}

	pending, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("pending continuation should remain recoverable: %v", err)
	}

	if pending.TaskKey != childID || pending.ExecutionID == "" {
		t.Fatalf("pending continuation mismatch: %+v", pending)
	}
}

func TestWorkerRunTaskExecution_TaskFanoutWaitingQueuesContinuation(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-task-reduce-waiting", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-task-reduce-waiting"
	def := `{"id":"job-worker-task-reduce-waiting","root":{"id":"root","uses":"builtins/parallel","steps":[{"id":"child","uses":"builtins/script","with":{"script":"echo child"}}]}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	queue := mocks.NewMockQueueClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-task-reduce-waiting",
		cellID:        "local",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: newLocalOrchestratorChoreographer(t),
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-task-reduce-waiting"
	rootID := "root"
	childID := "child"
	rootAction := "builtins/parallel"
	childAction := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &rootAction,
			Steps: []*api.Node{{
				Id:   &childID,
				Uses: &childAction,
				With: map[string]string{"script": "echo child"},
			}},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-waiting"}}
	if _, err := cell.AttachExecutionEnvelope(req, rootDispatch, 1); err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	w.handleJob(req)

	var runStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusQueued {
		t.Fatalf("run status after waiting reduction: got %q, want %q", runStatus, dal.RunStatusQueued)
	}

	owner, found, err := runs.GetRunHotStateOwner(ctx, runID)
	if err != nil {
		t.Fatalf("get run hot-state owner after waiting reduction: %v", err)
	}

	if !found {
		t.Fatal("expected run hot-state owner after waiting reduction")
	}

	if owner.RunID != runID || owner.OwnerID == "" || owner.OwnerEpoch == "" || !owner.LeaseUntil.After(time.Now()) {
		t.Fatalf("run hot-state owner after waiting reduction: %+v", owner)
	}

	reqs := queue.GetJobRequests()
	if len(reqs) != 1 {
		t.Fatalf("queued continuation requests: got %d, want 1", len(reqs))
	}

	env, ok, err := cell.ExecutionEnvelopeFromRequest(reqs[0])
	if err != nil {
		t.Fatalf("queued child envelope: %v", err)
	}

	if !ok {
		t.Fatal("queued continuation missing execution envelope")
	}

	if env.ExecutionID == "" || env.TaskID == "" || env.TaskKey != "child" {
		t.Fatalf("queued child envelope mismatch: got %+v", env)
	}

	deadline, ok := dispatchmeta.StartDeadlineFromMetadata(reqs[0].GetMetadata())
	if !ok {
		t.Fatalf("queued child metadata missing %s", dispatchmeta.StartDeadlineUnixNanoKey)
	}

	var executionStatus string
	var storedDeadline sql.NullInt64
	if err := db.QueryRowContext(ctx, `SELECT status, start_deadline_unix_nano FROM segment_executions WHERE execution_id = ?`, env.ExecutionID).
		Scan(&executionStatus, &storedDeadline); err != nil {
		t.Fatalf("query child execution deadline: %v", err)
	}

	if executionStatus != dal.ExecutionStatusPending {
		t.Fatalf("child execution status: got %q, want %q", executionStatus, dal.ExecutionStatusPending)
	}

	if !storedDeadline.Valid || storedDeadline.Int64 != deadline {
		t.Fatalf("child execution deadline: stored=%v queued=%d", storedDeadline, deadline)
	}

}

func TestWorkerRunTaskExecution_TaskFanoutFailureFinalizesExecutionAndRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-task-failure-order", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-task-failure-order"
	def := `{"id":"job-worker-task-failure-order","root":{"id":"root","uses":"builtins/script","with":{"script":"false"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-task-failure-order",
		cellID:        "local",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-task-failure-order"
	rootID := "root"
	action := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"script": "false"},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-failure-order"}}
	if _, err := cell.AttachExecutionEnvelope(req, rootDispatch, 1); err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	w.handleJob(req)

	var runStatus string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_reason FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus, &failureReason); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusFailed {
		t.Fatalf("run status after failed task: got %q, want %q", runStatus, dal.RunStatusFailed)
	}

	if !failureReason.Valid || (!strings.Contains(failureReason.String, "command failed") && !strings.Contains(failureReason.String, "exit status")) {
		t.Fatalf("failure reason should describe command failure, got %+v", failureReason)
	}

	var executionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, rootDispatch.ExecutionID).Scan(&executionStatus); err != nil {
		t.Fatalf("query execution status: %v", err)
	}

	if executionStatus != dal.ExecutionStatusFailed {
		t.Fatalf("execution status after failed task: got %q, want %q", executionStatus, dal.ExecutionStatusFailed)
	}

	summary, err := runs.GetRunTaskCompletion(ctx, runID)
	if err != nil {
		t.Fatalf("get task completion: %v", err)
	}

	if summary.Total != 1 || summary.TerminalFailed != 1 || summary.Incomplete != 0 {
		t.Fatalf("task completion summary after failed task: %+v", summary)
	}
}

func TestWorkerRunTaskExecution_TaskFanoutCancelFinalizesExecutionAndRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-task-cancel-order", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-task-cancel-order"
	def := `{"id":"job-worker-task-cancel-order","root":{"id":"root","uses":"builtins/script","with":{"script":"sleep 5"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-task-cancel-order",
		cellID:        "local",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
		cancelCh:      make(chan string, 1),
	}

	deliveryID := "delivery-task-cancel-order"
	rootID := "root"
	action := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"script": "sleep 5"},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-cancel-order"}}
	env, err := cell.AttachExecutionEnvelope(req, rootDispatch, 1)
	if err != nil {
		t.Fatalf("attach root envelope: %v", err)
	}

	outcomeCh := make(chan string, 1)
	finished := make(chan struct{})
	go func() {
		outcomeCh <- w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
		close(finished)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		currentRunID, _ := w.getCurrentRunInfo()
		if currentRunID == runID {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for worker to start cancellable task run")
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancelTicker := time.NewTicker(10 * time.Millisecond)
	defer cancelTicker.Stop()
	go func() {
		for {
			select {
			case <-finished:
				return
			case <-cancelTicker.C:
				select {
				case w.cancelCh <- runID:
				default:
				}
			}
		}
	}()

	var outcome string
	select {
	case outcome = <-outcomeCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for canceled task run to finish")
	}

	if outcome != observability.WorkerOutcomeAborted {
		t.Fatalf("expected worker outcome aborted, got %q", outcome)
	}

	var runStatus string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_reason FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus, &failureReason); err != nil {
		t.Fatalf("query canceled run: %v", err)
	}

	if runStatus != dal.RunStatusCancelled {
		t.Fatalf("run status after canceled task: got %q, want %q", runStatus, dal.RunStatusCancelled)
	}

	if !failureReason.Valid || failureReason.String != dal.CancelReasonAPI {
		t.Fatalf("expected failure_reason %q, got %+v", dal.CancelReasonAPI, failureReason)
	}

	var executionStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM segment_executions WHERE execution_id = ?`, rootDispatch.ExecutionID).Scan(&executionStatus); err != nil {
		t.Fatalf("query execution status: %v", err)
	}

	if executionStatus != dal.ExecutionStatusAborted {
		t.Fatalf("execution status after canceled task: got %q, want %q", executionStatus, dal.ExecutionStatusAborted)
	}

	summary, err := runs.GetRunTaskCompletion(ctx, runID)
	if err != nil {
		t.Fatalf("get task completion: %v", err)
	}

	if summary.Total != 1 || summary.TerminalFailed != 1 || summary.Incomplete != 0 {
		t.Fatalf("task completion summary after canceled task: %+v", summary)
	}
}

func TestWorkerRunTaskExecution_TaskFanoutExecutesEnvelopeTaskOnly(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	jobID := "job-worker-task-scope"
	runID, runIndex, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	second, _, err := runs.EnsurePlannedTaskExecution(ctx, dal.TaskExecutionCreate{
		RunID:        runID,
		ParentTaskID: rootDispatch.TaskID,
		TaskKey:      "second",
		Name:         "second",
		SpecHash:     "sha256:second",
		TargetCellID: "local",
	})

	if err != nil {
		t.Fatalf("ensure second task: %v", err)
	}

	runfixture.FinalizeExecutionByClaim(t, ctx, repos, rootDispatch.ExecutionID, dal.ExecutionStatusSucceeded)

	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	clock := mocks.NewMockClock()
	executor := job.NewExecutor()
	streamCh := make(chan job.LogStreamWaiter, 1)
	executor.TestLogStreamHook = streamCh
	defer func() { executor.TestLogStreamHook = nil }()

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-task-scope",
		cellID:        "local",
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(executor),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-task-scope"
	rootID := "root-node"
	firstID := "first"
	secondID := "second"
	sequenceAction := "builtins/sequence"
	scriptAction := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &sequenceAction,
			Steps: []*api.Node{
				{
					Id:   &firstID,
					Uses: &scriptAction,
					With: map[string]string{"script": "echo worker-first-marker"},
				},
				{
					Id:   &secondID,
					Uses: &scriptAction,
					With: map[string]string{"script": "echo worker-second-marker"},
				},
			},
		},
	}

	req := &api.JobRequest{Job: j, Metadata: map[string]string{"traceparent": "trace-task-scope"}}
	dispatch := dal.ExecutionDispatchRecord{
		RunID:             runID,
		JobID:             jobID,
		RunIndex:          runIndex,
		TaskID:            second.TaskID,
		TaskKey:           second.TaskKey,
		TaskName:          second.Name,
		TaskAttemptID:     second.TaskAttemptID,
		SegmentID:         second.SegmentID,
		SegmentName:       second.SegmentName,
		SegmentStatus:     dal.SegmentStatusPending,
		ExecutionID:       second.ExecutionID,
		ExecutionStatus:   dal.ExecutionStatusPending,
		CellID:            "local",
		Attempt:           second.Attempt,
		DefinitionVersion: 1,
		DefinitionHash:    dal.DefinitionHash(`{"id":"job-worker-task-scope"}`),
		OwningCell:        "local",
	}
	if _, err := cell.AttachExecutionEnvelope(req, dispatch, 1); err != nil {
		t.Fatalf("attach child envelope: %v", err)
	}

	w.handleJob(req)

	select {
	case stream := <-streamCh:
		if err := stream.WaitForDone(5 * time.Second); err != nil {
			t.Fatalf("wait for log stream: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for log stream hook")
	}

	chunks := logClient.GetChunks()
	if len(chunks) == 0 {
		t.Fatal("expected log chunks")
	}

	var sawSecond bool
	for _, chunk := range chunks {
		data := string(chunk.GetData())
		if strings.Contains(data, "worker-first-marker") {
			t.Fatalf("worker replayed sibling task; chunks=%v", chunks)
		}
		if strings.Contains(data, "worker-second-marker") {
			sawSecond = true
		}
	}
	if !sawSecond {
		t.Fatalf("expected selected task marker in chunks=%v", chunks)
	}

	var runStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusSucceeded {
		t.Fatalf("run status after final task: got %q, want %q", runStatus, dal.RunStatusSucceeded)
	}
}

func TestWorkerRunTaskExecution_ChildDeliveryHydratesAfterOrchestratorRestart(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	jobID := "job-worker-child-hydrate"
	runID, runIndex, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootDispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get root dispatch: %v", err)
	}

	rootDispatch.DefinitionHash = "test-definition-hash"
	deliveryID := "delivery-child-hydrate"
	rootID := "root-node"
	childID := "child"
	parallelAction := "builtins/parallel"
	scriptAction := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &parallelAction,
			Steps: []*api.Node{{
				Id:   &childID,
				Uses: &scriptAction,
				With: map[string]string{"script": "echo child-ran"},
			}},
		},
	}

	rootReq := &api.JobRequest{Job: j}
	rootEnv, err := cell.AttachExecutionEnvelope(rootReq, rootDispatch, 1)
	if err != nil {
		t.Fatalf("attach root execution envelope: %v", err)
	}

	childReq := &api.JobRequest{Job: j}
	child := dal.TaskExecutionRecord{
		RunID:         runID,
		TaskID:        runID + ":" + childID,
		ParentTaskID:  runID + ":" + dal.RootTaskKey,
		TaskKey:       childID,
		Name:          childID,
		TaskAttemptID: runID + ":" + childID + ":attempt:1",
		SegmentID:     runID + ":" + childID + ":segment",
		SegmentName:   childID,
		ExecutionID:   runID + ":" + childID + ":attempt:1:execution",
		CellID:        "local",
		Attempt:       1,
	}

	childDispatch := executionDispatchRecordFromTaskExecution(j, rootEnv, child)
	childDispatch.RunIndex = runIndex
	childEnv, err := cell.AttachExecutionEnvelope(childReq, childDispatch, 2)
	if err != nil {
		t.Fatalf("attach child execution envelope: %v", err)
	}

	var childRowsBefore int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_tasks WHERE run_id = ? AND task_key = ?`, runID, childID).Scan(&childRowsBefore); err != nil {
		t.Fatalf("count child task rows before execution: %v", err)
	}

	if childRowsBefore != 0 {
		t.Fatalf("child task durable rows before hot-state execution: got %d, want 0", childRowsBefore)
	}

	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-child-hydrate",
		cellID:        "local",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: newLocalOrchestratorChoreographer(t),
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, childEnv)
	if outcome != observability.WorkerOutcomeSuccess {
		t.Fatalf("worker outcome: got %q, want %q", outcome, observability.WorkerOutcomeSuccess)
	}

	chunks := logClient.GetChunks()
	if len(chunks) == 0 {
		t.Fatal("expected child log chunks")
	}

	var sawChild bool
	for _, chunk := range chunks {
		data := string(chunk.GetData())
		if strings.Contains(data, "root-should-not-run") {
			t.Fatalf("worker replayed root task; chunks=%v", chunks)
		}
		if strings.Contains(data, "child-ran") {
			sawChild = true
		}
	}
	if !sawChild {
		t.Fatalf("expected child task marker in chunks=%v", chunks)
	}

	tasks, _, err := runs.ListRunTasks(ctx, runID, 0, 10)
	if err != nil {
		t.Fatalf("list child task durable state: %v", err)
	}

	var childTask *dal.TaskRecord
	for i := range tasks {
		if tasks[i].TaskKey == childID {
			childTask = &tasks[i]
			break
		}
	}

	if childTask == nil || childTask.Status != dal.TaskStatusSucceeded {
		t.Fatalf("child task durable state after terminal snapshot: got %+v in tasks=%+v, want %q", childTask, tasks, dal.TaskStatusSucceeded)
	}

	if len(childTask.Attempts) != 1 || childTask.Attempts[0].ExecutionStatus != dal.ExecutionStatusSucceeded {
		t.Fatalf("child task attempt durable state after terminal snapshot: %+v", childTask.Attempts)
	}

	var runStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != dal.RunStatusSucceeded {
		t.Fatalf("run durable state after terminal snapshot: got %q, want %q", runStatus, dal.RunStatusSucceeded)
	}

	events, err := repos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list catalog events: %v", err)
	}

	var terminalSnapshotEvents int
	for _, event := range events {
		if event.EventType == cell.CatalogEventTypeTerminalSnapshot && event.EventKey == cell.CatalogTerminalSnapshotEventKey(runID) {
			terminalSnapshotEvents++
		}
	}

	if terminalSnapshotEvents != 1 {
		t.Fatalf("terminal snapshot catalog events: got %d in %+v, want 1", terminalSnapshotEvents, events)
	}
}

func TestWorkerPrepareRunForExecutionUsesActionResolverForPlannedTasks(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	jobID := "job-worker-action-digest-plan"
	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	rootID := "root"
	fanoutID := "fanout"
	customID := "custom"
	sequenceAction := "builtins/sequence"
	parallelAction := "builtins/parallel"
	customAction := "examples/custom@v1"
	j := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &sequenceAction,
			Steps: []*api.Node{{
				Id:   &fanoutID,
				Uses: &parallelAction,
				Steps: []*api.Node{{
					Id:   &customID,
					Uses: &customAction,
				}},
			}},
		},
	}

	resolver := actionregistry.NewCompositeResolver(
		builtins.NewRegistry(),
		workerDescriptorResolver{
			customAction: {
				CanonicalName: "examples/custom",
				Version:       "v1",
				Digest:        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Source:        actionregistry.SourceLocalFilesystem,
				Runtime:       actionregistry.RuntimeProcess,
			},
		},
	)

	plan, err := job.PlanTaskExecutionsWithActions(j, resolver)
	if err != nil {
		t.Fatalf("plan task executions: %v", err)
	}

	if _, err := job.EnsurePlannedTaskExecutions(ctx, runs, runID, plan, "local"); err != nil {
		t.Fatalf("materialize planned tasks: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}
	dispatch.DefinitionHash = "test-definition-hash"

	env, err := cell.AttachExecutionEnvelopeWithActions(&api.JobRequest{Job: j}, dispatch, time.Now().UnixNano(), resolver)
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	w := &worker{
		runCtx:         ctx,
		store:          runs,
		actionResolver: resolver,
		choreographer:  newRestartOnCompleteChoreographer(t),
	}

	if err := w.prepareRunForExecution(ctx, j, env, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("prepare run for execution: %v", err)
	}
}

func TestWorkerFinalizeAbortedTaskRunByExecutionClaim_CancelsRun(t *testing.T) {
	t.Parallel()

	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()

	ctx, span := provider.Tracer("worker-test").Start(context.Background(), "finalize-aborted-by-claim")
	runs := mocks.NewMockRunsRepository()
	runs.ExecutionFinalization = dal.ExecutionFinalizationResult{
		ExecutionID: "execution-root",
		RunID:       "run-aborted",
		Outcome:     dal.ExecutionFinalizationOutcomeRunCancelled,
		Summary:     dal.RunTaskCompletion{RunID: "run-aborted", Total: 1, TerminalFailed: 1},
	}

	w := &worker{
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-a",
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		catalog:       cell.NewCatalogEventPublisher("local", nil),
	}

	env := &cell.ExecutionEnvelope{ExecutionID: "execution-root"}
	outcome := w.finalizeAbortedTaskRunByExecutionClaim(ctx, nil, newExecutionClaimState("execution-claim-token"), dal.CancelReasonAPI, env)
	span.End()
	if outcome != observability.WorkerOutcomeAborted {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeAborted)
	}

	if runs.LastFinalizedExecID != "execution-root" || runs.LastExecutionOwner != "worker-a" || runs.LastFinalizedStatus != dal.ExecutionStatusAborted {
		t.Fatalf("finalized execution call mismatch: exec=%q owner=%q status=%q", runs.LastFinalizedExecID, runs.LastExecutionOwner, runs.LastFinalizedStatus)
	}

	assertTaskFinalizeOutcome(t, recorder, taskfinalize.OutcomeExecutionAborted)
}

type scriptedAckQueue struct {
	ackErrors []error
	ackCalls  int
}

func (q *scriptedAckQueue) Enqueue(context.Context, *api.JobRequest) error {
	return errors.New("not implemented")
}

func (q *scriptedAckQueue) Dequeue(context.Context) (*api.JobRequest, error) {
	return nil, errors.New("not implemented")
}

func (q *scriptedAckQueue) TryDequeue(context.Context) (*api.JobRequest, error) {
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

func TestWorkerHandleJob_RunlessDeliveryIsMalformed(t *testing.T) {
	queue := &scriptedAckQueue{}
	logClient := mocks.NewMockLogClient()
	workerMetrics, err := observability.NewWorkerMetrics()
	if err != nil {
		t.Fatalf("worker metrics: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-test-runless",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		metrics:       workerMetrics,
	}

	jobID := "job-worker-runless"
	deliveryID := "delivery-runless"
	commandNodeID := "node-1"
	command := "echo should-not-run"
	action := "builtins/script"
	req := &api.JobRequest{Job: &api.Job{
		Id:         &jobID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &commandNodeID,
			Uses: &action,
			With: map[string]string{"script": command},
		},
	}}

	w.handleJob(req)

	if queue.ackCalls != 1 {
		t.Fatalf("ack calls: got %d, want 1", queue.ackCalls)
	}

	if logClient.GetStreamCount() != 0 {
		t.Fatalf("expected job execution to not start after runless delivery, got %d log streams", logClient.GetStreamCount())
	}

	if got := workerMetrics.LifecyclePhase(); got != observability.WorkerPhaseIdle {
		t.Fatalf("expected worker to return idle after malformed delivery, got %q", got)
	}
}

func TestWorkerRunTaskExecution_MissingExecutionEnvelopeFailsRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-missing-envelope", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	queue := &scriptedAckQueue{}
	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-test-missing-envelope",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
	}

	jobID := "job-worker-missing-envelope"
	deliveryID := "delivery-missing-envelope"
	commandNodeID := "node-1"
	command := "echo should-not-run"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID)
	if outcome != observability.WorkerOutcomeFailed {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeFailed)
	}

	var runStatus string
	var failureCode string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&runStatus, &failureCode, &failureReason); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusFailed {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusFailed)
	}

	if failureCode != dal.FailureCodeInvalidEnvelope {
		t.Fatalf("failure code: got %q, want %q", failureCode, dal.FailureCodeInvalidEnvelope)
	}

	if !failureReason.Valid || !strings.Contains(failureReason.String, "execution envelope") {
		t.Fatalf("failure reason should describe missing envelope, got %+v", failureReason)
	}

	if queue.ackCalls != 1 {
		t.Fatalf("ack calls: got %d, want 1", queue.ackCalls)
	}

	if logClient.GetStreamCount() != 0 {
		t.Fatalf("expected job execution to not start after missing envelope, got %d log streams", logClient.GetStreamCount())
	}
}

func TestWorkerRunTaskExecution_ExecutionClaimRequiredBeforeExecute(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-execution-claim-required", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-execution-claim-required"
	def := `{"id":"job-worker-execution-claim-required","root":{"id":"root","uses":"builtins/script","with":{"script":"echo should-not-run"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	claim, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, "other-worker", time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("preclaim execution: %v", err)
	}

	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected preclaimed execution, claim=%+v", claim)
	}

	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-claim-required",
		cellID:        "local",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-claim-required"
	rootID := "root"
	action := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"script": "echo should-not-run"},
		},
	}

	req := &api.JobRequest{Job: j}
	env, err := cell.AttachExecutionEnvelope(req, dispatch, 1)
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	if outcome != observability.WorkerOutcomeSkippedUnclaimed {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeSkippedUnclaimed)
	}

	if logClient.GetStreamCount() != 0 {
		t.Fatalf("expected job execution not to start without execution claim, got %d log streams", logClient.GetStreamCount())
	}

	var runStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusRunning {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusRunning)
	}

	var executionStatus string
	var leaseOwner sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, lease_owner
		FROM segment_executions
		WHERE execution_id = ?
	`, dispatch.ExecutionID).Scan(&executionStatus, &leaseOwner); err != nil {
		t.Fatalf("query execution state: %v", err)
	}

	if executionStatus != dal.ExecutionStatusAccepted || !leaseOwner.Valid || leaseOwner.String != "other-worker" {
		t.Fatalf("execution state: status=%q owner=%v", executionStatus, leaseOwner)
	}
}

func TestWorkerRunTaskExecution_MirroredExpiredDispatchDoesNotOrphan(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-expired-mirror", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-expired-mirror"
	def := `{"id":"job-worker-expired-mirror","root":{"id":"root","uses":"builtins/script","with":{"script":"echo should-not-run"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create definition snapshot: %v", err)
	}

	created, err := runs.CreateRunsInCellsWithAudit(ctx, jobID, nil, 1, []string{"local"}, dal.RunAuditMetadata{
		StartDeadlineUnixNano: time.Now().Add(-time.Second).UnixNano(),
	})

	if err != nil {
		t.Fatalf("create expired run: %v", err)
	}

	runID := created[0].RunID
	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-expired-mirror",
		cellID:        "local",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: newLocalOrchestratorChoreographer(t),
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-expired-mirror"
	rootID := "root"
	action := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"script": "echo should-not-run"},
		},
	}

	req := &api.JobRequest{Job: j}
	env, err := cell.AttachExecutionEnvelope(req, dispatch, 1)
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	if outcome != observability.WorkerOutcomeFailed {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeFailed)
	}

	var runStatus string
	var failureCode string
	if err := db.QueryRowContext(ctx, `SELECT status, failure_code FROM job_runs WHERE run_id = ?`, runID).
		Scan(&runStatus, &failureCode); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusFailed || failureCode != dal.FailureCodeDispatchExpired {
		t.Fatalf("run status changed incorrectly: status=%q failure_code=%q", runStatus, failureCode)
	}

	if logClient.GetStreamCount() != 0 {
		t.Fatalf("expected job execution not to start after expired dispatch, got %d log streams", logClient.GetStreamCount())
	}
}

func TestWorkerRunTaskExecution_AckTransientThenSuccess_Completes(t *testing.T) {
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
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
	}

	jobID := "job-worker-ack-retry"
	deliveryID := "delivery-ack-retry"
	commandNodeID := "node-1"
	command := "echo ack-retry"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)

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

func TestWorkerRunTaskExecution_AckPersistentFailure_OrphansRunWithoutExecution(t *testing.T) {
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
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
	}

	jobID := "job-worker-ack-persistent"
	deliveryID := "delivery-ack-persistent"
	commandNodeID := "node-1"
	command := "echo should-not-run"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)

	var statusVal string
	var reason sql.NullString
	var orphanReason sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT status, failure_reason, orphan_reason FROM job_runs WHERE run_id = ?`, runID).
		Scan(&statusVal, &reason, &orphanReason); err != nil {
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

	if queue.ackCalls != ackMaxAttempts {
		t.Fatalf("expected %d ack attempts, got %d", ackMaxAttempts, queue.ackCalls)
	}

	if logClient.GetStreamCount() != 0 {
		t.Fatalf("expected job execution to not start after persistent ack failure, got %d log streams", logClient.GetStreamCount())
	}
}

func TestWorkerRunTaskExecution_FinalizeSucceededRetriesOnTransientStoreFailure(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-finalize-retry", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-finalize-retry"
	clock := mocks.NewMockClock()
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	store := &flakyFinalizeRunsStore{
		RunsRepository:      runs,
		succeedFailuresLeft: 2,
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         store,
		choreographer: sqlExecutionChoreographer{runs: store},
	}

	jobID := "job-worker-finalize-retry"
	deliveryID := "delivery-finalize-retry"
	commandNodeID := "node-1"
	command := "echo finalize-retry"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query final status: %v", err)
	}

	if statusVal != "succeeded" {
		t.Fatalf("expected succeeded after transient finalize failures, got %q", statusVal)
	}

	sleeps := clock.GetSleeps()
	if len(sleeps) != 2 {
		t.Fatalf("expected 2 finalize-retry sleeps, got %d", len(sleeps))
	}
}

func TestWorkerRunTaskExecution_DurableFinalizationSurvivesCatalogRecordFailure(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-durable-finalize-catalog-failure", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-durable-finalize-catalog-failure"
	def := `{"id":"job-worker-durable-finalize-catalog-failure","root":{"id":"root","uses":"builtins/script","with":{"script":"echo durable"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create definition snapshot: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-durable-finalize-catalog-failure",
		cellID:        "local",
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: newLocalOrchestratorChoreographer(t),
		catalog:       cell.NewCatalogEventPublisher("", repos.CatalogEvents()),
	}

	deliveryID := "delivery-durable-finalize-catalog-failure"
	rootID := "root"
	action := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"script": "echo durable"},
		},
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	if outcome != observability.WorkerOutcomeSuccess {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeSuccess)
	}

	var runStatus string
	var executionStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT jr.status, se.status
		FROM job_runs jr
		JOIN segment_executions se ON se.run_id = jr.run_id
		WHERE jr.run_id = ?
	`, runID).Scan(&runStatus, &executionStatus); err != nil {
		t.Fatalf("query durable finalization status: %v", err)
	}

	if runStatus != dal.RunStatusSucceeded || executionStatus != dal.ExecutionStatusSucceeded {
		t.Fatalf("durable status: run=%q execution=%q", runStatus, executionStatus)
	}
}

func TestWorkerRunTaskExecution_DurableFinalizationFailurePreventsSuccess(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositoriesWithCellID(db, "local")
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-durable-finalize-required", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-durable-finalize-required"
	def := `{"id":"job-worker-durable-finalize-required","root":{"id":"root","uses":"builtins/script","with":{"script":"echo durable-required"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create definition snapshot: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	store := &flakyFinalizeRunsStore{
		RunsRepository:      runs,
		succeedFailuresLeft: finalizeMaxAttempts,
	}
	clock := mocks.NewMockClock()
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-durable-finalize-required",
		cellID:        "local",
		clock:         clock,
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         store,
		choreographer: newLocalOrchestratorChoreographer(t),
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	deliveryID := "delivery-durable-finalize-required"
	rootID := "root"
	action := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"script": "echo durable-required"},
		},
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	if outcome != observability.WorkerOutcomeFailed {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeFailed)
	}

	var runStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus == dal.RunStatusSucceeded {
		t.Fatalf("run reported success without durable finalization")
	}

	sleeps := clock.GetSleeps()
	if len(sleeps) != finalizeMaxAttempts-1 {
		t.Fatalf("durable finalization retries: got %d sleeps, want %d", len(sleeps), finalizeMaxAttempts-1)
	}
}

func TestWorkerRunTaskExecution_RecoversOrchestratorRestartDuringFinalize(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-orchestrator-restart-finalize", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-orchestrator-restart-finalize"
	choreographer := newRestartOnCompleteChoreographer(t)
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         mocks.NewMockClock(),
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: choreographer,
	}

	jobID := "job-worker-orchestrator-restart-finalize"
	deliveryID := "delivery-orchestrator-restart-finalize"
	commandNodeID := "node-1"
	command := "echo orchestrator-restart-finalize"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	if outcome != observability.WorkerOutcomeSuccess {
		t.Fatalf("worker outcome: got %q, want %q", outcome, observability.WorkerOutcomeSuccess)
	}

	tokens := choreographer.completeClaimTokens()
	if len(tokens) != 2 {
		t.Fatalf("complete calls: got %d tokens %+v, want 2", len(tokens), tokens)
	}

	if tokens[0] == "" || tokens[1] == "" || tokens[0] != tokens[1] {
		t.Fatalf("expected completion retry with recovered claim token, got %+v", tokens)
	}

	sawHydratedLoad := false
	for _, count := range choreographer.snapshotLoadCounts() {
		if count > 0 {
			sawHydratedLoad = true
			break
		}
	}

	if !sawHydratedLoad {
		t.Fatalf("expected recovery LoadRun with snapshots, got counts %+v", choreographer.snapshotLoadCounts())
	}
}

func TestWorkerRunTaskExecution_LifecyclePhaseShowsFinalizing(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-finalizing-phase", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerMetrics, err := observability.NewWorkerMetrics()
	if err != nil {
		t.Fatalf("worker metrics: %v", err)
	}

	store := &blockingSuccessStore{
		RunsRepository: runs,
		entered:        make(chan struct{}),
		release:        make(chan struct{}),
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-test-finalizing-phase",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         store,
		choreographer: sqlExecutionChoreographer{runs: store},
		metrics:       workerMetrics,
	}

	jobID := "job-worker-finalizing-phase"
	deliveryID := "delivery-finalizing-phase"
	commandNodeID := "node-1"
	command := "echo finalizing-phase"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}
	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)

	done := make(chan string, 1)
	go func() {
		done <- w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	}()

	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for finalization to begin")
	}

	if got := workerMetrics.LifecyclePhase(); got != observability.WorkerPhaseFinalizing {
		t.Fatalf("expected finalizing phase, got %q", got)
	}

	close(store.release)

	select {
	case outcome := <-done:
		if outcome != observability.WorkerOutcomeSuccess {
			t.Fatalf("expected success, got %q", outcome)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for finalizing run to finish")
	}

	if got := workerMetrics.LifecyclePhase(); got != observability.WorkerPhaseIdle {
		t.Fatalf("expected idle phase after completion, got %q", got)
	}
}

func TestWorkerRunTaskExecution_RenewExecutionLeaseTransientStoreFailure_StillSucceeds(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-renew-retry", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-renew-retry"
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	store := &flakyFinalizeRunsStore{
		RunsRepository:    runs,
		renewFailuresLeft: 2,
	}
	recordingStore := &recordingExecutionClaimStore{RunsRepository: store}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		clock:         interfaces.SystemClock{},
		renewInterval: 10 * time.Millisecond,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         recordingStore,
		choreographer: sqlExecutionChoreographer{runs: recordingStore},
	}

	jobID := "job-worker-renew-retry"
	deliveryID := "delivery-renew-retry"
	commandNodeID := "node-1"
	command := "echo renew-retry-start; sleep 0.06; echo renew-retry-end"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query final status: %v", err)
	}
	if statusVal != "succeeded" {
		t.Fatalf("expected succeeded after transient renew failures, got %q", statusVal)
	}

	claimedExecutions, renewedExecutions := recordingStore.executionClaimCounts()
	if claimedExecutions == 0 {
		t.Fatal("expected worker to claim the execution")
	}

	if renewedExecutions == 0 {
		t.Fatal("expected worker to renew the execution lease")
	}
}

func TestWorkerRestartMidRun_LeaseExpiryThenRequeue_AllowsRecovery(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-restart-recovery", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	jobID := "job-worker-restart-recovery"
	deliveryID := "delivery-restart-recovery"
	commandNodeID := "node-1"
	command := "echo restart-recovered"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	originalExecutionID := env.ExecutionID

	expiredLease := time.Now().Add(-1 * time.Minute)
	claim, err := runs.TryClaimExecution(ctx, env.ExecutionID, "worker-a", expiredLease)
	if err != nil {
		t.Fatalf("claim execution worker-a: %v", err)
	}

	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected worker-a execution claim and token, got %+v", claim)
	}

	orphaned, err := runs.MarkExpiredRunningAsOrphaned(ctx, time.Now().Unix())
	if err != nil {
		t.Fatalf("mark expired running as orphaned: %v", err)
	}

	if len(orphaned) != 1 || orphaned[0] != runID {
		t.Fatalf("expected run %s orphaned, got %+v", runID, orphaned)
	}

	if err := runs.RequeueRunForRetry(ctx, runID); err != nil {
		t.Fatalf("requeue run for retry: %v", err)
	}

	retryEnv := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	if retryEnv.ExecutionID == originalExecutionID {
		t.Fatalf("expected retry requeue to create a new execution, got %q", retryEnv.ExecutionID)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-b",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
	}

	w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, retryEnv)

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query final status: %v", err)
	}

	if statusVal != "succeeded" {
		t.Fatalf("expected succeeded after restart recovery path, got %q", statusVal)
	}
}

func TestWorkerRunTaskExecution_FinalizeSucceededExhausted_LeavesRunningForOrphanSweep(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-finalize-exhausted", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	workerID := "worker-test-finalize-exhausted"
	clock := mocks.NewMockClock()
	queue := mocks.NewMockQueueClient()
	logClient := mocks.NewMockLogClient()
	logger := mocks.NewMockLogger()
	store := &flakyFinalizeRunsStore{
		RunsRepository:      runs,
		succeedFailuresLeft: finalizeMaxAttempts,
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        logger,
		workerID:      workerID,
		clock:         clock,
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     logClient,
		core:          testWorkerCore(job.NewExecutor()),
		store:         store,
		choreographer: sqlExecutionChoreographer{runs: store},
	}

	jobID := "job-worker-finalize-exhausted"
	deliveryID := "delivery-finalize-exhausted"
	commandNodeID := "node-1"
	command := "echo finalize-exhausted"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)

	sleeps := clock.GetSleeps()
	if len(sleeps) != finalizeMaxAttempts-1 {
		t.Fatalf("expected %d finalize-retry sleeps, got %d", finalizeMaxAttempts-1, len(sleeps))
	}

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if statusVal != "running" {
		t.Fatalf("expected run to remain running after finalize retries exhausted, got %q", statusVal)
	}

	joinedInfo := strings.Join(logger.GetInfoCalls(), "\n")
	if strings.Contains(joinedInfo, "Job completed successfully") {
		t.Fatalf("should not log successful completion when run finalize exhausted; info logs: %v", logger.GetInfoCalls())
	}

	expiredLease := time.Now().Add(-1 * time.Minute).Unix()
	if _, err := db.ExecContext(ctx, `UPDATE job_runs SET lease_until = ? WHERE run_id = ?`, expiredLease, runID); err != nil {
		t.Fatalf("force lease expiry: %v", err)
	}

	if _, err := db.ExecContext(ctx, `UPDATE segment_executions SET lease_until = ? WHERE execution_id = ?`, expiredLease, env.ExecutionID); err != nil {
		t.Fatalf("force execution lease expiry: %v", err)
	}

	orphaned, err := runs.MarkExpiredRunningAsOrphaned(ctx, time.Now().Unix())
	if err != nil {
		t.Fatalf("mark expired running as orphaned: %v", err)
	}

	if len(orphaned) != 1 || orphaned[0] != runID {
		t.Fatalf("expected orphan sweep to include run %s, got %+v", runID, orphaned)
	}

	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query status after orphan sweep: %v", err)
	}

	if statusVal != "orphaned" {
		t.Fatalf("expected orphaned after orphan sweep, got %q", statusVal)
	}
}

func TestWorkerDrain_ShutdownDuringRun_StillFinalizesRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-drain", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	runCtx := context.Background()

	queue := mocks.NewMockQueueClient()
	jobID := "job-worker-drain"
	deliveryID := "delivery-drain"
	commandNodeID := "node-1"
	command := "sleep 0.08"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}

	req := &api.JobRequest{Job: j}
	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	if dispatch.DefinitionHash == "" {
		dispatch.DefinitionHash = "test-definition-hash"
	}

	if _, err := cell.AttachExecutionEnvelope(req, dispatch, time.Now().UnixNano()); err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	queue.AddJobRequest(req)
	workerMetrics, err := observability.NewWorkerMetrics()
	if err != nil {
		t.Fatalf("worker metrics: %v", err)
	}

	w := &worker{
		ctx:           shutdownCtx,
		runCtx:        runCtx,
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-drain",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         queue,
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		metrics:       workerMetrics,
	}

	done := make(chan struct{})
	go func() {
		w.run()
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		var st string
		if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&st); err != nil {
			t.Fatalf("query run status: %v", err)
		}

		if st == "running" {
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for run to reach running, last status=%q", st)
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancelShutdown()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker run() after shutdown")
	}

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query final status: %v", err)
	}

	if statusVal != "succeeded" {
		t.Fatalf("expected run succeeded after drain (shutdown during execution), got %q", statusVal)
	}

	if !workerMetrics.Draining() {
		t.Fatal("expected worker metrics to report draining after shutdown")
	}

	if got := workerMetrics.LifecyclePhase(); got != observability.WorkerPhaseIdle {
		t.Fatalf("expected worker lifecycle to return to idle after drain, got %q", got)
	}
}

func TestWorkerRunTaskExecution_RemoteCancel_MarksRunAborted(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-cancel", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-cancel",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core:          testWorkerCore(job.NewExecutor()),
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
		cancelCh:      make(chan string, 1),
	}

	jobID := "job-worker-cancel"
	deliveryID := "delivery-cancel"
	commandNodeID := "node-1"
	command := "sleep 5"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}
	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)

	outcomeCh := make(chan string, 1)
	finished := make(chan struct{})
	go func() {
		outcomeCh <- w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
		close(finished)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		currentRunID, _ := w.getCurrentRunInfo()
		if currentRunID == runID {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for worker to start cancellable run")
		}

		time.Sleep(5 * time.Millisecond)
	}

	cancelTicker := time.NewTicker(10 * time.Millisecond)
	defer cancelTicker.Stop()
	go func() {
		for {
			select {
			case <-finished:
				return
			case <-cancelTicker.C:
				select {
				case w.cancelCh <- runID:
				default:
				}
			}
		}
	}()

	var outcome string
	select {
	case outcome = <-outcomeCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for canceled run to finish")
	}

	if outcome != "aborted" {
		t.Fatalf("expected worker outcome aborted, got %q", outcome)
	}

	var statusVal string
	var failureCode string
	var failureReason sql.NullString
	var finishedAt sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason, CAST(finished_at AS TEXT)
		FROM job_runs WHERE run_id = ?
	`, runID).Scan(&statusVal, &failureCode, &failureReason, &finishedAt); err != nil {
		t.Fatalf("query canceled run: %v", err)
	}

	if statusVal != dal.RunStatusCancelled {
		t.Fatalf("expected cancelled status, got %q", statusVal)
	}

	if failureCode != "" {
		t.Fatalf("expected empty failure_code, got %q", failureCode)
	}

	if !failureReason.Valid || failureReason.String != dal.CancelReasonAPI {
		t.Fatalf("expected failure_reason %q, got %v", dal.CancelReasonAPI, failureReason)
	}

	if !finishedAt.Valid {
		t.Fatal("expected finished_at to be set for aborted run")
	}

	events, err := repos.CatalogEvents().ListPending(ctx, 10)
	if err != nil {
		t.Fatalf("list catalog events: %v", err)
	}

	wantKey := cell.CatalogRunStatusEventKey(runID, dal.RunStatusCancelled)
	found := false
	for _, event := range events {
		if event.EventKey == wantKey {
			found = true
			break
		}

		if event.EventType == cell.CatalogEventTypeRunStatus && strings.Contains(event.EventKey, dal.RunStatusAborted) {
			t.Fatalf("unexpected aborted run catalog event: %+v", event)
		}
	}

	if !found {
		t.Fatalf("expected cancelled catalog event %q, got %+v", wantKey, events)
	}
}

func TestWorkerExecuteWithLeaseRenewal_CancelDoesNotOverrideSuccessfulCoreResult(t *testing.T) {
	core := &successAfterContextCancelCore{started: make(chan struct{})}
	store := &mocks.MockRunsRepository{}
	runID := "run-cancel-success-race"
	env := &cell.ExecutionEnvelope{
		RunID:       runID,
		TaskID:      runID + ":root",
		TaskKey:     dal.RootTaskKey,
		ExecutionID: runID + ":root:attempt:1:execution",
		CellID:      "local",
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-cancel-success-race",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		core:          core,
		store:         store,
		choreographer: sqlExecutionChoreographer{runs: store},
		cancelCh:      make(chan string, 1),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.executeWithLeaseRenewal(context.Background(), runID, newExecutionClaimState("claim-race"), &api.Job{
			Id:    workerStrp("job-cancel-success-race"),
			RunId: workerStrp(runID),
		}, env)
	}()

	select {
	case <-core.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for core execution to start")
	}

	w.cancelCh <- runID

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("executeWithLeaseRenewal returned %v, want nil success", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for executeWithLeaseRenewal")
	}
}

func TestWorkerCancelCoreTaskSendsExecutionIdentity(t *testing.T) {
	core := &recordingWorkerCore{cancelled: make(chan workercore.CancelTaskRequest, 1)}
	w := &worker{
		runCtx: context.Background(),
		logger: interfaces.NewLogger("worker-test"),
		core:   core,
	}

	w.cancelCoreTask(context.Background(), "run-1", &cell.ExecutionEnvelope{
		ExecutionID: "execution-1",
		TaskKey:     "root",
	}, "remote request")

	select {
	case req := <-core.cancelled:
		if req.SessionID != "execution-1" || req.RunID != "run-1" || req.TaskKey != "root" || req.Reason != "remote request" {
			t.Fatalf("cancel request = %#v", req)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for core cancel request")
	}
}

type recordingWorkerCore struct {
	cancelled chan workercore.CancelTaskRequest
}

func (c *recordingWorkerCore) ExecuteTask(context.Context, workercore.ExecuteTaskRequest) error {
	return nil
}

func (c *recordingWorkerCore) CancelTask(_ context.Context, req workercore.CancelTaskRequest) error {
	c.cancelled <- req
	return nil
}

func TestWorkerRunTaskExecution_WorkerCoreCancelledResultCancelsRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-core-cancelled", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-core-cancelled",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core: errorWorkerCore{
			err: workercore.NewTaskResultError(api.RunOutcome_RUN_OUTCOME_UNKNOWN, workersdk.ReasonCancelled, "cancelled in external runner"),
		},
		store:         runs,
		choreographer: sqlExecutionChoreographer{runs: runs},
	}

	jobID := "job-worker-core-cancelled"
	deliveryID := "delivery-worker-core-cancelled"
	commandNodeID := "node-1"
	action := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &commandNodeID,
			Uses: &action,
		},
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	if outcome != observability.WorkerOutcomeAborted {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeAborted)
	}

	var runStatus string
	var failureCode string
	var failureReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&runStatus, &failureCode, &failureReason); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusCancelled {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusCancelled)
	}

	if failureCode != "" {
		t.Fatalf("failure code: got %q, want empty", failureCode)
	}

	if !failureReason.Valid || !strings.Contains(failureReason.String, workersdk.ReasonCancelled) || !strings.Contains(failureReason.String, "cancelled in external runner") {
		t.Fatalf("failure reason = %v, want worker core cancellation detail", failureReason)
	}

	var executionStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT status
		FROM segment_executions
		WHERE execution_id = ?
	`, env.ExecutionID).Scan(&executionStatus); err != nil {
		t.Fatalf("query execution status: %v", err)
	}

	if executionStatus != dal.ExecutionStatusAborted {
		t.Fatalf("execution status: got %q, want %q", executionStatus, dal.ExecutionStatusAborted)
	}
}

func TestWorkerRunTaskExecution_WorkerCoreUnknownResultOrphansRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-core-unknown", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-core-unknown",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core: errorWorkerCore{
			err: workercore.NewTaskResultError(api.RunOutcome_RUN_OUTCOME_UNKNOWN, workersdk.ReasonExternalUnavailable, "jenkins unavailable"),
		},
		store:         runs,
		choreographer: newLocalOrchestratorChoreographer(t),
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	jobID := "job-worker-core-unknown"
	deliveryID := "delivery-worker-core-unknown"
	commandNodeID := "node-1"
	action := "builtins/script"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &commandNodeID,
			Uses: &action,
		},
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	if outcome != observability.WorkerOutcomeFailed {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeFailed)
	}

	var runStatus string
	var failureCode string
	var failureReason sql.NullString
	var orphanReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason, orphan_reason
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&runStatus, &failureCode, &failureReason, &orphanReason); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusOrphaned {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusOrphaned)
	}

	if failureCode != "" {
		t.Fatalf("failure code: got %q, want empty", failureCode)
	}

	if !orphanReason.Valid || orphanReason.String != dal.OrphanReasonWorkerCoreUnknown {
		t.Fatalf("orphan reason = %v, want %q", orphanReason, dal.OrphanReasonWorkerCoreUnknown)
	}

	if !failureReason.Valid || !strings.Contains(failureReason.String, workersdk.ReasonExternalUnavailable) || !strings.Contains(failureReason.String, "jenkins unavailable") {
		t.Fatalf("failure reason = %v, want worker core unknown detail", failureReason)
	}
}

func TestWorkerRunTaskExecution_RemoteWorkerCoreUnavailableOrphansRun(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-core-unavailable", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	coreErr := fmt.Errorf("remote worker core execute task: %w", status.Error(codes.Unavailable, `closing transport due to: connection error: desc = "error reading from server: EOF", received prior goaway: code: NO_ERROR, debug data: "graceful_stop"`))
	w := &worker{
		ctx:           context.Background(),
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      "worker-core-unavailable",
		clock:         interfaces.SystemClock{},
		renewInterval: time.Hour,
		queue:         mocks.NewMockQueueClient(),
		logClient:     mocks.NewMockLogClient(),
		core: errorWorkerCore{
			err: coreErr,
		},
		store:         runs,
		choreographer: newLocalOrchestratorChoreographer(t),
		catalog:       cell.NewCatalogEventPublisher("local", repos.CatalogEvents()),
	}

	jobID := "job-worker-core-unavailable"
	deliveryID := "delivery-worker-core-unavailable"
	commandNodeID := "node-1"
	action := "builtins/shell"
	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root: &api.Node{
			Id:   &commandNodeID,
			Uses: &action,
		},
	}

	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)
	outcome := w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	if outcome != observability.WorkerOutcomeFailed {
		t.Fatalf("outcome: got %q, want %q", outcome, observability.WorkerOutcomeFailed)
	}

	var runStatus string
	var failureCode string
	var failureReason sql.NullString
	var orphanReason sql.NullString
	if err := db.QueryRowContext(ctx, `
		SELECT status, failure_code, failure_reason, orphan_reason
		FROM job_runs
		WHERE run_id = ?
	`, runID).Scan(&runStatus, &failureCode, &failureReason, &orphanReason); err != nil {
		t.Fatalf("query run status: %v", err)
	}

	if runStatus != dal.RunStatusOrphaned {
		t.Fatalf("run status: got %q, want %q", runStatus, dal.RunStatusOrphaned)
	}

	if failureCode != "" {
		t.Fatalf("failure code: got %q, want empty", failureCode)
	}

	if !orphanReason.Valid || orphanReason.String != dal.OrphanReasonWorkerCoreUnknown {
		t.Fatalf("orphan reason = %v, want %q", orphanReason, dal.OrphanReasonWorkerCoreUnknown)
	}

	if !failureReason.Valid || !strings.Contains(failureReason.String, "remote worker core execute task") || !strings.Contains(failureReason.String, "graceful_stop") {
		t.Fatalf("failure reason = %v, want worker core unavailable detail", failureReason)
	}
}

func TestWorkerRunTaskExecution_DurableCancel_MarksRunAborted(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	runID, _, err := runs.CreateRun(ctx, "job-worker-durable-cancel", nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	w := &worker{
		ctx:                context.Background(),
		runCtx:             context.Background(),
		logger:             interfaces.NewLogger("worker-test"),
		workerID:           "worker-durable-cancel",
		clock:              interfaces.SystemClock{},
		renewInterval:      time.Hour,
		cancelPollInterval: 10 * time.Millisecond,
		queue:              mocks.NewMockQueueClient(),
		logClient:          mocks.NewMockLogClient(),
		core:               testWorkerCore(job.NewExecutor()),
		store:              runs,
		choreographer:      sqlExecutionChoreographer{runs: runs},
		cancelCh:           make(chan string, 1),
	}

	jobID := "job-worker-durable-cancel"
	deliveryID := "delivery-durable-cancel"
	commandNodeID := "node-1"
	command := "sleep 5"
	action := "builtins/script"
	root := &api.Node{
		Id:   &commandNodeID,
		Uses: &action,
		With: map[string]string{"script": command},
	}

	j := &api.Job{
		Id:         &jobID,
		RunId:      &runID,
		DeliveryId: &deliveryID,
		Root:       root,
	}
	env := attachPendingExecutionEnvelopeForTest(t, runs, j, runID)

	outcomeCh := make(chan string, 1)
	go func() {
		outcomeCh <- w.runTaskExecution(context.Background(), j, jobID, runID, deliveryID, env)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		currentRunID, _ := w.getCurrentRunInfo()
		if currentRunID == runID {
			break
		}

		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for worker to start cancellable run")
		}

		time.Sleep(5 * time.Millisecond)
	}

	if _, err := runs.RequestRunCancel(ctx, runID, dal.CancelReasonAPI); err != nil {
		t.Fatalf("request durable cancel: %v", err)
	}

	var outcome string
	select {
	case outcome = <-outcomeCh:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for durable-canceled run to finish")
	}

	if outcome != "aborted" {
		t.Fatalf("expected worker outcome aborted, got %q", outcome)
	}

	requested, err := runs.RunCancelRequested(ctx, runID)
	if err != nil {
		t.Fatalf("run cancel requested after abort: %v", err)
	}

	if requested {
		t.Fatal("expected terminal transition to clear durable cancel request")
	}

	var statusVal string
	if err := db.QueryRowContext(ctx, `SELECT status FROM job_runs WHERE run_id = ?`, runID).Scan(&statusVal); err != nil {
		t.Fatalf("query durable-canceled run: %v", err)
	}

	if statusVal != dal.RunStatusCancelled {
		t.Fatalf("expected cancelled status, got %q", statusVal)
	}
}

func TestHandleDequeueError_ContextCanceledStops(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &worker{
		ctx:    ctx,
		logger: interfaces.NewLogger("worker-test"),
		clock:  interfaces.SystemClock{},
	}

	job, keep := w.handleDequeueError(context.Canceled)
	if job != nil || keep {
		t.Fatalf("expected exit, got job=%v keep=%v", job, keep)
	}
}

func TestHandleDequeueError_GRPCCanceledStops(t *testing.T) {
	t.Parallel()
	logger := mocks.NewMockLogger()
	w := &worker{
		ctx:    context.Background(),
		logger: logger,
		clock:  interfaces.SystemClock{},
	}

	job, keep := w.handleDequeueError(status.Error(codes.Canceled, "context canceled"))
	if job != nil || keep {
		t.Fatalf("expected exit, got job=%v keep=%v", job, keep)
	}

	if len(logger.GetWarnCalls()) != 0 {
		t.Fatalf("expected no warn on grpc Canceled shutdown, got %v", logger.GetWarnCalls())
	}
}

func TestHandleDequeueError_NonTransientRetriesWithBackoff(t *testing.T) {
	t.Parallel()
	logger := mocks.NewMockLogger()
	clock := mocks.NewMockClock()
	w := &worker{
		ctx:    context.Background(),
		logger: logger,
		clock:  clock,
	}

	job, keep := w.handleDequeueError(status.Error(codes.InvalidArgument, "bad request"))
	if job != nil || !keep {
		t.Fatalf("expected keep retrying, got job=%v keep=%v", job, keep)
	}

	if len(clock.GetSleeps()) != 1 {
		t.Fatalf("expected one backoff sleep, got %v", clock.GetSleeps())
	}

	errs := logger.GetErrorCalls()
	if len(errs) != 1 || !strings.Contains(errs[0], "self-healing") {
		t.Fatalf("expected one error log about self-healing backoff, got %v", errs)
	}
}

func TestWorker_Run_ExitsWhenDequeueCanceled(t *testing.T) {
	t.Parallel()
	q := mocks.NewMockQueueClient()
	q.SetDequeueError(context.Canceled)
	w := &worker{
		ctx:    context.Background(),
		logger: interfaces.NewLogger("worker-test"),
		clock:  interfaces.SystemClock{},
		queue:  q,
	}
	w.run()
}

func TestForwarderSocketPath_RespectsXDGRuntimeDir(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "/run/user/1000")
	got := forwarderSocketPath()
	want := "/run/user/1000/vectis/log-forwarder.sock"
	if got != want {
		t.Fatalf("forwarderSocketPath() = %q, want %q", got, want)
	}
}

func TestForwarderSocketPath_FallsBackToTempDir(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", "")
	got := forwarderSocketPath()
	if !strings.HasSuffix(got, "/log-forwarder.sock") {
		t.Fatalf("forwarderSocketPath() = %q, expected suffix /log-forwarder.sock", got)
	}
	if !strings.Contains(got, fmt.Sprintf("vectis-%d", os.Getuid())) {
		t.Fatalf("forwarderSocketPath() = %q, expected user-isolated directory", got)
	}
}

type fakeControlAddr string

func (a fakeControlAddr) Network() string { return "tcp" }

func (a fakeControlAddr) String() string { return string(a) }

type fakeControlListener struct {
	addr net.Addr
}

func (l *fakeControlListener) Accept() (net.Conn, error) {
	return nil, errors.New("fake control listener does not accept connections")
}

func (l *fakeControlListener) Close() error { return nil }

func (l *fakeControlListener) Addr() net.Addr { return l.addr }

func fakeControlListen(failures map[string]error, calls *[]string) controlListenFunc {
	return func(network, address string) (net.Listener, error) {
		*calls = append(*calls, address)
		if network != "tcp" {
			return nil, fmt.Errorf("unexpected network %q", network)
		}

		if err := failures[address]; err != nil {
			return nil, err
		}

		port := strings.TrimPrefix(address, ":")
		return &fakeControlListener{addr: fakeControlAddr("127.0.0.1:" + port)}, nil
	}
}

func assertControlListenCalls(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("listen calls = %v, want %v", got, want)
	}

	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("listen calls = %v, want %v", got, want)
		}
	}
}

func TestStartControlListener_StaticUsesConfiguredPort(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.mode", "static")
	viper.Set("control_port", 19084)

	var calls []string
	ln, addr, err := startControlListenerWithListen(fakeControlListen(nil, &calls))
	if err != nil {
		t.Fatalf("startControlListener(static): %v", err)
	}
	defer ln.Close()

	assertControlListenCalls(t, calls, ":19084")

	if addr != "127.0.0.1:19084" {
		t.Fatalf("addr = %q, want %q", addr, "127.0.0.1:19084")
	}
}

func TestStartControlListener_RangeUsesConfiguredPort(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.mode", "range")
	viper.Set("control_port_min", 19085)
	viper.Set("control_port_max", 19086)

	var calls []string
	failures := map[string]error{":19085": errors.New("port unavailable")}
	ln, addr, err := startControlListenerWithListen(fakeControlListen(failures, &calls))
	if err != nil {
		t.Fatalf("startControlListener(range): %v", err)
	}
	defer ln.Close()

	assertControlListenCalls(t, calls, ":19085", ":19086")

	if addr != "127.0.0.1:19086" {
		t.Fatalf("addr = %q, want %q", addr, "127.0.0.1:19086")
	}
}

func TestStartControlListener_EphemeralUsesZeroPort(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.mode", "ephemeral")

	var calls []string
	ln, addr, err := startControlListenerWithListen(fakeControlListen(nil, &calls))
	if err != nil {
		t.Fatalf("startControlListener(ephemeral): %v", err)
	}
	defer ln.Close()

	assertControlListenCalls(t, calls, ":0")

	if addr != "127.0.0.1:0" {
		t.Fatalf("addr = %q, want %q", addr, "127.0.0.1:0")
	}
}

func TestControlPublishAddress_NormalizesUnspecifiedHost(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	if got := controlPublishAddress("[::]:19084"); got != "localhost:19084" {
		t.Fatalf("IPv6 unspecified addr = %q, want localhost:19084", got)
	}

	if got := controlPublishAddress("0.0.0.0:19084"); got != "localhost:19084" {
		t.Fatalf("IPv4 unspecified addr = %q, want localhost:19084", got)
	}

	if got := controlPublishAddress("127.0.0.1:19084"); got != "127.0.0.1:19084" {
		t.Fatalf("loopback addr = %q, want 127.0.0.1:19084", got)
	}
}

func TestControlPublishAddress_UsesPublishAddressOverride(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.publish_address", "10.244.0.12:9084")
	if got := controlPublishAddress("[::]:19084"); got != "10.244.0.12:9084" {
		t.Fatalf("publish override addr = %q, want 10.244.0.12:9084", got)
	}
}

func TestControlPublishAddress_ExpandsPortPlaceholder(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.publish_address", "worker-1:{port}")
	if got := controlPublishAddress("[::]:19084"); got != "worker-1:19084" {
		t.Fatalf("publish placeholder addr = %q, want worker-1:19084", got)
	}
}

func TestStartControlListener_RangeExhausted(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	viper.Set("worker.control.mode", "range")
	viper.Set("control_port_min", 19085)
	viper.Set("control_port_max", 19086)

	var calls []string
	failures := map[string]error{
		":19085": errors.New("port unavailable"),
		":19086": errors.New("port unavailable"),
	}

	ln, _, err := startControlListenerWithListen(fakeControlListen(failures, &calls))
	if err == nil {
		_ = ln.Close()
		t.Fatal("startControlListener(range) succeeded, want exhaustion error")
	}

	assertControlListenCalls(t, calls, ":19085", ":19086")

	if !strings.Contains(err.Error(), "no available port in range 19085-19086") {
		t.Fatalf("error = %v, want no available port range", err)
	}
}

func TestExecutionWorkloadIdentityDisabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	got, err := executionWorkloadIdentity(workerTestExecutionEnvelope())
	if err != nil {
		t.Fatalf("executionWorkloadIdentity: %v", err)
	}

	if got != nil {
		t.Fatalf("executionWorkloadIdentity = %+v, want nil when disabled", got)
	}
}

func TestExecutionWorkloadIdentityEnabled(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")

	got, err := executionWorkloadIdentity(workerTestExecutionEnvelope())
	if err != nil {
		t.Fatalf("executionWorkloadIdentity: %v", err)
	}

	want := "spiffe://prod.example/cell/iad-a/namespace/team-a/job/job-1/run/run-1/execution/execution-1"
	if got == nil || got.SPIFFEID != want {
		t.Fatalf("SPIFFEID = %+v, want %q", got, want)
	}
}

func TestExecutionWorkloadIdentityEnabledRequiresEnvelope(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")

	if _, err := executionWorkloadIdentity(nil); err == nil {
		t.Fatal("executionWorkloadIdentity accepted missing envelope")
	}
}

func TestHandleJobExecutionIdentityEnabledRejectsJobWithoutRunContext(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.execution_identity.enabled", true)
	viper.Set("worker.execution_identity.trust_domain", "prod.example")

	logger := mocks.NewMockLogger()
	w := &worker{
		ctx:       context.Background(),
		runCtx:    context.Background(),
		logger:    logger,
		queue:     mocks.NewMockQueueClient(),
		logClient: mocks.NewMockLogClient(),
		core:      testWorkerCore(job.NewExecutor()),
	}

	jobID := "job-without-run-context"
	deliveryID := "delivery-without-run-context"
	action := "builtins/script"
	command := "echo should-not-run"
	w.handleJob(&api.JobRequest{
		Job: &api.Job{
			Id:         &jobID,
			DeliveryId: &deliveryID,
			Root: &api.Node{
				Uses: &action,
				With: map[string]string{"script": command},
			},
		},
	})

	errors := logger.GetErrorCalls()
	if !logContains(errors, "worker execution identity requires execution envelope") {
		t.Fatalf("expected missing envelope error, got %v", errors)
	}

	if logContains(errors, "job has no run id") {
		t.Fatalf("executor ran for job without run context: %v", errors)
	}
}

type fakeWorkerSVIDSource struct {
	svids []spire.X509SVID
	err   error
	wait  <-chan struct{}
}

func (s fakeWorkerSVIDSource) FetchX509SVIDs(ctx context.Context) ([]spire.X509SVID, error) {
	if s.err != nil {
		return nil, s.err
	}

	if s.wait != nil {
		select {
		case <-s.wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return append([]spire.X509SVID(nil), s.svids...), nil
}

type recordingSPIFFERegistrar struct {
	mu         sync.Mutex
	intents    []spire.RegistrationIntent
	releases   []spire.RegistrationHandle
	ensureErr  error
	releaseErr error
}

func (r *recordingSPIFFERegistrar) EnsureRegistration(_ context.Context, intent spire.RegistrationIntent) (spire.RegistrationResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.ensureErr != nil {
		return spire.RegistrationResult{}, r.ensureErr
	}

	intent.Selectors = append([]spire.Selector(nil), intent.Selectors...)
	r.intents = append(r.intents, intent)

	return spire.RegistrationResult{
		Handle: spire.RegistrationHandle{
			EntryID:   fmt.Sprintf("entry-%d", len(r.intents)),
			Key:       intent.Key,
			SPIFFEID:  intent.SPIFFEID,
			ExpiresAt: intent.ExpiresAt,
		},
		Created: len(r.intents) == 1,
	}, nil
}

func (r *recordingSPIFFERegistrar) ReleaseRegistration(_ context.Context, handle spire.RegistrationHandle) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.releaseErr != nil {
		return r.releaseErr
	}

	r.releases = append(r.releases, handle)
	return nil
}

func (r *recordingSPIFFERegistrar) Intents() []spire.RegistrationIntent {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]spire.RegistrationIntent, 0, len(r.intents))
	for _, intent := range r.intents {
		intent.Selectors = append([]spire.Selector(nil), intent.Selectors...)
		out = append(out, intent)
	}

	return out
}

func (r *recordingSPIFFERegistrar) Releases() []spire.RegistrationHandle {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]spire.RegistrationHandle(nil), r.releases...)
}

func TestEnsureExecutionSPIFFERegistrationBuildsIntent(t *testing.T) {
	clock := mocks.NewMockClock()
	now := time.Date(2026, 6, 9, 10, 0, 0, 0, time.UTC)
	clock.SetNow(now)

	env := workerTestExecutionEnvelope()
	identity, err := workloadidentity.NewIdentity("prod.example", "", executionFromEnvelope(env))
	if err != nil {
		t.Fatalf("NewIdentity: %v", err)
	}

	registrar := &recordingSPIFFERegistrar{}
	w := &worker{
		clock:                      clock,
		spiffeRegistrar:            registrar,
		spiffeRegistrationParentID: "spiffe://prod.example/vectis-spiffe/agent/worker-node",
		spiffeRegistrationSelectors: []spire.Selector{
			{Type: "unix", Value: "uid:1000"},
			{Type: "k8s", Value: "sa:vectis:worker"},
		},
		spiffeRegistrationMinTTL: time.Minute,
		spiffeRegistrationMaxTTL: 10 * time.Minute,
	}

	expiresAt := now.Add(5 * time.Minute)
	handle, registered, err := w.ensureExecutionSPIFFERegistration(context.Background(), identity, env, expiresAt)
	if err != nil {
		t.Fatalf("ensureExecutionSPIFFERegistration: %v", err)
	}

	if !registered {
		t.Fatal("ensureExecutionSPIFFERegistration registered = false, want true")
	}

	intents := registrar.Intents()
	if len(intents) != 1 {
		t.Fatalf("registrar intents = %d, want 1", len(intents))
	}

	intent := intents[0]

	if intent.SPIFFEID != identity.SPIFFEID {
		t.Fatalf("intent SPIFFEID = %q, want %q", intent.SPIFFEID, identity.SPIFFEID)
	}

	if intent.ParentSPIFFEID != "spiffe://prod.example/vectis-spiffe/agent/worker-node" {
		t.Fatalf("intent ParentSPIFFEID = %q", intent.ParentSPIFFEID)
	}

	if !intent.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("intent ExpiresAt = %s, want %s", intent.ExpiresAt, expiresAt)
	}

	if handle.Key != intent.Key || handle.SPIFFEID != intent.SPIFFEID || handle.EntryID == "" {
		t.Fatalf("handle = %+v, intent key=%q spiffe=%q", handle, intent.Key, intent.SPIFFEID)
	}

	wantSelectors := []spire.Selector{
		{Type: "k8s", Value: "sa:vectis:worker"},
		{Type: "unix", Value: "uid:1000"},
	}

	if fmt.Sprint(intent.Selectors) != fmt.Sprint(wantSelectors) {
		t.Fatalf("intent selectors = %+v, want %+v", intent.Selectors, wantSelectors)
	}

	if intent.Metadata.CellID != env.CellID ||
		intent.Metadata.NamespacePath != env.NamespacePath ||
		intent.Metadata.JobID != env.Job.GetId() ||
		intent.Metadata.RunID != env.RunID ||
		intent.Metadata.ExecutionID != env.ExecutionID ||
		intent.Metadata.Attempt != env.Attempt {
		t.Fatalf("intent metadata = %+v, env = %+v", intent.Metadata, env)
	}
}

func TestEnsureExecutionSPIFFERegistrationRequiresTrustedInputs(t *testing.T) {
	env := workerTestExecutionEnvelope()
	identity, err := workloadidentity.NewIdentity("prod.example", "", executionFromEnvelope(env))
	if err != nil {
		t.Fatalf("NewIdentity: %v", err)
	}

	expiresAt := time.Now().Add(5 * time.Minute)
	newBaseWorker := func() worker {
		return worker{
			workerID:                   "worker-1",
			cellID:                     "local",
			spiffeRegistrar:            &recordingSPIFFERegistrar{},
			spiffeRegistrationParentID: "spiffe://prod.example/vectis-spiffe/agent/worker-node",
			spiffeRegistrationSelectors: []spire.Selector{
				{Type: "unix", Value: "uid:1000"},
			},
		}
	}

	tests := []struct {
		name      string
		configure func(*worker)
		identity  *workloadidentity.Identity
		env       *cell.ExecutionEnvelope
		wantErr   string
	}{
		{
			name: "nil registrar is no-op",
			configure: func(w *worker) {
				*w = worker{}
			},
			identity: identity,
			env:      env,
			wantErr:  "",
		},
		{
			name:     "missing identity",
			identity: nil,
			env:      env,
			wantErr:  "execution identity",
		},
		{
			name:     "missing envelope",
			identity: identity,
			env:      nil,
			wantErr:  "execution envelope",
		},
		{
			name: "missing parent",
			configure: func(w *worker) {
				w.spiffeRegistrationParentID = ""
			},
			identity: identity,
			env:      env,
			wantErr:  "parent SPIFFE ID",
		},
		{
			name: "missing selectors",
			configure: func(w *worker) {
				w.spiffeRegistrationSelectors = nil
			},
			identity: identity,
			env:      env,
			wantErr:  "selector",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := newBaseWorker()
			if tt.configure != nil {
				tt.configure(&w)
			}

			_, registered, err := w.ensureExecutionSPIFFERegistration(context.Background(), tt.identity, tt.env, expiresAt)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ensureExecutionSPIFFERegistration error = %v, want nil", err)
				}

				if registered {
					t.Fatal("ensureExecutionSPIFFERegistration registered = true, want false")
				}

				return
			}

			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ensureExecutionSPIFFERegistration error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestLeaseRenewalLoopRenewsSPIFFERegistration(t *testing.T) {
	db := dbtest.NewTestDB(t)
	ctx := context.Background()
	repos := dal.NewSQLRepositories(db)
	runs := repos.Runs()

	_, err := repos.Namespaces().Create(ctx, "worker-spiffe-renew", nil)
	if err != nil {
		t.Fatalf("create namespace: %v", err)
	}

	jobID := "job-worker-spiffe-renew"
	def := `{"id":"job-worker-spiffe-renew","root":{"id":"root","uses":"builtins/script","with":{"script":"echo renew"}}}`
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, jobID, def); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	dispatch, err := runs.GetPendingExecution(ctx, runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	workerID := "worker-spiffe-renew"
	claim, err := runs.TryClaimExecution(ctx, dispatch.ExecutionID, workerID, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("claim execution: %v", err)
	}

	if !claim.Claimed || claim.ClaimToken == "" {
		t.Fatalf("expected execution claim, got %+v", claim)
	}

	action := "builtins/script"
	rootID := "root"
	j := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   &rootID,
			Uses: &action,
			With: map[string]string{"script": "echo renew"},
		},
	}

	env, err := cell.AttachExecutionEnvelope(&api.JobRequest{Job: j}, dispatch, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	identity, err := workloadidentity.NewIdentity("prod.example", "", executionFromEnvelope(env))
	if err != nil {
		t.Fatalf("NewIdentity: %v", err)
	}

	registrar := &recordingSPIFFERegistrar{}
	w := &worker{
		runCtx:        context.Background(),
		logger:        interfaces.NewLogger("worker-test"),
		workerID:      workerID,
		store:         runs,
		renewInterval: 5 * time.Millisecond,
		choreographer: sqlExecutionChoreographer{runs: runs},

		spiffeRegistrar:            registrar,
		spiffeRegistrationParentID: "spiffe://prod.example/vectis-spiffe/agent/worker-spiffe-renew",
		spiffeRegistrationSelectors: []spire.Selector{
			{Type: "unix", Value: "uid:1000"},
		},
	}

	initialHandle, registered, err := w.ensureExecutionSPIFFERegistration(ctx, identity, env, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatalf("initial ensureExecutionSPIFFERegistration: %v", err)
	}

	if !registered {
		t.Fatal("initial ensureExecutionSPIFFERegistration registered = false, want true")
	}

	registration := &executionSPIFFERegistration{
		identity: identity,
		env:      env,
		handle:   initialHandle,
	}

	execCtx := t.Context()
	stopRenew := make(chan struct{})
	doneRenew := make(chan struct{})
	go w.leaseRenewalLoop(execCtx, runID, nil, env, newExecutionClaimState(claim.ClaimToken), registration, stopRenew, doneRenew)

	time.Sleep(30 * time.Millisecond)
	close(stopRenew)
	<-doneRenew

	intents := registrar.Intents()
	if len(intents) < 2 {
		t.Fatalf("SPIFFE registration intents = %d, want initial plus renewal", len(intents))
	}

	first := intents[0]
	last := intents[len(intents)-1]
	if last.Key != first.Key {
		t.Fatalf("renewed registration key = %q, want %q", last.Key, first.Key)
	}

	if !last.ExpiresAt.After(first.ExpiresAt) {
		t.Fatalf("renewed expiry = %s, want after initial %s", last.ExpiresAt, first.ExpiresAt)
	}

	if registration.handle.Key != first.Key {
		t.Fatalf("registration handle key = %q, want %q", registration.handle.Key, first.Key)
	}
}

func TestAcquireExecutionSVIDDisabledDoesNotFetch(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	identity := workerTestWorkloadIdentity()
	w := &worker{spiffeSVIDSource: fakeWorkerSVIDSource{err: errors.New("should not fetch")}}

	got, err := w.acquireExecutionSVID(context.Background(), identity)
	if err != nil {
		t.Fatalf("acquireExecutionSVID disabled: %v", err)
	}

	if got != identity {
		t.Fatalf("acquireExecutionSVID returned a different identity when disabled: got=%p want=%p", got, identity)
	}

	if got.X509SVID != nil {
		t.Fatalf("acquireExecutionSVID attached SVID while disabled: %+v", got.X509SVID)
	}
}

func TestAcquireExecutionSVIDSPIFFEEnabledAttachesMatchedSVID(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.enabled", true)

	identity := workerTestWorkloadIdentity()
	w := &worker{spiffeSVIDSource: fakeWorkerSVIDSource{svids: []spire.X509SVID{
		{SPIFFEID: identity.SPIFFEID},
	}}}

	got, err := w.acquireExecutionSVID(context.Background(), identity)
	if err != nil {
		t.Fatalf("acquireExecutionSVID: %v", err)
	}

	if got == identity {
		t.Fatal("acquireExecutionSVID returned original identity, want SVID-bearing copy")
	}

	if identity.X509SVID != nil {
		t.Fatalf("acquireExecutionSVID mutated original identity: %+v", identity.X509SVID)
	}

	if got.X509SVID == nil || got.X509SVID.SPIFFEID != identity.SPIFFEID {
		t.Fatalf("acquireExecutionSVID X509SVID = %+v, want %q", got.X509SVID, identity.SPIFFEID)
	}
}

func TestAcquireExecutionSVIDSPIFFEEnabledRejectsMissingSVID(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.enabled", true)

	w := &worker{spiffeSVIDSource: fakeWorkerSVIDSource{svids: []spire.X509SVID{
		{SPIFFEID: "spiffe://prod.example/cell/iad-a/namespace/team-a/job/job-1/run/run-1/execution/other"},
	}}}

	_, err := w.acquireExecutionSVID(context.Background(), workerTestWorkloadIdentity())
	if err == nil || !strings.Contains(err.Error(), "no X.509-SVID") {
		t.Fatalf("acquireExecutionSVID error = %v, want missing SVID", err)
	}
}

func TestAcquireExecutionSVIDSPIFFEEnabledRequiresIdentity(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.enabled", true)

	w := &worker{spiffeSVIDSource: fakeWorkerSVIDSource{}}
	_, err := w.acquireExecutionSVID(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "execution identity is missing") {
		t.Fatalf("acquireExecutionSVID error = %v, want missing identity", err)
	}
}

func TestAcquireExecutionSVIDSPIFFEEnabledRequiresSource(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.enabled", true)

	w := &worker{}
	_, err := w.acquireExecutionSVID(context.Background(), workerTestWorkloadIdentity())
	if err == nil || !strings.Contains(err.Error(), "SPIFFE source is not configured") {
		t.Fatalf("acquireExecutionSVID error = %v, want missing source", err)
	}
}

func TestAcquireExecutionSVIDSPIFFEEnabledAcceptsMatchingSource(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.enabled", true)

	w := &worker{spiffeSVIDSource: fakeWorkerSVIDSource{svids: []spire.X509SVID{
		{SPIFFEID: workerTestWorkloadIdentity().SPIFFEID},
	}}}

	if _, err := w.acquireExecutionSVID(context.Background(), workerTestWorkloadIdentity()); err != nil {
		t.Fatalf("acquireExecutionSVID: %v", err)
	}
}

func TestAcquireExecutionSVIDSPIFFEEnabledRejectsMissingSVIDFromSource(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.enabled", true)

	w := &worker{spiffeSVIDSource: fakeWorkerSVIDSource{svids: []spire.X509SVID{
		{SPIFFEID: "spiffe://prod.example/cell/iad-a/namespace/team-a/job/job-1/run/run-1/execution/other"},
	}}}

	_, err := w.acquireExecutionSVID(context.Background(), workerTestWorkloadIdentity())
	if err == nil || !strings.Contains(err.Error(), "no X.509-SVID") {
		t.Fatalf("acquireExecutionSVID error = %v, want missing SVID", err)
	}
}

func TestAcquireExecutionSVIDSPIFFEEnabledUsesFetchTimeout(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)
	viper.Set("worker.spiffe.enabled", true)
	viper.Set("worker.spiffe.fetch_timeout", time.Millisecond)

	w := &worker{spiffeSVIDSource: fakeWorkerSVIDSource{wait: make(chan struct{})}}

	started := time.Now()
	_, err := w.acquireExecutionSVID(context.Background(), workerTestWorkloadIdentity())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquireExecutionSVID error = %v, want context deadline exceeded", err)
	}

	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("acquireExecutionSVID elapsed = %v, want bounded by fetch timeout", elapsed)
	}
}

func TestWorkerSPIFFESVIDFailureReason(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "invalid expected id",
			err:  fmt.Errorf("wrapped: %w", spire.ErrExpectedSPIFFEIDInvalid),
			want: observability.WorkerSPIFFESVIDReasonInvalidExpectedID,
		},
		{
			name: "mismatch",
			err:  fmt.Errorf("wrapped: %w", spire.ErrNoMatchingX509SVID),
			want: observability.WorkerSPIFFESVIDReasonMismatch,
		},
		{
			name: "missing source",
			err:  fmt.Errorf("wrapped: %w", spire.ErrX509SVIDSourceRequired),
			want: observability.WorkerSPIFFESVIDReasonMissingSource,
		},
		{
			name: "source timeout",
			err:  fmt.Errorf("wrapped: %w", context.DeadlineExceeded),
			want: observability.WorkerSPIFFESVIDReasonSourceTimeout,
		},
		{
			name: "canceled",
			err:  fmt.Errorf("wrapped: %w", context.Canceled),
			want: observability.WorkerSPIFFESVIDReasonCanceled,
		},
		{
			name: "source error",
			err:  errors.New("workload API unavailable"),
			want: observability.WorkerSPIFFESVIDReasonSourceError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workerSPIFFESVIDFailureReason(tt.err); got != tt.want {
				t.Fatalf("workerSPIFFESVIDFailureReason() = %q, want %q", got, tt.want)
			}
		})
	}
}

func logContains(logs []string, substr string) bool {
	for _, msg := range logs {
		if strings.Contains(msg, substr) {
			return true
		}
	}

	return false
}

func workerTestWorkloadIdentity() *workloadidentity.Identity {
	return &workloadidentity.Identity{
		SPIFFEID: "spiffe://prod.example/cell/iad-a/namespace/team-a/job/job-1/run/run-1/execution/execution-1",
	}
}

func workerTestExecutionEnvelope() *cell.ExecutionEnvelope {
	jobID := "job-1"
	runID := "run-1"
	action := "builtins/script"
	return &cell.ExecutionEnvelope{
		EnvelopeVersion:   cell.ExecutionEnvelopeVersion,
		RunID:             runID,
		RunIndex:          7,
		NamespacePath:     "/team-a",
		SegmentID:         "segment-1",
		ExecutionID:       "execution-1",
		CellID:            "iad-a",
		Attempt:           1,
		DefinitionVersion: 3,
		DefinitionHash:    "sha256:abc123",
		Job: &api.Job{
			Id:    &jobID,
			RunId: &runID,
			Root: &api.Node{
				Uses: &action,
			},
		},
	}
}
