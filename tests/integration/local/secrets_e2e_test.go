//go:build integration

package local_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	api "vectis/api/gen/go"
	"vectis/internal/cell"
	"vectis/internal/dal"
	"vectis/internal/interfaces"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/job"
	"vectis/internal/job/validation"
	"vectis/internal/localpki"
	"vectis/internal/localspiffe"
	"vectis/internal/logserver"
	secretstore "vectis/internal/secrets"
	"vectis/internal/spire"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/grpcservices"
	"vectis/internal/workloadidentity"
)

const (
	secretPlaintext = "local-spiffe-secret"
	trustDomain     = "vectis.internal"
)

func TestIntegrationLocalSPIFFESecretsExample(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping local secrets E2E in short mode")
	}

	ctx := context.Background()
	logger := mocks.NewMockLogger()
	root := repoRoot(t)

	spiffeCfg := testSPIFFEConfig(t)
	authority, err := localspiffe.Start(ctx, spiffeCfg)
	if err != nil {
		t.Fatalf("start local SPIFFE authority: %v", err)
	}
	t.Cleanup(authority.Stop)

	registrar, registrarCleanup, err := spire.DialSPIREServerRegistrar(
		"unix://"+spiffeCfg.RegistrationSocketPath,
		spire.WithSPIREServerX509SVIDTTL(time.Minute),
	)

	if err != nil {
		t.Fatalf("dial local SPIFFE registration API: %v", err)
	}
	t.Cleanup(registrarCleanup)

	svidSource, err := spire.NewWorkloadAPISource("unix://" + spiffeCfg.WorkloadSocketPath)
	if err != nil {
		t.Fatalf("new workload API source: %v", err)
	}

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, dal.DefaultCellID)
	runs := repos.Runs()

	definitionJSON, runJob := loadExampleJob(t, root)
	if err := repos.Jobs().Create(ctx, runJob.GetId(), string(definitionJSON), 1); err != nil {
		t.Fatalf("create stored job: %v", err)
	}

	runID, _, err := runs.CreateRun(ctx, runJob.GetId(), nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	runJob.RunId = strp(runID)
	if _, err := job.EnsureJobTaskExecutions(ctx, runs, runJob, dal.DefaultCellID); err != nil {
		t.Fatalf("materialize task executions: %v", err)
	}

	logStore, err := logserver.NewLocalRunLogStore(t.TempDir())
	if err != nil {
		t.Fatalf("create log store: %v", err)
	}

	_, logClient := grpcservices.StartLogServer(t, logger, logStore)
	_, queueClient, _ := grpcservices.StartQueueServer(t, logger)

	secretsListener, secretsServerCA := startSecretsBroker(t, logger, runs, spiffeCfg.BundleFile)
	resolverForWorkload := func(workload *workloadidentity.Identity) (secretstore.Resolver, func(), error) {
		return newBufconnSecretsResolver(secretsListener, secretsServerCA, workload)
	}

	req := &api.JobRequest{Job: runJob}
	if _, err := cell.AttachPendingExecutionEnvelope(ctx, runs, req, runID, time.Now().UnixNano()); err != nil {
		t.Fatalf("attach execution envelope: %v", err)
	}

	if err := queueClient.Enqueue(ctx, req); err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	worker := &e2eWorker{
		logger:                    logger,
		workerID:                  "integration-local-secrets-worker",
		trustDomain:               spiffeCfg.TrustDomain,
		parentSPIFFEID:            "spiffe://" + spiffeCfg.TrustDomain + "/spire/agent/local",
		selector:                  spire.Selector{Type: "unix", Value: "uid:" + strconv.Itoa(os.Getuid())},
		queue:                     queueClient,
		logClient:                 logClient,
		executor:                  job.NewExecutor(),
		store:                     runs,
		registrar:                 registrar,
		svidSource:                svidSource,
		secretResolverForWorkload: resolverForWorkload,
	}

	if err := worker.runOne(ctx); err != nil {
		t.Fatalf("worker run: %v", err)
	}

	assertRunSucceeded(t, ctx, runs, runID)
	assertSecretDidNotLeak(t, logger, logStore, runID)
}

func loadExampleJob(t *testing.T, root string) ([]byte, *api.Job) {
	t.Helper()

	definitionJSON, err := os.ReadFile(filepath.Join(root, "examples", "secrets.json"))
	if err != nil {
		t.Fatalf("read examples/secrets.json: %v", err)
	}

	var runJob api.Job
	if err := job.DecodeDefinitionJSON(definitionJSON, &runJob); err != nil {
		t.Fatalf("decode examples/secrets.json: %v", err)
	}

	if err := validation.ValidateJob(&runJob, validation.Options{RequireJobID: true}); err != nil {
		t.Fatalf("validate examples/secrets.json: %v", err)
	}

	return definitionJSON, &runJob
}

func startSecretsBroker(t *testing.T, logger interfaces.Logger, runs dal.RunsRepository, clientCAFile string) (*bufconn.Listener, string) {
	t.Helper()

	root := t.TempDir()
	key := []byte("0123456789abcdef0123456789abcdef")
	if err := secretstore.WriteEncryptedFSSecretFile(root, "encryptedfs://team/smoke-token", []byte(secretPlaintext), key); err != nil {
		t.Fatalf("write encryptedfs secret: %v", err)
	}

	provider, err := secretstore.NewEncryptedFSProvider(root, secretstore.WithEncryptedFSKey(key))
	if err != nil {
		t.Fatalf("new encryptedfs provider: %v", err)
	}

	policy, err := secretstore.NewAccessPolicy([]string{"namespace=*;job=*;task=*;ref=encryptedfs://*"})
	if err != nil {
		t.Fatalf("new secrets access policy: %v", err)
	}

	server := secretstore.NewServer(
		provider,
		secretstore.NewClaimAuthorizer(
			runs,
			secretstore.WithExecutionScopeResolver(e2eExecutionScopeResolver{store: runs, trustDomain: trustDomain}),
			secretstore.WithAccessPolicy(policy),
		),
		secretstore.WithLogger(logger),
	)

	tlsMaterial, err := localpki.Ensure(t.TempDir())
	if err != nil {
		t.Fatalf("create local TLS material: %v", err)
	}

	serverCert, err := tls.LoadX509KeyPair(tlsMaterial.ServerCert, tlsMaterial.ServerKey)
	if err != nil {
		t.Fatalf("load server certificate: %v", err)
	}

	clientCAPEM, err := os.ReadFile(clientCAFile)
	if err != nil {
		t.Fatalf("read client CA bundle: %v", err)
	}

	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(clientCAPEM) {
		t.Fatal("client CA bundle did not contain a PEM certificate")
	}

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCAs,
	})))

	api.RegisterSecretsServiceServer(grpcServer, server)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("secrets", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("secrets broker stopped: %v", err)
		}
	}()

	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	return listener, tlsMaterial.CAFile
}

func newBufconnSecretsResolver(listener *bufconn.Listener, caFile string, workload *workloadidentity.Identity) (secretstore.Resolver, func(), error) {
	if workload == nil || workload.X509SVID == nil {
		return nil, nil, fmt.Errorf("workload X.509-SVID is required")
	}

	clientCert, err := workload.X509SVID.TLSCertificate()
	if err != nil {
		return nil, nil, err
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read server CA: %w", err)
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, nil, fmt.Errorf("server CA bundle did not contain a PEM certificate")
	}

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			ServerName:   "localhost",
			RootCAs:      roots,
			Certificates: []tls.Certificate{*clientCert},
		})),
	)

	if err != nil {
		return nil, nil, err
	}

	return secretstore.NewGRPCResolver(conn), func() { _ = conn.Close() }, nil
}

type e2eWorker struct {
	logger                    interfaces.Logger
	workerID                  string
	trustDomain               string
	parentSPIFFEID            string
	selector                  spire.Selector
	queue                     interfaces.QueueClient
	logClient                 interfaces.LogClient
	executor                  *job.Executor
	store                     dal.RunsRepository
	registrar                 spire.Registrar
	svidSource                spire.X509SVIDSource
	secretResolverForWorkload func(*workloadidentity.Identity) (secretstore.Resolver, func(), error)
}

func (w *e2eWorker) runOne(ctx context.Context) error {
	dequeueCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	req, err := w.queue.Dequeue(dequeueCtx)
	cancel()

	if err != nil {
		return fmt.Errorf("dequeue: %w", err)
	}

	if req == nil || req.GetJob() == nil {
		return fmt.Errorf("dequeue returned empty job")
	}

	env, ok, err := cell.ExecutionEnvelopeFromRequest(req)
	if err != nil {
		return fmt.Errorf("execution envelope: %w", err)
	}

	if !ok {
		return fmt.Errorf("execution envelope is missing")
	}

	if err := w.queue.Ack(ctx, req.GetJob().GetDeliveryId()); err != nil {
		return fmt.Errorf("ack delivery: %w", err)
	}

	claim, err := w.store.TryClaimExecution(ctx, env.ExecutionID, w.workerID, time.Now().Add(time.Minute))
	if err != nil {
		return fmt.Errorf("claim execution: %w", err)
	}

	if !claim.Claimed {
		return fmt.Errorf("execution %s was not claimed", env.ExecutionID)
	}

	workload, handle, err := w.prepareWorkload(ctx, env)
	if err != nil {
		return err
	}

	defer func() {
		if handle.Managed {
			_ = w.registrar.ReleaseRegistration(context.Background(), handle)
		}
	}()

	secretFiles, err := w.resolveSecrets(ctx, req.GetJob(), env, claim.ClaimToken, workload)
	if err != nil {
		return fmt.Errorf("resolve secrets: %w", err)
	}

	if err := w.store.MarkExecutionStarted(ctx, env.ExecutionID); err != nil {
		return fmt.Errorf("mark execution started: %w", err)
	}

	execErr := w.executor.ExecuteTaskWithOptions(ctx, req.GetJob(), env.TaskKey, w.logClient, w.logger, job.ExecuteOptions{
		WorkloadIdentity: workload,
		SecretFiles:      secretFiles,
	})

	if execErr != nil {
		_, _ = w.store.CompleteExecutionAndFinalizeRunByClaim(ctx, env.ExecutionID, w.workerID, claim.ClaimToken, dal.ExecutionStatusFailed, dal.FailureCodeExecution, execErr.Error())
		return execErr
	}

	if _, err := w.store.CompleteExecutionAndFinalizeRunByClaim(ctx, env.ExecutionID, w.workerID, claim.ClaimToken, dal.ExecutionStatusSucceeded, "", ""); err != nil {
		return fmt.Errorf("complete execution: %w", err)
	}

	return nil
}

func (w *e2eWorker) prepareWorkload(ctx context.Context, env *cell.ExecutionEnvelope) (*workloadidentity.Identity, spire.RegistrationHandle, error) {
	workload, err := workloadidentity.NewIdentity(w.trustDomain, workloadidentity.DefaultSPIFFEPathTemplate, executionFromEnvelope(env))
	if err != nil {
		return nil, spire.RegistrationHandle{}, fmt.Errorf("execution workload identity: %w", err)
	}

	intent, err := spire.NewExecutionRegistrationIntent(workload.SPIFFEID, executionFromEnvelope(env), spire.ExecutionRegistrationOptions{
		ParentSPIFFEID: w.parentSPIFFEID,
		Selectors:      []spire.Selector{w.selector},
		ExpiresAt:      time.Now().Add(time.Minute).UTC(),
		Now:            time.Now().UTC(),
		MinTTL:         time.Second,
		MaxTTL:         5 * time.Minute,
	})

	if err != nil {
		return nil, spire.RegistrationHandle{}, fmt.Errorf("execution SPIFFE registration intent: %w", err)
	}

	result, err := w.registrar.EnsureRegistration(ctx, intent)
	if err != nil {
		return nil, spire.RegistrationHandle{}, fmt.Errorf("ensure execution SPIFFE registration: %w", err)
	}

	svid, err := spire.FetchX509SVID(ctx, w.svidSource, workload.SPIFFEID)
	if err != nil {
		return nil, result.Handle, fmt.Errorf("fetch execution X.509-SVID: %w", err)
	}

	return workload.WithX509SVID(workloadidentity.X509SVID{
		SPIFFEID:     svid.SPIFFEID,
		Certificates: svid.Certificates,
		PrivateKey:   svid.PrivateKey,
	}), result.Handle, nil
}

func (w *e2eWorker) resolveSecrets(ctx context.Context, runJob *api.Job, env *cell.ExecutionEnvelope, claimToken string, workload *workloadidentity.Identity) ([]secretstore.FileMaterial, error) {
	refs := secretstore.ReferencesForTask(runJob, env.TaskKey)
	if len(refs) == 0 {
		return nil, fmt.Errorf("job declared no secrets for task key %q", env.TaskKey)
	}

	resolver, cleanup, err := w.secretResolverForWorkload(workload)
	if err != nil {
		return nil, err
	}

	if cleanup != nil {
		defer cleanup()
	}

	bundle, err := resolver.Resolve(ctx, secretstore.ResolveRequest{
		RunID:               env.RunID,
		ExecutionID:         env.ExecutionID,
		ExecutionClaimToken: claimToken,
		Workload:            workload,
		Secrets:             refs,
	})

	if err != nil {
		return nil, err
	}

	return bundle.Files, nil
}

type e2eExecutionScopeResolver struct {
	store interface {
		GetActiveExecutionDispatch(context.Context, string, string) (dal.ExecutionDispatchRecord, error)
	}
	trustDomain string
}

func (r e2eExecutionScopeResolver) ResolveExecutionScope(ctx context.Context, runID, executionID string) (secretstore.ExecutionScope, error) {
	if r.store == nil {
		return secretstore.ExecutionScope{}, errors.New("active execution store is not configured")
	}

	dispatch, err := r.store.GetActiveExecutionDispatch(ctx, runID, executionID)
	if err != nil {
		return secretstore.ExecutionScope{}, err
	}

	identity, err := workloadidentity.NewIdentity(r.trustDomain, workloadidentity.DefaultSPIFFEPathTemplate, workloadidentity.Execution{
		CellID:            dispatch.CellID,
		NamespacePath:     dispatch.NamespacePath,
		JobID:             dispatch.JobID,
		RunID:             dispatch.RunID,
		RunIndex:          dispatch.RunIndex,
		SegmentID:         dispatch.SegmentID,
		ExecutionID:       dispatch.ExecutionID,
		Attempt:           dispatch.Attempt,
		DefinitionVersion: dispatch.DefinitionVersion,
		DefinitionHash:    dispatch.DefinitionHash,
	})

	if err != nil {
		return secretstore.ExecutionScope{}, err
	}

	return secretstore.ExecutionScope{
		SPIFFEID:          identity.SPIFFEID,
		TrustDomain:       identity.TrustDomain,
		NamespacePath:     identity.NamespacePath,
		CellID:            identity.CellID,
		JobID:             identity.JobID,
		RunID:             identity.RunID,
		RunIndex:          dispatch.RunIndex,
		TaskID:            dispatch.TaskID,
		TaskKey:           dispatch.TaskKey,
		SegmentID:         dispatch.SegmentID,
		ExecutionID:       identity.ExecutionID,
		Attempt:           dispatch.Attempt,
		DefinitionVersion: dispatch.DefinitionVersion,
		DefinitionHash:    dispatch.DefinitionHash,
	}, nil
}

func executionFromEnvelope(env *cell.ExecutionEnvelope) workloadidentity.Execution {
	return workloadidentity.Execution{
		CellID:            env.CellID,
		NamespacePath:     env.NamespacePath,
		JobID:             env.Job.GetId(),
		RunID:             env.RunID,
		RunIndex:          env.RunIndex,
		SegmentID:         env.SegmentID,
		ExecutionID:       env.ExecutionID,
		Attempt:           env.Attempt,
		DefinitionVersion: env.DefinitionVersion,
		DefinitionHash:    env.DefinitionHash,
	}
}

func assertRunSucceeded(t *testing.T, ctx context.Context, runs dal.RunsRepository, runID string) {
	t.Helper()

	status, found, err := runs.GetRunStatus(ctx, runID)
	if err != nil {
		t.Fatalf("get run status: %v", err)
	}

	if !found || status != dal.RunStatusSucceeded {
		t.Fatalf("run status = %q found=%v, want %q", status, found, dal.RunStatusSucceeded)
	}
}

func assertSecretDidNotLeak(t *testing.T, logger *mocks.MockLogger, logStore *logserver.LocalRunLogStore, runID string) {
	t.Helper()

	joinedLogs := allEntriesJoined(t, logStore, runID)
	if strings.Contains(joinedLogs, secretPlaintext) {
		t.Fatalf("secret plaintext leaked to run logs:\n%s", joinedLogs)
	}

	if !strings.Contains(joinedLogs, "Command completed successfully") {
		t.Fatalf("run logs did not contain successful command completion:\n%s", joinedLogs)
	}

	for _, call := range logger.GetCalls() {
		if strings.Contains(call.Message, secretPlaintext) {
			t.Fatalf("secret plaintext leaked to service logs: %+v", call)
		}
	}
}

func allEntriesJoined(t *testing.T, store *logserver.LocalRunLogStore, runID string) string {
	t.Helper()

	entries, err := store.List(runID)
	if err != nil {
		t.Fatalf("list run logs: %v", err)
	}

	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		parts = append(parts, entry.Data)
	}

	return strings.Join(parts, "\n")
}

func repoRoot(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func testSPIFFEConfig(t *testing.T) localspiffe.Config {
	t.Helper()

	dir, err := os.MkdirTemp("/tmp", "vectis-secrets-e2e-*")
	if err != nil {
		t.Fatalf("create short temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return localspiffe.Config{
		TrustDomain:            trustDomain,
		DataDir:                filepath.Join(dir, "data"),
		RuntimeDir:             filepath.Join(dir, "run"),
		WorkloadSocketPath:     filepath.Join(dir, "run", "workload.sock"),
		RegistrationSocketPath: filepath.Join(dir, "run", "registration.sock"),
		BundleFile:             filepath.Join(dir, "data", "bundle.pem"),
		Selectors:              []string{"unix:uid:" + strconv.Itoa(os.Getuid())},
	}
}

func strp(s string) *string {
	return &s
}
