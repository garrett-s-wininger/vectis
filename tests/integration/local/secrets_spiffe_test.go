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
	"runtime"
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
	"vectis/internal/testutil/workertest"
	"vectis/internal/workloadidentity"
)

const (
	secretPlaintext = "spiffe-secret"
	trustDomain     = "vectis.internal"
)

func TestIntegrationLocalSPIFFESecretsExample(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping local secrets integration test in short mode")
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
	if err := repos.Jobs().CreateDefinitionSnapshot(ctx, runJob.GetId(), string(definitionJSON)); err != nil {
		t.Fatalf("create definition snapshot: %v", err)
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

	worker := &workertest.Runner{
		Logger:                    logger,
		WorkerID:                  "integration-local-secrets-worker",
		TrustDomain:               spiffeCfg.TrustDomain,
		ParentSPIFFEID:            "spiffe://" + spiffeCfg.TrustDomain + "/vectis-spiffe/agent/local",
		Selectors:                 []spire.Selector{{Type: "unix", Value: "uid:" + strconv.Itoa(os.Getuid())}},
		Queue:                     queueClient,
		LogClient:                 logClient,
		Executor:                  job.NewExecutor(),
		Store:                     runs,
		Registrar:                 registrar,
		SVIDSource:                svidSource,
		RegistrationMinTTL:        time.Second,
		RegistrationMaxTTL:        5 * time.Minute,
		SecretResolverForWorkload: resolverForWorkload,
	}

	if _, err := worker.RunOne(ctx); err != nil {
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
			secretstore.WithExecutionScopeResolver(integrationExecutionScopeResolver{store: runs, trustDomain: trustDomain}),
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

type integrationExecutionScopeResolver struct {
	store interface {
		GetActiveExecutionDispatch(context.Context, string, string) (dal.ExecutionDispatchRecord, error)
	}
	trustDomain string
}

func (r integrationExecutionScopeResolver) ResolveExecutionScope(ctx context.Context, runID, executionID string) (secretstore.ExecutionScope, error) {
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

	joinedLogs := waitForRunLogContains(t, logStore, runID, "Command completed successfully", 3*time.Second)
	if strings.Contains(joinedLogs, secretPlaintext) {
		t.Fatalf("secret plaintext leaked to run logs:\n%s", joinedLogs)
	}

	for _, call := range logger.GetCalls() {
		if strings.Contains(call.Message, secretPlaintext) {
			t.Fatalf("secret plaintext leaked to service logs: %+v", call)
		}
	}
}

func waitForRunLogContains(t *testing.T, store *logserver.LocalRunLogStore, runID, want string, timeout time.Duration) string {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var joined string
	for time.Now().Before(deadline) {
		joined = allEntriesJoined(t, store, runID)
		if strings.Contains(joined, want) {
			return joined
		}

		time.Sleep(25 * time.Millisecond)
	}

	t.Fatalf("run logs did not contain %q:\n%s", want, joined)
	return ""
}

func allEntriesJoined(t *testing.T, store *logserver.LocalRunLogStore, runID string) string {
	t.Helper()

	entries, err := store.List(runID)
	if err != nil {
		t.Fatalf("list run logs: %v", err)
	}

	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		parts = append(parts, string(entry.Data))
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

	dir, err := os.MkdirTemp(shortTempRoot(), "vectis-secrets-integration-*")
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

func shortTempRoot() string {
	if runtime.GOOS == "windows" {
		return ""
	}

	return "/tmp"
}

func strp(s string) *string {
	return &s
}
