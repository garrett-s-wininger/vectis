package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/artifact"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	jobexec "vectis/internal/job"
	"vectis/internal/testutil/dbtest"
	"vectis/internal/testutil/grpctest"

	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func TestWorkerArtifactPublisherPublishesWithExecutionAttribution(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	ctx := context.Background()
	store, err := artifact.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local artifact store: %v", err)
	}
	defer store.Close()

	artifactServer := grpctest.StartServer(t, func(srv *grpc.Server) {
		api.RegisterArtifactServiceServer(srv, artifact.NewServer(store))
	})
	viper.Set("discovery.artifact.address", artifactServer.Addr())

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	jobID := "job-worker-artifact"
	def := `{"id":"` + jobID + `","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	uses := "builtins/shell"
	job := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   strPtr("root"),
			Uses: &uses,
		},
	}
	env := attachPendingExecutionEnvelopeForTest(t, repos.Runs(), job, runID)

	w := &worker{
		logger:            mocks.NewMockLogger(),
		artifactManifests: repos.Artifacts(),
	}

	publisher := w.newArtifactPublisher(env)
	if publisher == nil {
		t.Fatal("expected artifact publisher")
	}
	defer publisher.Close()

	content := []byte("worker artifact content")
	got, err := publisher.PublishArtifact(ctx, action.ArtifactPublishRequest{
		Name:        "coverage",
		Path:        "coverage/out.txt",
		ContentType: "text/plain",
		Reader:      bytes.NewReader(content),
	})
	if err != nil {
		t.Fatalf("publish artifact: %v", err)
	}

	if got.Name != "coverage" || got.Path != "coverage/out.txt" || got.ContentType != "text/plain" {
		t.Fatalf("unexpected publish result: %+v", got)
	}

	if got.ArtifactShardID != "pinned" {
		t.Fatalf("artifact shard = %q, want pinned", got.ArtifactShardID)
	}

	rec, err := repos.Artifacts().GetByRunAndName(ctx, runID, "coverage")
	if err != nil {
		t.Fatalf("get artifact manifest: %v", err)
	}

	if rec.TaskID == nil || *rec.TaskID != env.TaskID {
		t.Fatalf("task id = %+v, want %q", rec.TaskID, env.TaskID)
	}

	if rec.TaskAttemptID == nil || *rec.TaskAttemptID != env.TaskAttemptID {
		t.Fatalf("task attempt id = %+v, want %q", rec.TaskAttemptID, env.TaskAttemptID)
	}

	if rec.ExecutionID == nil || *rec.ExecutionID != env.ExecutionID {
		t.Fatalf("execution id = %+v, want %q", rec.ExecutionID, env.ExecutionID)
	}

	if rec.CellID != "iad-a" {
		t.Fatalf("cell id = %q, want iad-a", rec.CellID)
	}

	if _, _, err := store.Open(ctx, rec.BlobKey); err != nil {
		t.Fatalf("expected blob in artifact store: %v", err)
	}
}

func TestWorkerUploadArtifactActionPublishesDownloadableBlob(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	ctx := context.Background()
	store, err := artifact.NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local artifact store: %v", err)
	}
	defer store.Close()

	artifactServer := grpctest.StartServer(t, func(srv *grpc.Server) {
		api.RegisterArtifactServiceServer(srv, artifact.NewServer(store))
	})

	viper.Set("discovery.artifact.address", artifactServer.Addr())
	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	jobID := "job-worker-upload-artifact"
	def := `{"id":"` + jobID + `","root":{"uses":"builtins/upload-artifact"}}`

	if err := repos.Jobs().Create(ctx, jobID, def, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	metadataJSON := `{"kind":"coverage"}`
	uses := "builtins/upload-artifact"
	job := &api.Job{
		Id:    &jobID,
		RunId: &runID,
		Root: &api.Node{
			Id:   strPtr("upload"),
			Uses: &uses,
			With: map[string]string{
				"name":          "coverage",
				"path":          "coverage/out.txt",
				"content_type":  "text/plain",
				"metadata_json": metadataJSON,
			},
		},
	}

	env := attachPendingExecutionEnvelopeForTest(t, repos.Runs(), job, runID)
	w := &worker{
		logger:            mocks.NewMockLogger(),
		artifactManifests: repos.Artifacts(),
	}

	publisher := w.newArtifactPublisher(env)
	if publisher == nil {
		t.Fatal("expected artifact publisher")
	}
	defer publisher.Close()

	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, "coverage"), 0o755); err != nil {
		t.Fatalf("create artifact dir: %v", err)
	}

	wantContent := []byte("mode: atomic\ncoverage: 87\n")
	if err := os.WriteFile(filepath.Join(workspace, "coverage", "out.txt"), wantContent, 0o644); err != nil {
		t.Fatalf("write artifact file: %v", err)
	}

	executor := jobexec.NewExecutor()
	logClient := mocks.NewMockLogClient()
	logger := mocks.NewMockLogger()
	err = executeJobInWorkspaceWithOptionsAndWait(t, ctx, executor, job, logClient, logger, workspace, jobexec.ExecuteOptions{
		ArtifactPublisher: publisher,
	})

	if err != nil {
		t.Fatalf("execute upload-artifact job: %v", err)
	}

	rec, err := repos.Artifacts().GetByRunAndName(ctx, runID, "coverage")
	if err != nil {
		t.Fatalf("get artifact manifest: %v", err)
	}

	if rec.Path != "coverage/out.txt" {
		t.Fatalf("artifact path = %q, want coverage/out.txt", rec.Path)
	}

	if rec.ContentType != "text/plain" {
		t.Fatalf("content type = %q, want text/plain", rec.ContentType)
	}

	if rec.SizeBytes != int64(len(wantContent)) {
		t.Fatalf("size bytes = %d, want %d", rec.SizeBytes, len(wantContent))
	}

	if rec.MetadataJSON == nil || *rec.MetadataJSON != metadataJSON {
		t.Fatalf("metadata json = %+v, want %q", rec.MetadataJSON, metadataJSON)
	}

	if rec.TaskID == nil || *rec.TaskID != env.TaskID {
		t.Fatalf("task id = %+v, want %q", rec.TaskID, env.TaskID)
	}

	if rec.TaskAttemptID == nil || *rec.TaskAttemptID != env.TaskAttemptID {
		t.Fatalf("task attempt id = %+v, want %q", rec.TaskAttemptID, env.TaskAttemptID)
	}

	if rec.ExecutionID == nil || *rec.ExecutionID != env.ExecutionID {
		t.Fatalf("execution id = %+v, want %q", rec.ExecutionID, env.ExecutionID)
	}

	if rec.CellID != "iad-a" {
		t.Fatalf("cell id = %q, want iad-a", rec.CellID)
	}

	if rec.ArtifactShardID != "pinned" {
		t.Fatalf("artifact shard = %q, want pinned", rec.ArtifactShardID)
	}

	desc, rc, err := store.Open(ctx, rec.BlobKey)
	if err != nil {
		t.Fatalf("open artifact blob: %v", err)
	}
	defer rc.Close()

	gotContent, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read artifact blob: %v", err)
	}

	if !bytes.Equal(gotContent, wantContent) {
		t.Fatalf("blob content = %q, want %q", gotContent, wantContent)
	}

	if desc.Key != rec.BlobKey || desc.Algorithm != rec.BlobAlgorithm || desc.Digest != rec.BlobDigest || desc.Size != rec.SizeBytes {
		t.Fatalf("blob descriptor = %+v, manifest = %+v", desc, rec)
	}

	list, next, err := repos.Artifacts().ListByRun(ctx, runID, 0, 10)
	if err != nil {
		t.Fatalf("list artifacts: %v", err)
	}

	if len(list) != 1 || next != 0 {
		t.Fatalf("list artifacts len=%d next=%d, want len=1 next=0", len(list), next)
	}
}

func TestWorkerNewArtifactPublisherRequiresManifestRepository(t *testing.T) {
	w := &worker{logger: mocks.NewMockLogger()}
	if got := w.newArtifactPublisher(workerTestExecutionEnvelope()); got != nil {
		t.Fatalf("expected nil publisher without manifest repository, got %+v", got)
	}
}

func executeJobInWorkspaceWithOptionsAndWait(t *testing.T, ctx context.Context, executor *jobexec.Executor, testJob *api.Job, logClient *mocks.MockLogClient, logger *mocks.MockLogger, workspace string, opts jobexec.ExecuteOptions) error {
	t.Helper()

	streamCh := make(chan jobexec.LogStreamWaiter, 1)
	executor.TestLogStreamHook = streamCh
	defer func() { executor.TestLogStreamHook = nil }()

	err := executor.ExecuteJobInWorkspaceWithOptions(ctx, testJob, logClient, logger, workspace, opts)

	select {
	case stream := <-streamCh:
		if waitErr := stream.WaitForDone(5 * time.Second); waitErr != nil {
			t.Errorf("wait for log stream done: %v", waitErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for log stream hook")
	}

	return err
}
