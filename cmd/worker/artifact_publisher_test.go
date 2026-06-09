package main

import (
	"bytes"
	"context"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/artifact"
	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
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

func TestWorkerNewArtifactPublisherRequiresManifestRepository(t *testing.T) {
	w := &worker{logger: mocks.NewMockLogger()}
	if got := w.newArtifactPublisher(workerTestExecutionEnvelope()); got != nil {
		t.Fatalf("expected nil publisher without manifest repository, got %+v", got)
	}
}
