package artifact

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/dal"
	"vectis/internal/testutil/dbtest"

	"google.golang.org/grpc"
)

func TestPublisher_PublishUploadsBlobAndRecordsManifest(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	db := dbtest.NewTestDB(t)
	repos := dal.NewSQLRepositoriesWithCellID(db, "iad-a")
	runID := createPublisherTestRun(t, context.Background(), repos, "job-publisher")
	dispatch, err := repos.Runs().GetPendingExecution(context.Background(), runID)
	if err != nil {
		t.Fatalf("get pending execution: %v", err)
	}

	publisher, err := NewPublisher(PublisherOptions{
		Client:           newArtifactServiceClient(t, store, ServerOptions{ReadBlobChunkBytes: 3}),
		Manifests:        repos.Artifacts(),
		ArtifactShardID:  "artifact-1",
		UploadChunkBytes: 4,
	})

	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	content := []byte("hello artifact")
	metadata := `{"kind":"coverage"}`
	got, err := publisher.Publish(context.Background(), PublishRequest{
		RunID:          runID,
		TaskID:         dispatch.TaskID,
		TaskAttemptID:  dispatch.TaskAttemptID,
		ExecutionID:    dispatch.ExecutionID,
		Name:           "coverage",
		Path:           "coverage/out.txt",
		ContentType:    "text/plain",
		MetadataJSON:   &metadata,
		Reader:         bytes.NewReader(content),
		ExpectedSHA256: sha256BytesHex(content),
		ExpectedSize:   int64(len(content)),
		RequireSize:    true,
	})

	if err != nil {
		t.Fatalf("publish artifact: %v", err)
	}

	assertBlobDescriptor(t, got.Blob, sha256BytesHex(content), int64(len(content)))

	manifest := got.Manifest
	if manifest.RunID != runID || manifest.Name != "coverage" || manifest.Path != "coverage/out.txt" {
		t.Fatalf("unexpected manifest identity: %+v", manifest)
	}

	if manifest.CellID != "iad-a" {
		t.Fatalf("manifest cell id = %q, want iad-a", manifest.CellID)
	}

	if manifest.TaskID == nil || *manifest.TaskID != dispatch.TaskID {
		t.Fatalf("manifest task id = %+v, want %q", manifest.TaskID, dispatch.TaskID)
	}

	if manifest.TaskAttemptID == nil || *manifest.TaskAttemptID != dispatch.TaskAttemptID {
		t.Fatalf("manifest task attempt id = %+v, want %q", manifest.TaskAttemptID, dispatch.TaskAttemptID)
	}

	if manifest.ExecutionID == nil || *manifest.ExecutionID != dispatch.ExecutionID {
		t.Fatalf("manifest execution id = %+v, want %q", manifest.ExecutionID, dispatch.ExecutionID)
	}

	if manifest.ContentType != "text/plain" {
		t.Fatalf("manifest content type = %q, want text/plain", manifest.ContentType)
	}

	if manifest.BlobKey != got.Blob.Key || manifest.BlobDigest != got.Blob.Digest || manifest.SizeBytes != got.Blob.Size {
		t.Fatalf("manifest blob fields do not match descriptor: manifest=%+v blob=%+v", manifest, got.Blob)
	}

	if manifest.ArtifactShardID != "artifact-1" {
		t.Fatalf("manifest artifact shard = %q, want artifact-1", manifest.ArtifactShardID)
	}

	if manifest.MetadataJSON == nil || *manifest.MetadataJSON != metadata {
		t.Fatalf("manifest metadata = %+v, want %q", manifest.MetadataJSON, metadata)
	}

	_, rc, err := store.Open(context.Background(), got.Blob.Key)
	if err != nil {
		t.Fatalf("open stored blob: %v", err)
	}
	defer rc.Close()

	stored, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read stored blob: %v", err)
	}

	if !bytes.Equal(stored, content) {
		t.Fatalf("stored blob = %q, want %q", stored, content)
	}

	fromRepo, err := repos.Artifacts().GetByRunAndName(context.Background(), runID, "coverage")
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}

	if fromRepo.ID != manifest.ID || fromRepo.BlobKey != manifest.BlobKey {
		t.Fatalf("repo manifest = %+v, want %+v", fromRepo, manifest)
	}
}

func TestPublisher_PublishRetryUpdatesManifest(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	repos := dal.NewSQLRepositories(dbtest.NewTestDB(t))
	runID := createPublisherTestRun(t, context.Background(), repos, "job-publisher-retry")
	publisher, err := NewPublisher(PublisherOptions{
		Client:          newArtifactServiceClient(t, store, ServerOptions{}),
		Manifests:       repos.Artifacts(),
		ArtifactShardID: "artifact-1",
	})

	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	first, err := publisher.Publish(context.Background(), PublishRequest{
		RunID:  runID,
		Name:   "report",
		Reader: strings.NewReader("first"),
	})

	if err != nil {
		t.Fatalf("publish first artifact: %v", err)
	}

	second, err := publisher.Publish(context.Background(), PublishRequest{
		RunID:  runID,
		Name:   "report",
		Reader: strings.NewReader("second"),
	})

	if err != nil {
		t.Fatalf("publish second artifact: %v", err)
	}

	if second.Manifest.ID != first.Manifest.ID {
		t.Fatalf("retry should update the existing manifest id: got %d want %d", second.Manifest.ID, first.Manifest.ID)
	}

	if second.Manifest.BlobKey == first.Manifest.BlobKey {
		t.Fatalf("retry did not update blob key: first=%q second=%q", first.Manifest.BlobKey, second.Manifest.BlobKey)
	}

	if second.Manifest.Path != "report" {
		t.Fatalf("empty path should default to artifact name, got %q", second.Manifest.Path)
	}
}

func TestPublisher_UploadFailureDoesNotRecordManifest(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	repos := dal.NewSQLRepositories(dbtest.NewTestDB(t))
	runID := createPublisherTestRun(t, context.Background(), repos, "job-publisher-failure")
	publisher, err := NewPublisher(PublisherOptions{
		Client:          newArtifactServiceClient(t, store, ServerOptions{}),
		Manifests:       repos.Artifacts(),
		ArtifactShardID: "artifact-1",
	})

	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	_, err = publisher.Publish(context.Background(), PublishRequest{
		RunID:          runID,
		Name:           "bad",
		Reader:         strings.NewReader("payload"),
		ExpectedSHA256: strings.Repeat("0", 64),
	})

	if err == nil {
		t.Fatal("expected publish to fail")
	}

	if _, err := repos.Artifacts().GetByRunAndName(context.Background(), runID, "bad"); !dal.IsNotFound(err) {
		t.Fatalf("expected no manifest after upload failure, got %v", err)
	}
}

func TestPublisherFault_ManifestFailureLeavesRetryableUploadedBlob(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	repos := dal.NewSQLRepositories(dbtest.NewTestDB(t))
	runID := createPublisherTestRun(t, context.Background(), repos, "job-publisher-manifest-fault")
	manifests := &failOnceArtifactsRepository{
		ArtifactsRepository: repos.Artifacts(),
		recordErr:           errors.New("manifest db unavailable"),
	}

	publisher, err := NewPublisher(PublisherOptions{
		Client:          newArtifactServiceClient(t, store, ServerOptions{}),
		Manifests:       manifests,
		ArtifactShardID: "artifact-1",
	})

	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	content := []byte("manifest retry payload")
	digest := sha256BytesHex(content)
	first, err := publisher.Publish(context.Background(), PublishRequest{
		RunID:          runID,
		Name:           "report",
		Reader:         bytes.NewReader(content),
		ExpectedSHA256: digest,
		ExpectedSize:   int64(len(content)),
		RequireSize:    true,
	})

	if !errors.Is(err, manifests.recordErr) {
		t.Fatalf("first publish error = %v, want %v", err, manifests.recordErr)
	}

	if first.Blob.Key != "" || first.Manifest.ID != 0 {
		t.Fatalf("failed publish returned partial result: %+v", first)
	}

	uploaded, err := store.Stat(context.Background(), BlobKeySHA256(digest))
	if err != nil {
		t.Fatalf("blob should remain uploaded after manifest failure: %v", err)
	}

	if uploaded.Size != int64(len(content)) {
		t.Fatalf("uploaded blob size = %d, want %d", uploaded.Size, len(content))
	}

	if _, err := repos.Artifacts().GetByRunAndName(context.Background(), runID, "report"); !dal.IsNotFound(err) {
		t.Fatalf("expected no manifest after injected manifest failure, got %v", err)
	}

	retried, err := publisher.Publish(context.Background(), PublishRequest{
		RunID:          runID,
		Name:           "report",
		Reader:         bytes.NewReader(content),
		ExpectedSHA256: digest,
		ExpectedSize:   int64(len(content)),
		RequireSize:    true,
	})

	if err != nil {
		t.Fatalf("retry publish after manifest recovery: %v", err)
	}

	assertBlobDescriptor(t, retried.Blob, digest, int64(len(content)))
	if retried.Manifest.BlobKey != BlobKeySHA256(digest) {
		t.Fatalf("retried manifest blob key = %q, want %q", retried.Manifest.BlobKey, BlobKeySHA256(digest))
	}

	fromRepo, err := repos.Artifacts().GetByRunAndName(context.Background(), runID, "report")
	if err != nil {
		t.Fatalf("get retried manifest: %v", err)
	}

	if fromRepo.ID != retried.Manifest.ID || fromRepo.BlobKey != retried.Manifest.BlobKey {
		t.Fatalf("repo manifest = %+v, want %+v", fromRepo, retried.Manifest)
	}
}

func TestPublisher_InvalidBlobDescriptorDoesNotRecordManifest(t *testing.T) {
	repos := dal.NewSQLRepositories(dbtest.NewTestDB(t))
	runID := createPublisherTestRun(t, context.Background(), repos, "job-publisher-invalid-descriptor")
	goodDigest := sha256BytesHex([]byte("payload"))
	badDigest := sha256BytesHex([]byte("other"))
	publisher, err := NewPublisher(PublisherOptions{
		Client: malformedArtifactClient{desc: &api.BlobDescriptor{
			Key:       strPointer(BlobKeySHA256(goodDigest)),
			Algorithm: strPointer(HashSHA256),
			Digest:    strPointer(badDigest),
			Size:      int64Pointer(7),
		}},
		Manifests:       repos.Artifacts(),
		ArtifactShardID: "artifact-1",
	})

	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	_, err = publisher.Publish(context.Background(), PublishRequest{
		RunID:  runID,
		Name:   "bad-descriptor",
		Reader: strings.NewReader("payload"),
	})

	if err == nil || !strings.Contains(err.Error(), "invalid blob descriptor") {
		t.Fatalf("expected invalid descriptor error, got %v", err)
	}

	if _, err := repos.Artifacts().GetByRunAndName(context.Background(), runID, "bad-descriptor"); !dal.IsNotFound(err) {
		t.Fatalf("expected no manifest after invalid descriptor, got %v", err)
	}
}

func TestPublisher_RunQuotaRejectsBeforeUpload(t *testing.T) {
	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	repos := dal.NewSQLRepositories(dbtest.NewTestDB(t))
	runID := createPublisherTestRun(t, context.Background(), repos, "job-publisher-quota")
	publisher, err := NewPublisher(PublisherOptions{
		Client:          newArtifactServiceClient(t, store, ServerOptions{}),
		Manifests:       repos.Artifacts(),
		ArtifactShardID: "artifact-1",
		MaxRunBytes:     8,
		MaxRunArtifacts: 2,
	})

	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	first := []byte("12345")
	if _, err := publisher.Publish(context.Background(), PublishRequest{
		RunID:        runID,
		Name:         "first",
		Reader:       bytes.NewReader(first),
		ExpectedSize: int64(len(first)),
		RequireSize:  true,
	}); err != nil {
		t.Fatalf("publish first artifact: %v", err)
	}

	second := []byte("6789")
	_, err = publisher.Publish(context.Background(), PublishRequest{
		RunID:        runID,
		Name:         "second",
		Reader:       bytes.NewReader(second),
		ExpectedSize: int64(len(second)),
		RequireSize:  true,
	})

	if !errors.Is(err, ErrRunArtifactQuotaExceeded) {
		t.Fatalf("expected byte quota error, got %v", err)
	}

	if _, err := repos.Artifacts().GetByRunAndName(context.Background(), runID, "second"); !dal.IsNotFound(err) {
		t.Fatalf("expected no manifest after quota failure, got %v", err)
	}

	if _, err := store.Stat(context.Background(), BlobKeySHA256(sha256BytesHex(second))); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("quota failure should not upload blob, got %v", err)
	}

	replacement := []byte("12")
	if _, err := publisher.Publish(context.Background(), PublishRequest{
		RunID:        runID,
		Name:         "first",
		Reader:       bytes.NewReader(replacement),
		ExpectedSize: int64(len(replacement)),
		RequireSize:  true,
	}); err != nil {
		t.Fatalf("replacement within quota should succeed: %v", err)
	}

	if _, err := publisher.Publish(context.Background(), PublishRequest{
		RunID:        runID,
		Name:         "second",
		Reader:       bytes.NewReader([]byte("34")),
		ExpectedSize: 2,
		RequireSize:  true,
	}); err != nil {
		t.Fatalf("second artifact should fit after replacement: %v", err)
	}

	_, err = publisher.Publish(context.Background(), PublishRequest{
		RunID:        runID,
		Name:         "third",
		Reader:       bytes.NewReader([]byte("5")),
		ExpectedSize: 1,
		RequireSize:  true,
	})

	if !errors.Is(err, ErrRunArtifactQuotaExceeded) {
		t.Fatalf("expected count quota error, got %v", err)
	}
}

func TestPublisher_Validation(t *testing.T) {
	if _, err := NewPublisher(PublisherOptions{}); err == nil {
		t.Fatal("expected missing client to fail")
	}

	store, err := NewLocalStore(t.TempDir())
	if err != nil {
		t.Fatalf("new local store: %v", err)
	}
	defer store.Close()

	client := newArtifactServiceClient(t, store, ServerOptions{})
	manifests := dal.NewSQLRepositories(dbtest.NewTestDB(t)).Artifacts()

	if _, err := NewPublisher(PublisherOptions{Client: client, ArtifactShardID: "artifact-1"}); err == nil {
		t.Fatal("expected missing manifests to fail")
	}

	if _, err := NewPublisher(PublisherOptions{Client: client, Manifests: manifests}); err == nil {
		t.Fatal("expected missing shard id to fail")
	}

	publisher, err := NewPublisher(PublisherOptions{
		Client:          client,
		Manifests:       manifests,
		ArtifactShardID: "artifact-1",
	})

	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	if _, err := publisher.Publish(context.Background(), PublishRequest{Name: "missing-reader"}); err == nil {
		t.Fatal("expected missing reader to fail")
	}

	digest := sha256BytesHex([]byte("payload"))
	if _, err := publisher.Publish(context.Background(), PublishRequest{
		Name:           "missing-run",
		Reader:         strings.NewReader("payload"),
		ExpectedSHA256: digest,
	}); !dal.IsConflict(err) {
		t.Fatalf("expected missing run to fail validation, got %v", err)
	}

	if _, err := store.Stat(context.Background(), BlobKeySHA256(digest)); !errors.Is(err, ErrBlobNotFound) {
		t.Fatalf("validation failure should not upload blob, got %v", err)
	}
}

type failOnceArtifactsRepository struct {
	dal.ArtifactsRepository
	recordErr error
	failed    bool
}

func (r *failOnceArtifactsRepository) Record(ctx context.Context, create dal.ArtifactCreate) (dal.ArtifactRecord, error) {
	if !r.failed {
		r.failed = true
		return dal.ArtifactRecord{}, r.recordErr
	}

	return r.ArtifactsRepository.Record(ctx, create)
}

func assertBlobDescriptor(t *testing.T, desc BlobDescriptor, digest string, size int64) {
	t.Helper()

	if desc.Key != BlobKeySHA256(digest) {
		t.Fatalf("key = %q, want %q", desc.Key, BlobKeySHA256(digest))
	}

	if desc.Algorithm != HashSHA256 {
		t.Fatalf("algorithm = %q, want %q", desc.Algorithm, HashSHA256)
	}

	if desc.Digest != digest {
		t.Fatalf("digest = %q, want %q", desc.Digest, digest)
	}

	if desc.Size != size {
		t.Fatalf("size = %d, want %d", desc.Size, size)
	}
}

func createPublisherTestRun(t *testing.T, ctx context.Context, repos *dal.SQLRepositories, jobID string) string {
	t.Helper()

	def := `{"id":"` + jobID + `","root":{"uses":"builtins/shell"}}`
	if err := repos.Jobs().Create(ctx, jobID, def, 1); err != nil {
		t.Fatalf("create job: %v", err)
	}

	runID, _, err := repos.Runs().CreateRun(ctx, jobID, nil, 1)
	if err != nil {
		t.Fatalf("create run: %v", err)
	}

	return runID
}

type malformedArtifactClient struct {
	desc *api.BlobDescriptor
}

func (c malformedArtifactClient) UploadBlob(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[api.UploadBlobRequest, api.BlobDescriptor], error) {
	return &malformedUploadStream{desc: c.desc}, nil
}

func (c malformedArtifactClient) StatBlob(context.Context, *api.GetBlobRequest, ...grpc.CallOption) (*api.BlobDescriptor, error) {
	return nil, errors.New("not implemented")
}

func (c malformedArtifactClient) ReadBlob(context.Context, *api.GetBlobRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[api.ReadBlobResponse], error) {
	return nil, errors.New("not implemented")
}

type malformedUploadStream struct {
	grpc.ClientStream
	desc *api.BlobDescriptor
}

func (s *malformedUploadStream) Send(*api.UploadBlobRequest) error {
	return nil
}

func (s *malformedUploadStream) CloseAndRecv() (*api.BlobDescriptor, error) {
	return s.desc, nil
}
