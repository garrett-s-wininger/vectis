package builtins

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vectis/internal/action"
	"vectis/internal/interfaces/mocks"
)

type recordingArtifactPublisher struct {
	req  action.ArtifactPublishRequest
	data []byte
	err  error
}

func (p *recordingArtifactPublisher) PublishArtifact(_ context.Context, req action.ArtifactPublishRequest) (action.ArtifactPublishResult, error) {
	p.req = req
	if req.Reader != nil {
		data, err := io.ReadAll(req.Reader)
		if err != nil {
			return action.ArtifactPublishResult{}, err
		}

		p.data = data
	}

	if p.err != nil {
		return action.ArtifactPublishResult{}, p.err
	}

	return action.ArtifactPublishResult{
		Name:            req.Name,
		Path:            req.Path,
		ContentType:     req.ContentType,
		BlobKey:         "sha256:abc",
		BlobAlgorithm:   "sha256",
		BlobDigest:      "abc",
		SizeBytes:       int64(len(p.data)),
		ArtifactShardID: "artifact-1",
	}, nil
}

func TestUploadArtifactAction_ExecutePublishesWorkspaceFile(t *testing.T) {
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, "coverage"), 0o755); err != nil {
		t.Fatalf("mkdir coverage: %v", err)
	}

	content := []byte("coverage data")
	if err := os.WriteFile(filepath.Join(workspace, "coverage", "out.json"), content, 0o644); err != nil {
		t.Fatalf("write artifact file: %v", err)
	}

	publisher := &recordingArtifactPublisher{}
	state := &action.ExecutionState{
		RunID:      "run-upload-artifact",
		Workspace:  workspace,
		Logger:     mocks.NewMockLogger(),
		LogStream:  &mockLogStream{},
		Artifacts:  publisher,
		ProcessEnv: action.SanitizedProcessEnv(workspace, nil),
	}

	metadataJSON := `{"kind":"coverage"}`
	result := (&UploadArtifactAction{}).Execute(context.Background(), state, map[string]any{
		"name":          "coverage",
		"path":          "coverage/out.json",
		"content_type":  "application/json",
		"metadata_json": metadataJSON,
		"max_bytes":     "1024",
	}, nil)

	if result.Status != action.StatusSuccess {
		t.Fatalf("expected success, got %v err=%v", result.Status, result.Error)
	}

	if publisher.req.Name != "coverage" || publisher.req.Path != "coverage/out.json" || publisher.req.ContentType != "application/json" {
		t.Fatalf("unexpected publish request: %+v", publisher.req)
	}

	if publisher.req.MetadataJSON == nil || *publisher.req.MetadataJSON != metadataJSON {
		t.Fatalf("metadata json = %+v, want %q", publisher.req.MetadataJSON, metadataJSON)
	}

	if publisher.req.MaxBytes != 1024 {
		t.Fatalf("max bytes = %d, want 1024", publisher.req.MaxBytes)
	}

	if publisher.req.ExpectedSize != int64(len(content)) || !publisher.req.RequireSize {
		t.Fatalf("size expectation = (%d, %v), want (%d, true)", publisher.req.ExpectedSize, publisher.req.RequireSize, len(content))
	}

	if !bytes.Equal(publisher.data, content) {
		t.Fatalf("published data = %q, want %q", publisher.data, content)
	}

	artifact, ok := result.Outputs["artifact"].(map[string]any)
	if !ok {
		t.Fatalf("artifact output missing or wrong type: %+v", result.Outputs)
	}

	if artifact["blob_key"] != "sha256:abc" || artifact["artifact_shard_id"] != "artifact-1" {
		t.Fatalf("unexpected artifact output: %+v", artifact)
	}
}

func TestUploadArtifactAction_ExecuteRequiresPublisher(t *testing.T) {
	state := &action.ExecutionState{
		Workspace: t.TempDir(),
		Logger:    mocks.NewMockLogger(),
	}

	result := (&UploadArtifactAction{}).Execute(context.Background(), state, map[string]any{
		"name": "coverage",
		"path": "coverage/out.json",
	}, nil)

	if result.Status != action.StatusFailure || result.Error == nil || !strings.Contains(result.Error.Error(), "not configured") {
		t.Fatalf("expected publisher configuration failure, got %+v", result)
	}
}

func TestUploadArtifactAction_ExecuteRejectsPathOutsideWorkspace(t *testing.T) {
	state := &action.ExecutionState{
		Workspace: t.TempDir(),
		Logger:    mocks.NewMockLogger(),
		Artifacts: &recordingArtifactPublisher{},
	}

	result := (&UploadArtifactAction{}).Execute(context.Background(), state, map[string]any{
		"name": "escape",
		"path": "../escape.txt",
	}, nil)

	if result.Status != action.StatusFailure || result.Error == nil || !strings.Contains(result.Error.Error(), "within the workspace") {
		t.Fatalf("expected workspace containment failure, got %+v", result)
	}
}

func TestUploadArtifactAction_ExecuteRejectsSymlinkOutsideWorkspace(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	outsideFile := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(outsideFile, []byte("secret"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}

	linkPath := filepath.Join(workspace, "secret-link.txt")
	if err := os.Symlink(outsideFile, linkPath); err != nil {
		t.Skipf("symlink not available: %v", err)
	}

	state := &action.ExecutionState{
		Workspace: workspace,
		Logger:    mocks.NewMockLogger(),
		Artifacts: &recordingArtifactPublisher{},
	}

	result := (&UploadArtifactAction{}).Execute(context.Background(), state, map[string]any{
		"name": "escape",
		"path": "secret-link.txt",
	}, nil)

	if result.Status != action.StatusFailure || result.Error == nil || !strings.Contains(result.Error.Error(), "within the workspace") {
		t.Fatalf("expected symlink containment failure, got %+v", result)
	}
}

func TestUploadArtifactAction_ExecutePropagatesPublisherError(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "artifact.txt"), []byte("data"), 0o644); err != nil {
		t.Fatalf("write artifact file: %v", err)
	}

	state := &action.ExecutionState{
		Workspace: workspace,
		Logger:    mocks.NewMockLogger(),
		Artifacts: &recordingArtifactPublisher{err: errors.New("artifact backend unavailable")},
	}

	result := (&UploadArtifactAction{}).Execute(context.Background(), state, map[string]any{
		"name": "artifact",
		"path": "artifact.txt",
	}, nil)

	if result.Status != action.StatusFailure || result.Error == nil || !strings.Contains(result.Error.Error(), "artifact backend unavailable") {
		t.Fatalf("expected publisher failure, got %+v", result)
	}
}

func TestUploadArtifactAction_ValidateWith(t *testing.T) {
	errs := (&UploadArtifactAction{}).ValidateWith(map[string]string{
		"name":          "coverage",
		"path":          "coverage/out.json",
		"metadata_json": "{",
		"max_bytes":     "-1",
	})

	if len(errs) != 2 {
		t.Fatalf("expected metadata and max_bytes validation errors, got %+v", errs)
	}
}
