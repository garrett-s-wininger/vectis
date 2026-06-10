package builtins

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	api "vectis/api/gen/go"
	"vectis/internal/action"
)

type UploadArtifactAction struct{}

func (a *UploadArtifactAction) ValidateWith(with map[string]string) []action.FieldError {
	errs := action.ValidateWithSpec(with, []action.FieldSpec{
		{Name: "name", Type: action.FieldString, Required: true},
		{Name: "path", Type: action.FieldString, Required: true},
		{Name: "content_type", Type: action.FieldString},
		{Name: "metadata_json", Type: action.FieldString},
		{Name: "max_bytes", Type: action.FieldString},
	})

	if raw := strings.TrimSpace(with["metadata_json"]); raw != "" && !json.Valid([]byte(raw)) {
		errs = append(errs, action.FieldError{Field: "metadata_json", Message: "must be valid JSON"})
	}

	if raw := strings.TrimSpace(with["max_bytes"]); raw != "" {
		maxBytes, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || maxBytes < 0 {
			errs = append(errs, action.FieldError{Field: "max_bytes", Message: "must be a non-negative integer"})
		}
	}

	return errs
}

func (a *UploadArtifactAction) Type() string {
	return "builtins/upload-artifact"
}

func (a *UploadArtifactAction) Execute(ctx context.Context, state *action.ExecutionState, inputs map[string]any, _ action.Ports) action.Result {
	if state.Artifacts == nil {
		return action.NewFailureResult(fmt.Errorf("artifact publisher is not configured"))
	}

	name := stringInput(inputs, "name")
	rawPath := stringInput(inputs, "path")
	if name == "" {
		return action.NewFailureResult(fmt.Errorf("upload-artifact action requires 'name' input"))
	}

	if rawPath == "" {
		return action.NewFailureResult(fmt.Errorf("upload-artifact action requires 'path' input"))
	}

	sourcePath, artifactPath, err := artifactSourcePath(state.Workspace, rawPath)
	if err != nil {
		return action.NewFailureResult(err)
	}

	file, err := os.Open(sourcePath)
	if err != nil {
		return action.NewFailureResult(fmt.Errorf("open artifact file: %w", err))
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return action.NewFailureResult(fmt.Errorf("stat artifact file: %w", err))
	}

	if info.IsDir() {
		return action.NewFailureResult(fmt.Errorf("artifact path is a directory: %s", artifactPath))
	}

	maxBytes, err := int64Input(inputs, "max_bytes")
	if err != nil {
		return action.NewFailureResult(err)
	}

	metadataJSON := optionalStringInput(inputs, "metadata_json")
	result, err := state.Artifacts.PublishArtifact(ctx, action.ArtifactPublishRequest{
		Name:         name,
		Path:         artifactPath,
		ContentType:  stringInput(inputs, "content_type"),
		MetadataJSON: metadataJSON,
		Reader:       file,
		ExpectedSize: info.Size(),
		RequireSize:  true,
		MaxBytes:     maxBytes,
	})

	if err != nil {
		return action.NewFailureResult(fmt.Errorf("publish artifact: %w", err))
	}

	state.Logger.Info("Published artifact %s from %s (%d bytes)", result.Name, artifactPath, result.SizeBytes)
	sendLog(state, api.Stream_STREAM_STDOUT, fmt.Sprintf("Published artifact: %s", result.Name))

	return action.NewSuccessResult(map[string]any{
		"artifact": map[string]any{
			"name":              result.Name,
			"path":              result.Path,
			"content_type":      result.ContentType,
			"blob_key":          result.BlobKey,
			"blob_algorithm":    result.BlobAlgorithm,
			"blob_digest":       result.BlobDigest,
			"size_bytes":        result.SizeBytes,
			"artifact_shard_id": result.ArtifactShardID,
		},
	})
}

func artifactSourcePath(workspace, rawPath string) (string, string, error) {
	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		return "", "", fmt.Errorf("workspace is required")
	}

	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return "", "", fmt.Errorf("artifact path is required")
	}

	if filepath.IsAbs(rawPath) {
		return "", "", fmt.Errorf("artifact path must be relative to the workspace")
	}

	cleanPath := filepath.Clean(rawPath)
	if cleanPath == "." || cleanPath == ".." || strings.HasPrefix(cleanPath, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("artifact path must stay within the workspace")
	}

	absWorkspace, err := filepath.Abs(workspace)
	if err != nil {
		return "", "", fmt.Errorf("resolve workspace: %w", err)
	}

	sourcePath := filepath.Join(absWorkspace, cleanPath)
	rel, err := filepath.Rel(absWorkspace, sourcePath)
	if err != nil {
		return "", "", fmt.Errorf("resolve artifact path: %w", err)
	}

	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("artifact path must stay within the workspace")
	}

	realWorkspace, err := filepath.EvalSymlinks(absWorkspace)
	if err != nil {
		return "", "", fmt.Errorf("resolve workspace symlinks: %w", err)
	}

	realSource, err := filepath.EvalSymlinks(sourcePath)
	if err != nil {
		return "", "", fmt.Errorf("resolve artifact symlinks: %w", err)
	}

	realRel, err := filepath.Rel(realWorkspace, realSource)
	if err != nil {
		return "", "", fmt.Errorf("resolve artifact path: %w", err)
	}

	if realRel == "." || realRel == ".." || strings.HasPrefix(realRel, ".."+string(filepath.Separator)) {
		return "", "", fmt.Errorf("artifact path must stay within the workspace")
	}

	return realSource, filepath.ToSlash(rel), nil
}

func stringInput(inputs map[string]any, key string) string {
	value, _ := inputs[key].(string)
	return strings.TrimSpace(value)
}

func optionalStringInput(inputs map[string]any, key string) *string {
	value := stringInput(inputs, key)
	if value == "" {
		return nil
	}

	return &value
}

func int64Input(inputs map[string]any, key string) (int64, error) {
	raw := stringInput(inputs, key)
	if raw == "" {
		return 0, nil
	}

	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}

	return value, nil
}
