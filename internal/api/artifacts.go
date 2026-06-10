package api

import (
	"context"
	"encoding/json"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"

	"vectis/internal/api/authz"
	artifactsvc "vectis/internal/artifact"
	"vectis/internal/dal"
)

type artifactResponse struct {
	ID              int64            `json:"id"`
	RunID           string           `json:"run_id"`
	TaskID          *string          `json:"task_id,omitempty"`
	TaskAttemptID   *string          `json:"task_attempt_id,omitempty"`
	ExecutionID     *string          `json:"execution_id,omitempty"`
	CellID          string           `json:"cell_id"`
	Name            string           `json:"name"`
	Path            string           `json:"path"`
	ContentType     string           `json:"content_type,omitempty"`
	BlobKey         string           `json:"blob_key"`
	BlobAlgorithm   string           `json:"blob_algorithm"`
	BlobDigest      string           `json:"blob_digest"`
	SizeBytes       int64            `json:"size_bytes"`
	ArtifactShardID string           `json:"artifact_shard_id"`
	Metadata        *json.RawMessage `json:"metadata,omitempty"`
	CreatedAt       int64            `json:"created_at"`
	UpdatedAt       int64            `json:"updated_at"`
}

func (s *APIServer) ListRunArtifacts(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	if !s.requireArtifacts(w) {
		return
	}

	params := parsePageParams(r)
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	if !s.authorizeRunArtifactRead(ctx, w, r, runID) {
		return
	}

	records, nextCursor, err := s.artifacts.ListByRun(ctx, runID, params.Cursor, params.Limit)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error listing artifacts for run %s: %v", runID, err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return
	}
	s.markDBRecovered()

	rows := make([]artifactResponse, 0, len(records))
	for _, rec := range records {
		rows = append(rows, artifactRecordResponse(rec))
	}

	writeJSON(w, http.StatusOK, buildPaginatedResponse(rows, nextCursor))
}

func (s *APIServer) GetRunArtifact(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	name := r.PathValue("name")
	if strings.TrimSpace(name) == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_name", "name is required", nil)
		return
	}

	if !s.requireArtifacts(w) {
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	if !s.authorizeRunArtifactRead(ctx, w, r, runID) {
		return
	}

	rec, ok := s.getRunArtifact(ctx, w, runID, name)
	if !ok {
		return
	}

	writeJSON(w, http.StatusOK, artifactRecordResponse(rec))
}

func (s *APIServer) DownloadRunArtifact(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	name := r.PathValue("name")
	if strings.TrimSpace(name) == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_name", "name is required", nil)
		return
	}

	if !s.requireArtifacts(w) {
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	if !s.authorizeRunArtifactRead(ctx, w, r, runID) {
		cancel()
		return
	}

	rec, ok := s.getRunArtifact(ctx, w, runID, name)
	cancel()
	if !ok {
		return
	}

	reader, err := artifactsvc.NewReaderForArtifact(
		r.Context(),
		rec,
		artifactsvc.DefaultReaderOptions(s.logger, s.retryMetrics),
	)
	if err != nil {
		s.logger.Warn("Failed to create artifact reader for run %s artifact %s: %v", runID, name, err)
		writeAPIError(w, http.StatusBadGateway, "artifact_service_error", "failed to connect to artifact service", nil)
		return
	}
	defer reader.Close()

	desc, err := reader.StatBlob(r.Context(), rec.BlobKey)
	if err != nil {
		s.logger.Warn("Failed to stat artifact blob for run %s artifact %s: %v", runID, name, err)
		writeAPIError(w, http.StatusBadGateway, "artifact_blob_unavailable", "artifact blob unavailable", nil)
		return
	}

	if !artifactDescriptorMatchesRecord(desc, rec) {
		s.logger.Error("Artifact manifest/blob mismatch for run %s artifact %s: manifest digest=%s size=%d blob digest=%s size=%d",
			runID, name, rec.BlobDigest, rec.SizeBytes, desc.Digest, desc.Size)
		writeAPIError(w, http.StatusBadGateway, "artifact_blob_mismatch", "artifact blob does not match manifest", nil)
		return
	}

	contentType := strings.TrimSpace(rec.ContentType)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Content-Length", strconv.FormatInt(rec.SizeBytes, 10))
	h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{
		"filename": artifactDownloadFilename(rec),
	}))
	h.Set("X-Content-Type-Options", "nosniff")
	setNoStore(w)
	w.WriteHeader(http.StatusOK)

	written, err := reader.WriteBlob(r.Context(), rec.BlobKey, w)
	if err != nil {
		s.logger.Warn("Artifact download interrupted for run %s artifact %s: %v", runID, name, err)
		return
	}

	if !artifactDescriptorMatchesRecord(written, rec) {
		s.logger.Error("Artifact stream descriptor mismatch for run %s artifact %s: manifest digest=%s size=%d stream digest=%s size=%d",
			runID, name, rec.BlobDigest, rec.SizeBytes, written.Digest, written.Size)
	}
}

func (s *APIServer) requireArtifacts(w http.ResponseWriter) bool {
	if s.artifacts == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "artifacts_not_configured", "artifact repository not configured", nil)
		return false
	}
	return true
}

func (s *APIServer) authorizeRunArtifactRead(ctx context.Context, w http.ResponseWriter, r *http.Request, runID string) bool {
	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return false
	}

	nsPath, err := s.getRunJobNamespacePath(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return false
		}

		if s.handleDBUnavailableError(w, err) {
			return false
		}

		s.logger.Error("Database error resolving run namespace for artifacts: %v", err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return false
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionRunRead, nsPath) {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return false
	}

	return true
}

func (s *APIServer) getRunArtifact(ctx context.Context, w http.ResponseWriter, runID, name string) (dal.ArtifactRecord, bool) {
	rec, err := s.artifacts.GetByRunAndName(ctx, runID, name)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "artifact_not_found", "artifact not found", nil)
			return dal.ArtifactRecord{}, false
		}

		if s.handleDBUnavailableError(w, err) {
			return dal.ArtifactRecord{}, false
		}

		s.logger.Error("Database error loading artifact %s for run %s: %v", name, runID, err)
		writeAPIError(w, http.StatusInternalServerError, "internal_error", "internal server error", nil)
		return dal.ArtifactRecord{}, false
	}
	s.markDBRecovered()

	return rec, true
}

func artifactRecordResponse(rec dal.ArtifactRecord) artifactResponse {
	return artifactResponse{
		ID:              rec.ID,
		RunID:           rec.RunID,
		TaskID:          rec.TaskID,
		TaskAttemptID:   rec.TaskAttemptID,
		ExecutionID:     rec.ExecutionID,
		CellID:          rec.CellID,
		Name:            rec.Name,
		Path:            rec.Path,
		ContentType:     rec.ContentType,
		BlobKey:         rec.BlobKey,
		BlobAlgorithm:   rec.BlobAlgorithm,
		BlobDigest:      rec.BlobDigest,
		SizeBytes:       rec.SizeBytes,
		ArtifactShardID: rec.ArtifactShardID,
		Metadata:        artifactMetadataResponse(rec.MetadataJSON),
		CreatedAt:       rec.CreatedAt,
		UpdatedAt:       rec.UpdatedAt,
	}
}

func artifactMetadataResponse(metadata *string) *json.RawMessage {
	if metadata == nil {
		return nil
	}

	raw := json.RawMessage(*metadata)
	return &raw
}

func artifactDescriptorMatchesRecord(desc artifactsvc.BlobDescriptor, rec dal.ArtifactRecord) bool {
	return desc.Algorithm == rec.BlobAlgorithm && desc.Digest == rec.BlobDigest && desc.Size == rec.SizeBytes
}

func artifactDownloadFilename(rec dal.ArtifactRecord) string {
	name := strings.TrimSpace(rec.Path)
	if name == "" {
		name = strings.TrimSpace(rec.Name)
	}

	name = strings.ReplaceAll(name, "\\", "/")
	name = strings.TrimSpace(path.Base(name))
	if name == "" || name == "." || name == "/" {
		name = strings.TrimSpace(rec.Name)
	}

	if name == "" {
		return "artifact"
	}

	return name
}
