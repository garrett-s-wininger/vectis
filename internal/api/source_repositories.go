package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"vectis/internal/api/audit"
	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/dal"
	jobvalidation "vectis/internal/job/validation"
	sourcepkg "vectis/internal/source"
)

type sourceRepositoryRequest struct {
	RepositoryID  string `json:"repository_id"`
	Namespace     string `json:"namespace"`
	SourceKind    string `json:"source_kind"`
	CheckoutPath  string `json:"checkout_path"`
	CanonicalURL  string `json:"canonical_url"`
	DefaultRef    string `json:"default_ref"`
	CredentialRef string `json:"credential_ref"`
	Enabled       *bool  `json:"enabled"`
}

type sourceRepositoryUpdateRequest struct {
	SourceKind    *string `json:"source_kind"`
	CheckoutPath  *string `json:"checkout_path"`
	CanonicalURL  *string `json:"canonical_url"`
	DefaultRef    *string `json:"default_ref"`
	CredentialRef *string `json:"credential_ref"`
	Enabled       *bool   `json:"enabled"`
}

type sourceRepositoryResponse struct {
	RepositoryID  string `json:"repository_id"`
	Namespace     string `json:"namespace"`
	SourceKind    string `json:"source_kind"`
	CheckoutPath  string `json:"checkout_path,omitempty"`
	CanonicalURL  string `json:"canonical_url,omitempty"`
	DefaultRef    string `json:"default_ref,omitempty"`
	CredentialRef string `json:"credential_ref,omitempty"`
	Enabled       bool   `json:"enabled"`
}

type sourceRepositoryStatusResponse struct {
	RepositoryID       string                       `json:"repository_id"`
	Namespace          string                       `json:"namespace"`
	SourceKind         string                       `json:"source_kind"`
	Enabled            bool                         `json:"enabled"`
	Status             string                       `json:"status"`
	CheckoutPath       string                       `json:"checkout_path,omitempty"`
	PathExists         bool                         `json:"path_exists"`
	PathIsDirectory    bool                         `json:"path_is_directory"`
	GitRepository      bool                         `json:"git_repository"`
	WorkTreePath       string                       `json:"work_tree_path,omitempty"`
	HeadRef            string                       `json:"head_ref,omitempty"`
	DefaultRef         string                       `json:"default_ref,omitempty"`
	DefaultRefResolved bool                         `json:"default_ref_resolved"`
	ResolvedCommit     string                       `json:"resolved_commit,omitempty"`
	Error              *sourceRepositoryStatusError `json:"error,omitempty"`
}

type sourceRepositoryStatusError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type jobSourceRequest struct {
	Namespace    string `json:"namespace"`
	RepositoryID string `json:"repository_id"`
	Ref          string `json:"ref"`
	Path         string `json:"path"`
}

type sourceDefinitionRequest struct {
	Ref  string `json:"ref"`
	Path string `json:"path"`
}

type sourceProvenanceResponse struct {
	RepositoryID   string `json:"repository_id"`
	RequestedRef   string `json:"requested_ref"`
	ResolvedCommit string `json:"resolved_commit"`
	Path           string `json:"path"`
	BlobSHA        string `json:"blob_sha,omitempty"`
}

type persistedSourceJobResponse struct {
	JobID          string                   `json:"job_id"`
	Version        int                      `json:"version"`
	DefinitionHash string                   `json:"definition_hash"`
	Source         sourceProvenanceResponse `json:"source"`
}

type storedJobSourceResponse struct {
	JobID          string                   `json:"job_id"`
	Version        int                      `json:"version"`
	DefinitionHash string                   `json:"definition_hash"`
	Source         sourceProvenanceResponse `json:"source"`
}

type resolvedSourceDefinitionResponse struct {
	RepositoryID   string                   `json:"repository_id"`
	DefinitionHash string                   `json:"definition_hash"`
	Definition     json.RawMessage          `json:"definition"`
	Source         sourceProvenanceResponse `json:"source"`
}

func (s *APIServer) CreateSourceRepository(w http.ResponseWriter, r *http.Request) {
	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONDocumentBodyBytes))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	var req sourceRepositoryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	req.RepositoryID = strings.TrimSpace(req.RepositoryID)
	req.Namespace = strings.TrimSpace(req.Namespace)
	req.SourceKind = strings.TrimSpace(req.SourceKind)
	req.CheckoutPath = strings.TrimSpace(req.CheckoutPath)
	req.CanonicalURL = strings.TrimSpace(req.CanonicalURL)
	req.DefaultRef = strings.TrimSpace(req.DefaultRef)
	req.CredentialRef = strings.TrimSpace(req.CredentialRef)

	if req.SourceKind == "" {
		req.SourceKind = dal.SourceKindLocalCheckout
	}

	if req.RepositoryID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_repository_id", "repository_id is required", nil)
		return
	}

	if req.SourceKind != dal.SourceKindLocalCheckout {
		writeAPIError(w, http.StatusBadRequest, "unsupported_source_kind", "source_kind is not supported", nil)
		return
	}

	if req.CheckoutPath == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_checkout_path", "checkout_path is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) {
		return
	}

	namespacePath := req.Namespace
	if namespacePath == "" {
		namespacePath = "/"
	}

	ns, err := s.namespaces.GetByPath(ctx, namespacePath)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "namespace_not_found", "namespace not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionJobWrite, ns.Path) {
		return
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	rec, err := s.sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID:  req.RepositoryID,
		NamespaceID:   ns.ID,
		SourceKind:    req.SourceKind,
		CheckoutPath:  req.CheckoutPath,
		CanonicalURL:  req.CanonicalURL,
		DefaultRef:    req.DefaultRef,
		CredentialRef: req.CredentialRef,
		Enabled:       enabled,
	})

	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusConflict, "source_repository_conflict", "source repository conflict", nil)
			return
		}

		s.logger.Error("Database error creating source repository: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventSourceRepositoryCreated, actorID, 0, map[string]any{
		"repository_id": rec.RepositoryID,
		"namespace":     ns.Path,
		"source_kind":   rec.SourceKind,
	})

	writeJSON(w, http.StatusCreated, sourceRepositoryRecordToResponse(rec, ns.Path))
}

func (s *APIServer) ListSourceRepositories(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) {
		return
	}

	namespacePath := strings.TrimSpace(r.URL.Query().Get("namespace"))
	if namespacePath == "" {
		namespacePath = "/"
	}

	ns, err := s.namespaces.GetByPath(ctx, namespacePath)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "namespace_not_found", "namespace not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionJobRead, ns.Path) {
		return
	}

	recs, err := s.sources.ListRepositories(ctx, ns.ID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error listing source repositories: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	resp := make([]sourceRepositoryResponse, 0, len(recs))
	for _, rec := range recs {
		resp = append(resp, sourceRepositoryRecordToResponse(rec, ns.Path))
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *APIServer) GetSourceRepository(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) {
		return
	}

	rec, nsPath, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionJobRead, false)
	if !ok {
		return
	}

	writeJSON(w, http.StatusOK, sourceRepositoryRecordToResponse(rec, nsPath))
}

func (s *APIServer) GetSourceRepositoryStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) {
		return
	}

	rec, nsPath, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionJobRead, false)
	if !ok {
		return
	}

	writeJSON(w, http.StatusOK, sourceRepositoryStatusFromRecord(ctx, rec, nsPath))
}

func (s *APIServer) UpdateSourceRepository(w http.ResponseWriter, r *http.Request) {
	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONDocumentBodyBytes))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	var req sourceRepositoryUpdateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) {
		return
	}

	rec, nsPath, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionJobWrite, false)
	if !ok {
		return
	}

	updated := rec
	if req.SourceKind != nil {
		updated.SourceKind = strings.TrimSpace(*req.SourceKind)
	}

	if req.CheckoutPath != nil {
		updated.CheckoutPath = strings.TrimSpace(*req.CheckoutPath)
	}

	if req.CanonicalURL != nil {
		updated.CanonicalURL = strings.TrimSpace(*req.CanonicalURL)
	}

	if req.DefaultRef != nil {
		updated.DefaultRef = strings.TrimSpace(*req.DefaultRef)
	}

	if req.CredentialRef != nil {
		updated.CredentialRef = strings.TrimSpace(*req.CredentialRef)
	}

	if req.Enabled != nil {
		updated.Enabled = *req.Enabled
	}

	updated, err = s.sources.UpdateRepository(ctx, updated)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_repository_not_found", "source repository not found", nil)
			return
		}

		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusConflict, "source_repository_conflict", "source repository conflict", nil)
			return
		}

		s.logger.Error("Database error updating source repository: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventSourceRepositoryUpdated, actorID, 0, map[string]any{
		"repository_id": updated.RepositoryID,
		"namespace":     nsPath,
		"source_kind":   updated.SourceKind,
		"enabled":       updated.Enabled,
	})

	writeJSON(w, http.StatusOK, sourceRepositoryRecordToResponse(updated, nsPath))
}

func (s *APIServer) ResolveSourceDefinition(w http.ResponseWriter, r *http.Request) {
	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONDocumentBodyBytes))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	var req sourceDefinitionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	req.Ref = strings.TrimSpace(req.Ref)
	req.Path = strings.TrimSpace(req.Path)
	if req.Path == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_path", "path is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) {
		return
	}

	rec, _, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionJobRead, true)
	if !ok {
		return
	}

	ref := req.Ref
	if ref == "" {
		ref = strings.TrimSpace(rec.DefaultRef)
	}

	repo, err := sourcepkg.NewRepositoryFromRecord(rec)
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	loaded, err := sourcepkg.LoadDefinition(ctx, repo, sourcepkg.DefinitionRequest{
		Ref:  ref,
		Path: req.Path,
	})

	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, resolvedSourceDefinitionResponse{
		RepositoryID:   rec.RepositoryID,
		DefinitionHash: dal.DefinitionHash(loaded.DefinitionJSON),
		Definition:     json.RawMessage([]byte(loaded.DefinitionJSON)),
		Source: sourceProvenanceResponse{
			RepositoryID:   rec.RepositoryID,
			RequestedRef:   loaded.Source.RequestedRef,
			ResolvedCommit: loaded.Source.Commit,
			Path:           loaded.Source.Path,
			BlobSHA:        loaded.Source.BlobSHA,
		},
	})
}

func (s *APIServer) CreateJobFromSource(w http.ResponseWriter, r *http.Request) {
	s.persistJobFromSource(w, r, false)
}

func (s *APIServer) UpdateJobFromSource(w http.ResponseWriter, r *http.Request) {
	s.persistJobFromSource(w, r, true)
}

func (s *APIServer) GetJobSource(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireSources(w) {
		return
	}

	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionJobRead, nsPath) {
		writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
		return
	}

	var definitionJSON string
	var version int
	if versionParam := strings.TrimSpace(r.URL.Query().Get("version")); versionParam != "" {
		v, err := strconv.Atoi(versionParam)
		if err != nil || v <= 0 {
			writeAPIError(w, http.StatusBadRequest, "invalid_version", "invalid version parameter", nil)
			return
		}

		definitionJSON, err = s.jobs.GetDefinitionVersion(ctx, jobID, v)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAPIError(w, http.StatusNotFound, "job_version_not_found", "job version not found", nil)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}

		version = v
	} else {
		definitionJSON, version, err = s.jobs.GetDefinition(ctx, jobID)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}
	}
	s.markDBRecovered()

	sourceRec, err := s.sources.GetDefinitionSource(ctx, jobID, version)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_source_not_found", "job source not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error getting job source: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	writeJSON(w, http.StatusOK, storedJobSourceResponse{
		JobID:          jobID,
		Version:        version,
		DefinitionHash: dal.DefinitionHash(definitionJSON),
		Source:         sourceRecordToProvenance(sourceRec),
	})
}

func (s *APIServer) persistJobFromSource(w http.ResponseWriter, r *http.Request, update bool) {
	jobID := strings.TrimSpace(r.PathValue("id"))
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_id", "id is required", nil)
		return
	}

	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONDocumentBodyBytes))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	var req jobSourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	req.RepositoryID = strings.TrimSpace(req.RepositoryID)
	req.Ref = strings.TrimSpace(req.Ref)
	req.Path = strings.TrimSpace(req.Path)
	req.Namespace = strings.TrimSpace(req.Namespace)

	if req.RepositoryID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_repository_id", "repository_id is required", nil)
		return
	}

	if req.Path == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_path", "path is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) || !s.requireSourceJobs(w) {
		return
	}

	var namespaceID int64
	var namespacePath string
	if update {
		namespacePath, err = s.getJobNamespacePath(ctx, jobID)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}
	} else {
		namespacePath = req.Namespace
		if namespacePath == "" {
			namespacePath = "/"
		}

		ns, err := s.namespaces.GetByPath(ctx, namespacePath)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAPIError(w, http.StatusNotFound, "namespace_not_found", "namespace not found", nil)
				return
			}

			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}

		namespaceID = ns.ID
		namespacePath = ns.Path
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionJobWrite, namespacePath) {
		return
	}

	if _, _, ok := s.getAuthorizedSourceRepository(ctx, w, p, req.RepositoryID, authz.ActionJobRead, true); !ok {
		return
	}

	persister := sourcepkg.DefinitionPersister{
		Jobs:    s.sourceJobs,
		Sources: s.sources,
	}

	persistReq := sourcepkg.PersistDefinitionRequest{
		JobID:        jobID,
		NamespaceID:  namespaceID,
		RepositoryID: req.RepositoryID,
		Ref:          req.Ref,
		Path:         req.Path,
	}

	var persisted sourcepkg.PersistedDefinition
	if update {
		persisted, err = persister.UpdateJob(ctx, persistReq)
	} else {
		persisted, err = persister.CreateJob(ctx, persistReq)
	}

	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	event := audit.EventJobCreated
	if update {
		event = audit.EventJobUpdated
	}

	s.auditLog(ctx, event, actorID, 0, map[string]any{
		"job_id":        jobID,
		"namespace":     namespacePath,
		"repository_id": req.RepositoryID,
		"source_ref":    persisted.Definition.Source.RequestedRef,
		"source_path":   persisted.Definition.Source.Path,
	})

	status := http.StatusCreated
	if update {
		status = http.StatusOK
	}

	writeJSON(w, status, persistedSourceJobResponse{
		JobID:          jobID,
		Version:        persisted.Version,
		DefinitionHash: dal.DefinitionHash(persisted.Definition.DefinitionJSON),
		Source: sourceProvenanceResponse{
			RepositoryID:   req.RepositoryID,
			RequestedRef:   persisted.Definition.Source.RequestedRef,
			ResolvedCommit: persisted.Definition.Source.Commit,
			Path:           persisted.Definition.Source.Path,
			BlobSHA:        persisted.Definition.Source.BlobSHA,
		},
	})
}

func (s *APIServer) getAuthorizedSourceRepository(ctx context.Context, w http.ResponseWriter, p *authn.Principal, repositoryID string, action authz.Action, requireEnabled bool) (dal.SourceRepositoryRecord, string, bool) {
	repositoryID = strings.TrimSpace(repositoryID)
	if repositoryID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_repository_id", "repository_id is required", nil)
		return dal.SourceRepositoryRecord{}, "", false
	}

	rec, err := s.sources.GetRepository(ctx, repositoryID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_repository_not_found", "source repository not found", nil)
			return dal.SourceRepositoryRecord{}, "", false
		}

		if s.handleDBUnavailableError(w, err) {
			return dal.SourceRepositoryRecord{}, "", false
		}

		s.logger.Error("Database error getting source repository: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return dal.SourceRepositoryRecord{}, "", false
	}
	s.markDBRecovered()

	ns, err := s.namespaces.GetByID(ctx, rec.NamespaceID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_repository_not_found", "source repository namespace not found", nil)
			return dal.SourceRepositoryRecord{}, "", false
		}

		if s.handleDBUnavailableError(w, err) {
			return dal.SourceRepositoryRecord{}, "", false
		}

		s.logger.Error("Database error getting source repository namespace: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return dal.SourceRepositoryRecord{}, "", false
	}

	if !s.authorizeNamespace(ctx, w, p, action, ns.Path) {
		return dal.SourceRepositoryRecord{}, "", false
	}

	if requireEnabled && !rec.Enabled {
		writeAPIError(w, http.StatusConflict, "source_repository_disabled", "source repository is disabled", nil)
		return dal.SourceRepositoryRecord{}, "", false
	}

	return rec, ns.Path, true
}

func sourceRepositoryRecordToResponse(rec dal.SourceRepositoryRecord, namespacePath string) sourceRepositoryResponse {
	return sourceRepositoryResponse{
		RepositoryID:  rec.RepositoryID,
		Namespace:     namespacePath,
		SourceKind:    rec.SourceKind,
		CheckoutPath:  rec.CheckoutPath,
		CanonicalURL:  rec.CanonicalURL,
		DefaultRef:    rec.DefaultRef,
		CredentialRef: rec.CredentialRef,
		Enabled:       rec.Enabled,
	}
}

func sourceRepositoryStatusFromRecord(ctx context.Context, rec dal.SourceRepositoryRecord, namespacePath string) sourceRepositoryStatusResponse {
	resp := sourceRepositoryStatusResponse{
		RepositoryID: rec.RepositoryID,
		Namespace:    namespacePath,
		SourceKind:   rec.SourceKind,
		Enabled:      rec.Enabled,
		Status:       "ok",
	}

	if !rec.Enabled {
		resp.Status = "disabled"
	}

	switch strings.TrimSpace(rec.SourceKind) {
	case dal.SourceKindLocalCheckout:
		checkoutStatus := sourcepkg.NewGitCheckout(rec.CheckoutPath).Status(ctx, rec.DefaultRef)
		resp.CheckoutPath = checkoutStatus.CheckoutPath
		resp.PathExists = checkoutStatus.PathExists
		resp.PathIsDirectory = checkoutStatus.PathIsDirectory
		resp.GitRepository = checkoutStatus.GitRepository
		resp.WorkTreePath = checkoutStatus.WorkTreePath
		resp.HeadRef = checkoutStatus.HeadRef
		resp.DefaultRef = checkoutStatus.DefaultRef
		resp.DefaultRefResolved = checkoutStatus.DefaultRefResolved
		resp.ResolvedCommit = checkoutStatus.ResolvedCommit

		if checkoutStatus.ErrorCode != "" {
			resp.Error = &sourceRepositoryStatusError{
				Code:    checkoutStatus.ErrorCode,
				Message: checkoutStatus.ErrorMessage,
			}

			if rec.Enabled {
				resp.Status = "unavailable"
			}
		}
	default:
		resp.Error = &sourceRepositoryStatusError{
			Code:    "unsupported_source_kind",
			Message: "source kind is not supported",
		}

		if rec.Enabled {
			resp.Status = "unavailable"
		}
	}

	return resp
}

func sourceRecordToProvenance(rec dal.JobDefinitionSourceRecord) sourceProvenanceResponse {
	return sourceProvenanceResponse{
		RepositoryID:   rec.RepositoryID,
		RequestedRef:   rec.RequestedRef,
		ResolvedCommit: rec.ResolvedCommit,
		Path:           rec.DefinitionPath,
		BlobSHA:        rec.BlobSHA,
	}
}

func (s *APIServer) definitionSourceProvenance(ctx context.Context, jobID string, version int) (*sourceProvenanceResponse, error) {
	sources, err := s.definitionSourceProvenanceByVersion(ctx, jobID, []int{version})
	if err != nil {
		return nil, err
	}

	source, ok := sources[version]
	if !ok {
		return nil, nil
	}

	return &source, nil
}

func (s *APIServer) definitionSourceProvenanceByVersion(ctx context.Context, jobID string, versions []int) (map[int]sourceProvenanceResponse, error) {
	out := map[int]sourceProvenanceResponse{}
	if s.sources == nil || strings.TrimSpace(jobID) == "" || len(versions) == 0 {
		return out, nil
	}

	records, err := s.sources.GetDefinitionSources(ctx, jobID, versions)
	if err != nil {
		return nil, err
	}

	for version, rec := range records {
		out[version] = sourceRecordToProvenance(rec)
	}

	return out, nil
}

func (s *APIServer) writeSourceDefinitionError(w http.ResponseWriter, err error) {
	if s.handleDBUnavailableError(w, err) {
		return
	}

	switch {
	case dal.IsConflict(err):
		writeAPIError(w, http.StatusConflict, "source_job_conflict", "source job conflict", nil)
	case dal.IsNotFound(err):
		writeAPIError(w, http.StatusNotFound, "source_repository_not_found", "source repository not found", nil)
	case errors.Is(err, sourcepkg.ErrInvalidDefinition):
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", jobvalidation.ErrorDetails(err))
	case errors.Is(err, sourcepkg.ErrInvalidReference):
		writeAPIError(w, http.StatusBadRequest, "invalid_source_reference", "invalid source reference", nil)
	case errors.Is(err, sourcepkg.ErrTooLarge):
		writeAPIError(w, http.StatusRequestEntityTooLarge, "source_file_too_large", "source file too large", nil)
	case errors.Is(err, sourcepkg.ErrNotFound):
		writeAPIError(w, http.StatusNotFound, "source_not_found", "source not found", nil)
	default:
		s.logger.Error("Source definition operation failed: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
	}
}
