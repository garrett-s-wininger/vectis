package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/api/audit"
	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/config"
	"vectis/internal/dal"
	jobvalidation "vectis/internal/job/validation"
	"vectis/internal/observability"
	sourcepkg "vectis/internal/source"
	"vectis/internal/utils"
)

type sourceRepositoryRequest struct {
	RepositoryID  string `json:"repository_id"`
	Namespace     string `json:"namespace"`
	SourceKind    string `json:"source_kind"`
	CheckoutPath  string `json:"checkout_path"`
	CheckoutMode  string `json:"checkout_mode"`
	AuthoringMode string `json:"authoring_mode"`
	CanonicalURL  string `json:"canonical_url"`
	DefaultRef    string `json:"default_ref"`
	CredentialRef string `json:"credential_ref"`
	Enabled       *bool  `json:"enabled"`
}

type sourceRepositoryUpdateRequest struct {
	SourceKind    *string `json:"source_kind"`
	CheckoutPath  *string `json:"checkout_path"`
	CheckoutMode  *string `json:"checkout_mode"`
	AuthoringMode *string `json:"authoring_mode"`
	CanonicalURL  *string `json:"canonical_url"`
	DefaultRef    *string `json:"default_ref"`
	CredentialRef *string `json:"credential_ref"`
	Enabled       *bool   `json:"enabled"`
}

type sourceRepositoryResponse struct {
	RepositoryID  string                       `json:"repository_id"`
	Namespace     string                       `json:"namespace"`
	SourceKind    string                       `json:"source_kind"`
	CheckoutPath  string                       `json:"checkout_path,omitempty"`
	CheckoutMode  string                       `json:"checkout_mode"`
	AuthoringMode string                       `json:"authoring_mode"`
	Authoring     sourceRepositoryAuthoring    `json:"authoring"`
	CanonicalURL  string                       `json:"canonical_url,omitempty"`
	DefaultRef    string                       `json:"default_ref,omitempty"`
	CredentialRef string                       `json:"credential_ref,omitempty"`
	Enabled       bool                         `json:"enabled"`
	Sync          sourceRepositorySyncResponse `json:"sync"`
}

type sourceRepositorySyncResponse struct {
	Status             string `json:"status"`
	LastStartedAtUnix  int64  `json:"last_started_at_unix,omitempty"`
	LastFinishedAtUnix int64  `json:"last_finished_at_unix,omitempty"`
	Ref                string `json:"ref,omitempty"`
	Commit             string `json:"commit,omitempty"`
	Error              string `json:"error,omitempty"`
}

type sourceRepositoryStatusResponse struct {
	RepositoryID       string                       `json:"repository_id"`
	Namespace          string                       `json:"namespace"`
	SourceKind         string                       `json:"source_kind"`
	Enabled            bool                         `json:"enabled"`
	Status             string                       `json:"status"`
	CheckoutMode       string                       `json:"checkout_mode"`
	AuthoringMode      string                       `json:"authoring_mode"`
	Authoring          sourceRepositoryAuthoring    `json:"authoring"`
	CheckoutPath       string                       `json:"checkout_path,omitempty"`
	PathExists         bool                         `json:"path_exists"`
	PathIsDirectory    bool                         `json:"path_is_directory"`
	GitRepository      bool                         `json:"git_repository"`
	WorkTreePath       string                       `json:"work_tree_path,omitempty"`
	HeadRef            string                       `json:"head_ref,omitempty"`
	DefaultRef         string                       `json:"default_ref,omitempty"`
	DefaultRefResolved bool                         `json:"default_ref_resolved"`
	ResolvedCommit     string                       `json:"resolved_commit,omitempty"`
	Sync               sourceRepositorySyncResponse `json:"sync"`
	Error              *sourceRepositoryStatusError `json:"error,omitempty"`
}

type sourceRepositoryStatusError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type sourceRepositoryAuthoring struct {
	Mode                   string `json:"mode"`
	WriteDefinitions       bool   `json:"write_definitions"`
	LocalCommits           bool   `json:"local_commits"`
	ExternalChangeRequests bool   `json:"external_change_requests"`
	Reason                 string `json:"reason,omitempty"`
}

type sourceRepositoryBranchResponse struct {
	Name   string `json:"name"`
	Ref    string `json:"ref"`
	Commit string `json:"commit"`
	Remote string `json:"remote,omitempty"`
}

type sourceRepositoryBranchesResponse struct {
	RepositoryID string                           `json:"repository_id"`
	Prefix       string                           `json:"prefix,omitempty"`
	Limit        int                              `json:"limit"`
	Branches     []sourceRepositoryBranchResponse `json:"branches"`
}

type sourceRepositoryTreeEntryResponse struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	Mode      string `json:"mode"`
	ObjectSHA string `json:"object_sha"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

type sourceRepositoryTreeResponse struct {
	RepositoryID   string                              `json:"repository_id"`
	RequestedRef   string                              `json:"requested_ref"`
	ResolvedCommit string                              `json:"resolved_commit"`
	Path           string                              `json:"path,omitempty"`
	Recursive      bool                                `json:"recursive"`
	Limit          int                                 `json:"limit"`
	Entries        []sourceRepositoryTreeEntryResponse `json:"entries"`
}

type sourceRepositoryDefinitionFileResponse struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	BlobSHA   string `json:"blob_sha"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

type sourceRepositoryDefinitionsResponse struct {
	RepositoryID   string                                   `json:"repository_id"`
	RequestedRef   string                                   `json:"requested_ref"`
	ResolvedCommit string                                   `json:"resolved_commit"`
	Path           string                                   `json:"path"`
	Limit          int                                      `json:"limit"`
	Definitions    []sourceRepositoryDefinitionFileResponse `json:"definitions"`
}

type sourceRepositoryJobResponse struct {
	JobID     string                   `json:"job_id"`
	Path      string                   `json:"path"`
	Name      string                   `json:"name"`
	BlobSHA   string                   `json:"blob_sha"`
	SizeBytes int64                    `json:"size_bytes,omitempty"`
	Source    sourceProvenanceResponse `json:"source"`
}

type invalidSourceRepositoryJobResponse struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	BlobSHA   string `json:"blob_sha"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
	Error     string `json:"error"`
}

type sourceRepositoryJobsResponse struct {
	RepositoryID   string                               `json:"repository_id"`
	RequestedRef   string                               `json:"requested_ref"`
	ResolvedCommit string                               `json:"resolved_commit"`
	Path           string                               `json:"path"`
	Limit          int                                  `json:"limit"`
	Jobs           []sourceRepositoryJobResponse        `json:"jobs"`
	Invalid        []invalidSourceRepositoryJobResponse `json:"invalid,omitempty"`
}

type sourceCronScheduleResponse struct {
	ScheduleID   string `json:"schedule_id"`
	RepositoryID string `json:"repository_id"`
	Namespace    string `json:"namespace"`
	JobID        string `json:"job_id"`
	CronSpec     string `json:"cron_spec"`
	NextRunAt    string `json:"next_run_at"`
	Ref          string `json:"ref,omitempty"`
	Path         string `json:"path,omitempty"`
	PathDerived  bool   `json:"path_derived"`
	Enabled      bool   `json:"enabled"`
}

type sourceCronSchedulesResponse struct {
	Namespace    string                       `json:"namespace"`
	RepositoryID string                       `json:"repository_id,omitempty"`
	Schedules    []sourceCronScheduleResponse `json:"schedules"`
}

type sourceRepositoryJobDefinitionResponse struct {
	JobID          string                   `json:"job_id"`
	DefinitionHash string                   `json:"definition_hash"`
	Definition     json.RawMessage          `json:"definition"`
	Source         sourceProvenanceResponse `json:"source"`
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

type sourceRepositoryJobDefinitionWriteRequest struct {
	Ref          string          `json:"ref"`
	Branch       string          `json:"branch"`
	Path         string          `json:"path"`
	Message      string          `json:"message"`
	ExpectedHead string          `json:"expected_head"`
	Definition   json.RawMessage `json:"definition"`
}

type sourceDefinitionsImportRequest struct {
	Ref            string `json:"ref"`
	Path           string `json:"path"`
	Limit          int    `json:"limit"`
	DryRun         bool   `json:"dry_run"`
	UpdateExisting bool   `json:"update_existing"`
}

type sourceJobTriggerRequest struct {
	Ref          string `json:"ref"`
	Path         string `json:"path"`
	CellID       string `json:"cell_id"`
	TargetCellID string `json:"target_cell_id"`
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

type importedSourceDefinitionResponse struct {
	JobID          string                    `json:"job_id,omitempty"`
	Status         string                    `json:"status"`
	Version        int                       `json:"version,omitempty"`
	DefinitionHash string                    `json:"definition_hash,omitempty"`
	Error          string                    `json:"error,omitempty"`
	Source         *sourceProvenanceResponse `json:"source,omitempty"`
}

type importedSourceDefinitionsSummary struct {
	Total       int `json:"total"`
	Created     int `json:"created"`
	Updated     int `json:"updated"`
	Unchanged   int `json:"unchanged"`
	WouldCreate int `json:"would_create"`
	WouldUpdate int `json:"would_update"`
	Conflicted  int `json:"conflicted"`
	Invalid     int `json:"invalid"`
}

type importedSourceDefinitionsResponse struct {
	RepositoryID   string                             `json:"repository_id"`
	RequestedRef   string                             `json:"requested_ref"`
	ResolvedCommit string                             `json:"resolved_commit"`
	Path           string                             `json:"path"`
	Limit          int                                `json:"limit"`
	DryRun         bool                               `json:"dry_run"`
	UpdateExisting bool                               `json:"update_existing"`
	Summary        importedSourceDefinitionsSummary   `json:"summary"`
	Results        []importedSourceDefinitionResponse `json:"results"`
}

type sourceJobTriggerResponse struct {
	JobID             string                   `json:"job_id"`
	RunID             string                   `json:"run_id"`
	RunIndex          int                      `json:"run_index"`
	DefinitionVersion int                      `json:"definition_version"`
	DefinitionHash    string                   `json:"definition_hash"`
	Source            sourceProvenanceResponse `json:"source"`
}

type storedJobSourceResponse struct {
	JobID          string                   `json:"job_id"`
	Version        int                      `json:"version"`
	DefinitionHash string                   `json:"definition_hash"`
	Source         sourceProvenanceResponse `json:"source"`
}

type storedJobSourceDefinitionResponse struct {
	JobID          string                   `json:"job_id"`
	Version        int                      `json:"version"`
	DefinitionHash string                   `json:"definition_hash"`
	Definition     json.RawMessage          `json:"definition"`
	Source         sourceProvenanceResponse `json:"source"`
}

type resolvedSourceDefinitionResponse struct {
	RepositoryID   string                   `json:"repository_id"`
	DefinitionHash string                   `json:"definition_hash"`
	Definition     json.RawMessage          `json:"definition"`
	Source         sourceProvenanceResponse `json:"source"`
}

type storedJobDefinitionSource struct {
	JobID          string
	Version        int
	DefinitionJSON string
	Source         dal.JobDefinitionSourceRecord
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
	req.CheckoutMode = strings.TrimSpace(req.CheckoutMode)
	req.AuthoringMode = strings.TrimSpace(req.AuthoringMode)
	req.CanonicalURL = strings.TrimSpace(req.CanonicalURL)
	req.DefaultRef = strings.TrimSpace(req.DefaultRef)
	req.CredentialRef = strings.TrimSpace(req.CredentialRef)

	if req.SourceKind == "" {
		req.SourceKind = dal.SourceKindLocalCheckout
	}

	if req.CheckoutMode == "" {
		req.CheckoutMode = dal.SourceCheckoutModeExternal
	}

	if req.AuthoringMode == "" {
		req.AuthoringMode = dal.SourceAuthoringModeReadOnly
	}

	if req.RepositoryID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_repository_id", "repository_id is required", nil)
		return
	}

	if req.SourceKind != dal.SourceKindLocalCheckout {
		writeAPIError(w, http.StatusBadRequest, "unsupported_source_kind", "source_kind is not supported", nil)
		return
	}

	if !validSourceCheckoutMode(req.CheckoutMode) {
		writeAPIError(w, http.StatusBadRequest, "unsupported_checkout_mode", "checkout_mode is not supported", nil)
		return
	}

	if !validSourceAuthoringMode(req.AuthoringMode) {
		writeAPIError(w, http.StatusBadRequest, "unsupported_authoring_mode", "authoring_mode is not supported", nil)
		return
	}

	if !sourceAuthoringModeCompatible(req.AuthoringMode, req.CheckoutMode) {
		writeAPIError(w, http.StatusBadRequest, "incompatible_authoring_mode", "authoring_mode is not compatible with checkout_mode", nil)
		return
	}

	if req.CheckoutPath == "" {
		if req.CheckoutMode != dal.SourceCheckoutModeManaged {
			writeAPIError(w, http.StatusBadRequest, "missing_checkout_path", "checkout_path is required", nil)
			return
		}

		checkoutPath, err := managedSourceCheckoutPath(req.RepositoryID)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_repository_id", "repository_id cannot be mapped to a managed checkout path", nil)
			return
		}

		req.CheckoutPath = checkoutPath
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
		CheckoutMode:  req.CheckoutMode,
		AuthoringMode: req.AuthoringMode,
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

	writeJSON(w, http.StatusCreated, s.sourceRepositoryRecordToResponse(rec, ns.Path))
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
		resp = append(resp, s.sourceRepositoryRecordToResponse(rec, ns.Path))
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

	writeJSON(w, http.StatusOK, s.sourceRepositoryRecordToResponse(rec, nsPath))
}

func (s *APIServer) ListSourceSchedules(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSchedules(w) {
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

		s.logger.Error("Database error getting namespace for source schedules: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionJobRead, ns.Path) {
		return
	}

	recs, err := s.schedules.ListSourceCronSchedules(ctx, ns.ID, "")
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error listing source schedules: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	writeJSON(w, http.StatusOK, sourceCronSchedulesResponse{
		Namespace: ns.Path,
		Schedules: sourceCronScheduleRecordsToResponse(recs, ns.Path),
	})
}

func (s *APIServer) ListSourceRepositorySchedules(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) || !s.requireSchedules(w) {
		return
	}

	rec, nsPath, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionJobRead, false)
	if !ok {
		return
	}

	recs, err := s.schedules.ListSourceCronSchedules(ctx, rec.NamespaceID, rec.RepositoryID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error listing source repository schedules: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	writeJSON(w, http.StatusOK, sourceCronSchedulesResponse{
		Namespace:    nsPath,
		RepositoryID: rec.RepositoryID,
		Schedules:    sourceCronScheduleRecordsToResponse(recs, nsPath),
	})
}

func (s *APIServer) DeleteSourceRepository(w http.ResponseWriter, r *http.Request) {
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

	if err := s.sources.DeleteRepository(ctx, rec.RepositoryID); err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_repository_not_found", "source repository not found", nil)
			return
		}

		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusConflict, "source_repository_in_use", "source repository has recorded definition provenance", nil)
			return
		}

		s.logger.Error("Database error deleting source repository: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventSourceRepositoryDeleted, actorID, 0, map[string]any{
		"repository_id": rec.RepositoryID,
		"namespace":     nsPath,
		"source_kind":   rec.SourceKind,
	})

	w.WriteHeader(http.StatusNoContent)
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

	writeJSON(w, http.StatusOK, s.sourceRepositoryStatusFromRecord(ctx, rec, nsPath))
}

func (s *APIServer) ListSourceRepositoryBranches(w http.ResponseWriter, r *http.Request) {
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

	limit := sourceRepositoryBranchListLimit(r)
	prefix := strings.TrimSpace(r.URL.Query().Get("prefix"))
	checkout := newGitCheckoutForSourceRepository(rec)
	branches, err := checkout.ListBranches(ctx, sourcepkg.ListBranchesOptions{
		Prefix: prefix,
		Limit:  limit,
	})
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	respBranches := make([]sourceRepositoryBranchResponse, 0, len(branches))
	for _, branch := range branches {
		respBranches = append(respBranches, sourceRepositoryBranchResponse{
			Name:   branch.Name,
			Ref:    branch.Ref,
			Commit: branch.Commit,
			Remote: branch.Remote,
		})
	}

	writeJSON(w, http.StatusOK, sourceRepositoryBranchesResponse{
		RepositoryID: rec.RepositoryID,
		Prefix:       prefix,
		Limit:        limit,
		Branches:     respBranches,
	})
}

func (s *APIServer) ListSourceRepositoryTree(w http.ResponseWriter, r *http.Request) {
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

	recursive, ok := sourceRepositoryTreeRecursive(w, r)
	if !ok {
		return
	}

	ref := strings.TrimSpace(r.URL.Query().Get("ref"))
	if ref == "" {
		ref = strings.TrimSpace(rec.DefaultRef)
	}
	if ref == "" {
		ref = "HEAD"
	}

	limit := sourceRepositoryTreeListLimit(r)
	checkout := newGitCheckoutForSourceRepository(rec)
	listing, err := checkout.ListTree(ctx, sourcepkg.ListTreeOptions{
		Ref:       ref,
		Path:      r.URL.Query().Get("path"),
		Recursive: recursive,
		Limit:     limit,
	})
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	respEntries := make([]sourceRepositoryTreeEntryResponse, 0, len(listing.Entries))
	for _, entry := range listing.Entries {
		respEntries = append(respEntries, sourceRepositoryTreeEntryResponse{
			Path:      entry.Path,
			Name:      entry.Name,
			Type:      entry.Type,
			Mode:      entry.Mode,
			ObjectSHA: entry.ObjectSHA,
			SizeBytes: entry.SizeBytes,
		})
	}

	writeJSON(w, http.StatusOK, sourceRepositoryTreeResponse{
		RepositoryID:   rec.RepositoryID,
		RequestedRef:   listing.RequestedRef,
		ResolvedCommit: listing.Revision.Commit,
		Path:           listing.Path,
		Recursive:      listing.Recursive,
		Limit:          limit,
		Entries:        respEntries,
	})
}

func (s *APIServer) ListSourceRepositoryDefinitions(w http.ResponseWriter, r *http.Request) {
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

	ref := strings.TrimSpace(r.URL.Query().Get("ref"))
	if ref == "" {
		ref = strings.TrimSpace(rec.DefaultRef)
	}
	if ref == "" {
		ref = "HEAD"
	}

	limit := sourceRepositoryTreeListLimit(r)
	checkout := newGitCheckoutForSourceRepository(rec)
	listing, err := checkout.ListDefinitionFiles(ctx, sourcepkg.ListDefinitionFilesOptions{
		Ref:   ref,
		Path:  r.URL.Query().Get("path"),
		Limit: limit,
	})
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	respFiles := make([]sourceRepositoryDefinitionFileResponse, 0, len(listing.Files))
	for _, file := range listing.Files {
		respFiles = append(respFiles, sourceRepositoryDefinitionFileResponse{
			Path:      file.Path,
			Name:      file.Name,
			BlobSHA:   file.BlobSHA,
			SizeBytes: file.SizeBytes,
		})
	}

	writeJSON(w, http.StatusOK, sourceRepositoryDefinitionsResponse{
		RepositoryID:   rec.RepositoryID,
		RequestedRef:   listing.RequestedRef,
		ResolvedCommit: listing.Revision.Commit,
		Path:           listing.Path,
		Limit:          limit,
		Definitions:    respFiles,
	})
}

func (s *APIServer) ListSourceRepositoryJobs(w http.ResponseWriter, r *http.Request) {
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

	ref := strings.TrimSpace(r.URL.Query().Get("ref"))
	if ref == "" {
		ref = strings.TrimSpace(rec.DefaultRef)
	}
	if ref == "" {
		ref = "HEAD"
	}

	limit := sourceRepositoryTreeListLimit(r)
	checkout := newGitCheckoutForSourceRepository(rec)
	listing, err := checkout.ListDefinitionFiles(ctx, sourcepkg.ListDefinitionFilesOptions{
		Ref:   ref,
		Path:  r.URL.Query().Get("path"),
		Limit: limit,
	})
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	jobs := make([]sourceRepositoryJobResponse, 0, len(listing.Files))
	invalid := make([]invalidSourceRepositoryJobResponse, 0)
	seenJobIDs := make(map[string]string, len(listing.Files))
	for _, file := range listing.Files {
		jobID, err := sourceImportJobIDFromPath(listing.Path, file.Path)
		if err != nil {
			invalid = append(invalid, invalidSourceRepositoryJobResponse{
				Path:      file.Path,
				Name:      file.Name,
				BlobSHA:   file.BlobSHA,
				SizeBytes: file.SizeBytes,
				Error:     err.Error(),
			})
			continue
		}

		if previous, ok := seenJobIDs[jobID]; ok {
			invalid = append(invalid, invalidSourceRepositoryJobResponse{
				Path:      file.Path,
				Name:      file.Name,
				BlobSHA:   file.BlobSHA,
				SizeBytes: file.SizeBytes,
				Error:     "duplicate derived job_id " + jobID + " from " + previous,
			})
			continue
		}
		seenJobIDs[jobID] = file.Path

		source := sourceProvenanceResponse{
			RepositoryID:   rec.RepositoryID,
			RequestedRef:   listing.RequestedRef,
			ResolvedCommit: listing.Revision.Commit,
			Path:           file.Path,
			BlobSHA:        file.BlobSHA,
		}
		jobs = append(jobs, sourceRepositoryJobResponse{
			JobID:     jobID,
			Path:      file.Path,
			Name:      file.Name,
			BlobSHA:   file.BlobSHA,
			SizeBytes: file.SizeBytes,
			Source:    source,
		})
	}

	writeJSON(w, http.StatusOK, sourceRepositoryJobsResponse{
		RepositoryID:   rec.RepositoryID,
		RequestedRef:   listing.RequestedRef,
		ResolvedCommit: listing.Revision.Commit,
		Path:           listing.Path,
		Limit:          limit,
		Jobs:           jobs,
		Invalid:        invalid,
	})
}

func (s *APIServer) GetSourceRepositoryJobDefinition(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_job_id", "job_id is required", nil)
		return
	}

	definitionPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if definitionPath == "" {
		var err error
		definitionPath, err = sourceTriggerDefinitionPath(jobID)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_job_id", "job_id cannot be mapped to a source definition path", nil)
			return
		}
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

	ref := strings.TrimSpace(r.URL.Query().Get("ref"))
	if ref == "" {
		ref = strings.TrimSpace(rec.DefaultRef)
	}
	if ref == "" {
		ref = "HEAD"
	}

	repo, err := sourcepkg.NewRepositoryFromRecord(rec)
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	loaded, err := sourcepkg.LoadDefinition(ctx, repo, sourcepkg.DefinitionRequest{
		Ref:  ref,
		Path: definitionPath,
	})
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, sourceRepositoryJobDefinitionResponse{
		JobID:          jobID,
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

func (s *APIServer) PutSourceRepositoryJobDefinition(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_job_id", "job_id is required", nil)
		return
	}

	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJobDefinitionBodyBytes))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	var req sourceRepositoryJobDefinitionWriteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}
	if req.Definition == nil {
		req.Definition = bytes.TrimSpace(body)
	}

	var job api.Job
	if err := json.Unmarshal(req.Definition, &job); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", nil)
		return
	}

	if err := jobvalidation.ValidateJob(&job, jobvalidation.Options{}); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", jobvalidation.ErrorDetails(err))
		return
	}

	definitionJSON, err := json.Marshal(&job)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "invalid job definition", nil)
		return
	}

	definitionPath := strings.TrimSpace(req.Path)
	if definitionPath == "" {
		definitionPath, err = sourceTriggerDefinitionPath(jobID)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_job_id", "job_id cannot be mapped to a source definition path", nil)
			return
		}
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

	rec, nsPath, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionJobWrite, true)
	if !ok {
		return
	}

	author, err := s.newSourceDefinitionAuthor(rec)
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	written, err := author.WriteDefinition(ctx, sourcepkg.WriteDefinitionRequest{
		Ref:            req.Ref,
		Branch:         req.Branch,
		Path:           definitionPath,
		DefinitionJSON: string(definitionJSON),
		Message:        req.Message,
		ExpectedHead:   req.ExpectedHead,
	})
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventJobUpdated, actorID, 0, map[string]any{
		"job_id":        jobID,
		"namespace":     nsPath,
		"repository_id": rec.RepositoryID,
		"source_ref":    written.RequestedRef,
		"source_path":   written.Path,
		"source_commit": written.Commit,
	})

	writeJSON(w, http.StatusOK, sourceRepositoryJobDefinitionResponse{
		JobID:          jobID,
		DefinitionHash: dal.DefinitionHash(string(definitionJSON)),
		Definition:     json.RawMessage(definitionJSON),
		Source: sourceProvenanceResponse{
			RepositoryID:   rec.RepositoryID,
			RequestedRef:   written.RequestedRef,
			ResolvedCommit: written.Commit,
			Path:           written.Path,
			BlobSHA:        written.BlobSHA,
		},
	})
}

func (s *APIServer) ImportSourceRepositoryDefinitions(w http.ResponseWriter, r *http.Request) {
	if !s.requireStoredJobs(w) {
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

	var req sourceDefinitionsImportRequest
	if len(strings.TrimSpace(string(body))) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
			return
		}
	}

	req.Ref = strings.TrimSpace(req.Ref)
	req.Path = strings.TrimSpace(req.Path)

	ctx, cancel := s.handlerDBCtx(r)
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) || !s.requireSourceJobs(w) {
		return
	}

	rec, nsPath, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionJobWrite, true)
	if !ok {
		return
	}

	ref := req.Ref
	if ref == "" {
		ref = strings.TrimSpace(rec.DefaultRef)
	}

	if ref == "" {
		ref = "HEAD"
	}

	limit := sourceRepositoryImportDefinitionsLimit(req.Limit)
	checkout := newGitCheckoutForSourceRepository(rec)
	listing, err := checkout.ListDefinitionFiles(ctx, sourcepkg.ListDefinitionFilesOptions{
		Ref:   ref,
		Path:  req.Path,
		Limit: limit,
	})

	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	seenJobIDs := make(map[string]string, len(listing.Files))
	results := make([]importedSourceDefinitionResponse, 0, len(listing.Files))
	summary := importedSourceDefinitionsSummary{Total: len(listing.Files)}
	for _, file := range listing.Files {
		result, err := s.importSourceDefinitionFile(ctx, rec, nsPath, checkout, listing, file, req, seenJobIDs, actorID)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Source definition import failed: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}

		incrementSourceImportSummary(&summary, result.Status)
		results = append(results, result)
	}

	writeJSON(w, http.StatusOK, importedSourceDefinitionsResponse{
		RepositoryID:   rec.RepositoryID,
		RequestedRef:   listing.RequestedRef,
		ResolvedCommit: listing.Revision.Commit,
		Path:           listing.Path,
		Limit:          limit,
		DryRun:         req.DryRun,
		UpdateExisting: req.UpdateExisting,
		Summary:        summary,
		Results:        results,
	})
}

func (s *APIServer) TriggerSourceRepositoryJob(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxJobDefinitionBodyBytes))
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, "request_read_failed", "failed to read request body", nil)
		return
	}

	if len(bytes.TrimSpace(body)) > 0 && !requestContentTypeIsJSON(r) {
		writeAPIError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "content type must be application/json", nil)
		return
	}

	var req sourceJobTriggerRequest
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_request_body", "invalid request body", nil)
			return
		}
	}

	jobID := strings.TrimSpace(r.PathValue("job_id"))
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_job_id", "job_id is required", nil)
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

	sourceRunStarter, ok := s.ephemeralRuns.(dal.SourceDefinitionRunStarter)
	if !ok {
		writeAPIError(w, http.StatusServiceUnavailable, "source_run_starter_not_configured", "source-backed run persistence not configured", nil)
		return
	}

	rec, nsPath, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionRunTrigger, true)
	if !ok {
		return
	}

	ref := strings.TrimSpace(req.Ref)
	if ref == "" {
		ref = strings.TrimSpace(rec.DefaultRef)
	}
	if ref == "" {
		ref = "HEAD"
	}

	definitionPath := strings.TrimSpace(req.Path)
	if definitionPath == "" {
		definitionPath, err = sourceTriggerDefinitionPath(jobID)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_job_id", "job_id cannot be mapped to a source definition path", nil)
			return
		}
	}

	idempotencyKey := idempotencyKeyFromRequest(r)
	idempotencyScope := principalIdempotencyScope("source-trigger:"+rec.RepositoryID+":"+jobID, p)
	idempotencyHash := hashIdempotencyRequest(http.MethodPost, "/api/v1/source-repositories/"+rec.RepositoryID+"/jobs/"+jobID+"/trigger", string(bytes.TrimSpace(body)))
	idempotencyRecord, idempotencyReserved, ok := s.reserveIdempotency(w, ctx, idempotencyScope, idempotencyKey, idempotencyHash)
	if !ok {
		return
	}

	if idempotencyRecord.ResponseJSON != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, *idempotencyRecord.ResponseJSON)
		return
	}

	checkout := newGitCheckoutForSourceRepository(rec)
	loaded, err := sourcepkg.LoadDefinition(ctx, checkout, sourcepkg.DefinitionRequest{
		Ref:  ref,
		Path: definitionPath,
	})
	if err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}
		s.writeSourceDefinitionError(w, err)
		return
	}

	targetCellID := runTargetOptions{CellID: req.CellID, TargetCellID: req.TargetCellID}.targetCellID()
	definitionHash := dal.DefinitionHash(loaded.DefinitionJSON)
	triggerPayload := string(bytes.TrimSpace(body))
	invocationID, err := s.recordTriggerInvocation(ctx, jobID, dal.TriggerTypeManual, triggerPayload, []string{targetCellID})
	if err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error recording source trigger invocation: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	sourceRec := dal.JobDefinitionSourceRecord{
		JobID:          jobID,
		RepositoryID:   rec.RepositoryID,
		RequestedRef:   loaded.Source.RequestedRef,
		ResolvedCommit: loaded.Source.Commit,
		DefinitionPath: loaded.Source.Path,
		BlobSHA:        loaded.Source.BlobSHA,
	}

	runID, runIndex, definitionVersion, err := sourceRunStarter.CreateSourceDefinitionAndRunInCellWithAudit(ctx, jobID, loaded.DefinitionJSON, sourceRec, targetCellID, dal.RunAuditMetadata{TriggerInvocationID: invocationID})
	if err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error creating source-backed run: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	loaded.Job.Id = &jobID
	loaded.Job.RunId = &runID

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventRunTriggered, actorID, 0, map[string]any{
		"job_id":             jobID,
		"run_id":             runID,
		"run_index":          runIndex,
		"namespace":          nsPath,
		"repository_id":      rec.RepositoryID,
		"source_ref":         loaded.Source.RequestedRef,
		"source_path":        loaded.Source.Path,
		"definition_version": definitionVersion,
		"invocation":         invocationID,
	})

	s.runBroadcaster.Broadcast(sourceRepositoryRunBroadcastKey(rec.RepositoryID, jobID), runID, runIndex)
	s.recordDispatchEvent(ctx, runID, dal.DispatchSourceAPI, dal.DispatchEventAccepted, targetCellID, nil)
	s.recordAPIEnqueueMetric(ctx, observability.APIEnqueueRunKindSource, observability.APIEnqueueOutcomeAccepted)

	resp := sourceJobTriggerResponse{
		JobID:             jobID,
		RunID:             runID,
		RunIndex:          runIndex,
		DefinitionVersion: definitionVersion,
		DefinitionHash:    definitionHash,
		Source:            sourceRecordToProvenance(sourceRec),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(resp); err != nil {
		s.logger.Error("Failed to encode source trigger response: %v", err)
		return
	}

	_, _ = w.Write(buf.Bytes())
	s.completeIdempotency(ctx, idempotencyScope, idempotencyKey, buf.Bytes())

	bgCtx := detachedTraceContextFromRequest(r)
	go s.finishRunJobEnqueueWithKind(bgCtx, observability.APIEnqueueRunKindSource, jobID, runID, loaded.Job, definitionHash)
}

func (s *APIServer) GetSourceRepositoryJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_job_id", "job_id is required", nil)
		return
	}

	opts, ok := parseRunListRequestOptions(w, r)
	if !ok {
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

	runLister, ok := s.runs.(dal.SourceRepositoryRunLister)
	if !ok {
		writeAPIError(w, http.StatusServiceUnavailable, "source_run_lister_not_configured", "source-backed run listing not configured", nil)
		return
	}

	rec, _, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionRunRead, false)
	if !ok {
		return
	}

	runRows, nextCursor, err := runLister.ListBySourceRepositoryJob(ctx, rec.RepositoryID, jobID, opts.afterIndex, opts.since, opts.owningCell, opts.cursor, opts.limit)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error listing source repository job runs: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	s.writeJobRunsResponse(w, ctx, jobID, runRows, nextCursor)
}

func (s *APIServer) GetSourceRepositoryJobRunLogs(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_job_id", "job_id is required", nil)
		return
	}

	runID := strings.TrimSpace(r.PathValue("run_id"))
	if runID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_run_id", "run_id is required", nil)
		return
	}

	replay, ok := parseLogReplayRequest(w, r)
	if !ok {
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

	rec, _, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionRunRead, false)
	if !ok {
		return
	}

	runRec, err := s.runs.GetRun(ctx, runID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error getting source repository job run: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if runRec.JobID != jobID {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return
	}

	source, err := s.sources.GetDefinitionSource(ctx, runRec.JobID, runRec.DefinitionVersion)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
			return
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error getting source repository job run source: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if source.RepositoryID != rec.RepositoryID {
		writeAPIError(w, http.StatusNotFound, "run_not_found", "run not found", nil)
		return
	}
	s.markDBRecovered()

	s.streamRunLogs(w, r, runID, replay)
}

func (s *APIServer) HandleSSESourceRepositoryJobRuns(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimSpace(r.PathValue("job_id"))
	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_job_id", "job_id is required", nil)
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

	rec, _, ok := s.getAuthorizedSourceRepository(ctx, w, p, r.PathValue("id"), authz.ActionRunRead, false)
	if !ok {
		return
	}

	subscriptionKey := sourceRepositoryRunBroadcastKey(rec.RepositoryID, jobID)
	s.streamRunEvents(w, r, subscriptionKey, "source repository "+rec.RepositoryID+" job "+jobID)
}

func (s *APIServer) broadcastSourceRepositoryRunEvent(ctx context.Context, jobID string, definitionVersion int, runID string, runIndex int) {
	if s.sources == nil {
		return
	}

	source, err := s.sources.GetDefinitionSource(ctx, jobID, definitionVersion)
	if err != nil {
		if !dal.IsNotFound(err) {
			s.logger.Error("Failed to resolve source repository run broadcast target for job %s version %d: %v", jobID, definitionVersion, err)
		}
		return
	}

	s.runBroadcaster.Broadcast(sourceRepositoryRunBroadcastKey(source.RepositoryID, jobID), runID, runIndex)
}

func (s *APIServer) SyncSourceRepository(w http.ResponseWriter, r *http.Request) {
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

	syncRef := sourceRepositorySyncRef(rec)
	releaseSync, syncStarted := s.tryBeginSourceRepositorySync(rec.RepositoryID)
	if !syncStarted {
		s.writeRunningSourceRepositorySync(w, rec, nsPath, syncRef)
		return
	}
	defer releaseSync()

	startedAt := time.Now().Unix()
	running, began, err := s.sources.BeginRepositorySync(ctx, dal.SourceRepositorySyncRecord{
		RepositoryID:           rec.RepositoryID,
		StartedAtUnix:          startedAt,
		Ref:                    syncRef,
		RunningStaleBeforeUnix: sourceSyncStaleBeforeUnix(startedAt),
	})
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

		s.logger.Error("Database error updating source repository sync status: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	if !began {
		s.writeRunningSourceRepositorySync(w, running, nsPath, syncRef)
		return
	}

	syncRecord := dal.SourceRepositorySyncRecord{
		RepositoryID:  rec.RepositoryID,
		StartedAtUnix: startedAt,
		Ref:           syncRef,
	}

	switch strings.TrimSpace(rec.SourceKind) {
	case dal.SourceKindLocalCheckout:
		checkoutStatus := s.sourceRepositorySyncCheckoutStatus(ctx, rec, syncRef)
		if checkoutStatus.ErrorCode != "" {
			syncRecord.Status = dal.SourceSyncStatusFailed
			syncRecord.Error = sourceRepositoryStatusSyncError(checkoutStatus)
		} else {
			syncRecord.Status = dal.SourceSyncStatusSucceeded
			syncRecord.Commit = checkoutStatus.ResolvedCommit
		}
	default:
		syncRecord.Status = dal.SourceSyncStatusFailed
		syncRecord.Error = "unsupported_source_kind: source kind is not supported"
	}

	syncRecord.FinishedAtUnix = time.Now().Unix()
	updated, err := s.sources.UpdateRepositorySync(ctx, syncRecord)
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

		s.logger.Error("Database error updating source repository sync result: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	writeJSON(w, http.StatusOK, s.sourceRepositoryRecordToResponse(updated, nsPath))
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

	if req.CheckoutMode != nil {
		updated.CheckoutMode = strings.TrimSpace(*req.CheckoutMode)
		if updated.CheckoutMode == "" {
			updated.CheckoutMode = dal.SourceCheckoutModeExternal
		}
	}

	if req.AuthoringMode != nil {
		updated.AuthoringMode = strings.TrimSpace(*req.AuthoringMode)
		if updated.AuthoringMode == "" {
			updated.AuthoringMode = dal.SourceAuthoringModeReadOnly
		}
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

	if !validSourceCheckoutMode(updated.CheckoutMode) {
		writeAPIError(w, http.StatusBadRequest, "unsupported_checkout_mode", "checkout_mode is not supported", nil)
		return
	}

	if !validSourceAuthoringMode(updated.AuthoringMode) {
		writeAPIError(w, http.StatusBadRequest, "unsupported_authoring_mode", "authoring_mode is not supported", nil)
		return
	}

	if !sourceAuthoringModeCompatible(updated.AuthoringMode, updated.CheckoutMode) {
		writeAPIError(w, http.StatusBadRequest, "incompatible_authoring_mode", "authoring_mode is not compatible with checkout_mode", nil)
		return
	}

	if updated.CheckoutPath == "" {
		if updated.CheckoutMode != dal.SourceCheckoutModeManaged {
			writeAPIError(w, http.StatusBadRequest, "missing_checkout_path", "checkout_path is required", nil)
			return
		}

		checkoutPath, err := managedSourceCheckoutPath(updated.RepositoryID)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_repository_id", "repository_id cannot be mapped to a managed checkout path", nil)
			return
		}

		updated.CheckoutPath = checkoutPath
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

	writeJSON(w, http.StatusOK, s.sourceRepositoryRecordToResponse(updated, nsPath))
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
	if !s.requireStoredJobs(w) {
		return
	}

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

	source, ok := s.getAuthorizedJobDefinitionSource(ctx, w, p, jobID, r.URL.Query().Get("version"))
	if !ok {
		return
	}

	writeJSON(w, http.StatusOK, storedJobSourceResponse{
		JobID:          source.JobID,
		Version:        source.Version,
		DefinitionHash: dal.DefinitionHash(source.DefinitionJSON),
		Source:         sourceRecordToProvenance(source.Source),
	})
}

func (s *APIServer) GetJobSourceDefinition(w http.ResponseWriter, r *http.Request) {
	if !s.requireStoredJobs(w) {
		return
	}

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

	source, ok := s.getAuthorizedJobDefinitionSource(ctx, w, p, jobID, r.URL.Query().Get("version"))
	if !ok {
		return
	}

	repoRec, err := s.sources.GetRepository(ctx, source.Source.RepositoryID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_repository_not_found", "source repository not found", nil)
			return
		}

		s.logger.Error("Database error getting source repository: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	repo, err := sourcepkg.NewRepositoryFromRecord(repoRec)
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	file, err := repo.ReadFile(ctx, sourcepkg.Revision{Commit: source.Source.ResolvedCommit}, source.Source.DefinitionPath)
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	if !json.Valid(file.Content) {
		writeAPIError(w, http.StatusBadRequest, "invalid_job_definition", "source definition is not valid JSON", nil)
		return
	}

	writeJSON(w, http.StatusOK, storedJobSourceDefinitionResponse{
		JobID:          source.JobID,
		Version:        source.Version,
		DefinitionHash: dal.DefinitionHash(source.DefinitionJSON),
		Definition:     json.RawMessage(file.Content),
		Source:         sourceRecordToProvenance(source.Source),
	})
}

func (s *APIServer) persistJobFromSource(w http.ResponseWriter, r *http.Request, update bool) {
	if !s.requireStoredJobs(w) {
		return
	}

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

func (s *APIServer) sourceRepositoryRecordToResponse(rec dal.SourceRepositoryRecord, namespacePath string) sourceRepositoryResponse {
	return sourceRepositoryResponse{
		RepositoryID:  rec.RepositoryID,
		Namespace:     namespacePath,
		SourceKind:    rec.SourceKind,
		CheckoutPath:  rec.CheckoutPath,
		CheckoutMode:  rec.CheckoutMode,
		AuthoringMode: rec.AuthoringMode,
		Authoring:     s.sourceRepositoryAuthoringFromRecord(rec),
		CanonicalURL:  rec.CanonicalURL,
		DefaultRef:    rec.DefaultRef,
		CredentialRef: rec.CredentialRef,
		Enabled:       rec.Enabled,
		Sync:          sourceRepositorySyncRecordToResponse(rec),
	}
}

func sourceCronScheduleRecordsToResponse(recs []dal.CronScheduleRecord, namespacePath string) []sourceCronScheduleResponse {
	resp := make([]sourceCronScheduleResponse, 0, len(recs))
	for _, rec := range recs {
		resp = append(resp, sourceCronScheduleRecordToResponse(rec, namespacePath))
	}

	return resp
}

func sourceCronScheduleRecordToResponse(rec dal.CronScheduleRecord, namespacePath string) sourceCronScheduleResponse {
	definitionPath := strings.TrimSpace(rec.SourcePath)
	pathDerived := false
	if definitionPath == "" {
		if path, err := sourcepkg.DefinitionPathForJobID(rec.JobID); err == nil {
			definitionPath = path
			pathDerived = true
		}
	}

	return sourceCronScheduleResponse{
		ScheduleID:   rec.ScheduleID,
		RepositoryID: rec.SourceRepositoryID,
		Namespace:    namespacePath,
		JobID:        rec.JobID,
		CronSpec:     rec.CronSpec,
		NextRunAt:    rec.NextRunAt.UTC().Format(time.RFC3339),
		Ref:          rec.SourceRef,
		Path:         definitionPath,
		PathDerived:  pathDerived,
		Enabled:      rec.Enabled,
	}
}

func (s *APIServer) sourceRepositoryStatusFromRecord(ctx context.Context, rec dal.SourceRepositoryRecord, namespacePath string) sourceRepositoryStatusResponse {
	resp := sourceRepositoryStatusResponse{
		RepositoryID:  rec.RepositoryID,
		Namespace:     namespacePath,
		SourceKind:    rec.SourceKind,
		Enabled:       rec.Enabled,
		Status:        "ok",
		CheckoutMode:  rec.CheckoutMode,
		AuthoringMode: rec.AuthoringMode,
		Authoring:     s.sourceRepositoryAuthoringFromRecord(rec),
		Sync:          sourceRepositorySyncRecordToResponse(rec),
	}

	if !rec.Enabled {
		resp.Status = "disabled"
	}

	switch strings.TrimSpace(rec.SourceKind) {
	case dal.SourceKindLocalCheckout:
		checkoutStatus := newGitCheckoutForSourceRepository(rec).Status(ctx, rec.DefaultRef)
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

func (s *APIServer) sourceRepositoryAuthoringFromRecord(rec dal.SourceRepositoryRecord) sourceRepositoryAuthoring {
	resolve := s.sourceAuthoring
	if resolve == nil {
		resolve = sourcepkg.AuthoringCapabilityFromRecord
	}

	capability := resolve(rec)
	mode := strings.TrimSpace(capability.Mode)
	if mode == "" {
		mode = dal.SourceAuthoringModeReadOnly
	}

	return sourceRepositoryAuthoring{
		Mode:                   mode,
		WriteDefinitions:       capability.WriteDefinitions,
		LocalCommits:           capability.LocalCommits,
		ExternalChangeRequests: capability.ExternalChangeRequests,
		Reason:                 capability.Reason,
	}
}

func sourceRepositorySyncRecordToResponse(rec dal.SourceRepositoryRecord) sourceRepositorySyncResponse {
	status := strings.TrimSpace(rec.SyncStatus)
	if status == "" {
		status = dal.SourceSyncStatusNever
	}

	return sourceRepositorySyncResponse{
		Status:             status,
		LastStartedAtUnix:  rec.LastSyncStartedAtUnix,
		LastFinishedAtUnix: rec.LastSyncFinishedAtUnix,
		Ref:                rec.LastSyncRef,
		Commit:             rec.LastSyncCommit,
		Error:              rec.LastSyncError,
	}
}

func sourceRepositorySyncRef(rec dal.SourceRepositoryRecord) string {
	ref := strings.TrimSpace(rec.DefaultRef)
	if ref == "" {
		return "HEAD"
	}

	return ref
}

func sourceRepositoryStatusSyncError(status sourcepkg.GitCheckoutStatus) string {
	if status.ErrorCode == "" {
		return ""
	}

	if status.ErrorMessage == "" {
		return status.ErrorCode
	}

	return status.ErrorCode + ": " + status.ErrorMessage
}

func (s *APIServer) writeRunningSourceRepositorySync(w http.ResponseWriter, rec dal.SourceRepositoryRecord, namespacePath, syncRef string) {
	running := rec
	running.SyncStatus = dal.SourceSyncStatusRunning
	if strings.TrimSpace(running.LastSyncRef) == "" {
		running.LastSyncRef = syncRef
	}

	if running.LastSyncStartedAtUnix == 0 {
		running.LastSyncStartedAtUnix = time.Now().Unix()
	}

	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusAccepted, s.sourceRepositoryRecordToResponse(running, namespacePath))
}

func sourceSyncStaleBeforeUnix(nowUnix int64) int64 {
	timeout := config.SourceSyncRunningTimeout()
	if timeout <= 0 {
		return 0
	}

	return time.Unix(nowUnix, 0).Add(-timeout).Unix()
}

func sourceRepositoryBranchListLimit(r *http.Request) int {
	limit := sourcepkg.DefaultBranchListLimit
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = min(parsed, maxPageLimit)
		}
	}

	return limit
}

func sourceRepositoryTreeListLimit(r *http.Request) int {
	limit := sourcepkg.DefaultTreeListLimit
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = min(parsed, maxPageLimit)
		}
	}

	return limit
}

func sourceRepositoryTreeRecursive(w http.ResponseWriter, r *http.Request) (bool, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get("recursive"))
	if raw == "" {
		return false, true
	}

	recursive, err := strconv.ParseBool(raw)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_recursive", "recursive must be a boolean", nil)
		return false, false
	}

	return recursive, true
}

func sourceRepositoryImportDefinitionsLimit(limit int) int {
	if limit <= 0 {
		return sourcepkg.DefaultTreeListLimit
	}

	return min(limit, maxPageLimit)
}

func (s *APIServer) importSourceDefinitionFile(
	ctx context.Context,
	rec dal.SourceRepositoryRecord,
	namespacePath string,
	checkout *sourcepkg.GitCheckout,
	listing sourcepkg.DefinitionFileListing,
	file sourcepkg.DefinitionFile,
	req sourceDefinitionsImportRequest,
	seenJobIDs map[string]string,
	actorID int64,
) (importedSourceDefinitionResponse, error) {
	result := importedSourceDefinitionResponse{
		Source: &sourceProvenanceResponse{
			RepositoryID:   rec.RepositoryID,
			RequestedRef:   listing.RequestedRef,
			ResolvedCommit: listing.Revision.Commit,
			Path:           file.Path,
			BlobSHA:        file.BlobSHA,
		},
	}

	jobID, err := sourceImportJobIDFromPath(listing.Path, file.Path)
	if err != nil {
		result.Status = "invalid"
		result.Error = err.Error()
		return result, nil
	}
	result.JobID = jobID

	if previous, ok := seenJobIDs[jobID]; ok {
		result.Status = "invalid"
		result.Error = "duplicate derived job_id " + jobID + " from " + previous
		return result, nil
	}
	seenJobIDs[jobID] = file.Path

	loaded, err := sourcepkg.LoadDefinition(ctx, checkout, sourcepkg.DefinitionRequest{
		Ref:  listing.Revision.Commit,
		Path: file.Path,
	})
	if err != nil {
		result.Status = "invalid"
		result.Error = err.Error()
		return result, nil
	}

	definitionHash := dal.DefinitionHash(loaded.DefinitionJSON)
	result.DefinitionHash = definitionHash
	result.Source.BlobSHA = loaded.Source.BlobSHA

	sourceRec := dal.JobDefinitionSourceRecord{
		JobID:          jobID,
		RepositoryID:   rec.RepositoryID,
		RequestedRef:   listing.RequestedRef,
		ResolvedCommit: listing.Revision.Commit,
		DefinitionPath: loaded.Source.Path,
		BlobSHA:        loaded.Source.BlobSHA,
	}

	currentJSON, currentVersion, err := s.jobs.GetDefinition(ctx, jobID)
	switch {
	case err == nil:
		s.markDBRecovered()
		namespaceID, err := s.jobs.GetNamespaceID(ctx, jobID)
		if err != nil {
			return result, err
		}
		s.markDBRecovered()

		if namespaceID != rec.NamespaceID {
			result.Status = "conflict"
			result.Version = currentVersion
			result.Error = "job already exists in another namespace"
			return result, nil
		}

		result.Version = currentVersion
		if dal.DefinitionHash(currentJSON) == definitionHash {
			result.Status = "unchanged"
			return result, nil
		}

		if !req.UpdateExisting {
			result.Status = "conflict"
			result.Error = "job already exists; set update_existing to update it"
			return result, nil
		}

		if req.DryRun {
			result.Status = "would_update"
			result.Version = currentVersion + 1
			return result, nil
		}

		version, err := s.sourceJobs.UpdateDefinitionWithSource(ctx, jobID, loaded.DefinitionJSON, sourceRec)
		if err != nil {
			if dal.IsConflict(err) {
				result.Status = "conflict"
				result.Error = err.Error()
				return result, nil
			}
			return result, err
		}
		s.markDBRecovered()
		result.Status = "updated"
		result.Version = version
		s.auditLog(ctx, audit.EventJobUpdated, actorID, 0, map[string]any{
			"job_id":        jobID,
			"namespace":     namespacePath,
			"repository_id": rec.RepositoryID,
			"source_ref":    listing.RequestedRef,
			"source_path":   file.Path,
		})
		return result, nil
	case dal.IsNotFound(err):
		if req.DryRun {
			result.Status = "would_create"
			result.Version = 1
			return result, nil
		}

		version, err := s.sourceJobs.CreateWithSource(ctx, jobID, loaded.DefinitionJSON, rec.NamespaceID, sourceRec)
		if err != nil {
			if dal.IsConflict(err) {
				result.Status = "conflict"
				result.Error = err.Error()
				return result, nil
			}
			return result, err
		}
		s.markDBRecovered()
		result.Status = "created"
		result.Version = version
		s.auditLog(ctx, audit.EventJobCreated, actorID, 0, map[string]any{
			"job_id":        jobID,
			"namespace":     namespacePath,
			"repository_id": rec.RepositoryID,
			"source_ref":    listing.RequestedRef,
			"source_path":   file.Path,
		})
		return result, nil
	default:
		return result, err
	}
}

func sourceImportJobIDFromPath(rootPath, filePath string) (string, error) {
	rootPath = strings.Trim(path.Clean(strings.TrimSpace(rootPath)), "/")
	filePath = strings.Trim(path.Clean(strings.TrimSpace(filePath)), "/")
	if rootPath == "" || filePath == "" || filePath == "." {
		return "", errors.New("definition path cannot derive a job_id")
	}

	prefix := rootPath + "/"
	if !strings.HasPrefix(filePath, prefix) {
		return "", errors.New("definition path is outside import root")
	}

	rel := strings.TrimPrefix(filePath, prefix)
	if !strings.HasSuffix(rel, ".json") {
		return "", errors.New("definition path must end in .json")
	}

	rel = strings.TrimSuffix(rel, ".json")
	if rel == "" || rel == "." {
		return "", errors.New("definition path cannot derive a job_id")
	}

	parts := strings.Split(rel, "/")
	for _, part := range parts {
		if !validSourceImportJobIDPart(part) {
			return "", errors.New("definition path contains an unsupported job_id segment")
		}
	}

	jobID := strings.Join(parts, ".")
	if jobID == "" || strings.Contains(jobID, "..") {
		return "", errors.New("definition path cannot derive a safe job_id")
	}

	return jobID, nil
}

func sourceTriggerDefinitionPath(jobID string) (string, error) {
	return sourcepkg.DefinitionPathForJobID(jobID)
}

func validSourceImportJobIDPart(part string) bool {
	if part == "" || part == "." || part == ".." {
		return false
	}

	for _, r := range part {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '-' ||
			r == '_' ||
			r == '.' {
			continue
		}

		return false
	}

	return true
}

func incrementSourceImportSummary(summary *importedSourceDefinitionsSummary, status string) {
	switch status {
	case "created":
		summary.Created++
	case "updated":
		summary.Updated++
	case "unchanged":
		summary.Unchanged++
	case "would_create":
		summary.WouldCreate++
	case "would_update":
		summary.WouldUpdate++
	case "conflict":
		summary.Conflicted++
	case "invalid":
		summary.Invalid++
	}
}

func (s *APIServer) tryBeginSourceRepositorySync(repositoryID string) (func(), bool) {
	repositoryID = strings.TrimSpace(repositoryID)
	if repositoryID == "" {
		return func() {}, true
	}

	s.sourceSyncMu.Lock()
	defer s.sourceSyncMu.Unlock()

	if s.sourceSyncRunning == nil {
		s.sourceSyncRunning = make(map[string]struct{})
	}

	if _, ok := s.sourceSyncRunning[repositoryID]; ok {
		return nil, false
	}

	s.sourceSyncRunning[repositoryID] = struct{}{}
	return func() {
		s.sourceSyncMu.Lock()
		defer s.sourceSyncMu.Unlock()
		delete(s.sourceSyncRunning, repositoryID)
	}, true
}

func (s *APIServer) sourceRepositorySyncCheckoutStatus(ctx context.Context, rec dal.SourceRepositoryRecord, syncRef string) sourcepkg.GitCheckoutStatus {
	if s.sourceSyncCheckoutStatus != nil {
		return s.sourceSyncCheckoutStatus(ctx, rec, syncRef)
	}

	if strings.TrimSpace(rec.CheckoutMode) == dal.SourceCheckoutModeManaged {
		return sourcepkg.SyncManagedGitCheckout(ctx, sourcepkg.ManagedGitCheckoutRequest{
			CheckoutPath: rec.CheckoutPath,
			RemoteURL:    rec.CanonicalURL,
			DefaultRef:   syncRef,
		})
	}

	return sourcepkg.NewGitCheckout(rec.CheckoutPath).Status(ctx, syncRef)
}

func (s *APIServer) newSourceDefinitionAuthor(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionAuthor, error) {
	factory := s.sourceDefinitionAuthor
	if factory == nil {
		factory = sourcepkg.NewDefinitionAuthorFromRecord
	}

	return factory(rec)
}

func managedSourceCheckoutPath(repositoryID string) (string, error) {
	store, err := sourcepkg.NewCheckoutStore(config.SourceCheckoutRoot(utils.DataHome()))
	if err != nil {
		return "", err
	}

	return store.Path(repositoryID)
}

func newGitCheckoutForSourceRepository(rec dal.SourceRepositoryRecord) *sourcepkg.GitCheckout {
	if strings.TrimSpace(rec.CheckoutMode) == dal.SourceCheckoutModeManaged {
		return sourcepkg.NewManagedGitCheckout(rec.CheckoutPath)
	}

	return sourcepkg.NewGitCheckout(rec.CheckoutPath)
}

func validSourceCheckoutMode(mode string) bool {
	switch strings.TrimSpace(mode) {
	case dal.SourceCheckoutModeExternal, dal.SourceCheckoutModeManaged:
		return true
	default:
		return false
	}
}

func validSourceAuthoringMode(mode string) bool {
	switch strings.TrimSpace(mode) {
	case dal.SourceAuthoringModeReadOnly, dal.SourceAuthoringModeLocalCommit, dal.SourceAuthoringModeExternalChangeRequest:
		return true
	default:
		return false
	}
}

func sourceAuthoringModeCompatible(authoringMode, checkoutMode string) bool {
	if strings.TrimSpace(authoringMode) != dal.SourceAuthoringModeLocalCommit {
		return true
	}

	return strings.TrimSpace(checkoutMode) == dal.SourceCheckoutModeManaged
}

func (s *APIServer) getAuthorizedJobDefinitionSource(ctx context.Context, w http.ResponseWriter, p *authn.Principal, jobID string, versionParam string) (storedJobDefinitionSource, bool) {
	nsPath, err := s.getJobNamespacePath(ctx, jobID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
			return storedJobDefinitionSource{}, false
		}

		if s.handleDBUnavailableError(w, err) {
			return storedJobDefinitionSource{}, false
		}

		s.logger.Error("Database error: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return storedJobDefinitionSource{}, false
	}

	if !s.checkNamespaceAuth(ctx, p, authz.ActionJobRead, nsPath) {
		writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
		return storedJobDefinitionSource{}, false
	}

	var definitionJSON string
	var version int
	if versionParam = strings.TrimSpace(versionParam); versionParam != "" {
		v, err := strconv.Atoi(versionParam)
		if err != nil || v <= 0 {
			writeAPIError(w, http.StatusBadRequest, "invalid_version", "invalid version parameter", nil)
			return storedJobDefinitionSource{}, false
		}

		definitionJSON, err = s.jobs.GetDefinitionVersion(ctx, jobID, v)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAPIError(w, http.StatusNotFound, "job_version_not_found", "job version not found", nil)
				return storedJobDefinitionSource{}, false
			}

			if s.handleDBUnavailableError(w, err) {
				return storedJobDefinitionSource{}, false
			}

			s.logger.Error("Database error: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return storedJobDefinitionSource{}, false
		}

		version = v
	} else {
		definitionJSON, version, err = s.jobs.GetDefinition(ctx, jobID)
		if err != nil {
			if dal.IsNotFound(err) {
				writeAPIError(w, http.StatusNotFound, "job_not_found", "job not found", nil)
				return storedJobDefinitionSource{}, false
			}

			if s.handleDBUnavailableError(w, err) {
				return storedJobDefinitionSource{}, false
			}

			s.logger.Error("Database error: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return storedJobDefinitionSource{}, false
		}
	}
	s.markDBRecovered()

	sourceRec, err := s.sources.GetDefinitionSource(ctx, jobID, version)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "job_source_not_found", "job source not found", nil)
			return storedJobDefinitionSource{}, false
		}

		if s.handleDBUnavailableError(w, err) {
			return storedJobDefinitionSource{}, false
		}

		s.logger.Error("Database error getting job source: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return storedJobDefinitionSource{}, false
	}
	s.markDBRecovered()

	return storedJobDefinitionSource{
		JobID:          jobID,
		Version:        version,
		DefinitionJSON: definitionJSON,
		Source:         sourceRec,
	}, true
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
	case errors.Is(err, sourcepkg.ErrAuthoringUnavailable):
		writeAPIError(w, http.StatusConflict, "source_authoring_unavailable", "source repository does not support local definition authoring", nil)
	case errors.Is(err, sourcepkg.ErrConflict):
		writeAPIError(w, http.StatusConflict, "source_conflict", "source conflict", nil)
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
