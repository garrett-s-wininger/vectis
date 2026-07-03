package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
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
	"vectis/internal/platform"
	sourcepkg "vectis/internal/source"
	"vectis/internal/source/refspec"
)

const (
	defaultSourceRefAvailabilityTTL            = 30 * time.Second
	defaultSourceRefMissTTL                    = 5 * time.Second
	defaultSourceRefHydrationLeaseTTL          = 30 * time.Second
	defaultSourceRefHydrationLeaseWait         = 2 * time.Second
	defaultSourceRefHydrationLeasePollInterval = 100 * time.Millisecond
	sourceRefHydrationRetryAfterSeconds        = 1
	sourceRefHydrationCoalescedTier            = "coalesced"
	sourceRefHydrationInFlightErrorCode        = "source_ref_hydration_in_flight"
	sourceRefHydrationNotFoundErrorCode        = "source_ref_not_found"
)

type sourceRefHydrationPendingError struct {
	repositoryID string
	ref          string
}

func (e sourceRefHydrationPendingError) Error() string {
	return "source ref hydration is in flight"
}

type sourceRepositoryRequest struct {
	RepositoryID            string   `json:"repository_id"`
	Namespace               string   `json:"namespace"`
	SourceKind              string   `json:"source_kind"`
	CheckoutPath            string   `json:"checkout_path"`
	CheckoutMode            string   `json:"checkout_mode"`
	AuthoringMode           string   `json:"authoring_mode"`
	WorkerCacheMode         string   `json:"worker_cache_mode"`
	CanonicalURL            string   `json:"canonical_url"`
	FallbackRemoteURLs      []string `json:"fallback_remote_urls,omitempty"`
	WorkerCacheWarmRefspecs []string `json:"worker_cache_warm_refspecs,omitempty"`
	DefaultRef              string   `json:"default_ref"`
	CredentialRef           string   `json:"credential_ref"`
	Enabled                 *bool    `json:"enabled"`
}

type sourceRepositoryUpdateRequest struct {
	SourceKind              *string   `json:"source_kind"`
	CheckoutPath            *string   `json:"checkout_path"`
	CheckoutMode            *string   `json:"checkout_mode"`
	AuthoringMode           *string   `json:"authoring_mode"`
	WorkerCacheMode         *string   `json:"worker_cache_mode"`
	CanonicalURL            *string   `json:"canonical_url"`
	FallbackRemoteURLs      *[]string `json:"fallback_remote_urls"`
	WorkerCacheWarmRefspecs *[]string `json:"worker_cache_warm_refspecs"`
	DefaultRef              *string   `json:"default_ref"`
	CredentialRef           *string   `json:"credential_ref"`
	Enabled                 *bool     `json:"enabled"`
}

type sourceRepositoryResponse struct {
	RepositoryID            string                       `json:"repository_id"`
	Namespace               string                       `json:"namespace"`
	SourceKind              string                       `json:"source_kind"`
	CheckoutPath            string                       `json:"checkout_path,omitempty"`
	CheckoutMode            string                       `json:"checkout_mode"`
	AuthoringMode           string                       `json:"authoring_mode"`
	WorkerCacheMode         string                       `json:"worker_cache_mode"`
	Authoring               sourceRepositoryAuthoring    `json:"authoring"`
	CanonicalURL            string                       `json:"canonical_url,omitempty"`
	FallbackRemoteURLs      []string                     `json:"fallback_remote_urls,omitempty"`
	WorkerCacheWarmRefspecs []string                     `json:"worker_cache_warm_refspecs,omitempty"`
	DefaultRef              string                       `json:"default_ref,omitempty"`
	CredentialRef           string                       `json:"credential_ref,omitempty"`
	Declared                bool                         `json:"declared"`
	Enabled                 bool                         `json:"enabled"`
	Sync                    sourceRepositorySyncResponse `json:"sync"`
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
	Declared           bool                         `json:"declared"`
	Enabled            bool                         `json:"enabled"`
	Status             string                       `json:"status"`
	CheckoutMode       string                       `json:"checkout_mode"`
	AuthoringMode      string                       `json:"authoring_mode"`
	WorkerCacheMode    string                       `json:"worker_cache_mode"`
	Authoring          sourceRepositoryAuthoring    `json:"authoring"`
	CredentialRef      string                       `json:"credential_ref,omitempty"`
	CheckoutPath       string                       `json:"checkout_path,omitempty"`
	PathExists         bool                         `json:"path_exists"`
	PathIsDirectory    bool                         `json:"path_is_directory"`
	GitRepository      bool                         `json:"git_repository"`
	ObjectStore        *sourceRepositoryObjectStore `json:"object_store,omitempty"`
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

type sourceRepositoryObjectStore struct {
	PackFiles                 int                                  `json:"pack_files"`
	PackBytes                 int64                                `json:"pack_bytes"`
	PackKeepFiles             int                                  `json:"pack_keep_files"`
	LooseObjects              int                                  `json:"loose_objects"`
	LooseObjectsTruncated     bool                                 `json:"loose_objects_truncated,omitempty"`
	LooseObjectScanLimit      int                                  `json:"loose_object_scan_limit"`
	HydratedRefs              int                                  `json:"hydrated_refs"`
	HydratedRefsTruncated     bool                                 `json:"hydrated_refs_truncated,omitempty"`
	HydratedRefScanLimit      int                                  `json:"hydrated_ref_scan_limit"`
	CommitGraph               bool                                 `json:"commit_graph"`
	MultiPackIndex            bool                                 `json:"multi_pack_index"`
	MaintenanceIndicatorFiles []string                             `json:"maintenance_indicator_files,omitempty"`
	Pressure                  string                               `json:"pressure"`
	Warnings                  []sourceRepositoryObjectStoreWarning `json:"warnings,omitempty"`
}

type sourceRepositoryObjectStoreWarning struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Message  string `json:"message"`
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
	Truncated    bool                             `json:"truncated"`
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
	Truncated      bool                                `json:"truncated"`
	NextCursor     string                              `json:"next_cursor,omitempty"`
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
	Truncated      bool                                     `json:"truncated"`
	NextCursor     string                                   `json:"next_cursor,omitempty"`
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
	Truncated      bool                                 `json:"truncated"`
	NextCursor     string                               `json:"next_cursor,omitempty"`
	RepositorySync sourceRepositorySyncResponse         `json:"repository_sync"`
	Jobs           []sourceRepositoryJobResponse        `json:"jobs"`
	Invalid        []invalidSourceRepositoryJobResponse `json:"invalid,omitempty"`
}

type sourceCronScheduleResponse struct {
	ScheduleID     string                              `json:"schedule_id"`
	RepositoryID   string                              `json:"repository_id"`
	Namespace      string                              `json:"namespace"`
	JobID          string                              `json:"job_id"`
	CronSpec       string                              `json:"cron_spec"`
	NextRunAt      string                              `json:"next_run_at"`
	Ref            string                              `json:"ref,omitempty"`
	Path           string                              `json:"path,omitempty"`
	PathDerived    bool                                `json:"path_derived"`
	ConfiguredRef  string                              `json:"configured_ref"`
	ConfiguredPath string                              `json:"configured_path"`
	Override       *sourceCronScheduleOverrideResponse `json:"override,omitempty"`
	RepositorySync *sourceRepositorySyncResponse       `json:"repository_sync,omitempty"`
	Declared       bool                                `json:"declared"`
	Enabled        bool                                `json:"enabled"`
}

type sourceCronScheduleOverrideRequest struct {
	Ref    string `json:"ref"`
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

type sourceCronScheduleUpdateRequest struct {
	Enabled *bool `json:"enabled"`
}

type sourceCronScheduleOverrideResponse struct {
	Ref           string `json:"ref,omitempty"`
	Path          string `json:"path,omitempty"`
	Reason        string `json:"reason,omitempty"`
	CreatedAtUnix int64  `json:"created_at_unix,omitempty"`
}

type sourceCronSchedulesResponse struct {
	Namespace    string                       `json:"namespace"`
	RepositoryID string                       `json:"repository_id,omitempty"`
	Schedules    []sourceCronScheduleResponse `json:"schedules"`
}

type sourceRepositoryJobDefinitionResponse struct {
	JobID          string                       `json:"job_id"`
	DefinitionHash string                       `json:"definition_hash"`
	Definition     json.RawMessage              `json:"definition"`
	Source         sourceProvenanceResponse     `json:"source"`
	RepositorySync sourceRepositorySyncResponse `json:"repository_sync"`
}

type sourceRepositoryJobDeleteResponse struct {
	Status         string                       `json:"status"`
	JobID          string                       `json:"job_id"`
	Source         sourceProvenanceResponse     `json:"source"`
	RepositorySync sourceRepositorySyncResponse `json:"repository_sync"`
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
	CreateOnly   bool            `json:"create_only,omitempty"`
}

func sourceDefinitionAuditEvent(createOnly bool) (eventType string, operation string) {
	if createOnly {
		return audit.EventJobCreated, "create"
	}

	return audit.EventJobUpdated, "update"
}

func sourceDefinitionAuditMetadata(jobID, namespace, repositoryID, operation string, written sourcepkg.WrittenDefinition) map[string]any {
	return map[string]any{
		"job_id":               jobID,
		"namespace":            namespace,
		"repository_id":        repositoryID,
		"source_operation":     operation,
		"source_ref":           written.RequestedRef,
		"source_path":          written.Path,
		"source_commit":        written.Commit,
		"source_parent_commit": written.ParentCommit,
		"source_blob_sha":      written.BlobSHA,
	}
}

func sourceScheduleAuditMetadata(rec dal.CronScheduleRecord, namespace, operation string) map[string]any {
	metadata := map[string]any{
		"schedule_id":   rec.ScheduleID,
		"job_id":        rec.JobID,
		"namespace":     namespace,
		"repository_id": rec.SourceRepositoryID,
		"operation":     operation,
		"enabled":       rec.Enabled,
		"source_ref":    rec.SourceRef,
		"source_path":   rec.SourcePath,
	}

	if sourceCronScheduleHasOverride(rec) {
		metadata["override_ref"] = rec.SourceOverrideRef
		metadata["override_path"] = rec.SourceOverridePath
		metadata["override_reason"] = rec.SourceOverrideReason
	}

	return metadata
}

func sourceRepositorySyncAuditMetadata(rec dal.SourceRepositoryRecord, namespace, outcome, reason, syncRef string) map[string]any {
	metadata := map[string]any{
		"repository_id": rec.RepositoryID,
		"namespace":     namespace,
		"source_kind":   rec.SourceKind,
		"outcome":       outcome,
		"reason":        reason,
		"sync_status":   rec.SyncStatus,
		"sync_ref":      firstNonEmpty(syncRef, rec.LastSyncRef),
		"sync_commit":   rec.LastSyncCommit,
	}

	if rec.LastSyncError != "" {
		metadata["sync_error"] = rec.LastSyncError
	}

	return metadata
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

type sourceJobTriggerResponse struct {
	JobID             string                       `json:"job_id"`
	RunID             string                       `json:"run_id"`
	RunIndex          int                          `json:"run_index"`
	DefinitionVersion int                          `json:"definition_version"`
	DefinitionHash    string                       `json:"definition_hash"`
	Source            sourceProvenanceResponse     `json:"source"`
	RepositorySync    sourceRepositorySyncResponse `json:"repository_sync"`
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
	req.CheckoutMode = strings.TrimSpace(req.CheckoutMode)
	req.AuthoringMode = strings.TrimSpace(req.AuthoringMode)
	req.WorkerCacheMode = strings.TrimSpace(req.WorkerCacheMode)
	req.CanonicalURL = strings.TrimSpace(req.CanonicalURL)
	fallbackRemoteURLs, err := normalizeSourceRepositoryFallbackRemoteURLs(req.FallbackRemoteURLs)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_fallback_remote_url", "fallback_remote_urls contains an unsafe Git remote", nil)
		return
	}

	req.FallbackRemoteURLs = fallbackRemoteURLs
	workerCacheWarmRefspecs, err := normalizeSourceRepositoryWarmRefspecs(req.WorkerCacheWarmRefspecs)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_worker_cache_warm_refspec", "worker_cache_warm_refspecs contains an unsafe Git fetch refspec", nil)
		return
	}

	req.WorkerCacheWarmRefspecs = workerCacheWarmRefspecs
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

	if req.WorkerCacheMode == "" {
		req.WorkerCacheMode = dal.SourceWorkerCacheModeEphemeral
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

	if !validSourceWorkerCacheMode(req.WorkerCacheMode) {
		writeAPIError(w, http.StatusBadRequest, "unsupported_worker_cache_mode", "worker_cache_mode is not supported", nil)
		return
	}

	if !sourceAuthoringModeCompatible(req.AuthoringMode, req.CheckoutMode) {
		writeAPIError(w, http.StatusBadRequest, "incompatible_authoring_mode", "authoring_mode is not compatible with checkout_mode", nil)
		return
	}

	if req.DefaultRef != "" {
		ref, err := sourcepkg.NormalizeRef(req.DefaultRef)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_source_reference", "invalid source reference", nil)
			return
		}
		req.DefaultRef = ref
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

	if err := validateAPISourceCheckoutPath(req.CheckoutPath); err != nil {
		writeAPIError(w, http.StatusBadRequest, "checkout_path_forbidden", "checkout_path must be under the configured source checkout root", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
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

	declared, ok := s.sourceRepositoryDeclarationIDsForResponse(w, "creating")
	if !ok {
		return
	}

	rec, err := s.sources.CreateRepository(ctx, dal.SourceRepositoryRecord{
		RepositoryID:            req.RepositoryID,
		NamespaceID:             ns.ID,
		SourceKind:              req.SourceKind,
		CheckoutPath:            req.CheckoutPath,
		CheckoutMode:            req.CheckoutMode,
		AuthoringMode:           req.AuthoringMode,
		WorkerCacheMode:         req.WorkerCacheMode,
		CanonicalURL:            req.CanonicalURL,
		FallbackRemoteURLs:      req.FallbackRemoteURLs,
		WorkerCacheWarmRefspecs: req.WorkerCacheWarmRefspecs,
		DefaultRef:              req.DefaultRef,
		CredentialRef:           req.CredentialRef,
		Enabled:                 enabled,
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

	writeJSON(w, http.StatusCreated, s.sourceRepositoryRecordToResponse(rec, ns.Path, declared))
}

func (s *APIServer) ListSourceRepositories(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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

	declared, ok := s.sourceRepositoryDeclarationIDsForResponse(w, "listing")
	if !ok {
		return
	}

	resp := make([]sourceRepositoryResponse, 0, len(recs))
	for _, rec := range recs {
		resp = append(resp, s.sourceRepositoryRecordToResponse(rec, ns.Path, declared))
	}

	writeJSON(w, http.StatusOK, resp)
}

func (s *APIServer) GetSourceRepository(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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

	declared, ok := s.sourceRepositoryDeclarationIDsForResponse(w, "getting")
	if !ok {
		return
	}

	writeJSON(w, http.StatusOK, s.sourceRepositoryRecordToResponse(rec, nsPath, declared))
}

func (s *APIServer) ListSourceSchedules(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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

	declared, err := sourceCronScheduleDeclarationIDs()
	if err != nil {
		s.logger.Error("Configuration error listing source schedules: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	syncByRepositoryID, err := s.sourceRepositorySyncResponsesByID(ctx, ns.ID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error listing source repositories for schedule sync state: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, sourceCronSchedulesResponse{
		Namespace: ns.Path,
		Schedules: sourceCronScheduleRecordsToResponseWithSync(recs, ns.Path, declared, syncByRepositoryID),
	})
}

func (s *APIServer) ListSourceRepositorySchedules(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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

	declared, err := sourceCronScheduleDeclarationIDs()
	if err != nil {
		s.logger.Error("Configuration error listing source repository schedules: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, sourceCronSchedulesResponse{
		Namespace:    nsPath,
		RepositoryID: rec.RepositoryID,
		Schedules: sourceCronScheduleRecordsToResponseWithSync(recs, nsPath, declared, map[string]sourceRepositorySyncResponse{
			rec.RepositoryID: sourceRepositorySyncRecordToResponse(rec),
		}),
	})
}

func (s *APIServer) PatchSourceSchedule(w http.ResponseWriter, r *http.Request) {
	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONDocumentBodyBytes))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	var req sourceCronScheduleUpdateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}
	if req.Enabled == nil {
		writeAPIError(w, http.StatusBadRequest, "missing_source_schedule_enabled", "enabled is required", nil)
		return
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) || !s.requireSchedules(w) {
		return
	}

	scheduleID := r.PathValue("schedule_id")
	rec, namespacePath, ok := s.getAuthorizedSourceSchedule(ctx, w, p, scheduleID, authz.ActionJobWrite)
	if !ok {
		return
	}

	declared, err := sourceCronScheduleDeclarationIDs()
	if err != nil {
		s.logger.Error("Configuration error updating source schedule: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	rec.Enabled = *req.Enabled
	rec.NextRunAt = time.Time{}
	updated, err := s.schedules.UpdateCronSchedule(ctx, rec)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_schedule_not_found", "source schedule not found", nil)
			return
		}
		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusBadRequest, "invalid_source_schedule_update", "invalid source schedule update", nil)
			return
		}

		s.logger.Error("Database error updating source schedule: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	s.auditLog(ctx, audit.EventSourceScheduleUpdated, actorIDFromPrincipal(p), 0, sourceScheduleAuditMetadata(updated, namespacePath, "update"))

	writeJSON(w, http.StatusOK, sourceCronScheduleRecordToResponse(updated, namespacePath, declared))
}

func (s *APIServer) DeleteSourceSchedule(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) || !s.requireSchedules(w) {
		return
	}

	scheduleID := r.PathValue("schedule_id")
	rec, namespacePath, ok := s.getAuthorizedSourceSchedule(ctx, w, p, scheduleID, authz.ActionJobWrite)
	if !ok {
		return
	}

	declared, err := sourceCronScheduleDeclarationIDs()
	if err != nil {
		s.logger.Error("Configuration error deleting source schedule: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	if sourceCronScheduleIsDeclared(rec.ScheduleID, declared) {
		writeAPIError(w, http.StatusConflict, "source_schedule_declared", "source schedule is still declared in current config", nil)
		return
	}

	if rec.Enabled {
		writeAPIError(w, http.StatusConflict, "source_schedule_enabled", "source schedule must be disabled before deletion", nil)
		return
	}

	if sourceCronScheduleHasOverride(rec) {
		writeAPIError(w, http.StatusConflict, "source_schedule_override_active", "source schedule override must be cleared before deletion", nil)
		return
	}

	if err := s.schedules.DeleteSourceCronSchedule(ctx, scheduleID); err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}

		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_schedule_not_found", "source schedule not found", nil)
			return
		}

		s.logger.Error("Database error deleting source schedule: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	s.auditLog(ctx, audit.EventSourceScheduleDeleted, actorIDFromPrincipal(p), 0, sourceScheduleAuditMetadata(rec, namespacePath, "delete"))

	w.WriteHeader(http.StatusNoContent)
}

func (s *APIServer) PutSourceScheduleOverride(w http.ResponseWriter, r *http.Request) {
	if !requestContentTypeIsJSON(r) {
		writeAPIErrorCode(w, http.StatusUnsupportedMediaType, apiErrUnsupportedMediaType)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxJSONDocumentBodyBytes))
	if err != nil {
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrRequestReadFailed)
		return
	}

	var req sourceCronScheduleOverrideRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeAPIErrorCode(w, http.StatusBadRequest, apiErrInvalidRequestBody)
		return
	}

	req.Ref = strings.TrimSpace(req.Ref)
	req.Path = strings.TrimSpace(req.Path)
	req.Reason = strings.TrimSpace(req.Reason)
	if req.Ref == "" && req.Path == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_source_schedule_override", "ref or path is required", nil)
		return
	}

	if req.Ref != "" {
		ref, err := sourcepkg.NormalizeRef(req.Ref)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_source_reference", "invalid source reference", nil)
			return
		}

		req.Ref = ref
	}

	if req.Path != "" {
		filePath, err := sourcepkg.NormalizeTreePath(req.Path)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_source_reference", "invalid source reference", nil)
			return
		}

		req.Path = filePath
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) || !s.requireSchedules(w) {
		return
	}

	scheduleID := r.PathValue("schedule_id")
	_, namespacePath, ok := s.getAuthorizedSourceSchedule(ctx, w, p, scheduleID, authz.ActionJobWrite)
	if !ok {
		return
	}

	declared, err := sourceCronScheduleDeclarationIDs()
	if err != nil {
		s.logger.Error("Configuration error setting source schedule override: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	rec, err := s.schedules.SetSourceCronScheduleOverride(ctx, scheduleID, dal.SourceScheduleOverride{
		Ref:    req.Ref,
		Path:   req.Path,
		Reason: req.Reason,
	})
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_schedule_not_found", "source schedule not found", nil)
			return
		}
		if dal.IsConflict(err) {
			writeAPIError(w, http.StatusBadRequest, "invalid_source_schedule_override", "invalid source schedule override", nil)
			return
		}

		s.logger.Error("Database error setting source schedule override: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	s.auditLog(ctx, audit.EventSourceScheduleOverrideSet, actorIDFromPrincipal(p), 0, sourceScheduleAuditMetadata(rec, namespacePath, "override_set"))

	writeJSON(w, http.StatusOK, sourceCronScheduleRecordToResponse(rec, namespacePath, declared))
}

func (s *APIServer) DeleteSourceScheduleOverride(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok {
		return
	}

	if !s.requireNamespaces(w) || !s.requireSources(w) || !s.requireSchedules(w) {
		return
	}

	scheduleID := r.PathValue("schedule_id")
	_, namespacePath, ok := s.getAuthorizedSourceSchedule(ctx, w, p, scheduleID, authz.ActionJobWrite)
	if !ok {
		return
	}

	declared, err := sourceCronScheduleDeclarationIDs()
	if err != nil {
		s.logger.Error("Configuration error clearing source schedule override: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	rec, err := s.schedules.ClearSourceCronScheduleOverride(ctx, scheduleID)
	if err != nil {
		if s.handleDBUnavailableError(w, err) {
			return
		}
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_schedule_not_found", "source schedule not found", nil)
			return
		}

		s.logger.Error("Database error clearing source schedule override: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}
	s.markDBRecovered()

	s.auditLog(ctx, audit.EventSourceScheduleOverrideCleared, actorIDFromPrincipal(p), 0, sourceScheduleAuditMetadata(rec, namespacePath, "override_cleared"))

	writeJSON(w, http.StatusOK, sourceCronScheduleRecordToResponse(rec, namespacePath, declared))
}

func (s *APIServer) DeleteSourceRepository(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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

	declared, ok := s.sourceRepositoryDeclarationIDsForResponse(w, "deleting")
	if !ok {
		return
	}

	if sourceRepositoryIsDeclared(rec.RepositoryID, declared) {
		writeAPIError(w, http.StatusConflict, "source_repository_declared", "source repository is still declared in current config", nil)
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
			writeAPIError(w, http.StatusConflict, "source_repository_in_use", "source repository has recorded source references", nil)
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
	ctx, cancel := s.handlerDBCtx(r.Context())
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

	declared, ok := s.sourceRepositoryDeclarationIDsForResponse(w, "getting status for")
	if !ok {
		return
	}

	writeJSON(w, http.StatusOK, s.sourceRepositoryStatusFromRecord(ctx, rec, nsPath, declared))
}

func (s *APIServer) ListSourceRepositoryBranches(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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
	listing, err := checkout.ListBranches(ctx, sourcepkg.ListBranchesOptions{
		Prefix: prefix,
		Limit:  limit,
	})
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	respBranches := make([]sourceRepositoryBranchResponse, 0, len(listing.Branches))
	for _, branch := range listing.Branches {
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
		Truncated:    listing.Truncated,
		Branches:     respBranches,
	})
}

func (s *APIServer) ListSourceRepositoryTree(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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
	listing, err := s.listSourceRepositoryTree(ctx, rec, sourcepkg.ListTreeOptions{
		Ref:       ref,
		Path:      r.URL.Query().Get("path"),
		Recursive: recursive,
		Limit:     limit,
		Cursor:    r.URL.Query().Get("cursor"),
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
		Truncated:      listing.Truncated,
		NextCursor:     listing.NextCursor,
		Entries:        respEntries,
	})
}

func (s *APIServer) ListSourceRepositoryDefinitions(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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
	listing, err := s.listSourceRepositoryDefinitionFiles(ctx, rec, sourcepkg.ListDefinitionFilesOptions{
		Ref:    ref,
		Path:   r.URL.Query().Get("path"),
		Limit:  limit,
		Cursor: r.URL.Query().Get("cursor"),
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
		Truncated:      listing.Truncated,
		NextCursor:     listing.NextCursor,
		Definitions:    respFiles,
	})
}

func (s *APIServer) ListSourceRepositoryJobs(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
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
	listing, err := s.listSourceRepositoryDefinitionFiles(ctx, rec, sourcepkg.ListDefinitionFilesOptions{
		Ref:    ref,
		Path:   r.URL.Query().Get("path"),
		Limit:  limit,
		Cursor: r.URL.Query().Get("cursor"),
	})

	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	jobs := make([]sourceRepositoryJobResponse, 0, len(listing.Files))
	invalid := make([]invalidSourceRepositoryJobResponse, 0)
	seenJobIDs := make(map[string]string, len(listing.Files))
	for _, file := range listing.Files {
		jobID, err := sourceJobIDFromDefinitionPath(listing.Path, file.Path)
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
		Truncated:      listing.Truncated,
		NextCursor:     listing.NextCursor,
		RepositorySync: sourceRepositorySyncRecordToResponse(rec),
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

	ctx, cancel := s.handlerDBCtx(r.Context())
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

	target, err := sourcepkg.ResolveDefinitionTarget(sourcepkg.DefinitionTargetRequest{
		JobID:      jobID,
		Ref:        r.URL.Query().Get("ref"),
		DefaultRef: rec.DefaultRef,
		Path:       r.URL.Query().Get("path"),
	})

	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	loaded, err := s.resolveSourceRepositoryDefinition(ctx, rec, sourcepkg.DefinitionRequest{
		Ref:  target.Ref,
		Path: target.Path,
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
		RepositorySync: sourceRepositorySyncRecordToResponse(rec),
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
		definitionPath, err = sourcepkg.DefinitionPathForJobID(jobID)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_job_id", "job_id cannot be mapped to a source definition path", nil)
			return
		}
	}

	ctx, cancel := s.handlerDBCtx(r.Context())
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
		CreateOnly:     req.CreateOnly,
	})
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	eventType, operation := sourceDefinitionAuditEvent(req.CreateOnly)
	s.auditLog(ctx, eventType, actorID, 0, sourceDefinitionAuditMetadata(jobID, nsPath, rec.RepositoryID, operation, written))

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
		RepositorySync: sourceRepositorySyncRecordToResponse(rec),
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

	ctx, cancel := s.handlerDBCtx(r.Context())
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

	target, err := sourcepkg.ResolveDefinitionTarget(sourcepkg.DefinitionTargetRequest{
		JobID:      jobID,
		Ref:        req.Ref,
		DefaultRef: rec.DefaultRef,
		Path:       req.Path,
	})

	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	idempotencyKey := idempotencyKeyFromRequest(r)
	idempotencyScope := principalIdempotencyScope("source-trigger:"+rec.RepositoryID+":"+jobID, p)
	idempotencyHash := hashIdempotencyRequest(http.MethodPost, "/api/v1/source-repositories/"+rec.RepositoryID+"/jobs/"+jobID+"/trigger", string(bytes.TrimSpace(body)))
	idempotencyRecord, idempotencyReserved, idempotencyInProgress, ok := s.reserveRecoverableIdempotency(w, ctx, idempotencyScope, idempotencyKey, idempotencyHash)
	if !ok {
		return
	}

	if idempotencyRecord.ResponseJSON != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, *idempotencyRecord.ResponseJSON)
		return
	}

	if idempotencyInProgress {
		if s.recoverRunCreationIdempotency(w, ctx, idempotencyScope, idempotencyKey, idempotencyRecord, func(createdRuns []dal.CreatedRun) (any, bool) {
			if len(createdRuns) != 1 || createdRuns[0].JobID != jobID || createdRuns[0].Source == nil {
				return nil, false
			}

			createdRun := createdRuns[0]
			return sourceJobTriggerResponse{
				JobID:             createdRun.JobID,
				RunID:             createdRun.RunID,
				RunIndex:          createdRun.RunIndex,
				DefinitionVersion: createdRun.DefinitionVersion,
				DefinitionHash:    createdRun.DefinitionHash,
				Source:            sourceRecordToProvenance(*createdRun.Source),
				RepositorySync:    sourceRepositorySyncRecordToResponse(rec),
			}, true
		}) {
			return
		}

		writeAPIError(w, http.StatusConflict, "idempotency_in_progress", "idempotent request is still in progress", nil)
		return
	}

	loaded, err := s.resolveSourceRepositoryDefinition(ctx, rec, sourcepkg.DefinitionRequest{
		Ref:  target.Ref,
		Path: target.Path,
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
	invocationID, err := s.recordTriggerInvocation(ctx, jobID, dal.TriggerTypeManual, nil, triggerPayload, []string{targetCellID})
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

	if err := s.attachIdempotencyResource(ctx, idempotencyScope, idempotencyKey, idempotencyResourceTriggerInvocation, invocationID); err != nil {
		if idempotencyReserved {
			s.releaseIdempotency(ctx, idempotencyScope, idempotencyKey)
		}

		if s.handleDBUnavailableError(w, err) {
			return
		}

		s.logger.Error("Database error attaching source trigger idempotency resource: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	sourceRec := sourcepkg.NewJobDefinitionSourceRecord(jobID, rec.RepositoryID, loaded)

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
		RepositorySync:    sourceRepositorySyncRecordToResponse(rec),
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

	bgCtx := detachedTraceContextFromContext(context.WithoutCancel(r.Context()))
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

	ctx, cancel := s.handlerDBCtx(r.Context())
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

	ctx, cancel := s.handlerDBCtx(r.Context())
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

	ctx, cancel := s.handlerDBCtx(r.Context())
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
	ctx, cancel := s.handlerDBCtx(r.Context())
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
	actorID := actorIDFromPrincipal(p)

	declared, ok := s.sourceRepositoryDeclarationIDsForResponse(w, "syncing")
	if !ok {
		return
	}

	attemptStarted := time.Now()
	recordSync := func(outcome, reason string) {
		s.recordSourceRepositorySyncMetric(ctx, observability.SourceSyncTriggerManual, rec, outcome, reason, time.Since(attemptStarted))
	}

	syncRef := sourceRepositorySyncRef(rec)
	releaseSync, syncStarted := s.tryBeginSourceRepositorySync(rec.RepositoryID)
	if !syncStarted {
		recordSync(observability.SourceSyncOutcomeAlreadyRunning, observability.SourceSyncReasonInMemoryLock)
		s.auditLog(ctx, audit.EventSourceRepositorySyncRequested, actorID, 0, sourceRepositorySyncAuditMetadata(rec, nsPath, observability.SourceSyncOutcomeAlreadyRunning, observability.SourceSyncReasonInMemoryLock, syncRef))
		s.writeRunningSourceRepositorySync(w, rec, nsPath, syncRef, declared)
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
		recordSync(observability.SourceSyncOutcomeFailed, observability.SourceSyncReasonDatabaseBeginFailed)
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
		recordSync(observability.SourceSyncOutcomeAlreadyRunning, observability.SourceSyncReasonDatabaseLock)
		s.auditLog(ctx, audit.EventSourceRepositorySyncRequested, actorID, 0, sourceRepositorySyncAuditMetadata(running, nsPath, observability.SourceSyncOutcomeAlreadyRunning, observability.SourceSyncReasonDatabaseLock, syncRef))
		s.writeRunningSourceRepositorySync(w, running, nsPath, syncRef, declared)
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
		s.recordSourceRepositoryObjectStoreMetric(ctx, rec, checkoutStatus)
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
		recordSync(observability.SourceSyncOutcomeFailed, observability.SourceSyncReasonDatabaseUpdateFailed)
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

	if syncRecord.Status == dal.SourceSyncStatusFailed {
		reason := sourceRepositorySyncMetricReason(syncRecord.Error)
		recordSync(observability.SourceSyncOutcomeFailed, reason)
		s.auditLog(ctx, audit.EventSourceRepositorySyncRequested, actorID, 0, sourceRepositorySyncAuditMetadata(updated, nsPath, observability.SourceSyncOutcomeFailed, reason, syncRef))
	} else {
		recordSync(observability.SourceSyncOutcomeSucceeded, observability.SourceSyncReasonNone)
		s.auditLog(ctx, audit.EventSourceRepositorySyncRequested, actorID, 0, sourceRepositorySyncAuditMetadata(updated, nsPath, observability.SourceSyncOutcomeSucceeded, observability.SourceSyncReasonNone, syncRef))
	}

	writeJSON(w, http.StatusOK, s.sourceRepositoryRecordToResponse(updated, nsPath, declared))
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

	ctx, cancel := s.handlerDBCtx(r.Context())
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

	if req.WorkerCacheMode != nil {
		updated.WorkerCacheMode = strings.TrimSpace(*req.WorkerCacheMode)
		if updated.WorkerCacheMode == "" {
			updated.WorkerCacheMode = dal.SourceWorkerCacheModeEphemeral
		}
	}

	if req.CanonicalURL != nil {
		updated.CanonicalURL = strings.TrimSpace(*req.CanonicalURL)
	}

	if req.FallbackRemoteURLs != nil {
		fallbackRemoteURLs, err := normalizeSourceRepositoryFallbackRemoteURLs(*req.FallbackRemoteURLs)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_fallback_remote_url", "fallback_remote_urls contains an unsafe Git remote", nil)
			return
		}

		updated.FallbackRemoteURLs = fallbackRemoteURLs
	}

	if req.WorkerCacheWarmRefspecs != nil {
		workerCacheWarmRefspecs, err := normalizeSourceRepositoryWarmRefspecs(*req.WorkerCacheWarmRefspecs)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_worker_cache_warm_refspec", "worker_cache_warm_refspecs contains an unsafe Git fetch refspec", nil)
			return
		}

		updated.WorkerCacheWarmRefspecs = workerCacheWarmRefspecs
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

	if !validSourceWorkerCacheMode(updated.WorkerCacheMode) {
		writeAPIError(w, http.StatusBadRequest, "unsupported_worker_cache_mode", "worker_cache_mode is not supported", nil)
		return
	}

	if !sourceAuthoringModeCompatible(updated.AuthoringMode, updated.CheckoutMode) {
		writeAPIError(w, http.StatusBadRequest, "incompatible_authoring_mode", "authoring_mode is not compatible with checkout_mode", nil)
		return
	}

	if updated.DefaultRef != "" {
		ref, err := sourcepkg.NormalizeRef(updated.DefaultRef)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid_source_reference", "invalid source reference", nil)
			return
		}

		updated.DefaultRef = ref
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

	if req.CheckoutPath != nil || (req.CheckoutMode != nil && updated.CheckoutMode == dal.SourceCheckoutModeManaged) {
		if err := validateAPISourceCheckoutPath(updated.CheckoutPath); err != nil {
			writeAPIError(w, http.StatusBadRequest, "checkout_path_forbidden", "checkout_path must be under the configured source checkout root", nil)
			return
		}
	}

	declared, ok := s.sourceRepositoryDeclarationIDsForResponse(w, "updating")
	if !ok {
		return
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

	writeJSON(w, http.StatusOK, s.sourceRepositoryRecordToResponse(updated, nsPath, declared))
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
	ctx, cancel := s.handlerDBCtx(r.Context())
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

	loaded, err := s.resolveSourceRepositoryDefinition(ctx, rec, sourcepkg.DefinitionRequest{
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

func (s *APIServer) getAuthorizedSourceSchedule(ctx context.Context, w http.ResponseWriter, p *authn.Principal, scheduleID string, action authz.Action) (dal.CronScheduleRecord, string, bool) {
	scheduleID = strings.TrimSpace(scheduleID)
	if scheduleID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_schedule_id", "schedule_id is required", nil)
		return dal.CronScheduleRecord{}, "", false
	}

	rec, err := s.schedules.GetCronScheduleByScheduleID(ctx, scheduleID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_schedule_not_found", "source schedule not found", nil)
			return dal.CronScheduleRecord{}, "", false
		}

		if s.handleDBUnavailableError(w, err) {
			return dal.CronScheduleRecord{}, "", false
		}

		s.logger.Error("Database error getting source schedule: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return dal.CronScheduleRecord{}, "", false
	}
	s.markDBRecovered()

	if strings.TrimSpace(rec.SourceRepositoryID) == "" {
		writeAPIError(w, http.StatusNotFound, "source_schedule_not_found", "source schedule not found", nil)
		return dal.CronScheduleRecord{}, "", false
	}

	repo, err := s.sources.GetRepository(ctx, rec.SourceRepositoryID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_repository_not_found", "source repository not found", nil)
			return dal.CronScheduleRecord{}, "", false
		}

		if s.handleDBUnavailableError(w, err) {
			return dal.CronScheduleRecord{}, "", false
		}

		s.logger.Error("Database error getting source schedule repository: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return dal.CronScheduleRecord{}, "", false
	}

	ns, err := s.namespaces.GetByID(ctx, repo.NamespaceID)
	if err != nil {
		if dal.IsNotFound(err) {
			writeAPIError(w, http.StatusNotFound, "source_repository_not_found", "source repository namespace not found", nil)
			return dal.CronScheduleRecord{}, "", false
		}

		if s.handleDBUnavailableError(w, err) {
			return dal.CronScheduleRecord{}, "", false
		}

		s.logger.Error("Database error getting source schedule namespace: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return dal.CronScheduleRecord{}, "", false
	}

	if !s.authorizeNamespace(ctx, w, p, action, ns.Path) {
		return dal.CronScheduleRecord{}, "", false
	}

	return rec, ns.Path, true
}

func (s *APIServer) sourceRepositoryDeclarationIDsForResponse(w http.ResponseWriter, operation string) (map[string]struct{}, bool) {
	declared, err := sourceRepositoryDeclarationIDs()
	if err != nil {
		s.logger.Error("Configuration error %s source repository: %v", operation, err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return nil, false
	}

	return declared, true
}

func (s *APIServer) sourceRepositoryRecordToResponse(rec dal.SourceRepositoryRecord, namespacePath string, declared map[string]struct{}) sourceRepositoryResponse {
	return sourceRepositoryResponse{
		RepositoryID:            rec.RepositoryID,
		Namespace:               namespacePath,
		SourceKind:              rec.SourceKind,
		CheckoutPath:            rec.CheckoutPath,
		CheckoutMode:            rec.CheckoutMode,
		AuthoringMode:           rec.AuthoringMode,
		WorkerCacheMode:         rec.WorkerCacheMode,
		Authoring:               s.sourceRepositoryAuthoringFromRecord(rec),
		CanonicalURL:            rec.CanonicalURL,
		FallbackRemoteURLs:      rec.FallbackRemoteURLs,
		WorkerCacheWarmRefspecs: rec.WorkerCacheWarmRefspecs,
		DefaultRef:              rec.DefaultRef,
		CredentialRef:           rec.CredentialRef,
		Declared:                sourceRepositoryIsDeclared(rec.RepositoryID, declared),
		Enabled:                 rec.Enabled,
		Sync:                    sourceRepositorySyncRecordToResponse(rec),
	}
}

func sourceRepositoryDeclarationIDs() (map[string]struct{}, error) {
	decls, err := config.SourceRepositoryDeclarations()
	if err != nil {
		return nil, err
	}

	declared := make(map[string]struct{}, len(decls))
	for _, decl := range decls {
		repositoryID := strings.TrimSpace(decl.RepositoryID)
		if repositoryID != "" {
			declared[repositoryID] = struct{}{}
		}
	}

	return declared, nil
}

func sourceRepositoryIsDeclared(repositoryID string, declared map[string]struct{}) bool {
	if len(declared) == 0 {
		return false
	}

	_, ok := declared[strings.TrimSpace(repositoryID)]
	return ok
}

func sourceCronScheduleRecordsToResponse(recs []dal.CronScheduleRecord, namespacePath string, declared map[string]struct{}) []sourceCronScheduleResponse {
	return sourceCronScheduleRecordsToResponseWithSync(recs, namespacePath, declared, nil)
}

func sourceCronScheduleRecordsToResponseWithSync(recs []dal.CronScheduleRecord, namespacePath string, declared map[string]struct{}, syncByRepositoryID map[string]sourceRepositorySyncResponse) []sourceCronScheduleResponse {
	resp := make([]sourceCronScheduleResponse, 0, len(recs))
	for _, rec := range recs {
		resp = append(resp, sourceCronScheduleRecordToResponseWithSync(rec, namespacePath, declared, syncByRepositoryID))
	}

	return resp
}

func sourceCronScheduleRecordToResponse(rec dal.CronScheduleRecord, namespacePath string, declared map[string]struct{}) sourceCronScheduleResponse {
	return sourceCronScheduleRecordToResponseWithSync(rec, namespacePath, declared, nil)
}

func sourceCronScheduleRecordToResponseWithSync(rec dal.CronScheduleRecord, namespacePath string, declared map[string]struct{}, syncByRepositoryID map[string]sourceRepositorySyncResponse) sourceCronScheduleResponse {
	effectiveRef := strings.TrimSpace(rec.SourceRef)
	if ref := strings.TrimSpace(rec.SourceOverrideRef); ref != "" {
		effectiveRef = ref
	}

	definitionPath := strings.TrimSpace(rec.SourcePath)
	if path := strings.TrimSpace(rec.SourceOverridePath); path != "" {
		definitionPath = path
	}

	pathDerived := false
	if definitionPath == "" {
		if path, err := sourcepkg.DefinitionPathForJobID(rec.JobID); err == nil {
			definitionPath = path
			pathDerived = true
		}
	}

	resp := sourceCronScheduleResponse{
		ScheduleID:     rec.ScheduleID,
		RepositoryID:   rec.SourceRepositoryID,
		Namespace:      namespacePath,
		JobID:          rec.JobID,
		CronSpec:       rec.CronSpec,
		NextRunAt:      rec.NextRunAt.UTC().Format(time.RFC3339),
		Ref:            effectiveRef,
		Path:           definitionPath,
		PathDerived:    pathDerived,
		ConfiguredRef:  rec.SourceRef,
		ConfiguredPath: rec.SourcePath,
		Declared:       sourceCronScheduleIsDeclared(rec.ScheduleID, declared),
		Enabled:        rec.Enabled,
	}

	if rec.SourceOverrideRef != "" || rec.SourceOverridePath != "" {
		resp.Override = &sourceCronScheduleOverrideResponse{
			Ref:           rec.SourceOverrideRef,
			Path:          rec.SourceOverridePath,
			Reason:        rec.SourceOverrideReason,
			CreatedAtUnix: rec.SourceOverrideCreatedAtUnix,
		}
	}

	if sync, ok := syncByRepositoryID[rec.SourceRepositoryID]; ok {
		syncCopy := sync
		resp.RepositorySync = &syncCopy
	}

	return resp
}

func (s *APIServer) sourceRepositorySyncResponsesByID(ctx context.Context, namespaceID int64) (map[string]sourceRepositorySyncResponse, error) {
	if s.sources == nil {
		return map[string]sourceRepositorySyncResponse{}, nil
	}

	recs, err := s.sources.ListRepositories(ctx, namespaceID)
	if err != nil {
		return nil, err
	}

	resp := make(map[string]sourceRepositorySyncResponse, len(recs))
	for _, rec := range recs {
		resp[rec.RepositoryID] = sourceRepositorySyncRecordToResponse(rec)
	}

	return resp, nil
}

func sourceCronScheduleDeclarationIDs() (map[string]struct{}, error) {
	decls, err := config.SourceScheduleDeclarations()
	if err != nil {
		return nil, err
	}

	declared := make(map[string]struct{}, len(decls))
	for _, decl := range decls {
		scheduleID := strings.TrimSpace(decl.ScheduleID)
		if scheduleID != "" {
			declared[scheduleID] = struct{}{}
		}
	}

	return declared, nil
}

func sourceCronScheduleIsDeclared(scheduleID string, declared map[string]struct{}) bool {
	if len(declared) == 0 {
		return false
	}

	_, ok := declared[strings.TrimSpace(scheduleID)]
	return ok
}

func sourceCronScheduleHasOverride(rec dal.CronScheduleRecord) bool {
	return strings.TrimSpace(rec.SourceOverrideRef) != "" || strings.TrimSpace(rec.SourceOverridePath) != ""
}

func (s *APIServer) sourceRepositoryStatusFromRecord(ctx context.Context, rec dal.SourceRepositoryRecord, namespacePath string, declared map[string]struct{}) sourceRepositoryStatusResponse {
	resp := sourceRepositoryStatusResponse{
		RepositoryID:    rec.RepositoryID,
		Namespace:       namespacePath,
		SourceKind:      rec.SourceKind,
		Declared:        sourceRepositoryIsDeclared(rec.RepositoryID, declared),
		Enabled:         rec.Enabled,
		Status:          "ok",
		CheckoutMode:    rec.CheckoutMode,
		AuthoringMode:   rec.AuthoringMode,
		WorkerCacheMode: rec.WorkerCacheMode,
		Authoring:       s.sourceRepositoryAuthoringFromRecord(rec),
		CredentialRef:   rec.CredentialRef,
		Sync:            sourceRepositorySyncRecordToResponse(rec),
	}

	if !rec.Enabled {
		resp.Status = "disabled"
	}

	switch strings.TrimSpace(rec.SourceKind) {
	case dal.SourceKindLocalCheckout:
		checkoutStatus := newGitCheckoutForSourceRepository(rec).Status(ctx, rec.DefaultRef)
		s.recordSourceRepositoryObjectStoreMetric(ctx, rec, checkoutStatus)
		resp.CheckoutPath = checkoutStatus.CheckoutPath
		resp.PathExists = checkoutStatus.PathExists
		resp.PathIsDirectory = checkoutStatus.PathIsDirectory
		resp.GitRepository = checkoutStatus.GitRepository
		if checkoutStatus.GitRepository {
			resp.ObjectStore = sourceRepositoryObjectStoreFromStatus(checkoutStatus.ObjectStore)
		}

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

func sourceRepositoryObjectStoreFromStatus(status sourcepkg.GitCheckoutObjectStoreStatus) *sourceRepositoryObjectStore {
	warnings := make([]sourceRepositoryObjectStoreWarning, 0, len(status.Warnings))
	for _, warning := range status.Warnings {
		warnings = append(warnings, sourceRepositoryObjectStoreWarning{
			Code:     warning.Code,
			Severity: warning.Severity,
			Message:  warning.Message,
		})
	}

	return &sourceRepositoryObjectStore{
		PackFiles:                 status.PackFiles,
		PackBytes:                 status.PackBytes,
		PackKeepFiles:             status.PackKeepFiles,
		LooseObjects:              status.LooseObjects,
		LooseObjectsTruncated:     status.LooseObjectsTruncated,
		LooseObjectScanLimit:      status.LooseObjectScanLimit,
		HydratedRefs:              status.HydratedRefs,
		HydratedRefsTruncated:     status.HydratedRefsTruncated,
		HydratedRefScanLimit:      status.HydratedRefScanLimit,
		CommitGraph:               status.CommitGraph,
		MultiPackIndex:            status.MultiPackIndex,
		MaintenanceIndicatorFiles: append([]string(nil), status.MaintenanceIndicatorFiles...),
		Pressure:                  status.Pressure,
		Warnings:                  warnings,
	}
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

func normalizeSourceRepositoryFallbackRemoteURLs(in []string) ([]string, error) {
	if len(in) == 0 {
		return nil, nil
	}

	out := make([]string, 0, len(in))
	seen := make(map[string]struct{}, len(in))
	for _, raw := range in {
		if strings.TrimSpace(raw) == "" {
			continue
		}

		remoteURL, err := sourcepkg.NormalizeGitRemoteURL(raw)
		if err != nil {
			return nil, err
		}

		if _, ok := seen[remoteURL]; ok {
			continue
		}

		seen[remoteURL] = struct{}{}
		out = append(out, remoteURL)
	}

	if len(out) == 0 {
		return nil, nil
	}

	return out, nil
}

func normalizeSourceRepositoryWarmRefspecs(in []string) ([]string, error) {
	return refspec.NormalizeFetchRefspecs(in)
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

func (s *APIServer) recordSourceRepositorySyncMetric(ctx context.Context, trigger string, rec dal.SourceRepositoryRecord, outcome, reason string, d time.Duration) {
	if s.sourceSyncMetrics == nil {
		return
	}

	s.sourceSyncMetrics.RecordSourceRepositorySync(ctx, trigger, rec.SourceKind, rec.CheckoutMode, outcome, reason, d)
}

func (s *APIServer) recordSourceRepositoryObjectStoreMetric(ctx context.Context, rec dal.SourceRepositoryRecord, status sourcepkg.GitCheckoutStatus) {
	if s.sourceObjectStoreMetrics == nil || !status.GitRepository {
		return
	}

	objectStore := status.ObjectStore
	s.sourceObjectStoreMetrics.RecordSourceRepositoryObjectStore(ctx,
		rec.RepositoryID,
		rec.SourceKind,
		rec.CheckoutMode,
		objectStore.Pressure,
		objectStore.PackFiles,
		objectStore.PackBytes,
		objectStore.LooseObjects,
		objectStore.HydratedRefs,
		sourceRepositoryObjectStoreMetricWarnings(objectStore.Warnings),
	)
}

func sourceRepositoryObjectStoreMetricWarnings(warnings []sourcepkg.GitCheckoutObjectStoreWarning) []observability.SourceRepositoryObjectStoreWarning {
	if len(warnings) == 0 {
		return nil
	}

	out := make([]observability.SourceRepositoryObjectStoreWarning, 0, len(warnings))
	for _, warning := range warnings {
		out = append(out, observability.SourceRepositoryObjectStoreWarning{
			Code:     warning.Code,
			Severity: warning.Severity,
		})
	}

	return out
}

func sourceRepositorySyncMetricReason(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return observability.SourceSyncReasonNone
	}

	code, _, _ := strings.Cut(raw, ":")
	return observability.SourceSyncReasonFromErrorCode(code)
}

func (s *APIServer) writeRunningSourceRepositorySync(w http.ResponseWriter, rec dal.SourceRepositoryRecord, namespacePath, syncRef string, declared map[string]struct{}) {
	running := rec
	running.SyncStatus = dal.SourceSyncStatusRunning
	if strings.TrimSpace(running.LastSyncRef) == "" {
		running.LastSyncRef = syncRef
	}

	if running.LastSyncStartedAtUnix == 0 {
		running.LastSyncStartedAtUnix = time.Now().Unix()
	}

	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusAccepted, s.sourceRepositoryRecordToResponse(running, namespacePath, declared))
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

func sourceJobIDFromDefinitionPath(rootPath, filePath string) (string, error) {
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
		if !validSourceJobIDPart(part) {
			return "", errors.New("definition path contains an unsupported job_id segment")
		}
	}

	jobID := strings.Join(parts, ".")
	if jobID == "" || strings.Contains(jobID, "..") {
		return "", errors.New("definition path cannot derive a safe job_id")
	}

	return jobID, nil
}

func validSourceJobIDPart(part string) bool {
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
			CheckoutPath:       rec.CheckoutPath,
			RemoteURL:          rec.CanonicalURL,
			DefaultRef:         syncRef,
			FallbackRemoteURLs: rec.FallbackRemoteURLs,
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
	store, err := sourcepkg.NewCheckoutStore(config.SourceCheckoutRoot(platform.DataHome()))
	if err != nil {
		return "", err
	}

	return store.Path(repositoryID)
}

func validateAPISourceCheckoutPath(checkoutPath string) error {
	store, err := sourcepkg.NewCheckoutStore(config.SourceCheckoutRoot(platform.DataHome()))
	if err != nil {
		return err
	}

	return validatePathWithinRoot(checkoutPath, store.Root())
}

func validatePathWithinRoot(rawPath, rawRoot string) error {
	rawPath = strings.TrimSpace(rawPath)
	rawRoot = strings.TrimSpace(rawRoot)
	if rawPath == "" || rawRoot == "" {
		return sourcepkg.ErrInvalidReference
	}

	if !filepath.IsAbs(rawPath) || !filepath.IsAbs(rawRoot) {
		return sourcepkg.ErrInvalidReference
	}

	pathClean := filepath.Clean(rawPath)
	rootClean := filepath.Clean(rawRoot)
	if !pathIsWithinRoot(pathClean, rootClean) {
		return sourcepkg.ErrInvalidReference
	}

	evalRoot, err := evalNearestExistingPath(rootClean)
	if err != nil {
		return err
	}

	evalPath, err := evalNearestExistingPath(pathClean)
	if err != nil {
		return err
	}

	if !pathIsWithinRoot(evalPath, evalRoot) {
		return sourcepkg.ErrInvalidReference
	}

	return nil
}

func evalNearestExistingPath(cleanPath string) (string, error) {
	cleanPath = filepath.Clean(cleanPath)
	var suffix []string
	for {
		resolved, err := filepath.EvalSymlinks(cleanPath)
		if err == nil {
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}

			return filepath.Clean(resolved), nil
		}

		if !os.IsNotExist(err) {
			return "", err
		}

		parent := filepath.Dir(cleanPath)
		if parent == cleanPath {
			return filepath.Clean(cleanPath), nil
		}

		suffix = append(suffix, filepath.Base(cleanPath))
		cleanPath = parent
	}
}

func pathIsWithinRoot(cleanPath, cleanRoot string) bool {
	rel, err := filepath.Rel(cleanRoot, cleanPath)
	if err != nil {
		return false
	}

	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func newGitCheckoutForSourceRepository(rec dal.SourceRepositoryRecord) *sourcepkg.GitCheckout {
	if strings.TrimSpace(rec.CheckoutMode) == dal.SourceCheckoutModeManaged {
		return sourcepkg.NewManagedGitCheckout(rec.CheckoutPath)
	}

	return sourcepkg.NewGitCheckout(rec.CheckoutPath)
}

func (s *APIServer) listSourceRepositoryTree(ctx context.Context, rec dal.SourceRepositoryRecord, opts sourcepkg.ListTreeOptions) (sourcepkg.TreeListing, error) {
	checkout := newGitCheckoutForSourceRepository(rec)
	listing, err := checkout.ListTree(ctx, opts)
	hydrated, hydrationErr := s.hydrateSourceRepositoryRefAfterNotFound(ctx, rec, opts.Ref, err)
	if hydrationErr != nil {
		return sourcepkg.TreeListing{}, hydrationErr
	}

	if err == nil || !hydrated {
		return listing, err
	}

	return newGitCheckoutForSourceRepository(rec).ListTree(ctx, opts)
}

func (s *APIServer) listSourceRepositoryDefinitionFiles(ctx context.Context, rec dal.SourceRepositoryRecord, opts sourcepkg.ListDefinitionFilesOptions) (sourcepkg.DefinitionFileListing, error) {
	store, err := sourcepkg.NewDefinitionStoreFromRecord(rec)
	if err != nil {
		return sourcepkg.DefinitionFileListing{}, err
	}

	listing, err := store.ListDefinitionFiles(ctx, opts)
	hydrated, hydrationErr := s.hydrateSourceRepositoryRefAfterNotFound(ctx, rec, opts.Ref, err)
	if hydrationErr != nil {
		return sourcepkg.DefinitionFileListing{}, hydrationErr
	}

	if err == nil || !hydrated {
		return listing, err
	}

	store, err = sourcepkg.NewDefinitionStoreFromRecord(rec)
	if err != nil {
		return sourcepkg.DefinitionFileListing{}, err
	}

	return store.ListDefinitionFiles(ctx, opts)
}

func (s *APIServer) resolveSourceRepositoryDefinition(ctx context.Context, rec dal.SourceRepositoryRecord, req sourcepkg.DefinitionRequest) (sourcepkg.Definition, error) {
	store, err := sourcepkg.NewDefinitionStoreFromRecord(rec)
	if err != nil {
		return sourcepkg.Definition{}, err
	}

	loaded, err := store.ResolveDefinition(ctx, req)
	hydrated, hydrationErr := s.hydrateSourceRepositoryRefAfterNotFound(ctx, rec, req.Ref, err)
	if hydrationErr != nil {
		return sourcepkg.Definition{}, hydrationErr
	}

	if err == nil || !hydrated {
		return loaded, err
	}

	store, err = sourcepkg.NewDefinitionStoreFromRecord(rec)
	if err != nil {
		return sourcepkg.Definition{}, err
	}

	return store.ResolveDefinition(ctx, req)
}

func (s *APIServer) hydrateSourceRepositoryRefAfterNotFound(ctx context.Context, rec dal.SourceRepositoryRecord, ref string, err error) (bool, error) {
	if !errors.Is(err, sourcepkg.ErrNotFound) ||
		strings.TrimSpace(rec.SourceKind) != dal.SourceKindLocalCheckout ||
		strings.TrimSpace(rec.CheckoutMode) != dal.SourceCheckoutModeManaged {
		return false, nil
	}

	ref = strings.TrimSpace(ref)
	if ref == "" {
		ref = strings.TrimSpace(rec.DefaultRef)
	}

	if ref == "" {
		ref = "HEAD"
	}

	status := s.hydrateSourceRepositoryRef(ctx, rec, ref)
	if status.ErrorCode == "" {
		return true, nil
	}

	if status.ErrorCode == sourceRefHydrationInFlightErrorCode {
		return false, sourceRefHydrationPendingError{repositoryID: rec.RepositoryID, ref: ref}
	}

	return false, nil
}

func (s *APIServer) hydrateSourceRepositoryRef(ctx context.Context, rec dal.SourceRepositoryRecord, ref string) sourcepkg.GitCheckoutStatus {
	key := sourceRefHydrationKey(rec.RepositoryID, ref)
	preferredRemote, cacheHit := s.cachedSourceRefHydrationRemote(key)
	if key == "" {
		return s.hydrateSourceRepositoryRefAttempt(ctx, rec, ref, key, preferredRemote, cacheHit)
	}

	if s.cachedSourceRefHydrationMiss(key) {
		startedAt := time.Now()
		status := sourceRefHydrationMissCachedStatus(rec, ref)
		status.HydrationCacheHit = true
		s.recordSourceRefHydrationMetric(ctx, rec, status, "", true, time.Since(startedAt))
		return status
	}

	s.sourceRefHydrationMu.Lock()
	if s.sourceRefHydration == nil {
		s.sourceRefHydration = make(map[string]*sourceRefHydrationCall)
	}

	if call, ok := s.sourceRefHydration[key]; ok {
		done := call.done
		s.sourceRefHydrationMu.Unlock()

		select {
		case <-done:
			return call.status
		case <-ctx.Done():
			return sourcepkg.GitCheckoutStatus{
				CheckoutPath: rec.CheckoutPath,
				DefaultRef:   ref,
				ErrorCode:    sourceHydrationContextErrorCode(ctx.Err()),
				ErrorMessage: ctx.Err().Error(),
			}
		}
	}

	call := &sourceRefHydrationCall{done: make(chan struct{})}
	s.sourceRefHydration[key] = call
	s.sourceRefHydrationMu.Unlock()

	call.status = s.hydrateSourceRepositoryRefAttempt(ctx, rec, ref, key, preferredRemote, cacheHit)

	s.sourceRefHydrationMu.Lock()
	delete(s.sourceRefHydration, key)
	close(call.done)
	s.sourceRefHydrationMu.Unlock()

	return call.status
}

func (s *APIServer) hydrateSourceRepositoryRefAttempt(ctx context.Context, rec dal.SourceRepositoryRecord, ref, key, preferredRemote string, cacheHit bool) sourcepkg.GitCheckoutStatus {
	startedAt := time.Now()
	status := s.hydrateSourceRepositoryRefWithLease(ctx, rec, ref, key, preferredRemote)
	status.HydrationCacheHit = cacheHit

	if status.ErrorCode == "" {
		s.forgetSourceRefHydrationMiss(key)
		if strings.TrimSpace(status.HydrationRemote) != "" {
			s.rememberSourceRefHydrationRemote(key, status.HydrationRemote)
		}
	} else if status.ErrorCode == sourceRefHydrationNotFoundErrorCode {
		s.rememberSourceRefHydrationMiss(key)
		if cacheHit {
			s.forgetSourceRefHydrationRemote(key, preferredRemote)
		}
	} else if cacheHit {
		s.forgetSourceRefHydrationRemote(key, preferredRemote)
	}

	s.recordSourceRefHydrationMetric(ctx, rec, status, preferredRemote, cacheHit, time.Since(startedAt))
	return status
}

func (s *APIServer) hydrateSourceRepositoryRefWithLease(ctx context.Context, rec dal.SourceRepositoryRecord, ref, key, preferredRemote string) sourcepkg.GitCheckoutStatus {
	if strings.TrimSpace(key) == "" || s.serviceLeases == nil {
		return s.hydrateSourceRepositoryRefDirect(ctx, rec, ref, preferredRemote)
	}

	releaseLease, acquired, err := s.tryAcquireSourceRefHydrationLease(ctx, key)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("Source ref hydration lease acquire failed for repository %s ref %s: %v", rec.RepositoryID, ref, err)
		}

		return s.hydrateSourceRepositoryRefDirect(ctx, rec, ref, preferredRemote)
	}

	if acquired {
		defer releaseLease()
		return s.hydrateSourceRepositoryRefDirect(ctx, rec, ref, preferredRemote)
	}

	return s.waitForSourceRefHydrationLease(ctx, rec, ref, key, preferredRemote)
}

func (s *APIServer) waitForSourceRefHydrationLease(ctx context.Context, rec dal.SourceRepositoryRecord, ref, key, preferredRemote string) sourcepkg.GitCheckoutStatus {
	wait := s.sourceRefHydrationLeaseWait
	if wait <= 0 {
		wait = defaultSourceRefHydrationLeaseWait
	}

	pollInterval := s.sourceRefHydrationLeasePollInterval
	if pollInterval <= 0 {
		pollInterval = defaultSourceRefHydrationLeasePollInterval
	}

	deadline := time.NewTimer(wait)
	defer deadline.Stop()

	for {
		status := sourceRefHydrationLocalStatus(ctx, rec, ref)
		if status.ErrorCode == "" {
			return status
		}

		releaseLease, acquired, err := s.tryAcquireSourceRefHydrationLease(ctx, key)
		if err != nil {
			if s.logger != nil {
				s.logger.Warn("Source ref hydration lease retry failed for repository %s ref %s: %v", rec.RepositoryID, ref, err)
			}
			return s.hydrateSourceRepositoryRefDirect(ctx, rec, ref, preferredRemote)
		}

		if acquired {
			defer releaseLease()

			status = sourceRefHydrationLocalStatus(ctx, rec, ref)
			if status.ErrorCode == "" {
				return status
			}

			return s.hydrateSourceRepositoryRefDirect(ctx, rec, ref, preferredRemote)
		}

		poll := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			poll.Stop()
			return sourceRefHydrationContextStatus(ctx, rec, ref)
		case <-deadline.C:
			poll.Stop()
			return sourceRefHydrationInFlightStatus(rec, ref)
		case <-poll.C:
		}
	}
}

func (s *APIServer) tryAcquireSourceRefHydrationLease(ctx context.Context, key string) (func(), bool, error) {
	if s.serviceLeases == nil {
		return func() {}, true, nil
	}

	name := sourceRefHydrationLeaseName(key)
	if name == "" {
		return func() {}, true, nil
	}

	ttl := s.sourceRefHydrationLeaseTTL
	if ttl <= 0 {
		ttl = defaultSourceRefHydrationLeaseTTL
	}

	now := time.Now()
	owner := sourceRefHydrationLeaseOwner(key)
	acquired, err := s.serviceLeases.TryAcquire(ctx, name, owner, now, now.Add(ttl))
	if err != nil || !acquired {
		return func() {}, acquired, err
	}

	return func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.serviceLeases.Release(releaseCtx, name, owner); err != nil && s.logger != nil {
			s.logger.Warn("Source ref hydration lease release failed for %s: %v", name, err)
		}
	}, true, nil
}

func sourceRefHydrationLocalStatus(ctx context.Context, rec dal.SourceRepositoryRecord, ref string) sourcepkg.GitCheckoutStatus {
	status := sourcepkg.NewManagedGitCheckout(rec.CheckoutPath).Status(ctx, ref)
	if status.ErrorCode == "" {
		status.HydrationTier = sourceRefHydrationCoalescedTier
	}

	return status
}

func sourceRefHydrationContextStatus(ctx context.Context, rec dal.SourceRepositoryRecord, ref string) sourcepkg.GitCheckoutStatus {
	err := ctx.Err()
	if err == nil {
		err = context.Canceled
	}

	return sourcepkg.GitCheckoutStatus{
		CheckoutPath: rec.CheckoutPath,
		DefaultRef:   ref,
		ErrorCode:    sourceHydrationContextErrorCode(err),
		ErrorMessage: err.Error(),
	}
}

func sourceRefHydrationInFlightStatus(rec dal.SourceRepositoryRecord, ref string) sourcepkg.GitCheckoutStatus {
	return sourcepkg.GitCheckoutStatus{
		CheckoutPath: rec.CheckoutPath,
		DefaultRef:   ref,
		ErrorCode:    sourceRefHydrationInFlightErrorCode,
		ErrorMessage: "source ref hydration is already running on another API replica",
	}
}

func (s *APIServer) hydrateSourceRepositoryRefDirect(ctx context.Context, rec dal.SourceRepositoryRecord, ref, preferredRemote string) sourcepkg.GitCheckoutStatus {
	if hydrator := s.sourceRefHydrator; hydrator != nil {
		return hydrator(ctx, rec, ref, preferredRemote)
	}

	return sourcepkg.HydrateManagedGitRef(ctx, sourcepkg.ManagedGitRefHydrationRequest{
		CheckoutPath:       rec.CheckoutPath,
		Ref:                ref,
		PreferredRemote:    preferredRemote,
		FallbackRemoteURLs: rec.FallbackRemoteURLs,
	})
}

func (s *APIServer) recordSourceRefHydrationMetric(ctx context.Context, rec dal.SourceRepositoryRecord, status sourcepkg.GitCheckoutStatus, preferredRemote string, cacheHit bool, d time.Duration) {
	metrics := s.sourceRefHydrationMetrics
	if metrics == nil {
		return
	}

	outcome := observability.SourceSyncOutcomeSucceeded
	reason := observability.SourceSyncReasonNone
	if status.ErrorCode != "" {
		outcome = observability.SourceSyncOutcomeFailed
		reason = sourceRepositorySyncMetricReason(status.ErrorCode)
	}

	cacheState := observability.SourceRefHydrationCacheMiss
	if cacheHit {
		cacheState = observability.SourceRefHydrationCacheHit
	}

	tier := sourceRefHydrationTierForMetric(status.HydrationTier, preferredRemote)
	metrics.RecordSourceRefHydration(ctx, rec.SourceKind, rec.CheckoutMode, outcome, reason, tier, cacheState, d)
}

func (s *APIServer) cachedSourceRefHydrationRemote(key string) (string, bool) {
	if strings.TrimSpace(key) == "" {
		return "", false
	}

	s.sourceRefAvailabilityMu.Lock()
	defer s.sourceRefAvailabilityMu.Unlock()

	entry, ok := s.sourceRefAvailability[key]
	if !ok {
		return "", false
	}

	if !entry.expiresAt.After(time.Now()) {
		delete(s.sourceRefAvailability, key)
		return "", false
	}

	return entry.remote, entry.remote != ""
}

func (s *APIServer) rememberSourceRefHydrationRemote(key, remote string) {
	key = strings.TrimSpace(key)
	remote = strings.TrimSpace(remote)
	if key == "" || remote == "" {
		return
	}

	ttl := s.sourceRefAvailabilityTTL
	if ttl <= 0 {
		ttl = defaultSourceRefAvailabilityTTL
	}

	s.sourceRefAvailabilityMu.Lock()
	if s.sourceRefAvailability == nil {
		s.sourceRefAvailability = make(map[string]sourceRefAvailabilityEntry)
	}

	s.sourceRefAvailability[key] = sourceRefAvailabilityEntry{
		remote:    remote,
		expiresAt: time.Now().Add(ttl),
	}

	s.sourceRefAvailabilityMu.Unlock()
}

func (s *APIServer) forgetSourceRefHydrationRemote(key, remote string) {
	key = strings.TrimSpace(key)
	remote = strings.TrimSpace(remote)
	if key == "" || remote == "" {
		return
	}

	s.sourceRefAvailabilityMu.Lock()
	if entry, ok := s.sourceRefAvailability[key]; ok && entry.remote == remote {
		delete(s.sourceRefAvailability, key)
	}

	s.sourceRefAvailabilityMu.Unlock()
}

func (s *APIServer) cachedSourceRefHydrationMiss(key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}

	s.sourceRefMissMu.Lock()
	defer s.sourceRefMissMu.Unlock()

	entry, ok := s.sourceRefMiss[key]
	if !ok {
		return false
	}

	if !entry.expiresAt.After(time.Now()) {
		delete(s.sourceRefMiss, key)
		return false
	}

	return true
}

func (s *APIServer) rememberSourceRefHydrationMiss(key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}

	ttl := s.sourceRefMissTTL
	if ttl <= 0 {
		ttl = defaultSourceRefMissTTL
	}

	s.sourceRefMissMu.Lock()
	if s.sourceRefMiss == nil {
		s.sourceRefMiss = make(map[string]sourceRefMissEntry)
	}

	s.sourceRefMiss[key] = sourceRefMissEntry{
		expiresAt: time.Now().Add(ttl),
	}

	s.sourceRefMissMu.Unlock()
}

func (s *APIServer) forgetSourceRefHydrationMiss(key string) {
	key = strings.TrimSpace(key)
	if key == "" {
		return
	}

	s.sourceRefMissMu.Lock()
	delete(s.sourceRefMiss, key)
	s.sourceRefMissMu.Unlock()
}

func sourceRefHydrationMissCachedStatus(rec dal.SourceRepositoryRecord, ref string) sourcepkg.GitCheckoutStatus {
	return sourcepkg.GitCheckoutStatus{
		CheckoutPath: rec.CheckoutPath,
		DefaultRef:   ref,
		ErrorCode:    sourceRefHydrationNotFoundErrorCode,
		ErrorMessage: "source ref was recently not found during hydration",
	}
}

func sourceRefHydrationTierForMetric(tier, remote string) string {
	tier = strings.TrimSpace(tier)
	if tier != "" {
		return tier
	}

	remote = strings.TrimSpace(remote)
	switch {
	case remote == "origin":
		return "origin"
	case strings.HasPrefix(remote, "vectis-fallback-"):
		return strings.TrimPrefix(remote, "vectis-")
	case remote != "":
		return "other"
	default:
		return "unknown"
	}
}

func sourceRefHydrationKey(repositoryID, ref string) string {
	repositoryID = strings.TrimSpace(repositoryID)
	ref = strings.TrimSpace(ref)
	if repositoryID == "" || ref == "" {
		return ""
	}

	return repositoryID + "\x00" + ref
}

func sourceRefHydrationLeaseName(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}

	return "source-ref-hydration:" + hashIdempotencyRequest(key)
}

func sourceRefHydrationLeaseOwner(key string) string {
	return "api:" + hashIdempotencyRequest(key, strconv.FormatInt(time.Now().UnixNano(), 10))
}

func sourceHydrationContextErrorCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "context_deadline_exceeded"
	}

	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}

	return "context_error"
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

func validSourceWorkerCacheMode(mode string) bool {
	switch strings.TrimSpace(mode) {
	case dal.SourceWorkerCacheModeEphemeral, dal.SourceWorkerCacheModePersistent:
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
		return nil, nil //nolint:nilnil // Missing provenance is optional and omitted from API responses.
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

	var pendingHydration sourceRefHydrationPendingError
	switch {
	case errors.As(err, &pendingHydration):
		w.Header().Set("Retry-After", strconv.Itoa(sourceRefHydrationRetryAfterSeconds))
		writeAPIError(w, http.StatusAccepted, sourceRefHydrationInFlightErrorCode, "source ref hydration is in flight", map[string]string{
			"repository_id": pendingHydration.repositoryID,
			"ref":           pendingHydration.ref,
			"retry":         "retry after the in-flight source ref hydration completes",
		})
	case dal.IsConflict(err):
		writeAPIError(w, http.StatusConflict, "source_job_conflict", "source job conflict", sourceDefinitionErrorDetails("job_version_conflict", "refresh the job definition and retry the source write"))
	case errors.Is(err, sourcepkg.ErrAlreadyExists):
		writeAPIError(w, http.StatusConflict, "source_definition_already_exists", "source definition already exists", sourceDefinitionErrorDetails("definition_already_exists", "use PUT to update the existing source definition or choose a different job_id/path"))
	case errors.Is(err, sourcepkg.ErrAuthoringUnavailable):
		writeAPIError(w, http.StatusConflict, "source_authoring_unavailable", "source repository does not support local definition authoring", sourceDefinitionErrorDetails("authoring_unavailable", "enable local_commit authoring on a managed source repository or choose a writable repository"))
	case errors.Is(err, sourcepkg.ErrConflict):
		writeAPIError(w, http.StatusConflict, "source_conflict", "source conflict", sourceDefinitionErrorDetails("stale_head", "refresh the branch head and retry with an updated expected_head"))
	case errors.Is(err, sourcepkg.ErrBusy):
		writeAPIError(w, http.StatusConflict, "source_busy", "source repository is busy", sourceDefinitionErrorDetails("source_busy", "retry after the current source operation completes"))
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

func sourceDefinitionErrorDetails(kind, retry string) map[string]string {
	details := map[string]string{"kind": kind}
	if retry != "" {
		details["retry"] = retry
	}

	return details
}
