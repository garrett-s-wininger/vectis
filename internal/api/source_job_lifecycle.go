package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"vectis/internal/api/audit"
	"vectis/internal/api/authn"
	"vectis/internal/api/authz"
	"vectis/internal/dal"
	sourcepkg "vectis/internal/source"
)

type sourceJobLifecycleResponse struct {
	SourceJSON   string               `json:"source_json,omitempty"`
	Registration *dal.SourceJobRecord `json:"registration,omitempty"`
	Source       json.RawMessage      `json:"source_definition,omitempty"`
	Effective    json.RawMessage      `json:"effective_definition,omitempty"`
	SourceCommit string               `json:"source_commit,omitempty"`
	SourceError  string               `json:"source_error,omitempty"`
}

func (s *APIServer) sourceJobOriginalFile(ctx context.Context, rec dal.SourceRepositoryRecord, jobID string, registration *dal.SourceJobRecord) (sourcepkg.File, error) {
	ref := rec.DefaultRef
	if ref == "" {
		ref = "HEAD"
	}

	var path string
	var revision sourcepkg.Revision
	if registration != nil {
		path = registration.Path
		if registration.Ref != "" {
			ref = registration.Ref
		}
	} else {
		resolved, pinned, err := newGitCheckoutForSourceRepository(rec).ResolveJobDefinitionPath(ctx, ref, jobID)
		if err != nil {
			return sourcepkg.File{}, err
		}

		path = resolved
		revision = pinned
	}

	store, err := sourcepkg.NewDefinitionStoreFromRecord(rec)
	if err != nil {
		return sourcepkg.File{}, err
	}

	return store.ReadDefinitionFile(ctx, sourcepkg.DefinitionFileRequest{Ref: ref, Path: path, Revision: revision})
}

func (s *APIServer) GetSourceJobLifecycle(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok || !s.requireSources(w) || !s.requireNamespaces(w) {
		return
	}

	rec, _, ok := s.getAuthorizedSourceRepository(ctx, w, p, r, r.PathValue("id"), authz.ActionJobRead, false)
	if !ok {
		return
	}

	jobID := r.PathValue("job_id")
	registration, err := s.sources.GetSourceJob(ctx, rec.RepositoryID, jobID)
	response := sourceJobLifecycleResponse{}
	if err == nil {
		if rec.NamespaceID != 0 && rec.NamespaceID != registration.NamespaceID &&
			!s.checkNamespaceAuth(ctx, p, authz.ActionAdmin, "/") {
			writeAPIError(w, http.StatusNotFound, "source_job_not_found", "job not found", nil)
			return
		}

		if !s.checkNamespaceAuth(ctx, p, authz.ActionJobRead, registration.Namespace) {
			writeAPIError(w, http.StatusNotFound, "source_job_not_found", "job not found", nil)
			return
		}

		response.Registration = &registration
	} else if !dal.IsNotFound(err) {
		s.writeSourceDefinitionError(w, err)
		return
	}

	file, fileErr := s.sourceJobOriginalFile(ctx, rec, jobID, response.Registration)
	if fileErr == nil {
		response.SourceCommit = file.Revision.Commit
		response.SourceJSON = string(file.Content)
	}

	var loaded sourcepkg.Definition
	err = fileErr
	if err == nil {
		loaded, err = sourcepkg.ParseDefinitionFile(file, "HEAD", sourcepkg.DefinitionRequest{}.Validation)
	}

	if err != nil {
		response.SourceError = "The current upstream definition could not be loaded or validated. Review the source contents below or inspect the repository diagnostics."
	} else {
		response.Source = json.RawMessage(loaded.DefinitionJSON)
		response.SourceCommit = loaded.Source.Commit
		response.Effective = response.Source
	}

	if response.Registration != nil && registration.Hotfix != nil {
		hotfix, err := sourcepkg.LoadHotfix(ctx, registration, sourcepkg.DefinitionRequest{})
		if err != nil {
			s.writeSourceDefinitionError(w, err)
			return
		}

		response.Effective = json.RawMessage(hotfix.DefinitionJSON)
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *APIServer) PutSourceJobOwnership(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok || !s.requireSources(w) || !s.requireNamespaces(w) {
		return
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, "/") {
		return
	}

	var req struct {
		Namespace string `json:"namespace"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request_body", "namespace is required", nil)
		return
	}

	ns, err := s.namespaces.GetByPath(ctx, req.Namespace)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "namespace_not_found", "namespace not found", nil)
		return
	}

	rec, err := s.sources.GetRepository(ctx, r.PathValue("id"))
	if err != nil {
		writeAPIError(w, http.StatusNotFound, "source_repository_not_found", "repository not found", nil)
		return
	}

	loaded, err := s.sourceJobOriginalFile(ctx, rec, r.PathValue("job_id"), nil)
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	registration, err := s.sources.RegisterSourceJob(ctx, dal.SourceJobRecord{RepositoryID: rec.RepositoryID, JobID: r.PathValue("job_id"), NamespaceID: ns.ID, Path: loaded.Path, Ref: ""})
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	_, actorID := sourceLifecycleActor(p)
	_ = s.auditLog(ctx, audit.EventJobUpdated, actorID, 0, map[string]any{"operation": "source_job_registered", "repository_id": rec.RepositoryID, "job_id": registration.JobID, "namespace": registration.Namespace})
	writeJSON(w, http.StatusOK, registration)
}

type sourceHotfixRequest struct {
	Definition           json.RawMessage `json:"definition"`
	Reason               string          `json:"reason"`
	ExpectedRevision     int             `json:"expected_revision"`
	ExpectedSourceCommit string          `json:"expected_source_commit"`
}

func (s *APIServer) ChangeSourceJobHotfix(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()

	p, ok := s.requirePrincipal(w, r)
	if !ok || !s.requireSources(w) || !s.requireNamespaces(w) {
		return
	}

	rec, err := s.sources.GetRepository(ctx, r.PathValue("id"))
	if err != nil {
		writeAPIError(w, http.StatusNotFound, "source_repository_not_found", "repository not found", nil)
		return
	}

	registration, err := s.sources.GetSourceJob(ctx, rec.RepositoryID, r.PathValue("job_id"))
	if err != nil {
		writeAPIError(w, http.StatusConflict, "source_job_unregistered", "assign this job an owning namespace first", nil)
		return
	}

	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, registration.Namespace) {
		return
	}

	grants, err := s.sources.RepositoryNamespaces(ctx, rec.RepositoryID)
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	granted := false
	for _, grant := range grants {
		if grant.ID == registration.NamespaceID {
			granted = true
			break
		}
	}

	if !granted {
		writeAPIError(w, http.StatusForbidden, "source_repository_access_denied", "the owning namespace no longer has repository access", nil)
		return
	}

	if !rec.Enabled {
		writeAPIError(w, http.StatusConflict, "source_repository_disabled", "repository is disabled", nil)
		return
	}

	var req sourceHotfixRequest
	if err = json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request_body", "invalid hotfix request", nil)
		return
	}

	if req.ExpectedRevision != registration.Revision {
		writeAPIError(w, http.StatusConflict, "source_conflict", "job changed; reload before applying the hotfix", nil)
		return
	}

	original, err := s.sourceJobOriginalFile(ctx, rec, registration.JobID, &registration)
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	if req.ExpectedSourceCommit == "" || req.ExpectedSourceCommit != original.Revision.Commit {
		writeAPIError(w, http.StatusConflict, "source_conflict", "source changed; review the current upstream definition", nil)
		return
	}

	operation := "source_hotfix_cleared"
	if r.Method == http.MethodPut {
		reason := strings.TrimSpace(req.Reason)
		if reason == "" || len(reason) > 512 {
			writeAPIError(w, http.StatusBadRequest, "invalid_hotfix_reason", "a reason of 1–512 bytes is required", nil)
			return
		}

		if len(req.Definition) == 0 {
			writeAPIError(w, http.StatusBadRequest, "missing_job_definition", "definition is required", nil)
			return
		}

		parsed, err := sourcepkg.ParseDefinitionFile(sourcepkg.File{Path: registration.Path, Revision: sourcepkg.Revision{Commit: original.Revision.Commit}, Content: req.Definition}, func() string {
			if rec.DefaultRef != "" {
				return rec.DefaultRef
			}

			return "HEAD"
		}(), sourcepkg.DefinitionRequest{}.Validation)

		if err != nil {
			s.writeSourceDefinitionError(w, err)
			return
		}

		store := s.sourceHotfixStore(rec.RepositoryID)
		actor, _ := sourceLifecycleActor(p)
		metadata, err := sourcepkg.WriteHotfix(ctx, store, []byte(parsed.DefinitionJSON), dal.SourceJobHotfix{Reason: reason, Actor: actor, BaseCommit: original.Revision.Commit, BaseRef: func() string {
			if rec.DefaultRef != "" {
				return rec.DefaultRef
			}

			return "HEAD"
		}(), BasePath: registration.Path, CreatedAt: time.Now().Unix()})

		if err != nil {
			s.writeSourceDefinitionError(w, err)
			return
		}

		registration.HotfixStore = store
		registration.Hotfix = &metadata
		operation = "source_hotfix_activated"
	} else {
		if _, err := sourcepkg.ParseDefinitionFile(original, "HEAD", sourcepkg.DefinitionRequest{}.Validation); err != nil {
			s.writeSourceDefinitionError(w, err)
			return
		}

		registration.Hotfix = nil
	}

	updated, err := s.sources.SetSourceJobHotfix(ctx, registration, req.ExpectedRevision)
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	_, actorID := sourceLifecycleActor(p)
	_ = s.auditLog(ctx, audit.EventJobUpdated, actorID, 0, map[string]any{"operation": operation, "repository_id": rec.RepositoryID, "job_id": registration.JobID, "namespace": registration.Namespace, "hotfix": updated.Hotfix})
	writeJSON(w, http.StatusOK, updated)
}

func (s *APIServer) sourceHotfixStore(repoID string) string {
	hash := sha256.Sum256([]byte(repoID))
	return filepath.Join(s.sourceCheckoutRootPath(), ".hotfixes", hex.EncodeToString(hash[:])+".git")
}

func decodeSourceHotfix(raw string) *dal.SourceJobHotfix {
	if raw == "" {
		return nil
	}

	var metadata dal.SourceJobHotfix
	if json.Unmarshal([]byte(raw), &metadata) != nil {
		return nil
	}

	return &metadata
}

func (s *APIServer) prepareOneOffSourceHotfix(ctx context.Context, rec dal.SourceRepositoryRecord, jobID, actor string, req sourceJobTriggerRequest) (sourcepkg.Definition, error) {
	registration, err := s.sources.GetSourceJob(ctx, rec.RepositoryID, jobID)
	if err != nil {
		return sourcepkg.Definition{}, fmt.Errorf("%w: assign an owning namespace before running with changes", sourcepkg.ErrConflict)
	}

	if rec.NamespaceID != registration.NamespaceID {
		return sourcepkg.Definition{}, fmt.Errorf("%w: job belongs to %s", sourcepkg.ErrConflict, registration.Namespace)
	}

	if registration.Revision != req.ExpectedRevision {
		return sourcepkg.Definition{}, fmt.Errorf("%w: job changed; reload before running with changes", sourcepkg.ErrConflict)
	}

	original, err := s.sourceJobOriginalFile(ctx, rec, jobID, &registration)
	if err != nil {
		return sourcepkg.Definition{}, err
	}

	if req.ExpectedSourceCommit == "" || original.Revision.Commit != req.ExpectedSourceCommit {
		return sourcepkg.Definition{}, fmt.Errorf("%w: source changed; review the current upstream definition", sourcepkg.ErrConflict)
	}

	if strings.TrimSpace(req.Reason) == "" || len(req.Reason) > 512 {
		return sourcepkg.Definition{}, fmt.Errorf("%w: a reason of 1–512 bytes is required", sourcepkg.ErrInvalidDefinition)
	}

	parsed, err := sourcepkg.ParseDefinitionFile(sourcepkg.File{Path: registration.Path, Revision: original.Revision, Content: req.Definition}, "HEAD", sourcepkg.DefinitionRequest{}.Validation)
	if err != nil {
		return sourcepkg.Definition{}, err
	}

	store := s.sourceHotfixStore(rec.RepositoryID)
	metadata, err := sourcepkg.WriteHotfix(ctx, store, []byte(parsed.DefinitionJSON), dal.SourceJobHotfix{Reason: strings.TrimSpace(req.Reason), Actor: actor, BaseCommit: original.Revision.Commit, BaseRef: rec.DefaultRef, BasePath: registration.Path, CreatedAt: time.Now().Unix(), OneOff: true})
	if err != nil {
		return sourcepkg.Definition{}, err
	}

	registration.Hotfix = &metadata
	registration.HotfixStore = store
	return sourcepkg.LoadHotfix(ctx, registration, sourcepkg.DefinitionRequest{})
}

func sourceLifecycleActor(p *authn.Principal) (string, int64) {
	if p == nil {
		return "anonymous", 0
	}

	if p.Username != "" {
		return p.Username, p.LocalUserID
	}

	return fmt.Sprintf("user:%d", p.LocalUserID), p.LocalUserID
}

func registeredHotfixJob(registration dal.SourceJobRecord) sourceRepositoryJobResponse {
	return sourceRepositoryJobResponse{JobID: registration.JobID, Name: registration.JobID, Path: registration.Path, OwnerNamespace: registration.Namespace, Hotfix: registration.Hotfix, Source: sourceProvenanceResponse{RepositoryID: registration.RepositoryID, RequestedRef: "refs/vectis/hotfixes/" + registration.Hotfix.Commit, ResolvedCommit: registration.Hotfix.Commit, Path: registration.Path, Hotfix: registration.Hotfix}}
}
