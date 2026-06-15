package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"vectis/internal/api/audit"
	"vectis/internal/api/authz"
	sourcepkg "vectis/internal/source"
)

type sourceJobDefinitionFacadeRequest struct {
	RepositoryID string          `json:"repository_id"`
	JobID        string          `json:"job_id"`
	Ref          string          `json:"ref"`
	Branch       string          `json:"branch"`
	Path         string          `json:"path"`
	Message      string          `json:"message"`
	ExpectedHead string          `json:"expected_head"`
	Job          json.RawMessage `json:"job"`
	Definition   json.RawMessage `json:"definition"`
}

func sourceJobRepositoryIDFromQuery(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("repository_id"))
}

func requireSourceJobRepositoryIDFromQuery(w http.ResponseWriter, r *http.Request) (string, bool) {
	repositoryID := sourceJobRepositoryIDFromQuery(r)
	if repositoryID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_repository_id", "repository_id is required for reusable jobs", nil)
		return "", false
	}

	return repositoryID, true
}

func setSourceJobPathValues(r *http.Request, repositoryID, jobID string) {
	r.SetPathValue("id", repositoryID)
	r.SetPathValue("job_id", jobID)
}

func sourceJobRepositoryIDFromTriggerBody(w http.ResponseWriter, r *http.Request) (string, bool) {
	body, ok := readRequestBody(w, r, maxJobDefinitionBodyBytes)
	if !ok {
		return "", false
	}

	r.Body = io.NopCloser(bytes.NewReader(body))
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return "", true
	}

	var req struct {
		RepositoryID string `json:"repository_id"`
	}

	if err := json.Unmarshal(trimmed, &req); err != nil {
		return "", true
	}

	return strings.TrimSpace(req.RepositoryID), true
}

func (s *APIServer) writeSourceJobDefinitionFromJobsFacade(w http.ResponseWriter, r *http.Request, body []byte, fallbackJobID string, createOnly bool) bool {
	trimmed := bytes.TrimSpace(body)

	var req sourceJobDefinitionFacadeRequest
	parsedWrapper := false
	if len(trimmed) > 0 {
		parsedWrapper = json.Unmarshal(trimmed, &req) == nil
	}

	queryRepositoryID := sourceJobRepositoryIDFromQuery(r)
	repositoryID := strings.TrimSpace(req.RepositoryID)
	if repositoryID == "" {
		repositoryID = queryRepositoryID
	} else if queryRepositoryID != "" && queryRepositoryID != repositoryID {
		writeAPIError(w, http.StatusBadRequest, "repository_id_mismatch", "repository_id mismatch", nil)
		return true
	}

	if repositoryID == "" {
		return false
	}

	definition := bytes.TrimSpace(req.Job)
	if len(definition) == 0 {
		definition = bytes.TrimSpace(req.Definition)
	}

	if len(definition) == 0 && queryRepositoryID != "" {
		definition = trimmed
	}

	if len(definition) == 0 {
		if !parsedWrapper {
			writeAPIError(w, http.StatusBadRequest, "invalid_request_body", "invalid request body", nil)
			return true
		}

		writeAPIError(w, http.StatusBadRequest, "missing_job_definition", "job definition is required", nil)
		return true
	}

	jobID := strings.TrimSpace(fallbackJobID)
	bodyJobID := strings.TrimSpace(req.JobID)
	if jobID == "" {
		jobID = bodyJobID
	} else if bodyJobID != "" && bodyJobID != jobID {
		writeAPIError(w, http.StatusBadRequest, "job_id_mismatch", "job id mismatch", nil)
		return true
	}

	definitionJobID := sourceJobIDFromDefinition(definition)
	if jobID == "" {
		jobID = definitionJobID
	} else if definitionJobID != "" && definitionJobID != jobID {
		writeAPIError(w, http.StatusBadRequest, "job_id_mismatch", "job id mismatch", nil)
		return true
	}

	if jobID == "" {
		writeAPIError(w, http.StatusBadRequest, "missing_job_id", "job_id is required", nil)
		return true
	}

	q := r.URL.Query()
	writeReq := sourceRepositoryJobDefinitionWriteRequest{
		Ref:          firstNonEmpty(req.Ref, q.Get("ref")),
		Branch:       firstNonEmpty(req.Branch, q.Get("branch")),
		Path:         firstNonEmpty(req.Path, q.Get("path")),
		Message:      firstNonEmpty(req.Message, q.Get("message")),
		ExpectedHead: firstNonEmpty(req.ExpectedHead, q.Get("expected_head")),
		Definition:   json.RawMessage(definition),
		CreateOnly:   createOnly,
	}
	payload, err := json.Marshal(writeReq)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid_request_body", "invalid request body", nil)
		return true
	}

	setSourceJobPathValues(r, repositoryID, jobID)
	r.Body = io.NopCloser(bytes.NewReader(payload))
	r.ContentLength = int64(len(payload))
	r.Header.Set("Content-Type", "application/json")
	s.PutSourceRepositoryJobDefinition(w, r)
	return true
}

func sourceJobIDFromDefinition(definition []byte) string {
	var job struct {
		ID string `json:"id"`
	}

	if err := json.Unmarshal(definition, &job); err != nil {
		return ""
	}

	return strings.TrimSpace(job.ID)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}

	return ""
}

func (s *APIServer) deleteSourceJobDefinitionFromJobsFacade(w http.ResponseWriter, r *http.Request, repositoryID, jobID string) {
	setSourceJobPathValues(r, repositoryID, jobID)

	q := r.URL.Query()
	definitionPath := strings.TrimSpace(q.Get("path"))
	if definitionPath == "" {
		var err error
		definitionPath, err = sourcepkg.DefinitionPathForJobID(jobID)
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

	rec, nsPath, ok := s.getAuthorizedSourceRepository(ctx, w, p, repositoryID, authz.ActionJobWrite, true)
	if !ok {
		return
	}

	author, err := s.newSourceDefinitionAuthor(rec)
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	deleted, err := author.DeleteDefinition(ctx, sourcepkg.DeleteDefinitionRequest{
		Ref:          strings.TrimSpace(q.Get("ref")),
		Branch:       strings.TrimSpace(q.Get("branch")),
		Path:         definitionPath,
		Message:      strings.TrimSpace(q.Get("message")),
		ExpectedHead: strings.TrimSpace(q.Get("expected_head")),
	})

	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}

	actorID := int64(0)
	if p != nil {
		actorID = p.LocalUserID
	}

	s.auditLog(ctx, audit.EventJobDeleted, actorID, 0, map[string]any{
		"job_id":        jobID,
		"namespace":     nsPath,
		"repository_id": rec.RepositoryID,
		"source_ref":    deleted.RequestedRef,
		"source_path":   deleted.Path,
		"source_commit": deleted.Commit,
	})

	w.Header().Set("X-Vectis-Source-Commit", deleted.Commit)
	w.WriteHeader(http.StatusNoContent)
}
