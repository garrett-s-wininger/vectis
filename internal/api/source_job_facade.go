package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

func sourceJobRepositoryIDFromQuery(r *http.Request) string {
	return strings.TrimSpace(r.URL.Query().Get("repository_id"))
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
