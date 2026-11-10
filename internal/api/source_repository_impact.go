package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"vectis/internal/api/authz"
	"vectis/internal/dal"
	sourcepkg "vectis/internal/source"
)

type sourceImpactCandidate struct {
	JobID string `json:"job_id"`
	Path  string `json:"path"`
}
type sourceImpactResponse struct {
	Jobs           []dal.SourceJobRecord     `json:"jobs"`
	Automations    []dal.SourceJobAutomation `json:"automations"`
	Namespaces     []string                  `json:"namespaces"`
	Candidates     []sourceImpactCandidate   `json:"unregistered_jobs"`
	DiscoveryError string                    `json:"discovery_error,omitempty"`
	Commit         string                    `json:"commit,omitempty"`
	Revision       string                    `json:"revision"`
}

func (s *APIServer) sourceRepositoryImpact(ctx context.Context, id string) (sourceImpactResponse, error) {
	out := sourceImpactResponse{Namespaces: []string{}, Candidates: []sourceImpactCandidate{}}
	rec, err := s.sources.GetRepository(ctx, id)
	if err != nil {
		return out, err
	}
	out.Jobs, err = s.sources.ListSourceJobs(ctx, id)
	if err != nil {
		return out, err
	}
	out.Automations, err = s.sources.SourceJobAutomations(ctx, id)
	if err != nil {
		return out, err
	}
	grants, err := s.sources.RepositoryNamespaces(ctx, id)
	if err != nil {
		return out, err
	}
	for _, ns := range grants {
		out.Namespaces = append(out.Namespaces, ns.Path)
	}
	ref := rec.DefaultRef
	if ref == "" {
		ref = "HEAD"
	}
	listing, discoveryErr := s.listSourceRepositoryDefinitionFiles(ctx, rec, sourcepkg.ListDefinitionFilesOptions{Conventional: true, Ref: ref, Limit: 1000})
	if discoveryErr != nil {
		out.DiscoveryError = "Source discovery is unavailable; the registered jobs and automation below are still affected."
	} else {
		out.Commit = listing.Revision.Commit
		registered := map[string]bool{}
		for _, job := range out.Jobs {
			registered[job.JobID] = true
		}
		for _, file := range listing.Files {
			if jobID, err := sourcepkg.ConventionalJobID(file.Path); err == nil && !registered[jobID] {
				out.Candidates = append(out.Candidates, sourceImpactCandidate{jobID, file.Path})
			}
		}
		if listing.Truncated {
			out.DiscoveryError = "Source discovery exceeds 1000 files. Additional unregistered jobs may also be affected."
		}
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return out, err
	}
	digest := sha256.Sum256(encoded)
	out.Revision = hex.EncodeToString(digest[:])
	return out, nil
}
func (s *APIServer) GetSourceRepositoryImpact(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := s.handlerDBCtx(r.Context())
	defer cancel()
	p, ok := s.requirePrincipal(w, r)
	if !ok || !s.requireSources(w) || !s.requireNamespaces(w) {
		return
	}
	if !s.authorizeNamespace(ctx, w, p, authz.ActionAdmin, "/") {
		return
	}
	impact, err := s.sourceRepositoryImpact(ctx, r.PathValue("id"))
	if err != nil {
		s.writeSourceDefinitionError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, impact)
}
