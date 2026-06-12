package api

import (
	"net/http"

	"vectis/internal/config"
)

type sourceStatusResponse struct {
	StoredJobsEnabled      bool `json:"stored_jobs_enabled"`
	RepositoriesConfigured bool `json:"repositories_configured"`
	SourceJobsConfigured   bool `json:"source_jobs_configured"`
	SchedulesConfigured    bool `json:"schedules_configured"`
	DeclaredRepositories   int  `json:"declared_repositories"`
	DeclaredSchedules      int  `json:"declared_schedules"`
}

func (s *APIServer) GetSourceStatus(w http.ResponseWriter, _ *http.Request) {
	repositoryDecls, err := config.SourceRepositoryDeclarations()
	if err != nil {
		s.logger.Error("Configuration error getting source status repositories: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	scheduleDecls, err := config.SourceScheduleDeclarations()
	if err != nil {
		s.logger.Error("Configuration error getting source status schedules: %v", err)
		writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, sourceStatusResponse{
		StoredJobsEnabled:      config.SourceStoredJobsEnabled(),
		RepositoriesConfigured: s.sources != nil,
		SourceJobsConfigured:   s.sourceJobs != nil,
		SchedulesConfigured:    s.schedules != nil,
		DeclaredRepositories:   len(repositoryDecls),
		DeclaredSchedules:      len(scheduleDecls),
	})
}
