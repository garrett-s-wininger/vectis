package api

import (
	"net/http"
	"strings"

	"vectis/internal/config"
	"vectis/internal/dal"
)

type sourceStatusResponse struct {
	StoredJobsEnabled      bool                         `json:"stored_jobs_enabled"`
	RepositoriesConfigured bool                         `json:"repositories_configured"`
	SourceJobsConfigured   bool                         `json:"source_jobs_configured"`
	SchedulesConfigured    bool                         `json:"schedules_configured"`
	DeclaredRepositories   int                          `json:"declared_repositories"`
	DeclaredSchedules      int                          `json:"declared_schedules"`
	Repositories           sourceStatusRepositoryCounts `json:"repositories"`
	Schedules              sourceStatusScheduleCounts   `json:"schedules"`
}

type sourceStatusRepositoryCounts struct {
	Total         int `json:"total"`
	Enabled       int `json:"enabled"`
	Disabled      int `json:"disabled"`
	Declared      int `json:"declared"`
	StaleEnabled  int `json:"stale_enabled"`
	StaleDisabled int `json:"stale_disabled"`
	SyncSucceeded int `json:"sync_succeeded"`
	SyncFailed    int `json:"sync_failed"`
	SyncRunning   int `json:"sync_running"`
	SyncNever     int `json:"sync_never"`
}

type sourceStatusScheduleCounts struct {
	Total           int `json:"total"`
	Enabled         int `json:"enabled"`
	Disabled        int `json:"disabled"`
	Declared        int `json:"declared"`
	StaleEnabled    int `json:"stale_enabled"`
	StaleDisabled   int `json:"stale_disabled"`
	ActiveOverrides int `json:"active_overrides"`
}

func (s *APIServer) GetSourceStatus(w http.ResponseWriter, r *http.Request) {
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

	repositoryDeclared := sourceStatusRepositoryDeclarationIDs(repositoryDecls)
	scheduleDeclared := sourceStatusScheduleDeclarationIDs(scheduleDecls)

	repositoryCounts := sourceStatusRepositoryCounts{}
	scheduleCounts := sourceStatusScheduleCounts{}
	if s.sources != nil {
		counts, err := s.sources.CountRepositories(r.Context(), repositoryDeclared)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error counting repositories for source status: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}

		repositoryCounts = sourceStatusRepositoryCountsFromDAL(counts)
	}

	if s.schedules != nil {
		counts, err := s.schedules.CountSourceCronSchedules(r.Context(), scheduleDeclared)
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error counting schedules for source status: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}

		scheduleCounts = sourceStatusScheduleCountsFromDAL(counts)
	}

	writeJSON(w, http.StatusOK, sourceStatusResponse{
		StoredJobsEnabled:      config.SourceStoredJobsEnabled(),
		RepositoriesConfigured: s.sources != nil,
		SourceJobsConfigured:   s.sourceJobs != nil,
		SchedulesConfigured:    s.schedules != nil,
		DeclaredRepositories:   len(repositoryDecls),
		DeclaredSchedules:      len(scheduleDecls),
		Repositories:           repositoryCounts,
		Schedules:              scheduleCounts,
	})
}

func sourceStatusRepositoryCountsFromDAL(counts dal.SourceRepositoryCountSummary) sourceStatusRepositoryCounts {
	return sourceStatusRepositoryCounts{
		Total:         counts.Total,
		Enabled:       counts.Enabled,
		Disabled:      counts.Disabled,
		Declared:      counts.Declared,
		StaleEnabled:  counts.StaleEnabled,
		StaleDisabled: counts.StaleDisabled,
		SyncSucceeded: counts.SyncSucceeded,
		SyncFailed:    counts.SyncFailed,
		SyncRunning:   counts.SyncRunning,
		SyncNever:     counts.SyncNever,
	}
}

func sourceStatusScheduleCountsFromDAL(counts dal.SourceCronScheduleCountSummary) sourceStatusScheduleCounts {
	return sourceStatusScheduleCounts{
		Total:           counts.Total,
		Enabled:         counts.Enabled,
		Disabled:        counts.Disabled,
		Declared:        counts.Declared,
		StaleEnabled:    counts.StaleEnabled,
		StaleDisabled:   counts.StaleDisabled,
		ActiveOverrides: counts.ActiveOverrides,
	}
}

func sourceStatusRepositoryDeclarationIDs(decls []config.SourceRepositoryDeclaration) []string {
	declared := make([]string, 0, len(decls))
	for _, decl := range decls {
		if repositoryID := strings.TrimSpace(decl.RepositoryID); repositoryID != "" {
			declared = append(declared, repositoryID)
		}
	}

	return declared
}

func sourceStatusScheduleDeclarationIDs(decls []config.SourceScheduleDeclaration) []string {
	declared := make([]string, 0, len(decls))
	for _, decl := range decls {
		if scheduleID := strings.TrimSpace(decl.ScheduleID); scheduleID != "" {
			declared = append(declared, scheduleID)
		}
	}

	return declared
}
