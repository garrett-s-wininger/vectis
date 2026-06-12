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

	repositoryDeclared := sourceRepositoryDeclarationIDSet(repositoryDecls)
	scheduleDeclared := sourceScheduleDeclarationIDSet(scheduleDecls)

	repositoryCounts := sourceStatusRepositoryCounts{}
	scheduleCounts := sourceStatusScheduleCounts{}
	if s.namespaces != nil && (s.sources != nil || s.schedules != nil) {
		namespaces, err := s.namespaces.List(r.Context())
		if err != nil {
			if s.handleDBUnavailableError(w, err) {
				return
			}

			s.logger.Error("Database error listing namespaces for source status: %v", err)
			writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
			return
		}

		if s.sources != nil {
			repositoryCounts, err = s.sourceStatusRepositoryCounts(r, namespaces, repositoryDeclared)
			if err != nil {
				if s.handleDBUnavailableError(w, err) {
					return
				}

				s.logger.Error("Database error listing repositories for source status: %v", err)
				writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
				return
			}
		}

		if s.schedules != nil {
			scheduleCounts, err = s.sourceStatusScheduleCounts(r, namespaces, scheduleDeclared)
			if err != nil {
				if s.handleDBUnavailableError(w, err) {
					return
				}

				s.logger.Error("Database error listing schedules for source status: %v", err)
				writeAPIErrorCode(w, http.StatusInternalServerError, apiErrInternal)
				return
			}
		}
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

func (s *APIServer) sourceStatusRepositoryCounts(r *http.Request, namespaces []dal.NamespaceRecord, declared map[string]struct{}) (sourceStatusRepositoryCounts, error) {
	counts := sourceStatusRepositoryCounts{}
	for _, ns := range namespaces {
		recs, err := s.sources.ListRepositories(r.Context(), ns.ID)
		if err != nil {
			return sourceStatusRepositoryCounts{}, err
		}

		for _, rec := range recs {
			counts.Total++
			if rec.Enabled {
				counts.Enabled++
			} else {
				counts.Disabled++
			}

			if sourceRepositoryIsDeclared(rec.RepositoryID, declared) {
				counts.Declared++
			} else if rec.Enabled {
				counts.StaleEnabled++
			} else {
				counts.StaleDisabled++
			}

			switch strings.TrimSpace(rec.SyncStatus) {
			case dal.SourceSyncStatusSucceeded:
				counts.SyncSucceeded++
			case dal.SourceSyncStatusFailed:
				counts.SyncFailed++
			case dal.SourceSyncStatusRunning:
				counts.SyncRunning++
			default:
				counts.SyncNever++
			}
		}
	}

	return counts, nil
}

func (s *APIServer) sourceStatusScheduleCounts(r *http.Request, namespaces []dal.NamespaceRecord, declared map[string]struct{}) (sourceStatusScheduleCounts, error) {
	counts := sourceStatusScheduleCounts{}
	for _, ns := range namespaces {
		recs, err := s.schedules.ListSourceCronSchedules(r.Context(), ns.ID, "")
		if err != nil {
			return sourceStatusScheduleCounts{}, err
		}

		for _, rec := range recs {
			counts.Total++
			if rec.Enabled {
				counts.Enabled++
			} else {
				counts.Disabled++
			}

			if sourceCronScheduleIsDeclared(rec.ScheduleID, declared) {
				counts.Declared++
			} else if rec.Enabled {
				counts.StaleEnabled++
			} else {
				counts.StaleDisabled++
			}

			if sourceCronScheduleHasOverride(rec) {
				counts.ActiveOverrides++
			}
		}
	}

	return counts, nil
}

func sourceRepositoryDeclarationIDSet(decls []config.SourceRepositoryDeclaration) map[string]struct{} {
	declared := make(map[string]struct{}, len(decls))
	for _, decl := range decls {
		if repositoryID := strings.TrimSpace(decl.RepositoryID); repositoryID != "" {
			declared[repositoryID] = struct{}{}
		}
	}
	return declared
}

func sourceScheduleDeclarationIDSet(decls []config.SourceScheduleDeclaration) map[string]struct{} {
	declared := make(map[string]struct{}, len(decls))
	for _, decl := range decls {
		if scheduleID := strings.TrimSpace(decl.ScheduleID); scheduleID != "" {
			declared[scheduleID] = struct{}{}
		}
	}
	return declared
}
