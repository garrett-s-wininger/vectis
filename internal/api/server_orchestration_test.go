package api_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"vectis/internal/api"
	"vectis/internal/interfaces/mocks"
)

func TestAPIServer_TriggerJob_RequiresRepositoryID(t *testing.T) {
	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), mocks.NewMockJobsRepository(), mocks.NewMockRunsRepository(), mocks.StubEphemeralRunStarter{})

	req := httptest.NewRequest(http.MethodPost, "/api/v1/jobs/trigger/job-1", nil)
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.TriggerJob(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "missing_repository_id")
}

func TestAPIServer_GetJobRuns_RequiresRepositoryID(t *testing.T) {
	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), mocks.NewMockJobsRepository(), mocks.NewMockRunsRepository(), mocks.StubEphemeralRunStarter{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-1/runs?after_index=1", nil)
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.GetJobRuns(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "missing_repository_id")
}

func TestAPIServer_GetJobRuns_ValidatesSourceRunFilters(t *testing.T) {
	server := api.NewAPIServerWithRepositories(mocks.NewMockLogger(), mocks.NewMockJobsRepository(), mocks.NewMockRunsRepository(), mocks.StubEphemeralRunStarter{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/job-1/runs?repository_id=repo-1&since=not-time", nil)
	req.SetPathValue("id", "job-1")
	rec := httptest.NewRecorder()

	server.GetJobRuns(rec, req)

	assertAPIError(t, rec, http.StatusBadRequest, "invalid_since")
}
