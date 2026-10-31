package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"vectis/internal/interfaces/mocks"
)

func TestRunAttributionUsesAuthenticatedSubmitter(t *testing.T) {
	s, token, _ := securityBoundaryServer(t)
	s.SetQueueClient(mocks.NewMockQueueService())
	handler := s.Handler()
	created := securityBoundaryRequest(t, handler, http.MethodPost, "/api/v1/jobs/run", token, map[string]any{
		"submitted_by": "forged-user",
		"job":          map[string]any{"id": "actor-check", "root": map[string]any{"id": "root", "uses": "builtins/script", "with": map[string]any{"script": "true"}}},
	})

	if created.Code != http.StatusAccepted {
		t.Fatalf("create=%d %s", created.Code, created.Body.String())
	}

	var run struct {
		RunID string `json:"run_id"`
	}

	if err := json.Unmarshal(created.Body.Bytes(), &run); err != nil {
		t.Fatal(err)
	}

	uid, err := s.authRepo.CreateLocalUser(t.Context(), "viewer", dummyBcryptHash)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.roleBindings.Create(t.Context(), uid, 1, "admin"); err != nil {
		t.Fatal(err)
	}

	viewerToken := "actor-viewer-token"
	if _, err := s.authRepo.CreateAPIToken(t.Context(), uid, hashAPIToken(viewerToken), "viewer", nil); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{"/api/v1/runs/" + run.RunID, "/api/v1/runs"} {
		got := securityBoundaryRequest(t, handler, http.MethodGet, path, viewerToken, nil)
		if got.Code != http.StatusOK {
			t.Fatalf("read %s=%d %s", path, got.Code, got.Body.String())
		}

		var detail struct {
			SubmittedBy string `json:"submitted_by"`
			Data        []struct {
				SubmittedBy string `json:"submitted_by"`
			} `json:"data"`
		}

		if err := json.Unmarshal(got.Body.Bytes(), &detail); err != nil {
			t.Fatal(err)
		}

		actor := detail.SubmittedBy
		if len(detail.Data) > 0 {
			actor = detail.Data[0].SubmittedBy
		}

		if actor != "audit-root" {
			t.Fatalf("actor=%q, want original submitter audit-root", actor)
		}
	}
}
