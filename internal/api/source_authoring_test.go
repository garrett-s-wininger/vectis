package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"vectis/internal/dal"
	"vectis/internal/interfaces/mocks"
	sourcepkg "vectis/internal/source"
	"vectis/internal/testutil/dbtest"
)

type definitionAuthorFunc func(context.Context, sourcepkg.WriteDefinitionRequest) (sourcepkg.WrittenDefinition, error)

func (f definitionAuthorFunc) WriteDefinition(ctx context.Context, req sourcepkg.WriteDefinitionRequest) (sourcepkg.WrittenDefinition, error) {
	return f(ctx, req)
}

func (f definitionAuthorFunc) DeleteDefinition(context.Context, sourcepkg.DeleteDefinitionRequest) (sourcepkg.WrittenDefinition, error) {
	return sourcepkg.WrittenDefinition{}, sourcepkg.ErrAuthoringUnavailable
}

func TestAPIServer_SourceDefinitionAuthoringHooks(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	db := dbtest.NewTestDB(t)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	handler := server.Handler()

	if _, err := dal.NewSQLRepositories(db).Sources().CreateRepository(context.Background(), dal.SourceRepositoryRecord{
		RepositoryID:  "external-author",
		NamespaceID:   1,
		SourceKind:    dal.SourceKindLocalCheckout,
		CheckoutPath:  filepath.Join(t.TempDir(), "mirror"),
		CheckoutMode:  dal.SourceCheckoutModeExternal,
		AuthoringMode: dal.SourceAuthoringModeExternalChangeRequest,
		DefaultRef:    "main",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	var gotRec dal.SourceRepositoryRecord
	var gotReq sourcepkg.WriteDefinitionRequest
	server.SetSourceDefinitionAuthoring(
		func(rec dal.SourceRepositoryRecord) (sourcepkg.DefinitionAuthor, error) {
			gotRec = rec
			return definitionAuthorFunc(func(_ context.Context, req sourcepkg.WriteDefinitionRequest) (sourcepkg.WrittenDefinition, error) {
				gotReq = req
				return sourcepkg.WrittenDefinition{
					RequestedRef: req.Branch,
					Commit:       "0123456789abcdef0123456789abcdef01234567",
					Path:         req.Path,
					BlobSHA:      "abcdef0123456789abcdef0123456789abcdef01",
				}, nil
			}), nil
		},
		func(rec dal.SourceRepositoryRecord) sourcepkg.AuthoringCapability {
			return sourcepkg.AuthoringCapability{
				Mode:                   rec.AuthoringMode,
				WriteDefinitions:       true,
				ExternalChangeRequests: true,
			}
		},
	)

	getRec := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/api/v1/source-repositories/external-author", nil)
	handler.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get source repository: status=%d body=%s", getRec.Code, getRec.Body.String())
	}

	var getResp sourceRepositoryResponse
	if err := json.NewDecoder(getRec.Body).Decode(&getResp); err != nil {
		t.Fatalf("decode source repository response: %v", err)
	}

	if getResp.Authoring.Mode != dal.SourceAuthoringModeExternalChangeRequest ||
		!getResp.Authoring.WriteDefinitions ||
		!getResp.Authoring.ExternalChangeRequests ||
		getResp.Authoring.LocalCommits ||
		getResp.Authoring.Reason != "" {
		t.Fatalf("authoring response mismatch: %+v", getResp.Authoring)
	}

	body, err := json.Marshal(map[string]any{
		"branch":        "feature/source-authoring",
		"path":          ".vectis/jobs/custom-build.json",
		"message":       "open change request",
		"expected_head": "fedcba9876543210fedcba9876543210fedcba98",
		"definition": map[string]any{
			"root": map[string]any{
				"id":   "root",
				"uses": "builtins/script",
				"with": map[string]any{"script": "true"},
			},
		},
	})

	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	putRec := httptest.NewRecorder()
	putReq := httptest.NewRequest(http.MethodPut, "/api/v1/source-repositories/external-author/jobs/build/definition", bytes.NewReader(body))
	putReq.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("put source definition: status=%d body=%s", putRec.Code, putRec.Body.String())
	}

	if gotRec.RepositoryID != "external-author" {
		t.Fatalf("author factory repository = %+v", gotRec)
	}

	if gotReq.Branch != "feature/source-authoring" ||
		gotReq.Path != ".vectis/jobs/custom-build.json" ||
		gotReq.Message != "open change request" ||
		gotReq.ExpectedHead != "fedcba9876543210fedcba9876543210fedcba98" ||
		!strings.Contains(gotReq.DefinitionJSON, "builtins/script") {
		t.Fatalf("write definition request mismatch: %+v", gotReq)
	}

	var putResp sourceRepositoryJobDefinitionResponse
	if err := json.NewDecoder(putRec.Body).Decode(&putResp); err != nil {
		t.Fatalf("decode put response: %v", err)
	}

	if putResp.JobID != "build" ||
		putResp.Source.RepositoryID != "external-author" ||
		putResp.Source.RequestedRef != "feature/source-authoring" ||
		putResp.Source.ResolvedCommit != "0123456789abcdef0123456789abcdef01234567" ||
		putResp.Source.Path != ".vectis/jobs/custom-build.json" ||
		putResp.Source.BlobSHA != "abcdef0123456789abcdef0123456789abcdef01" {
		t.Fatalf("put response mismatch: %+v", putResp)
	}
}

func TestAPIServer_SourceDefinitionAuthoringBusyError(t *testing.T) {
	t.Setenv("VECTIS_API_AUTH_ENABLED", "false")

	db := dbtest.NewTestDB(t)
	server := NewAPIServer(mocks.NewMockLogger(), db)
	handler := server.Handler()

	if _, err := dal.NewSQLRepositories(db).Sources().CreateRepository(context.Background(), dal.SourceRepositoryRecord{
		RepositoryID:  "managed-author",
		NamespaceID:   1,
		SourceKind:    dal.SourceKindLocalCheckout,
		CheckoutPath:  filepath.Join(t.TempDir(), "managed"),
		CheckoutMode:  dal.SourceCheckoutModeManaged,
		AuthoringMode: dal.SourceAuthoringModeLocalCommit,
		DefaultRef:    "main",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("CreateRepository: %v", err)
	}

	server.SetSourceDefinitionAuthoring(
		func(dal.SourceRepositoryRecord) (sourcepkg.DefinitionAuthor, error) {
			return definitionAuthorFunc(func(context.Context, sourcepkg.WriteDefinitionRequest) (sourcepkg.WrittenDefinition, error) {
				return sourcepkg.WrittenDefinition{}, sourcepkg.ErrBusy
			}), nil
		},
		func(rec dal.SourceRepositoryRecord) sourcepkg.AuthoringCapability {
			return sourcepkg.AuthoringCapability{
				Mode:             rec.AuthoringMode,
				WriteDefinitions: true,
				LocalCommits:     true,
			}
		},
	)

	body, err := json.Marshal(map[string]any{
		"definition": map[string]any{
			"root": map[string]any{
				"id":   "root",
				"uses": "builtins/script",
				"with": map[string]any{"script": "true"},
			},
		},
	})

	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/api/v1/source-repositories/managed-author/jobs/build/definition", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("expected source busy status %d, got %d: %s", http.StatusConflict, rec.Code, rec.Body.String())
	}

	var errResp struct {
		Code    string            `json:"code"`
		Details map[string]string `json:"details"`
	}

	if err := json.NewDecoder(rec.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode source busy response: %v; body=%s", err, rec.Body.String())
	}

	if errResp.Code != "source_busy" || errResp.Details["kind"] != "source_busy" {
		t.Fatalf("source busy response mismatch: %+v", errResp)
	}
}
