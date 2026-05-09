package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestEffectiveToken_envOverridesFile(t *testing.T) {
	t.Setenv("VECTIS_API_TOKEN", "env-token")

	if got := effectiveToken(); got != "env-token" {
		t.Fatalf("expected env-token, got %s", got)
	}
}

func TestEffectiveToken_fallbackToFile(t *testing.T) {
	t.Setenv("VECTIS_API_TOKEN", "")

	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	path, err := cliTokenFilePath()
	if err != nil {
		t.Fatalf("token path: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir token dir: %v", err)
	}

	if err := os.WriteFile(path, []byte("file-token\n"), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}

	if got := effectiveToken(); got != "file-token" {
		t.Fatalf("expected file-token, got %s", got)
	}
}

func TestEffectiveToken_empty(t *testing.T) {
	t.Setenv("VECTIS_API_TOKEN", "")
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	if got := effectiveToken(); got != "" {
		t.Fatalf("expected empty, got %s", got)
	}
}

func TestTokenPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	// Write
	if err := writePersistedToken("secret"); err != nil {
		t.Fatal(err)
	}

	// Read
	if got := readPersistedToken(); got != "secret" {
		t.Fatalf("expected secret, got %s", got)
	}

	// Delete
	if err := deletePersistedToken(); err != nil {
		t.Fatal(err)
	}

	if got := readPersistedToken(); got != "" {
		t.Fatalf("expected empty after delete, got %s", got)
	}
}

func TestWritePersistedToken_createsDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	path, _ := cliTokenFilePath()
	_ = os.RemoveAll(filepath.Dir(path))

	if err := writePersistedToken("tok"); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("token file not created: %v", err)
	}
}

func TestResetTargets(t *testing.T) {
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "data")
	cacheDir := filepath.Join(tmpDir, "cache")
	deployDir := filepath.Join(tmpDir, "deploy")
	t.Setenv("HOME", tmpDir)
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmpDir, "config"))
	t.Setenv("XDG_DATA_HOME", dataDir)
	t.Setenv("XDG_CACHE_HOME", cacheDir)
	t.Setenv(envDeployConfigDir, deployDir)

	configDir, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("config dir: %v", err)
	}

	cacheHome, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("cache dir: %v", err)
	}

	targets, err := resetTargets()
	if err != nil {
		t.Fatalf("reset targets: %v", err)
	}

	want := []string{
		filepath.Join(cacheHome, "vectis"),
		filepath.Join(configDir, "vectis"),
		filepath.Join(dataDir, "vectis"),
		filepath.Join(deployDir, "podman"),
	}
	sort.Strings(want)

	if strings.Join(targets, "\n") != strings.Join(want, "\n") {
		t.Fatalf("targets mismatch\ngot:\n%s\nwant:\n%s", strings.Join(targets, "\n"), strings.Join(want, "\n"))
	}
}

// rewriteTransport rewrites all outgoing requests to a test server URL.
type rewriteTransport struct {
	testURL    string
	underlying http.RoundTripper
}

func (rt *rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(rt.testURL, "http://")
	return rt.underlying.RoundTrip(req)
}

func setupTestAPIClient(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	oldClient := apiHTTPClient
	apiHTTPClient = &http.Client{
		Transport: &rewriteTransport{testURL: srv.URL, underlying: http.DefaultTransport},
	}
	t.Cleanup(func() { apiHTTPClient = oldClient })

	return srv
}

func TestTokenList_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/tokens" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if auth := r.Header.Get("Authorization"); auth != "Bearer test-token" {
			t.Errorf("Authorization=%q", auth)
		}

		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 1, "label": "prod", "expires_at": nil, "created_at": "2024-01-01", "last_used_at": nil},
		})
	})

	t.Setenv("VECTIS_API_TOKEN", "test-token")

	var buf bytes.Buffer
	if err := tokenList(&buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "prod") {
		t.Fatalf("expected output to contain 'prod', got: %s", out)
	}
}

func TestTokenList_unexpectedStatus(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	if err := tokenList(io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestTokenCreate_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/tokens" {
			t.Errorf("path=%s", r.URL.Path)
		}

		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["label"] != "my-label" {
			t.Errorf("label=%v", body["label"])
		}

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": 42, "label": "my-label", "token": "secret-token", "expires_at": "",
		})
	})

	var buf bytes.Buffer
	if err := tokenCreate("my-label", "never", 0, &buf); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.Contains(out, "secret-token") {
		t.Fatalf("expected token in output, got: %s", out)
	}
}

func TestTokenCreate_forbidden(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	})

	if err := tokenCreate("x", "never", 0, io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestTokenDelete_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/tokens/7" {
			t.Errorf("path=%s", r.URL.Path)
		}

		w.WriteHeader(http.StatusNoContent)
	})

	if err := tokenDelete("7"); err != nil {
		t.Fatal(err)
	}
}

func TestTokenDelete_notFound(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	if err := tokenDelete("99"); err == nil {
		t.Fatal("expected error")
	}
}

func TestListJobNames_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/jobs" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"name": "z-job"},
				{"name": "a-job"},
			},
		})
	})

	var buf bytes.Buffer
	if err := listJobNames(&buf); err != nil {
		t.Fatal(err)
	}

	if got, want := buf.String(), "a-job\nz-job\n"; got != want {
		t.Fatalf("output: want %q, got %q", want, got)
	}
}

func TestListJobNames_rejectsUnexpectedShape(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"name": "legacy-shape"},
		})
	})

	if err := listJobNames(io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestNamespaceList_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/namespaces" {
			t.Errorf("path=%s", r.URL.Path)
		}

		parent := int64(1)
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{"id": 2, "name": "team-a", "path": "/team-a", "parent_id": parent, "break_inheritance": false},
		})
	})

	var buf bytes.Buffer
	if err := namespaceList(&buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.String(); !strings.Contains(got, "2\t/team-a\tname=team-a\tparent=1") {
		t.Fatalf("unexpected output: %s", got)
	}
}

func TestUserCreate_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/users" {
			t.Errorf("path=%s", r.URL.Path)
		}

		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["username"] != "alice" || body["password"] != "secret-password" {
			t.Errorf("unexpected body: %v", body)
		}

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": 3, "username": "alice", "enabled": true, "created_at": "2026-05-09T00:00:00Z",
		})
	})

	var buf bytes.Buffer
	if err := userCreate("alice", "secret-password", &buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.String(); !strings.Contains(got, "User created: 3 alice") {
		t.Fatalf("unexpected output: %s", got)
	}
}

func TestBindingDelete_escapesRole(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/namespaces/2/bindings/3" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if role := r.URL.Query().Get("role"); role != "admin:*" {
			t.Errorf("role=%q", role)
		}

		w.WriteHeader(http.StatusNoContent)
	})

	if err := bindingDelete(2, 3, "admin:*"); err != nil {
		t.Fatal(err)
	}
}

func TestGetRun_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/runs/run-1" {
			t.Errorf("path=%s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":         "run-1",
			"run_index":      3,
			"status":         "failed",
			"failure_code":   "execution",
			"failure_reason": "exit code 1",
		})
	})

	var buf bytes.Buffer
	if err := getRun("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	want := strings.Join([]string{
		"run_id=run-1",
		"run_index=3",
		"status=failed",
		"failure_code=execution",
		"failure_reason=exit code 1",
		"",
	}, "\n")
	if got := buf.String(); got != want {
		t.Fatalf("output: want %q, got %q", want, got)
	}
}

func TestGetRun_notFound(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	if err := getRun("missing", io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestCancelRun_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/runs/run-1/cancel" {
			t.Errorf("path=%s", r.URL.Path)
		}

		w.WriteHeader(http.StatusNoContent)
	})

	var buf bytes.Buffer
	if err := cancelRun("run-1", &buf); err != nil {
		t.Fatal(err)
	}

	if got := buf.String(); !strings.Contains(got, "Run run-1 cancel requested.") {
		t.Fatalf("unexpected output: %s", got)
	}
}

func TestCancelRun_conflict(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})

	if err := cancelRun("done", io.Discard); err == nil {
		t.Fatal("expected error")
	}
}

func TestDoLogin_success(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/login" {
			t.Errorf("path=%s", r.URL.Path)
		}

		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["username"] != "admin" || body["password"] != "secret" {
			t.Errorf("unexpected body: %v", body)
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"token": "login-token", "user_id": 1, "expires_at": "2025-01-01",
		})
	})

	token, err := doLogin("admin", "secret")
	if err != nil {
		t.Fatal(err)
	}

	if token != "login-token" {
		t.Fatalf("expected login-token, got %s", token)
	}
}

func TestDoLogin_unauthorized(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})

	_, err := doLogin("admin", "wrong")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDoLogin_serviceUnavailable(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not ready"})
	})

	_, err := doLogin("admin", "secret")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDoLogin_unexpectedStatus(t *testing.T) {
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	_, err := doLogin("admin", "secret")
	if err == nil {
		t.Fatal("expected error")
	}
}
