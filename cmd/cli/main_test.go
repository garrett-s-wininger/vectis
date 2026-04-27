package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	t.Setenv("XDG_CONFIG_HOME", tmpDir)

	path := filepath.Join(tmpDir, "vectis", "token")
	_ = os.MkdirAll(filepath.Dir(path), 0o700)
	_ = os.WriteFile(path, []byte("file-token\n"), 0o600)

	if got := effectiveToken(); got != "file-token" {
		t.Fatalf("expected file-token, got %s", got)
	}
}

func TestEffectiveToken_empty(t *testing.T) {
	t.Setenv("VECTIS_API_TOKEN", "")
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	if got := effectiveToken(); got != "" {
		t.Fatalf("expected empty, got %s", got)
	}
}

func TestTokenPersistence(t *testing.T) {
	tmpDir := t.TempDir()
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

		_ = json.NewEncoder(w).Encode([]map[string]interface{}{
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

		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["label"] != "my-label" {
			t.Errorf("label=%v", body["label"])
		}

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
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

		_ = json.NewEncoder(w).Encode(map[string]interface{}{
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
