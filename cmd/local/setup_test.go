package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/spf13/viper"

	"vectis/internal/interfaces"
)

func TestLocalSetupSkipsAuto(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("auth_enabled", true)
	viper.Set("setup", localSetupAuto)

	if localSetupSkipsAuto("written to /tmp/local-bootstrap-token") {
		t.Fatal("generated local bootstrap token should finish setup")
	}

	if !localSetupSkipsAuto("from VECTIS_API_AUTH_BOOTSTRAP_TOKEN") {
		t.Fatal("operator bootstrap token should skip automatic setup")
	}

	viper.Set("setup", localSetupOff)
	if !localSetupSkipsAuto("written to /tmp/local-bootstrap-token") {
		t.Fatal("--setup=off should skip automatic setup")
	}

	viper.Set("setup", localSetupAuto)
	viper.Set("auth_enabled", false)
	if !localSetupSkipsAuto("written to /tmp/local-bootstrap-token") {
		t.Fatal("auth disabled should skip automatic setup")
	}
}

func TestLocalSetupAPIHostLoopback(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("host", "")
	if got := localSetupAPIHost(); got != "localhost" {
		t.Fatalf("empty host api host = %q, want localhost", got)
	}

	for _, host := range []string{"0.0.0.0", "::", "[::]"} {
		viper.Set("host", host)
		if got := localSetupAPIHost(); got != "127.0.0.1" {
			t.Fatalf("host %q api host = %q, want 127.0.0.1", host, got)
		}
	}

	viper.Set("host", "localhost")
	if got := localSetupAPIHost(); got != "localhost" {
		t.Fatalf("api host = %q, want localhost", got)
	}
}

func TestFinishLocalSetupGeneratesAdminAndToken(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("auth_enabled", true)
	viper.Set("setup", localSetupAuto)
	dataHome := t.TempDir()
	configHome := t.TempDir()
	t.Setenv("XDG_DATA_HOME", dataHome)
	t.Setenv("XDG_CONFIG_HOME", configHome)

	var sawComplete bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": false, "auth_enabled": true})
		case "/api/v1/setup/complete":
			sawComplete = true
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode setup body: %v", err)
			}

			if body["bootstrap_token"] != "bootstrap-token" || body["admin_username"] != "admin" ||
				len(body["admin_password"]) < localAdminPasswordMinLen {
				t.Errorf("setup body = %#v", body)
			}

			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"api_token": "cli-token", "username": "admin"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	in, _ := nonInteractivePipe(t)
	result := finishLocalSetup(
		t.Context(),
		interfaces.NewLogger("test"),
		server.URL,
		nil,
		"bootstrap-token",
		"written to token",
		in,
		os.Stderr,
	)
	if result.Err != nil {
		t.Fatalf("finishLocalSetup: %v", result.Err)
	}

	if !sawComplete || !result.Created || result.Username != "admin" {
		t.Fatalf("result = %#v, sawComplete=%v", result, sawComplete)
	}

	passwordPath := filepath.Join(dataHome, "vectis", localAdminPasswordFile)
	password, err := os.ReadFile(passwordPath)
	if err != nil {
		t.Fatalf("read password: %v", err)
	}

	if strings.TrimSpace(string(password)) == "" {
		t.Fatal("password file is empty")
	}

	assertMode0600(t, passwordPath)

	tokenPath := filepath.Join(configHome, "vectis", "token")
	token, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}

	if string(token) != "cli-token" {
		t.Fatalf("token = %q", token)
	}

	assertMode0600(t, tokenPath)
	if result.TokenPath != tokenPath || result.TokenExisted {
		t.Fatalf("token result = %#v", result)
	}
}

func TestFinishLocalSetupLeavesExistingCLIToken(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("auth_enabled", true)
	viper.Set("setup", localSetupAuto)
	t.Setenv("XDG_DATA_HOME", t.TempDir())

	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)

	tokenPath := filepath.Join(configHome, "vectis", "token")
	if err := writePrivateFile(tokenPath, []byte("existing-token")); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": false, "auth_enabled": true})
		case "/api/v1/setup/complete":
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]string{"api_token": "new-token", "username": "admin"})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	in, _ := nonInteractivePipe(t)
	result := finishLocalSetup(
		t.Context(),
		interfaces.NewLogger("test"),
		server.URL,
		nil,
		"bootstrap-token",
		"written to token",
		in,
		os.Stderr,
	)
	if result.Err != nil {
		t.Fatalf("finishLocalSetup: %v", result.Err)
	}

	if !result.TokenExisted {
		t.Fatal("existing CLI token was replaced")
	}

	token, err := os.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}

	if string(token) != "existing-token" {
		t.Fatalf("token = %q", token)
	}
}

func TestFinishLocalSetupConflictDoesNotWriteToken(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("auth_enabled", true)
	viper.Set("setup", localSetupAuto)
	t.Setenv("XDG_DATA_HOME", t.TempDir())

	configHome := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", configHome)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/setup/status":
			_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": false, "auth_enabled": true})
		case "/api/v1/setup/complete":
			w.WriteHeader(http.StatusConflict)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	in, _ := nonInteractivePipe(t)
	result := finishLocalSetup(
		t.Context(),
		interfaces.NewLogger("test"),
		server.URL,
		nil,
		"bootstrap-token",
		"written to token",
		in,
		os.Stderr,
	)
	if result.Err != nil {
		t.Fatalf("finishLocalSetup: %v", result.Err)
	}

	if !result.AlreadyExists || result.Created {
		t.Fatalf("result = %#v", result)
	}

	if _, err := os.Stat(filepath.Join(configHome, "vectis", "token")); !os.IsNotExist(err) {
		t.Fatalf("token file err = %v, want not exist", err)
	}
}

func TestFinishLocalSetupAlreadyCompleteSkipsCreate(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("auth_enabled", true)
	viper.Set("setup", localSetupAuto)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/setup/status" {
			t.Errorf("unexpected request %s", r.URL.Path)
		}

		_ = json.NewEncoder(w).Encode(map[string]bool{"setup_complete": true, "auth_enabled": true})
	}))
	t.Cleanup(server.Close)

	in, _ := nonInteractivePipe(t)
	result := finishLocalSetup(
		t.Context(),
		interfaces.NewLogger("test"),
		server.URL,
		nil,
		"bootstrap-token",
		"written to token",
		in,
		os.Stderr,
	)
	if result.Err != nil {
		t.Fatalf("finishLocalSetup: %v", result.Err)
	}

	if !result.AlreadyExists {
		t.Fatalf("result = %#v", result)
	}
}

func TestFinishLocalSetupUsesServerURL(t *testing.T) {
	resetLocalTestConfig(t)
	viper.Set("auth_enabled", true)
	viper.Set("setup", localSetupOff)

	called := false
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	t.Cleanup(server.Close)

	in, _ := nonInteractivePipe(t)
	result := finishLocalSetup(
		t.Context(),
		interfaces.NewLogger("test"),
		server.URL,
		nil,
		"bootstrap-token",
		"written to token",
		in,
		os.Stderr,
	)
	if result.Err != nil || !result.Skipped || called {
		t.Fatalf("result=%#v called=%v", result, called)
	}
}

func nonInteractivePipe(t *testing.T) (*os.File, *os.File) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}

	t.Cleanup(func() {
		_ = reader.Close()
		_ = writer.Close()
	})

	return reader, writer
}

func assertMode0600(t *testing.T, path string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		return
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}

	if info.Mode().Perm() != 0o600 {
		t.Fatalf("%s mode = %v, want 0600", path, info.Mode().Perm())
	}
}
