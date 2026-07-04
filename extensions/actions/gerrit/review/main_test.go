package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRequestFromEnvReadsWorkspacePassword(t *testing.T) {
	workspace := t.TempDir()
	passwordPath := filepath.Join(workspace, ".vectis", "secrets", "gerrit")
	if err := os.MkdirAll(passwordPath, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(passwordPath, "http-password"), []byte("secret-pass\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	req, err := requestFromEnv(mapEnv(map[string]string{
		"VECTIS_WORKSPACE":            workspace,
		"VECTIS_INPUT_URL":            "https://gerrit.example.com",
		"VECTIS_INPUT_CHANGE":         "project~master~Iabc",
		"VECTIS_INPUT_MESSAGE":        "Vectis succeeded",
		"VECTIS_INPUT_LABEL":          "Verified",
		"VECTIS_INPUT_VALUE":          "+1",
		"VECTIS_INPUT_USERNAME":       "ci-bot",
		"VECTIS_INPUT_PASSWORD_FILE":  ".vectis/secrets/gerrit/http-password",
		"VECTIS_INPUT_UNUSED_IGNORED": "ignored",
	}), os.ReadFile)

	if err != nil {
		t.Fatalf("requestFromEnv: %v", err)
	}

	if req.password != "secret-pass" {
		t.Fatalf("password = %q", req.password)
	}

	if req.revision != "current" {
		t.Fatalf("revision = %q", req.revision)
	}

	if req.labelVote != 1 {
		t.Fatalf("labelVote = %d", req.labelVote)
	}
}

func TestRequestFromEnvRejectsCredentialedURL(t *testing.T) {
	_, err := requestFromEnv(mapEnv(validEnvWith(t, map[string]string{
		"VECTIS_INPUT_URL": "https://ci-bot:secret@gerrit.example.com",
	})), os.ReadFile)

	if err == nil || !strings.Contains(err.Error(), "must not include embedded credentials") {
		t.Fatalf("requestFromEnv error = %v, want credentialed URL error", err)
	}
}

func TestRequestFromEnvRejectsPasswordFileEscape(t *testing.T) {
	_, err := requestFromEnv(mapEnv(validEnvWith(t, map[string]string{
		"VECTIS_INPUT_PASSWORD_FILE": "../outside",
	})), os.ReadFile)

	if err == nil || !strings.Contains(err.Error(), "must stay inside the workspace") {
		t.Fatalf("requestFromEnv error = %v, want workspace escape error", err)
	}
}

func TestRequestFromEnvRejectsValueWithoutLabel(t *testing.T) {
	_, err := requestFromEnv(mapEnv(validEnvWith(t, map[string]string{
		"VECTIS_INPUT_LABEL": "",
		"VECTIS_INPUT_VALUE": "+1",
	})), os.ReadFile)

	if err == nil || !strings.Contains(err.Error(), "value requires label") {
		t.Fatalf("requestFromEnv error = %v, want value requires label", err)
	}
}

func validEnvWith(t *testing.T, overrides map[string]string) map[string]string {
	t.Helper()

	workspace := tempWorkspaceForEnv(t)
	env := map[string]string{
		"VECTIS_WORKSPACE":           workspace,
		"VECTIS_INPUT_URL":           "https://gerrit.example.com",
		"VECTIS_INPUT_CHANGE":        "project~master~Iabc",
		"VECTIS_INPUT_MESSAGE":       "Vectis succeeded",
		"VECTIS_INPUT_USERNAME":      "ci-bot",
		"VECTIS_INPUT_PASSWORD_FILE": ".vectis/secrets/gerrit/http-password",
	}

	for key, value := range overrides {
		env[key] = value
	}

	return env
}

func tempWorkspaceForEnv(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	passwordPath := filepath.Join(dir, ".vectis", "secrets", "gerrit")
	if err := os.MkdirAll(passwordPath, 0o700); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(passwordPath, "http-password"), []byte("secret-pass"), 0o600); err != nil {
		t.Fatal(err)
	}

	return dir
}

func mapEnv(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}
