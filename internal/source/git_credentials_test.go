package source

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestParseGitCredentials(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		username string
		password string
	}{
		{
			name:     "raw token",
			input:    "ghp_token\n",
			username: gitTokenUsername,
			password: "ghp_token",
		},
		{
			name:     "json username password",
			input:    `{"username":"alice","password":"secret"}`,
			username: "alice",
			password: "secret",
		},
		{
			name:     "json token",
			input:    `{"token":"ghp_json"}`,
			username: gitTokenUsername,
			password: "ghp_json",
		},
		{
			name:     "json token with username",
			input:    `{"username":"oauth2","token":"ghp_json"}`,
			username: "oauth2",
			password: "ghp_json",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creds, err := ParseGitCredentials([]byte(tc.input))
			if err != nil {
				t.Fatalf("ParseGitCredentials: %v", err)
			}
			if creds.Username != tc.username || creds.Password != tc.password {
				t.Fatalf("credentials = %+v, want username=%q password=%q", creds, tc.username, tc.password)
			}
		})
	}
}

func TestParseGitCredentialsRejectsInvalidMaterial(t *testing.T) {
	for _, input := range []string{
		"",
		`{"username":"alice"}`,
		`{"password":"secret"}`,
		`{"username":"alice","password":`,
		"bad\nsecret",
		`{"username":"alice","password":"bad` + "\n" + `secret"}`,
	} {
		t.Run(strings.ReplaceAll(input, "\n", `\n`), func(t *testing.T) {
			_, err := ParseGitCredentials([]byte(input))
			if !errors.Is(err, ErrInvalidReference) {
				t.Fatalf("expected ErrInvalidReference, got %v", err)
			}
		})
	}
}

func TestGitCredentialEnvironmentProvidesAskpassAndCleansUp(t *testing.T) {
	env, cleanup, err := gitCredentialEnvironment(GitCredentials{Username: "alice", Password: "secret"})
	if err != nil {
		t.Fatalf("gitCredentialEnvironment: %v", err)
	}

	askpass := ""
	for _, pair := range env {
		if strings.HasPrefix(pair, "GIT_ASKPASS=") {
			askpass = strings.TrimPrefix(pair, "GIT_ASKPASS=")
			break
		}
	}
	if askpass == "" {
		t.Fatalf("GIT_ASKPASS missing from env: %v", env)
	}

	assertAskpass := func(prompt, want string) {
		t.Helper()

		cmd := exec.Command(askpass, prompt)
		cmd.Env = append(os.Environ(), env...)
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("run askpass %q: %v", prompt, err)
		}
		if got := strings.TrimSpace(string(out)); got != want {
			t.Fatalf("askpass %q = %q, want %q", prompt, got, want)
		}
	}

	assertAskpass("Username for https://git.example", "alice")
	assertAskpass("Password for https://alice@git.example", "secret")

	cleanup()
	if _, err := os.Stat(askpass); !os.IsNotExist(err) {
		t.Fatalf("expected askpass helper to be removed, stat err=%v", err)
	}
}
