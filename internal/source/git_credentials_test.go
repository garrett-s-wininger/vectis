package source

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

const testSSHPrivateKey = "-----BEGIN OPENSSH PRIVATE KEY-----\nabc123\n-----END OPENSSH PRIVATE KEY-----"

func TestParseGitCredentials(t *testing.T) {
	for _, tc := range []struct {
		name          string
		input         string
		username      string
		password      string
		sshPrivateKey string
		sshKnownHosts string
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
		{
			name:          "json ssh private key",
			input:         `{"ssh_private_key":"` + strings.ReplaceAll(testSSHPrivateKey, "\n", `\n`) + `","known_hosts":"git.example ssh-ed25519 AAAA"}`,
			sshPrivateKey: testSSHPrivateKey,
			sshKnownHosts: "git.example ssh-ed25519 AAAA",
		},
		{
			name:          "json private key alias",
			input:         `{"private_key":"` + strings.ReplaceAll(testSSHPrivateKey, "\n", `\n`) + `"}`,
			sshPrivateKey: testSSHPrivateKey,
		},
		{
			name:          "raw ssh private key",
			input:         testSSHPrivateKey,
			sshPrivateKey: testSSHPrivateKey,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			creds, err := ParseGitCredentials([]byte(tc.input))
			if err != nil {
				t.Fatalf("ParseGitCredentials: %v", err)
			}
			if creds.Username != tc.username ||
				creds.Password != tc.password ||
				creds.SSHPrivateKey != tc.sshPrivateKey ||
				creds.SSHKnownHosts != tc.sshKnownHosts {
				t.Fatalf("credentials = %+v, want username=%q password=%q ssh_private_key=%q known_hosts=%q", creds, tc.username, tc.password, tc.sshPrivateKey, tc.sshKnownHosts)
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
		`{"known_hosts":"git.example ssh-ed25519 AAAA"}`,
		`{"username":"alice","ssh_private_key":"` + strings.ReplaceAll(testSSHPrivateKey, "\n", `\n`) + `"}`,
		`{"ssh_private_key":"bad\u0000key"}`,
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

func TestGitCredentialEnvironmentProvidesSSHCommandAndCleansUp(t *testing.T) {
	env, cleanup, err := gitCredentialEnvironment(GitCredentials{
		SSHPrivateKey: testSSHPrivateKey,
		SSHKnownHosts: "git.example ssh-ed25519 AAAA",
	})

	if err != nil {
		t.Fatalf("gitCredentialEnvironment: %v", err)
	}

	if got := envValue(env, "GIT_ASKPASS"); got != "" {
		t.Fatalf("GIT_ASKPASS should not be set for SSH credentials: %v", env)
	}

	sshCommand := envValue(env, "GIT_SSH_COMMAND")
	if sshCommand == "" {
		t.Fatalf("GIT_SSH_COMMAND missing from env: %v", env)
	}

	if !strings.Contains(sshCommand, "IdentitiesOnly=yes") ||
		!strings.Contains(sshCommand, "BatchMode=yes") ||
		!strings.Contains(sshCommand, "StrictHostKeyChecking=yes") {
		t.Fatalf("GIT_SSH_COMMAND missing expected options: %q", sshCommand)
	}

	keyPath, ok := sshCommandArgAfter(sshCommand, "-i")
	if !ok {
		t.Fatalf("GIT_SSH_COMMAND missing -i identity file: %q", sshCommand)
	}

	keyData, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatalf("read SSH key file: %v", err)
	}

	if got := strings.TrimSpace(string(keyData)); got != testSSHPrivateKey {
		t.Fatalf("SSH key file = %q, want source key", got)
	}

	knownHostsPath, ok := sshCommandOptionValue(sshCommand, "UserKnownHostsFile")
	if !ok {
		t.Fatalf("GIT_SSH_COMMAND missing known_hosts file: %q", sshCommand)
	}

	knownHostsData, err := os.ReadFile(knownHostsPath)
	if err != nil {
		t.Fatalf("read known_hosts file: %v", err)
	}

	if got := strings.TrimSpace(string(knownHostsData)); got != "git.example ssh-ed25519 AAAA" {
		t.Fatalf("known_hosts file = %q, want pinned host key", got)
	}

	cleanup()
	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Fatalf("expected SSH key file to be removed, stat err=%v", err)
	}

	if _, err := os.Stat(knownHostsPath); !os.IsNotExist(err) {
		t.Fatalf("expected known_hosts file to be removed, stat err=%v", err)
	}
}

func TestGitCredentialEnvironmentUsesOpenSSHHostKeyDefaultsForSSHWithoutPinnedHosts(t *testing.T) {
	env, cleanup, err := gitCredentialEnvironment(GitCredentials{SSHPrivateKey: testSSHPrivateKey})
	if err != nil {
		t.Fatalf("gitCredentialEnvironment: %v", err)
	}
	defer cleanup()

	sshCommand := envValue(env, "GIT_SSH_COMMAND")
	if strings.Contains(sshCommand, "UserKnownHostsFile=") ||
		strings.Contains(sshCommand, "StrictHostKeyChecking=") {
		t.Fatalf("GIT_SSH_COMMAND should use OpenSSH host-key defaults when known_hosts is omitted: %q", sshCommand)
	}

	keyPath, ok := sshCommandArgAfter(sshCommand, "-i")
	if !ok {
		t.Fatalf("GIT_SSH_COMMAND missing -i identity file: %q", sshCommand)
	}

	if _, err := os.Stat(keyPath); err != nil {
		t.Fatalf("SSH key file should exist: %v", err)
	}
}

func envValue(env []string, key string) string {
	prefix := key + "="
	for _, pair := range env {
		if strings.HasPrefix(pair, prefix) {
			return strings.TrimPrefix(pair, prefix)
		}
	}

	return ""
}

func sshCommandArgAfter(command, flag string) (string, bool) {
	fields := strings.Fields(command)
	for i, field := range fields {
		if field == flag && i+1 < len(fields) {
			return strings.Trim(fields[i+1], "'"), true
		}
	}

	return "", false
}

func sshCommandOptionValue(command, option string) (string, bool) {
	prefix := option + "="
	for _, field := range strings.Fields(command) {
		if strings.HasPrefix(field, prefix) {
			return strings.Trim(strings.TrimPrefix(field, prefix), "'"), true
		}
	}

	return "", false
}
