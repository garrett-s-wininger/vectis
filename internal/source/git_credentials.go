package source

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
)

const gitTokenUsername = "x-access-token"

type GitCredentials struct {
	Username      string
	Password      string
	SSHPrivateKey string
	SSHKnownHosts string
}

func (c GitCredentials) IsZero() bool {
	return strings.TrimSpace(c.Username) == "" &&
		strings.TrimSpace(c.Password) == "" &&
		strings.TrimSpace(c.SSHPrivateKey) == "" &&
		strings.TrimSpace(c.SSHKnownHosts) == ""
}

func ParseGitCredentials(data []byte) (GitCredentials, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return GitCredentials{}, fmt.Errorf("%w: git credential secret is empty", ErrInvalidReference)
	}

	var payload struct {
		Username      string `json:"username"`
		Password      string `json:"password"`
		Token         string `json:"token"`
		SSHPrivateKey string `json:"ssh_private_key"`
		PrivateKey    string `json:"private_key"`
		SSHKnownHosts string `json:"known_hosts"`
	}

	if err := json.Unmarshal([]byte(trimmed), &payload); err == nil {
		creds := GitCredentials{
			Username:      strings.TrimSpace(payload.Username),
			Password:      strings.TrimSpace(payload.Password),
			SSHPrivateKey: strings.TrimSpace(payload.SSHPrivateKey),
			SSHKnownHosts: strings.TrimSpace(payload.SSHKnownHosts),
		}

		privateKey := strings.TrimSpace(payload.PrivateKey)
		if creds.SSHPrivateKey == "" {
			creds.SSHPrivateKey = privateKey
		}

		token := strings.TrimSpace(payload.Token)
		if token != "" {
			if creds.Username == "" {
				creds.Username = gitTokenUsername
			}

			if creds.Password == "" {
				creds.Password = token
			}
		}

		return validateGitCredentials(creds)
	} else if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		return GitCredentials{}, fmt.Errorf("%w: git credential JSON is invalid", ErrInvalidReference)
	}

	if looksLikeSSHPrivateKey(trimmed) {
		return validateGitCredentials(GitCredentials{
			SSHPrivateKey: trimmed,
		})
	}

	return validateGitCredentials(GitCredentials{
		Username: gitTokenUsername,
		Password: trimmed,
	})
}

func validateGitCredentials(creds GitCredentials) (GitCredentials, error) {
	creds.SSHPrivateKey = strings.TrimSpace(creds.SSHPrivateKey)
	creds.SSHKnownHosts = strings.TrimSpace(creds.SSHKnownHosts)
	if creds.SSHPrivateKey != "" || creds.SSHKnownHosts != "" {
		return validateGitSSHCredentials(creds)
	}

	return validateGitHTTPCredentials(creds)
}

func validateGitHTTPCredentials(creds GitCredentials) (GitCredentials, error) {
	creds.Username = strings.TrimSpace(creds.Username)
	creds.Password = strings.TrimSpace(creds.Password)
	if creds.Username == "" {
		return GitCredentials{}, fmt.Errorf("%w: git credential username is required", ErrInvalidReference)
	}
	if creds.Password == "" {
		return GitCredentials{}, fmt.Errorf("%w: git credential password or token is required", ErrInvalidReference)
	}
	if strings.ContainsAny(creds.Username, "\x00\n\r") || strings.ContainsAny(creds.Password, "\x00\n\r") {
		return GitCredentials{}, fmt.Errorf("%w: git credential contains unsafe control characters", ErrInvalidReference)
	}

	return creds, nil
}

func validateGitSSHCredentials(creds GitCredentials) (GitCredentials, error) {
	creds.Username = strings.TrimSpace(creds.Username)
	creds.Password = strings.TrimSpace(creds.Password)
	if creds.Username != "" || creds.Password != "" {
		return GitCredentials{}, fmt.Errorf("%w: git credential cannot mix SSH key and HTTP credentials", ErrInvalidReference)
	}

	if creds.SSHPrivateKey == "" {
		return GitCredentials{}, fmt.Errorf("%w: git credential ssh_private_key is required", ErrInvalidReference)
	}

	if strings.ContainsRune(creds.SSHPrivateKey, 0) || strings.ContainsRune(creds.SSHKnownHosts, 0) {
		return GitCredentials{}, fmt.Errorf("%w: git credential contains unsafe NUL bytes", ErrInvalidReference)
	}

	return creds, nil
}

func looksLikeSSHPrivateKey(value string) bool {
	return strings.Contains(value, "-----BEGIN ") && strings.Contains(value, "PRIVATE KEY-----")
}

func gitCredentialEnvironment(creds GitCredentials) ([]string, func(), error) {
	creds, err := validateGitCredentials(creds)
	if err != nil {
		return nil, nil, err
	}

	if creds.SSHPrivateKey != "" {
		return gitSSHCredentialEnvironment(creds)
	}

	return gitHTTPCredentialEnvironment(creds)
}

func gitHTTPCredentialEnvironment(creds GitCredentials) ([]string, func(), error) {
	pattern := "vectis-git-askpass-*"
	if runtime.GOOS == "windows" {
		pattern += ".cmd"
	}

	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: create git askpass helper: %w", ErrInvalidReference, err)
	}

	path := f.Name()
	cleanup := func() { _ = os.Remove(path) }
	script := gitAskpassScript()
	if _, err := f.WriteString(script); err != nil {
		_ = f.Close()
		cleanup()
		return nil, nil, fmt.Errorf("%w: write git askpass helper: %w", ErrInvalidReference, err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("%w: close git askpass helper: %w", ErrInvalidReference, err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("%w: chmod git askpass helper: %w", ErrInvalidReference, err)
	}

	return []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ASKPASS=" + path,
		"VECTIS_GIT_USERNAME=" + creds.Username,
		"VECTIS_GIT_PASSWORD=" + creds.Password,
	}, cleanup, nil
}

func gitAskpassScript() string {
	if runtime.GOOS == "windows" {
		return `@echo off
echo %1 | findstr /I "Username" >nul
if not errorlevel 1 (
  echo %VECTIS_GIT_USERNAME%
) else (
  echo %VECTIS_GIT_PASSWORD%
)
`
	}

	return `#!/bin/sh
case "$1" in
*Username*) printf '%s\n' "$VECTIS_GIT_USERNAME" ;;
*) printf '%s\n' "$VECTIS_GIT_PASSWORD" ;;
esac
`
}

func gitSSHCredentialEnvironment(creds GitCredentials) ([]string, func(), error) {
	keyData := creds.SSHPrivateKey
	if !strings.HasSuffix(keyData, "\n") {
		keyData += "\n"
	}

	keyPath, keyCleanup, err := writeGitCredentialTempFile("vectis-git-ssh-key-*", []byte(keyData), 0o600)
	if err != nil {
		return nil, nil, err
	}

	cleanup := keyCleanup
	sshCommandParts := []string{
		"ssh",
		"-F", "/dev/null",
		"-i", shellQuote(keyPath),
		"-o", "IdentitiesOnly=yes",
		"-o", "BatchMode=yes",
	}

	if creds.SSHKnownHosts != "" {
		knownHostsData := creds.SSHKnownHosts
		if !strings.HasSuffix(knownHostsData, "\n") {
			knownHostsData += "\n"
		}

		knownHostsPath, knownHostsCleanup, err := writeGitCredentialTempFile("vectis-git-known-hosts-*", []byte(knownHostsData), 0o600)
		if err != nil {
			keyCleanup()
			return nil, nil, err
		}

		cleanup = cleanupGitCredentialFiles(knownHostsCleanup, keyCleanup)
		sshCommandParts = append(sshCommandParts,
			"-o", "UserKnownHostsFile="+shellQuote(knownHostsPath),
			"-o", "StrictHostKeyChecking=yes",
		)
	}

	sshCommand := strings.Join(sshCommandParts, " ")

	return []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_SSH_COMMAND=" + sshCommand,
	}, cleanup, nil
}

func writeGitCredentialTempFile(pattern string, data []byte, perm os.FileMode) (string, func(), error) {
	f, err := os.CreateTemp("", pattern)
	if err != nil {
		return "", nil, fmt.Errorf("%w: create git credential file: %w", ErrInvalidReference, err)
	}

	path := f.Name()
	cleanup := func() { _ = os.Remove(path) }
	if len(data) > 0 {
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			cleanup()
			return "", nil, fmt.Errorf("%w: write git credential file: %w", ErrInvalidReference, err)
		}
	}

	if err := f.Close(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("%w: close git credential file: %w", ErrInvalidReference, err)
	}

	if err := os.Chmod(path, perm); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("%w: chmod git credential file: %w", ErrInvalidReference, err)
	}

	return path, cleanup, nil
}

func cleanupGitCredentialFiles(cleanups ...func()) func() {
	return func() {
		for _, cleanup := range cleanups {
			if cleanup != nil {
				cleanup()
			}
		}
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
