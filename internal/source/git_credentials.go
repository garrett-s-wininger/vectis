package source

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

const gitTokenUsername = "x-access-token"

type GitCredentials struct {
	Username string
	Password string
}

func (c GitCredentials) IsZero() bool {
	return strings.TrimSpace(c.Username) == "" && strings.TrimSpace(c.Password) == ""
}

func ParseGitCredentials(data []byte) (GitCredentials, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return GitCredentials{}, fmt.Errorf("%w: git credential secret is empty", ErrInvalidReference)
	}

	var payload struct {
		Username string `json:"username"`
		Password string `json:"password"`
		Token    string `json:"token"`
	}
	if err := json.Unmarshal([]byte(trimmed), &payload); err == nil {
		creds := GitCredentials{
			Username: strings.TrimSpace(payload.Username),
			Password: strings.TrimSpace(payload.Password),
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

	return validateGitCredentials(GitCredentials{
		Username: gitTokenUsername,
		Password: trimmed,
	})
}

func validateGitCredentials(creds GitCredentials) (GitCredentials, error) {
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

func gitCredentialEnvironment(creds GitCredentials) ([]string, func(), error) {
	creds, err := validateGitCredentials(creds)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.CreateTemp("", "vectis-git-askpass-*")
	if err != nil {
		return nil, nil, fmt.Errorf("%w: create git askpass helper: %v", ErrInvalidReference, err)
	}

	path := f.Name()
	cleanup := func() { _ = os.Remove(path) }
	script := `#!/bin/sh
case "$1" in
*Username*) printf '%s\n' "$VECTIS_GIT_USERNAME" ;;
*) printf '%s\n' "$VECTIS_GIT_PASSWORD" ;;
esac
`
	if _, err := f.WriteString(script); err != nil {
		_ = f.Close()
		cleanup()
		return nil, nil, fmt.Errorf("%w: write git askpass helper: %v", ErrInvalidReference, err)
	}
	if err := f.Close(); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("%w: close git askpass helper: %v", ErrInvalidReference, err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("%w: chmod git askpass helper: %v", ErrInvalidReference, err)
	}

	return []string{
		"GIT_TERMINAL_PROMPT=0",
		"GIT_ASKPASS=" + path,
		"VECTIS_GIT_USERNAME=" + creds.Username,
		"VECTIS_GIT_PASSWORD=" + creds.Password,
	}, cleanup, nil
}
