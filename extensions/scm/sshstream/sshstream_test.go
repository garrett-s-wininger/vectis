package sshstream

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestNormalizeOptionsDefaults(t *testing.T) {
	opts, err := NormalizeOptions(Options{
		Host:    " gerrit.example.com ",
		Port:    29418,
		User:    " ci ",
		Command: " gerrit stream-events ",
	})

	if err != nil {
		t.Fatalf("NormalizeOptions returned error: %v", err)
	}

	if opts.Host != "gerrit.example.com" || opts.User != "ci" || opts.Command != "gerrit stream-events" {
		t.Fatalf("normalized options = %+v, want trimmed host/user/command", opts)
	}

	if opts.ConnectTimeout != DefaultConnectTimeout {
		t.Fatalf("connect timeout = %v, want default", opts.ConnectTimeout)
	}

	if got := opts.Address(); got != "gerrit.example.com:29418" {
		t.Fatalf("Address = %q, want gerrit.example.com:29418", got)
	}
}

func TestNormalizeOptionsRejectsMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name string
		opts Options
	}{
		{name: "host", opts: Options{Port: 29418, User: "ci", Command: "cmd"}},
		{name: "port", opts: Options{Host: "gerrit.example.com", User: "ci", Command: "cmd"}},
		{name: "command", opts: Options{Host: "gerrit.example.com", Port: 29418, User: "ci"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NormalizeOptions(tt.opts); err == nil {
				t.Fatal("NormalizeOptions returned nil error, want error")
			}
		})
	}
}

func TestHostKeyCallbackRequiresKnownHosts(t *testing.T) {
	_, err := HostKeyCallback(Options{
		KnownHostsFile: "/path/that/does/not/exist",
	})

	if err == nil || !strings.Contains(err.Error(), "known hosts") {
		t.Fatalf("HostKeyCallback error = %v, want known hosts error", err)
	}
}

func TestNewClientConfigRequiresAuth(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	_, closeAuth, err := NewClientConfig(Options{
		Host:                  "gerrit.example.com",
		Port:                  29418,
		User:                  "ci",
		Command:               "cmd",
		InsecureIgnoreHostKey: true,
		UseAgent:              false,
		ConnectTimeout:        time.Second,
	})

	if closeAuth != nil {
		_ = closeAuth()
	}

	if err == nil || !strings.Contains(err.Error(), "requires --ssh-key-file") {
		t.Fatalf("NewClientConfig error = %v, want auth error", err)
	}
}

func TestNewClientConfigAcceptsKeyFile(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	path := t.TempDir() + "/id_rsa"
	if err := os.WriteFile(path, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	config, closeAuth, err := NewClientConfig(Options{
		Host:                  "gerrit.example.com",
		Port:                  29418,
		User:                  "ci",
		Command:               "cmd",
		KeyFile:               path,
		InsecureIgnoreHostKey: true,
		UseAgent:              false,
		ConnectTimeout:        time.Second,
	})

	if closeAuth != nil {
		defer func() { _ = closeAuth() }()
	}

	if err != nil {
		t.Fatalf("NewClientConfig returned error: %v", err)
	}

	if config.User != "ci" || len(config.Auth) != 1 || config.HostKeyCallback == nil {
		t.Fatalf("config = %+v, want user, one auth method, host key callback", config)
	}
}

func TestConsumeWithReconnectRetriesWithBackoff(t *testing.T) {
	clock := &mockClock{}
	logger := &mockLogger{}
	attempts := 0
	streamErr := errors.New("stream dropped")

	err := consumeWithReconnect(context.Background(), ReconnectOptions{
		BaseDelay:   time.Second,
		MaxDelay:    5 * time.Second,
		MaxAttempts: 3,
		Clock:       clock,
		Logger:      logger,
		Label:       "test stream",
	}, func() error {
		attempts++
		return streamErr
	})

	if !errors.Is(err, streamErr) {
		t.Fatalf("consumeWithReconnect error = %v, want %v", err, streamErr)
	}

	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}

	sleeps := clock.sleeps
	if len(sleeps) != 2 || sleeps[0] != time.Second || sleeps[1] != 2*time.Second {
		t.Fatalf("sleeps = %v, want [1s 2s]", sleeps)
	}

	if len(logger.warns) != 2 {
		t.Fatalf("warn calls = %d, want 2", len(logger.warns))
	}
}

type mockClock struct {
	sleeps []time.Duration
}

func (c *mockClock) Sleep(ctx context.Context, d time.Duration) error {
	c.sleeps = append(c.sleeps, d)
	return ctx.Err()
}

type mockLogger struct {
	infos []string
	warns []string
}

func (l *mockLogger) Info(msg string, args ...any) {
	l.infos = append(l.infos, fmt.Sprintf(msg, args...))
}

func (l *mockLogger) Warn(msg string, args ...any) {
	l.warns = append(l.warns, fmt.Sprintf(msg, args...))
}
