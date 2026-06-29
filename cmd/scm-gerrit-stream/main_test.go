package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	scmgerrit "vectis/extensions/scm/gerrit"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/scmstream"
	"vectis/sdk/scm"
)

func TestRouteGerritStreamEventBuildsTargetFromPayload(t *testing.T) {
	event, ok, err := scmgerrit.NormalizeStreamEvent([]byte(`{
		"type": "patchset-created",
		"change": {"project": "project", "branch": "master", "id": "Iabc", "number": 42, "status": "NEW"},
		"patchSet": {"revision": "abc123", "ref": "refs/changes/42/42/1"}
	}`), scmgerrit.StreamOptions{BaseURL: "http://gerrit.example.com"})

	if err != nil {
		t.Fatalf("NormalizeStreamEvent returned error: %v", err)
	}

	if !ok {
		t.Fatal("NormalizeStreamEvent filtered event")
	}

	router := &recordingGerritStreamRouter{result: scmstream.RouteResult{Candidates: 2, Matched: 1, Handled: 1}}
	result, err := routeGerritStreamEvent(context.Background(), "http://gerrit.example.com/", router, event)
	if err != nil {
		t.Fatalf("routeGerritStreamEvent returned error: %v", err)
	}

	if result.Handled != 1 {
		t.Fatalf("result.Handled = %d, want 1", result.Handled)
	}

	if router.target.Provider != "gerrit" || router.target.BaseURL != "http://gerrit.example.com/" ||
		router.target.Project != "project" || router.target.Branch != "master" {
		t.Fatalf("target = %+v, want Gerrit project/master target", router.target)
	}

	if router.event.Key != event.Key {
		t.Fatalf("event key = %q, want %q", router.event.Key, event.Key)
	}
}

func TestRouteGerritStreamEventPropagatesRouterError(t *testing.T) {
	event, ok, err := scmgerrit.NormalizeStreamEvent([]byte(`{
		"type": "patchset-created",
		"change": {"project": "project", "branch": "master", "id": "Iabc", "number": 42, "status": "NEW"},
		"patchSet": {"revision": "abc123", "ref": "refs/changes/42/42/1"}
	}`), scmgerrit.StreamOptions{BaseURL: "http://gerrit.example.com"})

	if err != nil {
		t.Fatalf("NormalizeStreamEvent returned error: %v", err)
	}

	if !ok {
		t.Fatal("NormalizeStreamEvent filtered event")
	}

	routerErr := errors.New("route failed")
	_, err = routeGerritStreamEvent(context.Background(), "http://gerrit.example.com", &recordingGerritStreamRouter{err: routerErr}, event)
	if !errors.Is(err, routerErr) {
		t.Fatalf("routeGerritStreamEvent error = %v, want %v", err, routerErr)
	}
}

func TestStreamInputAndInstanceDefaults(t *testing.T) {
	if got := streamInputLabel(""); got != "stdin" {
		t.Fatalf("streamInputLabel(empty) = %q, want stdin", got)
	}

	if got := streamInputLabel("-"); got != "stdin" {
		t.Fatalf("streamInputLabel(-) = %q, want stdin", got)
	}

	if got := streamInputLabel("events.json"); got != "events.json" {
		t.Fatalf("streamInputLabel(path) = %q, want path", got)
	}

	if got := streamInstanceID(" \t"); got != "scm-gerrit-stream" {
		t.Fatalf("streamInstanceID(empty) = %q, want default", got)
	}

	if got := streamInstanceID(" stream-a "); got != "stream-a" {
		t.Fatalf("streamInstanceID(custom) = %q, want stream-a", got)
	}
}

func TestOpenStreamInputFile(t *testing.T) {
	path := t.TempDir() + "/events.json"
	if err := os.WriteFile(path, []byte("hello\n"), 0o600); err != nil {
		t.Fatalf("write input fixture: %v", err)
	}

	reader, closeInput, err := openStreamInput(path)
	if err != nil {
		t.Fatalf("openStreamInput returned error: %v", err)
	}
	defer func() { _ = closeInput() }()

	var b strings.Builder
	if _, err := io.Copy(&b, reader); err != nil {
		t.Fatalf("read opened input: %v", err)
	}

	if got := b.String(); got != "hello\n" {
		t.Fatalf("input content = %q, want hello newline", got)
	}
}

func TestResolveStreamTransport(t *testing.T) {
	tests := []struct {
		name      string
		transport string
		sshHost   string
		want      string
		wantErr   bool
	}{
		{name: "auto input", transport: "auto", want: streamTransportInput},
		{name: "empty auto input", want: streamTransportInput},
		{name: "auto ssh", transport: "auto", sshHost: "gerrit.example.com", want: streamTransportSSH},
		{name: "explicit input", transport: "input", sshHost: "gerrit.example.com", want: streamTransportInput},
		{name: "explicit ssh", transport: "ssh", want: streamTransportSSH},
		{name: "invalid", transport: "serial", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveStreamTransport(tt.transport, tt.sshHost)
			if tt.wantErr {
				if err == nil {
					t.Fatal("resolveStreamTransport returned nil error, want error")
				}

				return
			}

			if err != nil {
				t.Fatalf("resolveStreamTransport returned error: %v", err)
			}

			if got != tt.want {
				t.Fatalf("resolveStreamTransport = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizeSSHStreamOptionsDefaults(t *testing.T) {
	opts, err := normalizeSSHStreamOptions(sshStreamOptions{
		Host: " gerrit.example.com ",
		User: " ci ",
	})

	if err != nil {
		t.Fatalf("normalizeSSHStreamOptions returned error: %v", err)
	}

	if opts.Host != "gerrit.example.com" || opts.User != "ci" {
		t.Fatalf("normalized host/user = %q/%q, want trimmed values", opts.Host, opts.User)
	}

	if opts.Port != defaultGerritSSHPort {
		t.Fatalf("port = %d, want %d", opts.Port, defaultGerritSSHPort)
	}

	if opts.Command != defaultGerritSSHCommand {
		t.Fatalf("command = %q, want default", opts.Command)
	}

	if opts.ConnectTimeout != 10*time.Second {
		t.Fatalf("connect timeout = %v, want 10s", opts.ConnectTimeout)
	}

	if got := opts.Address(); got != "gerrit.example.com:29418" {
		t.Fatalf("Address = %q, want gerrit.example.com:29418", got)
	}
}

func TestNormalizeSSHStreamOptionsRequiresHost(t *testing.T) {
	if _, err := normalizeSSHStreamOptions(sshStreamOptions{User: "ci"}); err == nil {
		t.Fatal("normalizeSSHStreamOptions returned nil error, want host error")
	}
}

func TestSSHHostKeyCallbackRequiresKnownHosts(t *testing.T) {
	_, err := sshHostKeyCallback(sshStreamOptions{
		KnownHostsFile: "/path/that/does/not/exist",
	})

	if err == nil || !strings.Contains(err.Error(), "known hosts") {
		t.Fatalf("sshHostKeyCallback error = %v, want known hosts error", err)
	}
}

func TestNewSSHClientConfigRequiresAuth(t *testing.T) {
	t.Setenv("SSH_AUTH_SOCK", "")
	_, closeAuth, err := newSSHClientConfig(sshStreamOptions{
		Host:                  "gerrit.example.com",
		User:                  "ci",
		InsecureIgnoreHostKey: true,
		UseAgent:              false,
		ConnectTimeout:        time.Second,
	})

	if closeAuth != nil {
		_ = closeAuth()
	}

	if err == nil || !strings.Contains(err.Error(), "requires --ssh-key-file") {
		t.Fatalf("newSSHClientConfig error = %v, want auth error", err)
	}
}

func TestNewSSHClientConfigAcceptsKeyFile(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	path := t.TempDir() + "/id_rsa"
	if err := os.WriteFile(path, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}

	config, closeAuth, err := newSSHClientConfig(sshStreamOptions{
		Host:                  "gerrit.example.com",
		User:                  "ci",
		KeyFile:               path,
		InsecureIgnoreHostKey: true,
		UseAgent:              false,
		ConnectTimeout:        time.Second,
	})

	if closeAuth != nil {
		defer func() { _ = closeAuth() }()
	}

	if err != nil {
		t.Fatalf("newSSHClientConfig returned error: %v", err)
	}

	if config.User != "ci" || len(config.Auth) != 1 || config.HostKeyCallback == nil {
		t.Fatalf("config = %+v, want user, one auth method, host key callback", config)
	}
}

func TestConsumeWithReconnectRetriesWithBackoff(t *testing.T) {
	clock := mocks.NewMockClock()
	logger := mocks.NewMockLogger()
	attempts := 0
	streamErr := errors.New("stream dropped")

	err := consumeWithReconnect(context.Background(), streamReconnectOptions{
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

	sleeps := clock.GetSleeps()
	if len(sleeps) != 2 || sleeps[0] != time.Second || sleeps[1] != 2*time.Second {
		t.Fatalf("sleeps = %v, want [1s 2s]", sleeps)
	}

	if len(logger.GetWarnCalls()) != 2 {
		t.Fatalf("warn calls = %d, want 2", len(logger.GetWarnCalls()))
	}
}

type recordingGerritStreamRouter struct {
	target scmstream.EventTarget
	event  scm.Event
	result scmstream.RouteResult
	err    error
}

func (r *recordingGerritStreamRouter) HandleEvent(_ context.Context, target scmstream.EventTarget, event scm.Event) (scmstream.RouteResult, error) {
	r.target = target
	r.event = event
	return r.result, r.err
}
