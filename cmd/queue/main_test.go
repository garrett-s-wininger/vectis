package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultQueueInstanceIDForHostIncludesPortAndSanitizes(t *testing.T) {
	got := defaultQueueInstanceIDForHost("Build Box.local", 8081)
	if got != "build-box.local-8081" {
		t.Fatalf("expected sanitized hostname-port instance ID, got %q", got)
	}
}

func TestDefaultQueueInstanceIDForHostOmitsInvalidPort(t *testing.T) {
	got := defaultQueueInstanceIDForHost("Build Box.local", 0)
	if got != "build-box.local" {
		t.Fatalf("expected sanitized hostname without port, got %q", got)
	}
}

func TestDefaultQueueInstanceIDForHostFallsBackForEmptyHost(t *testing.T) {
	got := defaultQueueInstanceIDForHost("", 8081)
	if got != "localhost-8081" {
		t.Fatalf("expected localhost fallback, got %q", got)
	}
}

func TestDefaultQueuePersistenceDirIncludesPoolAndInstance(t *testing.T) {
	got := defaultQueuePersistenceDir("Test Pool", "Queue/A:1")
	wantSuffix := filepath.Join("vectis", "queue", "test-pool", "queue-a-1")
	if !strings.HasSuffix(got, wantSuffix) {
		t.Fatalf("expected path ending in %q, got %q", wantSuffix, got)
	}
}
