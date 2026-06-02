package main

import (
	"bytes"
	"strings"
	"testing"

	"vectis/internal/config"
	"vectis/internal/interfaces"
)

func TestBuildAccessLogger_json(t *testing.T) {
	log, closeLog := buildAccessLogger("json")
	if closeLog != nil {
		defer func() { _ = closeLog() }()
	}

	if log == nil {
		t.Fatal("expected non-nil logger for json format")
	}
}

func TestBuildAccessLogger_text(t *testing.T) {
	log, closeLog := buildAccessLogger("text")
	if closeLog != nil {
		defer func() { _ = closeLog() }()
	}

	if log != nil {
		t.Fatal("expected nil logger for text format")
	}
}

func TestBuildAccessLogger_caseInsensitive(t *testing.T) {
	log, closeLog := buildAccessLogger("JSON")
	if closeLog != nil {
		defer func() { _ = closeLog() }()
	}

	if log == nil {
		t.Fatal("expected non-nil logger for uppercase JSON")
	}
}

func TestBuildAccessLogger_empty(t *testing.T) {
	log, closeLog := buildAccessLogger("")
	if closeLog != nil {
		defer func() { _ = closeLog() }()
	}

	if log != nil {
		t.Fatal("expected nil logger for empty format")
	}
}

func TestWarnIfProcessLocalAPICache(t *testing.T) {
	var buf bytes.Buffer
	logger := interfaces.NewLogger("api-test").WithOutput(&buf)

	warnIfProcessLocalAPICache(logger, config.APICacheBackendDatabase, true)
	warnIfProcessLocalAPICache(logger, config.APICacheBackendMemory, false)
	if buf.Len() != 0 {
		t.Fatalf("unexpected warning: %s", buf.String())
	}

	warnIfProcessLocalAPICache(logger, config.APICacheBackendMemory, true)
	if got := buf.String(); !strings.Contains(got, "api.cache.backend=memory") || !strings.Contains(got, "process-local") {
		t.Fatalf("warning = %q, want process-local memory cache warning", got)
	}
}
