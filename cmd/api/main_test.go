package main

import (
	"testing"
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
