package log_test

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"vectis/internal/log"
)

func TestLogFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New("registry").WithOutput(&buf)
	logger.Info("Starting registry server...")

	out := buf.String()
	splits := strings.Split(out, " ")

	match, err := regexp.Match("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$", []byte(splits[0]))
	if err != nil || !match {
		t.Error("expected valid timestamp")
	}

	if splits[1] != "INFO" {
		t.Error("expected log level INFO")
	}

	if splits[2] != "[registry]" {
		t.Error("expected log component [registry]")
	}

	if !strings.HasSuffix(out, "Starting registry server...\n") {
		t.Error("expected log message 'Starting registry server...'")
	}
}
