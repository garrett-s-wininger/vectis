package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseOptionsDefaults(t *testing.T) {
	opts, err := parseOptions(nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	if opts.checksRaw != defaultChecks {
		t.Fatalf("checks = %q, want %q", opts.checksRaw, defaultChecks)
	}

	if opts.outRoot != defaultOutRoot {
		t.Fatalf("out = %q, want %q", opts.outRoot, defaultOutRoot)
	}

	if opts.timeout != defaultTimeout {
		t.Fatalf("timeout = %s, want %s", opts.timeout, defaultTimeout)
	}

	if opts.runName == "" {
		t.Fatal("run name should be defaulted")
	}
}

func TestParseOptionsSkipAndWaiveRequireReason(t *testing.T) {
	_, err := parseOptions([]string{"--skip", "postgres-integration"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "check=reason") {
		t.Fatalf("expected check=reason error, got %v", err)
	}

	_, err = parseOptions([]string{"--skip", "postgres-integration=no", "postgres"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unexpected positional") {
		t.Fatalf("expected unexpected positional error, got %v", err)
	}

	opts, err := parseOptions([]string{
		"--checks", "metadata,postgres-integration",
		"--skip", "postgres-integration=no postgres",
		"--waive", "metadata=covered elsewhere",
	}, io.Discard)

	if err != nil {
		t.Fatal(err)
	}

	if opts.skips["postgres-integration"] != "no postgres" {
		t.Fatalf("skip reason = %q", opts.skips["postgres-integration"])
	}

	if opts.waivers["metadata"] != "covered elsewhere" {
		t.Fatalf("waiver reason = %q", opts.waivers["metadata"])
	}
}

func TestResolveChecksRejectsUnknown(t *testing.T) {
	_, err := resolveChecks("metadata,nope", availableChecks())
	if err == nil || !strings.Contains(err.Error(), "unknown check") {
		t.Fatalf("expected unknown check error, got %v", err)
	}
}

func TestDefaultChecksIncludeCleanTreeGate(t *testing.T) {
	checks, err := resolveChecks(defaultChecks, availableChecks())
	if err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, check := range checks {
		if check.ID == "git-clean" {
			found = true
		}
	}

	if !found {
		t.Fatalf("default checks %+v do not include git-clean", checks)
	}
}

func TestValidateSkipsAndWaiversRejectsUnselected(t *testing.T) {
	opts, err := parseOptions([]string{
		"--checks", "metadata",
		"--skip", "postgres-integration=no postgres",
	}, io.Discard)

	if err != nil {
		t.Fatal(err)
	}

	checks, err := resolveChecks(opts.checksRaw, availableChecks())
	if err != nil {
		t.Fatal(err)
	}

	err = validateSkipsAndWaivers(checks, opts)
	if err == nil || !strings.Contains(err.Error(), "unselected") {
		t.Fatalf("expected unselected skip error, got %v", err)
	}
}

func TestRunCheckRecordsPassAndFailure(t *testing.T) {
	dir := t.TempDir()
	opts := options{timeout: defaultTimeout}
	spec := checkSpec{
		ID:      "unit",
		Title:   "Unit check",
		Command: []string{"unit-check"},
	}

	pass := runCheck(spec, opts, dir, dir, dir, func(context.Context, string, []string, io.Writer) (int, error) {
		return 0, nil
	})

	if pass.Status != statusPassed {
		t.Fatalf("pass status = %q", pass.Status)
	}

	if pass.LogPath == "" {
		t.Fatal("pass result missing log path")
	}

	fail := runCheck(spec, opts, dir, dir, dir, func(context.Context, string, []string, io.Writer) (int, error) {
		return 7, errors.New("boom")
	})

	if fail.Status != statusFailed {
		t.Fatalf("fail status = %q", fail.Status)
	}

	if fail.ExitCode != 7 {
		t.Fatalf("exit code = %d, want 7", fail.ExitCode)
	}
}

func TestCollectArtifacts(t *testing.T) {
	cwd := t.TempDir()
	binDir := filepath.Join(cwd, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(filepath.Join(binDir, "vectis-cli"), []byte("binary"), 0o644); err != nil {
		t.Fatal(err)
	}

	records, err := collectArtifacts(cwd)
	if err != nil {
		t.Fatal(err)
	}

	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}

	if records[0].Path != "bin/vectis-cli" {
		t.Fatalf("path = %q", records[0].Path)
	}

	if records[0].SHA256 == "" {
		t.Fatal("missing checksum")
	}
}

func TestGitStatusIsDirty(t *testing.T) {
	if gitStatusIsDirty("## main") {
		t.Fatal("branch header alone should be clean")
	}

	if !gitStatusIsDirty("## main\n M Makefile") {
		t.Fatal("modified file should be dirty")
	}

	if !gitStatusIsDirty("## main\n?? tools/release-readiness/") {
		t.Fatal("untracked file should be dirty")
	}
}
