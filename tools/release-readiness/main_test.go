package main

import (
	"context"
	"encoding/json"
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

	if opts.profile != defaultProfile {
		t.Fatalf("profile = %q, want %q", opts.profile, defaultProfile)
	}

	if opts.checksRaw != checkProfiles()[defaultProfile] {
		t.Fatalf("checks = %q, want %q", opts.checksRaw, checkProfiles()[defaultProfile])
	}

	if opts.outRoot != defaultOutRoot {
		t.Fatalf("out = %q, want %q", opts.outRoot, defaultOutRoot)
	}

	if opts.timeout != defaultTimeout {
		t.Fatalf("timeout = %s, want %s", opts.timeout, defaultTimeout)
	}

	if opts.failFast {
		t.Fatal("fail-fast should be disabled by default")
	}

	if opts.runName == "" {
		t.Fatal("run name should be defaulted")
	}

	if len(opts.artifactRoots) == 0 {
		t.Fatal("artifact roots should be defaulted")
	}
}

func TestParseOptionsProfilesAndExplicitChecks(t *testing.T) {
	opts, err := parseOptions([]string{"--profile", "metadata"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	if opts.checksRaw != "metadata" {
		t.Fatalf("metadata profile checks = %q", opts.checksRaw)
	}

	opts, err = parseOptions([]string{"--profile", "full", "--checks", "metadata"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	if opts.checksRaw != "metadata" {
		t.Fatalf("explicit checks should override profile, got %q", opts.checksRaw)
	}

	if !opts.checksProvided {
		t.Fatal("explicit checks should be recorded")
	}

	if got, want := reportProfile(opts), "custom"; got != want {
		t.Fatalf("report profile = %q, want %q", got, want)
	}

	_, err = parseOptions([]string{"--checks", ""}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "at least one check") {
		t.Fatalf("expected empty checks error, got %v", err)
	}

	_, err = parseOptions([]string{"--profile", "mars"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "unknown profile") {
		t.Fatalf("expected unknown profile error, got %v", err)
	}
}

func TestParseOptionsArtifactRoots(t *testing.T) {
	opts, err := parseOptions([]string{"--artifact-roots", "bin,dist/openapi.json"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	if got, want := strings.Join(opts.artifactRoots, ","), "bin,dist/openapi.json"; got != want {
		t.Fatalf("artifact roots = %q, want %q", got, want)
	}

	opts, err = parseOptions([]string{"--fail-fast"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}

	if !opts.failFast {
		t.Fatal("fail-fast should be enabled")
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
	checks, err := resolveChecks(checkProfiles()[defaultProfile], availableChecks())
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

func TestRunWithExecutorFailFastSkipsRemainingChecks(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)

	outRoot := filepath.Join(cwd, "reports")
	opts := options{
		checksRaw:     "build,test-quick",
		outRoot:       outRoot,
		runName:       "fail-fast",
		timeout:       defaultTimeout,
		failFast:      true,
		artifactRoots: nil,
		skips:         make(map[string]string),
		waivers:       make(map[string]string),
	}

	var ran []string
	err := runWithExecutor(opts, io.Discard, func(_ context.Context, _ string, command []string, _ io.Writer) (int, error) {
		ran = append(ran, commandString(command))
		return 1, errors.New("boom")
	})

	if !errors.Is(err, errReadinessFailed) {
		t.Fatalf("expected readiness failure, got %v", err)
	}

	if got, want := len(ran), 1; got != want {
		t.Fatalf("ran %d checks, want %d", got, want)
	}

	raw, err := os.ReadFile(filepath.Join(outRoot, "fail-fast", "summary.json"))
	if err != nil {
		t.Fatal(err)
	}

	var rep report
	if err := json.Unmarshal(raw, &rep); err != nil {
		t.Fatal(err)
	}

	if !rep.FailFast {
		t.Fatal("summary did not record fail-fast")
	}

	if got, want := rep.Counts.Failed, 1; got != want {
		t.Fatalf("failed count = %d, want %d", got, want)
	}

	if got, want := rep.Counts.Skipped, 1; got != want {
		t.Fatalf("skipped count = %d, want %d", got, want)
	}

	if got := rep.Checks[1].Status; got != statusSkipped {
		t.Fatalf("second check status = %q, want %q", got, statusSkipped)
	}

	if !strings.Contains(rep.Checks[1].Reason, "--fail-fast") {
		t.Fatalf("second check reason = %q", rep.Checks[1].Reason)
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

	records, err := collectArtifacts(cwd, []string{"bin"})
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
