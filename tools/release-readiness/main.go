package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	defaultProfile       = "local"
	defaultOutRoot       = "artifacts/release-readiness"
	defaultTimeout       = 60 * time.Minute
	defaultArtifactRoots = "bin,artifacts/packages,artifacts/deploy,website/static/openapi/v1.json"

	statusPassed  = "passed"
	statusFailed  = "failed"
	statusSkipped = "skipped"
	statusWaived  = "waived"
)

var errReadinessFailed = errors.New("release readiness checks did not pass")

type options struct {
	checksRaw        string
	checksProvided   bool
	profile          string
	outRoot          string
	runName          string
	timeout          time.Duration
	strict           bool
	failFast         bool
	list             bool
	artifactRootsRaw string
	artifactRoots    []string
	skips            valueMap
	waivers          valueMap
}

type valueMap map[string]string

func (m *valueMap) String() string {
	if m == nil || len(*m) == 0 {
		return ""
	}

	keys := make([]string, 0, len(*m))
	for key := range *m {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+(*m)[key])
	}

	return strings.Join(parts, ",")
}

func (m *valueMap) Set(raw string) error {
	key, value, ok := strings.Cut(raw, "=")
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if !ok || key == "" || value == "" {
		return fmt.Errorf("expected check=reason, got %q", raw)
	}

	if *m == nil {
		*m = make(map[string]string)
	}

	(*m)[key] = value
	return nil
}

type checkSpec struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Description string   `json:"description,omitempty"`
	Command     []string `json:"command,omitempty"`
	Env         []string `json:"env,omitempty"`
	Internal    string   `json:"internal,omitempty"`
}

type report struct {
	SchemaVersion int              `json:"schema_version"`
	GeneratedAt   string           `json:"generated_at"`
	RunName       string           `json:"run_name"`
	Workspace     string           `json:"workspace"`
	OutputDir     string           `json:"output_dir"`
	Strict        bool             `json:"strict"`
	FailFast      bool             `json:"fail_fast"`
	Profile       string           `json:"profile,omitempty"`
	ArtifactRoots []string         `json:"artifact_roots,omitempty"`
	Git           gitInfo          `json:"git"`
	Toolchain     toolchainInfo    `json:"toolchain"`
	Checks        []checkResult    `json:"checks"`
	Counts        checkCounts      `json:"counts"`
	Artifacts     []artifactRecord `json:"artifacts"`
	Warnings      []string         `json:"warnings,omitempty"`
}

type gitInfo struct {
	Commit      string `json:"commit,omitempty"`
	Describe    string `json:"describe,omitempty"`
	Branch      string `json:"branch,omitempty"`
	StatusShort string `json:"status_short,omitempty"`
	Dirty       bool   `json:"dirty"`
}

type toolchainInfo struct {
	GoVersion   string `json:"go_version,omitempty"`
	MageVersion string `json:"mage_version,omitempty"`
}

type checkResult struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Status      string `json:"status"`
	Command     string `json:"command,omitempty"`
	LogPath     string `json:"log_path,omitempty"`
	Reason      string `json:"reason,omitempty"`
	ExitCode    int    `json:"exit_code,omitempty"`
	StartedAt   string `json:"started_at,omitempty"`
	FinishedAt  string `json:"finished_at,omitempty"`
	DurationMS  int64  `json:"duration_ms,omitempty"`
	Description string `json:"description,omitempty"`
}

type checkCounts struct {
	Passed  int `json:"passed"`
	Failed  int `json:"failed"`
	Skipped int `json:"skipped"`
	Waived  int `json:"waived"`
}

type artifactRecord struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

type npmAuditReport struct {
	Metadata struct {
		Vulnerabilities npmAuditCounts `json:"vulnerabilities"`
	} `json:"metadata"`
}

type npmAuditCounts struct {
	Info     int `json:"info"`
	Low      int `json:"low"`
	Moderate int `json:"moderate"`
	High     int `json:"high"`
	Critical int `json:"critical"`
	Total    int `json:"total"`
}

type commandExecutor func(ctx context.Context, cwd string, command, env []string, log io.Writer) (int, error)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, errReadinessFailed) {
			os.Exit(1)
		}

		fmt.Fprintf(os.Stderr, "release-readiness: %v\n", err)
		os.Exit(2)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	opts, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}

	if opts.list {
		printChecks(stdout, availableChecks())
		return nil
	}

	return runWithExecutor(opts, stdout, executeCommand)
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	opts := options{
		profile:          defaultProfile,
		outRoot:          defaultOutRoot,
		timeout:          defaultTimeout,
		artifactRootsRaw: defaultArtifactRoots,
		skips:            make(map[string]string),
		waivers:          make(map[string]string),
	}

	timeoutRaw := opts.timeout.String()
	flags := flag.NewFlagSet("release-readiness", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&opts.profile, "profile", opts.profile, "check profile: local, candidate, full, or metadata")
	flags.StringVar(&opts.checksRaw, "checks", opts.checksRaw, "comma-separated check IDs to include; overrides --profile")
	flags.StringVar(&opts.outRoot, "out", opts.outRoot, "directory for readiness report runs")
	flags.StringVar(&opts.runName, "run-name", opts.runName, "report run directory name; defaults to UTC timestamp")
	flags.StringVar(&timeoutRaw, "timeout", timeoutRaw, "per-command timeout; use 0 to disable")
	flags.BoolVar(&opts.strict, "strict", opts.strict, "fail when any selected check is skipped or waived")
	flags.BoolVar(&opts.failFast, "fail-fast", opts.failFast, "stop running later selected checks after the first failed check")
	flags.BoolVar(&opts.list, "list-checks", opts.list, "list available check IDs and exit")
	flags.StringVar(&opts.artifactRootsRaw, "artifact-roots", opts.artifactRootsRaw, "comma-separated files or directories to checksum")
	flags.Var(&opts.skips, "skip", "mark a selected check as skipped without running it, as check=reason")
	flags.Var(&opts.waivers, "waive", "mark a selected check as waived without running it, as check=reason")

	if err := flags.Parse(args); err != nil {
		return options{}, err
	}

	flags.Visit(func(flag *flag.Flag) {
		if flag.Name == "checks" {
			opts.checksProvided = true
		}
	})

	if flags.NArg() > 0 {
		return options{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}

	timeout, err := time.ParseDuration(timeoutRaw)
	if err != nil {
		return options{}, fmt.Errorf("parse timeout %q: %w", timeoutRaw, err)
	}

	if timeout < 0 {
		return options{}, fmt.Errorf("timeout must be non-negative")
	}

	opts.timeout = timeout
	opts.profile = strings.TrimSpace(opts.profile)
	if opts.profile == "" {
		opts.profile = defaultProfile
	}

	opts.checksRaw = strings.TrimSpace(opts.checksRaw)
	if opts.checksRaw == "" && opts.checksProvided && !opts.list {
		return options{}, fmt.Errorf("at least one check is required")
	}

	if opts.checksRaw == "" {
		profileChecks, ok := checkProfiles()[opts.profile]
		if !ok && !opts.list {
			return options{}, fmt.Errorf("unknown profile %q; expected one of %s", opts.profile, strings.Join(sortedProfileNames(), ", "))
		}

		opts.checksRaw = profileChecks
	}

	if opts.checksRaw == "" && !opts.list {
		return options{}, fmt.Errorf("at least one check is required")
	}

	opts.outRoot = strings.TrimSpace(opts.outRoot)
	if opts.outRoot == "" {
		return options{}, fmt.Errorf("output directory is required")
	}

	opts.runName = strings.TrimSpace(opts.runName)
	if opts.runName == "" {
		opts.runName = time.Now().UTC().Format("20060102T150405Z")
	}

	if strings.ContainsAny(opts.runName, `/\`) {
		return options{}, fmt.Errorf("run name %q must not contain path separators", opts.runName)
	}

	opts.artifactRoots = splitCSV(opts.artifactRootsRaw)

	return opts, nil
}

func checkProfiles() map[string]string {
	return map[string]string{
		"metadata":  "metadata",
		"local":     "git-clean,docs-npm-audit,release-local",
		"candidate": "git-clean,docs-npm-audit,release-local,postgres-integration,package-linux",
		"full":      "git-clean,docs-npm-audit,release-local,postgres-integration,package-linux,perf-macro,vm-e2e",
	}
}

func availableChecks() map[string]checkSpec {
	return map[string]checkSpec{
		"git-clean": {
			ID:          "git-clean",
			Title:       "Clean git worktree",
			Description: "Fails when tracked or untracked files are present in the release worktree.",
			Internal:    "git-clean",
		},
		"metadata": {
			ID:          "metadata",
			Title:       "Collect release metadata",
			Description: "Records git/toolchain identity and artifact checksums without running a command.",
		},
		"docs-npm-audit": {
			ID:          "docs-npm-audit",
			Title:       "Docs dependency audit",
			Description: "Runs `npm audit --json` for the docs site and fails on high or critical findings.",
			Internal:    "docs-npm-audit",
		},
		"release-local": {
			ID:          "release-local",
			Title:       "Local release validation",
			Description: "`mage releaseLocalValidate`: quick tests, deploy artifact tests, package tests, and build.",
			Command:     []string{"mage", "releaseLocalValidate"},
		},
		"test-quick": {
			ID:          "test-quick",
			Title:       "Quick Go test lane",
			Description: "`mage testQuick` fast package coverage.",
			Command:     []string{"mage", "testQuick"},
		},
		"openapi-inventory": {
			ID:          "openapi-inventory",
			Title:       "OpenAPI route inventory",
			Description: "Checks that the OpenAPI document covers registered API routes.",
			Command:     []string{"go", "test", "./internal/api", "-run", "TestOpenAPISpec", "-count=1"},
		},
		"deploy-artifacts": {
			ID:          "deploy-artifacts",
			Title:       "Linux deploy artifact tests",
			Description: "`mage deployArtifactsTest` validates generated Linux deploy artifacts.",
			Command:     []string{"mage", "deployArtifactsTest"},
		},
		"test-package": {
			ID:          "test-package",
			Title:       "Package unit tests",
			Description: "`mage testPackage` validates DEB/RPM package builders.",
			Command:     []string{"mage", "testPackage"},
		},
		"build": {
			ID:          "build",
			Title:       "Build all binaries",
			Description: "`mage build` builds the configured Vectis binary set.",
			Command:     []string{"mage", "build"},
		},
		"postgres-integration": {
			ID:          "postgres-integration",
			Title:       "Postgres integration tests",
			Description: "`mage testPostgresIntegration`; select when DB or deploy-sensitive behavior changed.",
			Command:     []string{"mage", "testPostgresIntegration"},
		},
		"package-linux": {
			ID:          "package-linux",
			Title:       "Linux package build",
			Description: "`mage packageLinux` builds CLI and service Linux packages.",
			Command:     []string{"mage", "packageLinux"},
		},
		"perf-macro": {
			ID:          "perf-macro",
			Title:       "Macro performance check",
			Description: "`SUITE=macro mage perf`; select for queue/orchestrator/worker hot-path changes.",
			Command:     []string{"mage", "perf"},
			Env:         []string{"SUITE=macro"},
		},
		"vm-e2e": {
			ID:          "vm-e2e",
			Title:       "VM-backed end-to-end lanes",
			Description: "`mage testE2EVM`; select for package, systemd, deploy, VM, or isolation changes.",
			Command:     []string{"mage", "testE2EVM"},
		},
	}
}

func printChecks(stdout io.Writer, checks map[string]checkSpec) {
	fmt.Fprintln(stdout, "Profiles:")
	for _, name := range sortedProfileNames() {
		fmt.Fprintf(stdout, "%s\t%s\n", name, checkProfiles()[name])
	}

	fmt.Fprintln(stdout, "\nChecks:")
	ids := sortedCheckIDs(checks)
	for _, id := range ids {
		spec := checks[id]
		command := "(metadata only)"
		if spec.Internal != "" {
			command = "(internal: " + spec.Internal + ")"
		} else if len(spec.Command) > 0 {
			command = checkCommandString(spec)
		}

		fmt.Fprintf(stdout, "%s\t%s\t%s\n", spec.ID, spec.Title, command)
	}
}

func runWithExecutor(opts options, stdout io.Writer, executor commandExecutor) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	cwd, err = filepath.Abs(cwd)
	if err != nil {
		return err
	}

	checks, err := resolveChecks(opts.checksRaw, availableChecks())
	if err != nil {
		return err
	}

	if err := validateSkipsAndWaivers(checks, opts); err != nil {
		return err
	}

	outRoot := opts.outRoot
	if !filepath.IsAbs(outRoot) {
		outRoot = filepath.Join(cwd, outRoot)
	}

	runDir := filepath.Join(outRoot, opts.runName)
	logsDir := filepath.Join(runDir, "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return fmt.Errorf("create report directories: %w", err)
	}

	rep := report{
		SchemaVersion: 1,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		RunName:       opts.runName,
		Workspace:     cwd,
		OutputDir:     runDir,
		Strict:        opts.strict,
		FailFast:      opts.failFast,
		Profile:       reportProfile(opts),
		ArtifactRoots: opts.artifactRoots,
	}

	rep.Git, rep.Warnings = collectGitInfo(cwd)
	rep.Toolchain = collectToolchainInfo(cwd)

	haltRemaining := false
	for _, spec := range checks {
		result := checkResult{}
		if haltRemaining {
			result = skippedAfterFailure(spec)
		} else {
			result = runCheck(spec, opts, cwd, runDir, logsDir, executor)
			if opts.failFast && result.Status == statusFailed {
				haltRemaining = true
			}
		}

		rep.Checks = append(rep.Checks, result)
		addCount(&rep.Counts, result.Status)
		fmt.Fprintf(stdout, "%-22s %s\n", result.ID, result.Status)
	}

	artifacts, err := collectArtifacts(cwd, opts.artifactRoots)
	if err != nil {
		rep.Warnings = append(rep.Warnings, fmt.Sprintf("collect artifacts: %v", err))
	}

	rep.Artifacts = artifacts

	if err := writeChecksums(runDir, artifacts); err != nil {
		return err
	}

	if err := writeSummaryJSON(runDir, rep); err != nil {
		return err
	}

	if err := writeMarkdownReport(runDir, rep); err != nil {
		return err
	}

	fmt.Fprintf(stdout, "\nReport: %s\n", filepath.Join(runDir, "report.md"))
	fmt.Fprintf(stdout, "Summary: %s\n", filepath.Join(runDir, "summary.json"))

	if rep.Counts.Failed > 0 {
		return errReadinessFailed
	}

	if opts.strict && (rep.Counts.Skipped > 0 || rep.Counts.Waived > 0) {
		return errReadinessFailed
	}

	return nil
}

func reportProfile(opts options) string {
	if opts.checksProvided {
		return "custom"
	}

	return opts.profile
}

func skippedAfterFailure(spec checkSpec) checkResult {
	start := time.Now().UTC()
	result := checkResult{
		ID:          spec.ID,
		Title:       spec.Title,
		Description: spec.Description,
		Status:      statusSkipped,
		Command:     checkCommandString(spec),
		Reason:      "not run after earlier failure because --fail-fast was set",
		StartedAt:   start.Format(time.RFC3339),
	}

	finishCheck(&result, start)
	return result
}

func resolveChecks(raw string, available map[string]checkSpec) ([]checkSpec, error) {
	ids := splitCSV(raw)
	if len(ids) == 0 {
		return nil, fmt.Errorf("at least one check is required")
	}

	var checks []checkSpec
	seen := make(map[string]bool)
	for _, id := range ids {
		if seen[id] {
			continue
		}

		seen[id] = true
		spec, ok := available[id]
		if !ok {
			return nil, fmt.Errorf("unknown check %q; run with --list-checks", id)
		}

		checks = append(checks, spec)
	}

	return checks, nil
}

func validateSkipsAndWaivers(checks []checkSpec, opts options) error {
	selected := make(map[string]bool, len(checks))
	for _, check := range checks {
		selected[check.ID] = true
	}

	for id := range opts.skips {
		if !selected[id] {
			return fmt.Errorf("skip references unselected check %q", id)
		}
	}

	for id := range opts.waivers {
		if !selected[id] {
			return fmt.Errorf("waiver references unselected check %q", id)
		}
	}

	for id := range opts.skips {
		if _, ok := opts.waivers[id]; ok {
			return fmt.Errorf("check %q cannot be both skipped and waived", id)
		}
	}

	return nil
}

func splitCSV(raw string) []string {
	var values []string
	for _, part := range strings.Split(raw, ",") {
		value := strings.TrimSpace(part)
		if value != "" {
			values = append(values, value)
		}
	}

	return values
}

func sortedCheckIDs(checks map[string]checkSpec) []string {
	ids := make([]string, 0, len(checks))
	for id := range checks {
		ids = append(ids, id)
	}

	sort.Strings(ids)
	return ids
}

func sortedProfileNames() []string {
	profiles := checkProfiles()
	names := make([]string, 0, len(profiles))
	for name := range profiles {
		names = append(names, name)
	}

	sort.Strings(names)
	return names
}

func runCheck(spec checkSpec, opts options, cwd, runDir, logsDir string, executor commandExecutor) checkResult {
	start := time.Now().UTC()
	result := checkResult{
		ID:          spec.ID,
		Title:       spec.Title,
		Description: spec.Description,
		Status:      statusPassed,
		Command:     checkCommandString(spec),
		StartedAt:   start.Format(time.RFC3339),
	}

	if reason, ok := opts.skips[spec.ID]; ok {
		result.Status = statusSkipped
		result.Reason = reason
		finishCheck(&result, start)

		return result
	}

	if reason, ok := opts.waivers[spec.ID]; ok {
		result.Status = statusWaived
		result.Reason = reason
		finishCheck(&result, start)

		return result
	}

	logPath := filepath.Join(logsDir, spec.ID+".log")
	result.LogPath = slashRel(runDir, logPath)

	logFile, err := os.Create(logPath)
	if err != nil {
		result.Status = statusFailed
		result.Reason = fmt.Sprintf("create log: %v", err)
		finishCheck(&result, start)

		return result
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(logFile)

	fmt.Fprintf(logFile, "# %s\n", spec.Title)
	if len(spec.Command) > 0 {
		fmt.Fprintf(logFile, "$ %s\n\n", checkCommandString(spec))
	} else if spec.Internal != "" {
		fmt.Fprintf(logFile, "# internal check: %s\n\n", spec.Internal)
	} else {
		fmt.Fprintln(logFile, "No command selected; metadata will be collected by the report.")
	}

	if spec.Internal != "" {
		status, reason := runInternalCheck(spec.Internal, cwd, logFile)
		result.Status = status
		result.Reason = reason
		finishCheck(&result, start)

		return result
	}

	if len(spec.Command) == 0 {
		finishCheck(&result, start)
		return result
	}

	ctx := context.Background()
	cancel := func() {}
	if opts.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, opts.timeout)
	}
	defer cancel()

	exitCode, runErr := executor(ctx, cwd, spec.Command, spec.Env, logFile)
	result.ExitCode = exitCode
	if runErr != nil {
		result.Status = statusFailed
		result.Reason = runErr.Error()
	}

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		result.Status = statusFailed
		result.Reason = fmt.Sprintf("timed out after %s", opts.timeout)
	}

	finishCheck(&result, start)
	return result
}

func runInternalCheck(name, cwd string, log io.Writer) (string, string) {
	switch name {
	case "git-clean":
		status, err := commandOutput(cwd, "git", "status", "--short", "--branch")
		if status != "" {
			fmt.Fprintln(log, status)
		}

		if err != nil {
			return statusFailed, fmt.Sprintf("git status failed: %v", err)
		}

		if gitStatusIsDirty(status) {
			return statusFailed, "working tree has uncommitted or untracked changes"
		}

		return statusPassed, ""
	case "docs-npm-audit":
		return runDocsNpmAudit(cwd, log)
	default:
		return statusFailed, fmt.Sprintf("unknown internal check %q", name)
	}
}

func runDocsNpmAudit(cwd string, log io.Writer) (string, string) {
	websiteDir := filepath.Join(cwd, "website")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, "npm", "audit", "--json")
	cmd.Dir = websiteDir
	cmd.Env = os.Environ()

	fmt.Fprintln(log, "$ cd website && npm audit --json")
	raw, err := cmd.CombinedOutput()
	if len(raw) > 0 {
		fmt.Fprintln(log, string(raw))
	}

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return statusFailed, "npm audit timed out after 2m"
	}

	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			return statusFailed, fmt.Sprintf("npm audit failed: %v", err)
		}
	}

	counts, err := parseNpmAuditCounts(raw)
	if err != nil {
		return statusFailed, err.Error()
	}

	return npmAuditStatus(counts)
}

func parseNpmAuditCounts(raw []byte) (npmAuditCounts, error) {
	var audit npmAuditReport
	if err := json.Unmarshal(raw, &audit); err != nil {
		return npmAuditCounts{}, fmt.Errorf("parse npm audit JSON: %w", err)
	}

	return audit.Metadata.Vulnerabilities, nil
}

func npmAuditStatus(counts npmAuditCounts) (string, string) {
	reason := fmt.Sprintf(
		"critical=%d high=%d moderate=%d low=%d info=%d total=%d",
		counts.Critical,
		counts.High,
		counts.Moderate,
		counts.Low,
		counts.Info,
		counts.Total,
	)

	if counts.Critical > 0 || counts.High > 0 {
		return statusFailed, "npm audit high/critical findings: " + reason
	}

	return statusPassed, reason
}

func finishCheck(result *checkResult, start time.Time) {
	finished := time.Now().UTC()
	result.FinishedAt = finished.Format(time.RFC3339)
	result.DurationMS = finished.Sub(start).Milliseconds()
}

func executeCommand(ctx context.Context, cwd string, command, env []string, log io.Writer) (int, error) {
	cmd := exec.CommandContext(ctx, command[0], command[1:]...) // #nosec G204 -- release readiness intentionally runs configured local checks.
	cmd.Dir = cwd
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = log
	cmd.Stderr = log

	err := cmd.Run()
	if err == nil {
		return 0, nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode(), err
	}

	return -1, err
}

func collectGitInfo(cwd string) (gitInfo, []string) {
	var warnings []string
	runGit := func(args ...string) string {
		out, err := commandOutput(cwd, append([]string{"git"}, args...)...)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("git %s: %v", strings.Join(args, " "), err))
			return ""
		}

		return strings.TrimSpace(out)
	}

	status := runGit("status", "--short", "--branch")
	return gitInfo{
		Commit:      runGit("rev-parse", "HEAD"),
		Describe:    runGit("describe", "--tags", "--always", "--dirty"),
		Branch:      runGit("branch", "--show-current"),
		StatusShort: status,
		Dirty:       gitStatusIsDirty(status),
	}, warnings
}

func collectToolchainInfo(cwd string) toolchainInfo {
	goVersion, _ := commandOutput(cwd, "go", "version")
	mageVersion, _ := commandOutput(cwd, "mage", "--version")
	return toolchainInfo{
		GoVersion:   firstLine(goVersion),
		MageVersion: firstLine(mageVersion),
	}
}

func commandOutput(cwd string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, args[0], args[1:]...) // #nosec G204 -- commandOutput only runs fixed local tool probes.
	cmd.Dir = cwd
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), err
	}

	return string(out), nil
}

func gitStatusIsDirty(status string) bool {
	for _, line := range strings.Split(status, "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "##") {
			return true
		}
	}

	return false
}

func firstLine(raw string) string {
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}

	return ""
}

func collectArtifacts(cwd string, roots []string) ([]artifactRecord, error) {
	var records []artifactRecord
	for _, root := range roots {
		fullRoot := filepath.Join(cwd, root)
		info, err := os.Stat(fullRoot)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}

		if err != nil {
			return nil, err
		}

		if !info.IsDir() {
			record, err := checksumFile(cwd, fullRoot, info)
			if err != nil {
				return nil, err
			}

			records = append(records, record)
			continue
		}

		err = filepath.WalkDir(fullRoot, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}

			if entry.IsDir() {
				return nil
			}

			info, err := entry.Info()
			if err != nil {
				return err
			}

			record, err := checksumFile(cwd, path, info)
			if err != nil {
				return err
			}

			records = append(records, record)
			return nil
		})

		if err != nil {
			return nil, err
		}
	}

	sort.Slice(records, func(i, j int) bool {
		return records[i].Path < records[j].Path
	})

	return records, nil
}

func checksumFile(cwd, path string, info fs.FileInfo) (artifactRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return artifactRecord{}, err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(file)

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return artifactRecord{}, err
	}

	return artifactRecord{
		Path:   slashRel(cwd, path),
		Size:   info.Size(),
		SHA256: hex.EncodeToString(hash.Sum(nil)),
	}, nil
}

func writeChecksums(runDir string, artifacts []artifactRecord) error {
	var b strings.Builder
	for _, artifact := range artifacts {
		fmt.Fprintf(&b, "%s  %s\n", artifact.SHA256, artifact.Path)
	}

	return os.WriteFile(filepath.Join(runDir, "checksums.txt"), []byte(b.String()), 0o644)
}

func writeSummaryJSON(runDir string, rep report) error {
	raw, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}

	raw = append(raw, '\n')
	return os.WriteFile(filepath.Join(runDir, "summary.json"), raw, 0o644)
}

func writeMarkdownReport(runDir string, rep report) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Release Readiness Report\n\n")
	fmt.Fprintf(&b, "| Field | Value |\n")
	fmt.Fprintf(&b, "| --- | --- |\n")
	fmt.Fprintf(&b, "| Generated | %s |\n", rep.GeneratedAt)
	fmt.Fprintf(&b, "| Run | %s |\n", rep.RunName)
	fmt.Fprintf(&b, "| Commit | `%s` |\n", rep.Git.Commit)
	fmt.Fprintf(&b, "| Version | `%s` |\n", rep.Git.Describe)
	fmt.Fprintf(&b, "| Branch | `%s` |\n", valueOr(rep.Git.Branch, "(detached)"))
	fmt.Fprintf(&b, "| Dirty | %t |\n", rep.Git.Dirty)
	fmt.Fprintf(&b, "| Profile | `%s` |\n", valueOr(rep.Profile, "(explicit checks)"))
	fmt.Fprintf(&b, "| Artifact roots | `%s` |\n", strings.Join(rep.ArtifactRoots, ", "))
	fmt.Fprintf(&b, "| Strict | %t |\n", rep.Strict)
	fmt.Fprintf(&b, "| Fail fast | %t |\n\n", rep.FailFast)

	fmt.Fprintf(&b, "## Summary\n\n")
	fmt.Fprintf(&b, "| Passed | Failed | Skipped | Waived |\n")
	fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: |\n")
	fmt.Fprintf(&b, "| %d | %d | %d | %d |\n\n", rep.Counts.Passed, rep.Counts.Failed, rep.Counts.Skipped, rep.Counts.Waived)

	fmt.Fprintf(&b, "## Checks\n\n")
	fmt.Fprintf(&b, "| Check | Status | Duration | Log | Reason |\n")
	fmt.Fprintf(&b, "| --- | --- | ---: | --- | --- |\n")
	for _, check := range rep.Checks {
		logPath := ""
		if check.LogPath != "" {
			logPath = fmt.Sprintf("[%s](%s)", check.LogPath, check.LogPath)
		}

		fmt.Fprintf(&b, "| `%s` | %s | %dms | %s | %s |\n",
			check.ID,
			check.Status,
			check.DurationMS,
			logPath,
			escapeTable(check.Reason),
		)
	}

	fmt.Fprintln(&b)

	if len(rep.Artifacts) > 0 {
		fmt.Fprintf(&b, "## Artifact Checksums\n\n")
		fmt.Fprintf(&b, "| Path | Size | SHA-256 |\n")
		fmt.Fprintf(&b, "| --- | ---: | --- |\n")
		for _, artifact := range rep.Artifacts {
			fmt.Fprintf(&b, "| `%s` | %d | `%s` |\n", artifact.Path, artifact.Size, artifact.SHA256)
		}

		fmt.Fprintln(&b)
	} else {
		fmt.Fprintf(&b, "## Artifact Checksums\n\nNo matching release artifacts were found.\n\n")
	}

	if rep.Git.StatusShort != "" {
		fmt.Fprintf(&b, "## Git Status\n\n```text\n%s\n```\n\n", rep.Git.StatusShort)
	}

	if len(rep.Warnings) > 0 {
		fmt.Fprintf(&b, "## Warnings\n\n")
		for _, warning := range rep.Warnings {
			fmt.Fprintf(&b, "- %s\n", warning)
		}

		fmt.Fprintln(&b)
	}

	return os.WriteFile(filepath.Join(runDir, "report.md"), []byte(b.String()), 0o644)
}

func addCount(counts *checkCounts, status string) {
	switch status {
	case statusPassed:
		counts.Passed++
	case statusFailed:
		counts.Failed++
	case statusSkipped:
		counts.Skipped++
	case statusWaived:
		counts.Waived++
	}
}

func commandString(command []string) string {
	if len(command) == 0 {
		return ""
	}

	parts := make([]string, 0, len(command))
	for _, arg := range command {
		parts = append(parts, quoteArg(arg))
	}

	return strings.Join(parts, " ")
}

func checkCommandString(spec checkSpec) string {
	command := commandString(spec.Command)
	if command == "" {
		return ""
	}

	if len(spec.Env) == 0 {
		return command
	}

	parts := make([]string, 0, len(spec.Env)+1)
	for _, env := range spec.Env {
		parts = append(parts, quoteArg(env))
	}

	parts = append(parts, command)
	return strings.Join(parts, " ")
}

func quoteArg(arg string) string {
	if arg == "" {
		return "''"
	}

	if strings.ContainsAny(arg, " \t\n'\"$`\\") {
		return "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
	}

	return arg
}

func slashRel(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}

	return filepath.ToSlash(rel)
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}

	return value
}

func escapeTable(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "|", "\\|")
	return value
}
