package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	defaultQueueBench  = `BenchmarkQueue_(EnqueueDequeue_RoundTrip|ConcurrentEnqueueDequeue|SustainedLoad|RingLatency)`
	defaultDALBench    = `BenchmarkDAL_`
	defaultJobBench    = `BenchmarkExecutor_`
	defaultMacroBench  = `BenchmarkMacro_`
	defaultArtifactDir = "artifacts/perf"

	configErrorStatus = 2

	envPerfDatabaseDSN            = "VECTIS_PERF_DATABASE_DSN"
	envPerfDatabaseDriver         = "VECTIS_PERF_DATABASE_DRIVER"
	envPerfPGStatStatementsOutput = "VECTIS_PERF_PG_STAT_STATEMENTS_OUTPUT"
	envPerfPostgresDSN            = "VECTIS_PERF_POSTGRES_DSN"
	envPerfPostgresDurability     = "VECTIS_PERF_POSTGRES_DURABILITY"
	envPerfServe                  = "VECTIS_PERF_SERVE"
	envPerfServeHost              = "VECTIS_PERF_SERVE_HOST"
	envPerfServePort              = "VECTIS_PERF_SERVE_PORT"
	envPerfSQLiteDSN              = "VECTIS_PERF_SQLITE_DSN"
	envVectisDatabaseDSN          = "VECTIS_DATABASE_DSN"
	envVectisDatabaseDriver       = "VECTIS_DATABASE_DRIVER"

	postgresDurabilitySafe   = "safe"
	postgresDurabilityUnsafe = "unsafe"
)

var (
	benchmarkArchSuffixRE = regexp.MustCompile(`-\d+$`)
	benchmarkLineTagRE    = regexp.MustCompile(`^(.*)(-\d+)$`)
	sanitizePartRE        = regexp.MustCompile(`[^A-Za-z0-9_]+`)
	sanitizePathRE        = regexp.MustCompile(`[^A-Za-z0-9._-]+`)
	pgStatLineRE          = regexp.MustCompile(`^# pg_stat_statements\s+(?:database=([^ ]+)\s+)?benchmark=([^ ]+)\s+iterations=(\d+)\s+rank=(\d+)\s+calls=(\d+)\s+total_ms=([0-9.]+)\s+mean_ms=([0-9.]+)\s+rows=(\d+)\s+query="(.*)"$`)
)

type suiteConfig struct {
	name         string
	pkg          string
	defaultBench string
	benchEnv     string
}

var suites = map[string]suiteConfig{
	"queue": {
		name:         "queue",
		pkg:          "./internal/queue",
		defaultBench: defaultQueueBench,
		benchEnv:     "VECTIS_PERF_QUEUE_BENCH",
	},
	"dal": {
		name:         "dal",
		pkg:          "./internal/dal",
		defaultBench: defaultDALBench,
		benchEnv:     "VECTIS_PERF_DAL_BENCH",
	},
	"job": {
		name:         "job",
		pkg:          "./internal/job",
		defaultBench: defaultJobBench,
		benchEnv:     "VECTIS_PERF_JOB_BENCH",
	},
	"macro": {
		name:         "macro",
		pkg:          "./tests/perf",
		defaultBench: defaultMacroBench,
		benchEnv:     "VECTIS_PERF_MACRO_BENCH",
	},
}

type runArgs struct {
	suite       suiteConfig
	benchtime   string
	count       int
	bench       string
	goBin       string
	artifactDir string
	runName     string
	baseline    string
	benchstat   string
	databases   string
	serve       bool
	serveHost   string
	servePort   int
}

type BenchmarkMetric struct {
	Value string `json:"value"`
	Unit  string `json:"unit"`
}

type BenchmarkResult struct {
	Name       string            `json:"name"`
	Iterations string            `json:"iterations"`
	Metrics    []BenchmarkMetric `json:"metrics"`
}

type HarnessMetadata struct {
	Suite           string   `json:"suite"`
	StartedAt       string   `json:"started_at"`
	FinishedAt      string   `json:"finished_at"`
	DurationSeconds float64  `json:"duration_seconds"`
	Command         []string `json:"command"`
	Status          int      `json:"status"`
	GitCommit       string   `json:"git_commit"`
	GitDirty        bool     `json:"git_dirty"`
	GoVersion       string   `json:"go_version"`
	GOOS            string   `json:"goos"`
	GOARCH          string   `json:"goarch"`
	CPU             string   `json:"cpu"`
	Pkg             string   `json:"pkg"`
}

type PGStatStatement struct {
	Database   string  `json:"database,omitempty"`
	Benchmark  string  `json:"benchmark"`
	Iterations int     `json:"iterations"`
	Rank       int     `json:"rank"`
	Calls      int64   `json:"calls"`
	TotalMS    float64 `json:"total_ms"`
	MeanMS     float64 `json:"mean_ms"`
	Rows       int64   `json:"rows"`
	Query      string  `json:"query"`
}

type Summary struct {
	Metadata         HarnessMetadata   `json:"metadata"`
	Results          []BenchmarkResult `json:"results"`
	PGStatStatements []PGStatStatement `json:"pg_stat_statements,omitempty"`
}

type commandResult struct {
	output string
	status int
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		printUsage(os.Stderr)
		return configErrorStatus
	}

	if args[0] == "compare" {
		return runCompare(args[1:])
	}

	suite, ok := suites[args[0]]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown performance suite %q\n", args[0])
		printUsage(os.Stderr)
		return configErrorStatus
	}

	parsed, err := parseRunArgs(suite, args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return configErrorStatus
	}

	status, err := runGoBenchmarkSuite(parsed)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return configErrorStatus
	}

	return status
}

func printUsage(out *os.File) {
	fmt.Fprintln(out, "usage: vectis-perf <queue|dal|job|macro> [options]")
	fmt.Fprintln(out, "       vectis-perf compare --baseline <path> --current <path> [--benchstat benchstat]")
}

func parseRunArgs(suite suiteConfig, args []string) (runArgs, error) {
	fs := flag.NewFlagSet(suite.name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	parsed := runArgs{suite: suite}
	fs.StringVar(&parsed.benchtime, "benchtime", envString("VECTIS_PERF_BENCHTIME", "2s"), "Go benchmark duration per scenario.")
	fs.IntVar(&parsed.count, "count", envInt("VECTIS_PERF_COUNT", 1), "Go benchmark repetition count.")
	fs.StringVar(&parsed.bench, "bench", envString(suite.benchEnv, suite.defaultBench), "Go benchmark regex.")
	fs.StringVar(&parsed.goBin, "go", envString("GO", "go"), "Go binary to use.")
	fs.StringVar(&parsed.artifactDir, "artifact-dir", envString("VECTIS_PERF_ARTIFACT_DIR", defaultArtifactDir), "Directory where run artifacts are written.")
	fs.StringVar(&parsed.runName, "run-name", envString("VECTIS_PERF_RUN_NAME", ""), "Optional artifact run name.")
	fs.StringVar(&parsed.baseline, "baseline", envString("VECTIS_PERF_BASELINE", ""), "Optional baseline Go benchmark output for benchstat.")
	fs.StringVar(&parsed.benchstat, "benchstat", envString("BENCHSTAT", "benchstat"), "benchstat binary to use when --baseline is provided.")
	fs.BoolVar(&parsed.serve, "serve", envBool(envPerfServe, false), "Serve the artifact directory after the run and wait for Ctrl-C.")
	fs.StringVar(&parsed.serveHost, "serve-host", envString(envPerfServeHost, "127.0.0.1"), "Host used when --serve is enabled.")
	fs.IntVar(&parsed.servePort, "serve-port", envNonNegativeInt(envPerfServePort, 0), "Port used when --serve is enabled. Use 0 to auto-select.")

	if suite.name == "macro" {
		fs.StringVar(&parsed.databases, "databases", envString("VECTIS_PERF_MACRO_DATABASES", ""), "Optional comma-separated macro database matrix.")
	}

	if err := fs.Parse(args); err != nil {
		return runArgs{}, err
	}

	if parsed.count <= 0 {
		return runArgs{}, fmt.Errorf("--count must be positive, got %d", parsed.count)
	}

	if parsed.servePort < 0 {
		return runArgs{}, fmt.Errorf("--serve-port must be zero or positive, got %d", parsed.servePort)
	}

	return parsed, nil
}

func runCompare(args []string) int {
	fs := flag.NewFlagSet("compare", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var baseline string
	var current string
	var benchstat string
	fs.StringVar(&baseline, "baseline", "", "Baseline Go benchmark output.")
	fs.StringVar(&current, "current", "", "Current Go benchmark output.")
	fs.StringVar(&benchstat, "benchstat", envString("BENCHSTAT", "benchstat"), "benchstat binary to use.")

	if err := fs.Parse(args); err != nil {
		return configErrorStatus
	}

	if baseline == "" || current == "" {
		fmt.Fprintln(os.Stderr, "compare requires --baseline and --current")
		return configErrorStatus
	}

	return runBenchstat(benchstat, baseline, current, "")
}

func runGoBenchmarkSuite(args runArgs) (int, error) {
	command := goBenchmarkCommand(args)
	started := time.Now().UTC()
	startedMonotonic := time.Now()
	runDir, err := goBenchmarkRunDir(args, started)
	if err != nil {
		return configErrorStatus, err
	}

	printHeading("Vectis Performance Harness")
	emit(fmt.Sprintf("Suite              : %s", args.suite.name))
	emit(fmt.Sprintf("Package            : %s", args.suite.pkg))
	emit(fmt.Sprintf("Benchmark duration : %s", args.benchtime))
	emit(fmt.Sprintf("Repetitions        : %d", args.count))
	emit(fmt.Sprintf("Benchmark pattern  : %s", args.bench))
	databaseMatrix := macroDatabaseMatrix(args)

	if len(databaseMatrix) > 0 {
		emit(fmt.Sprintf("Database matrix    : %s", strings.Join(databaseMatrix, ", ")))
	}

	emit(fmt.Sprintf("Artifacts          : %s", runDir))

	rawOutput, status, metadataCommand, err := captureGoBenchmarkOutput(args, command, databaseMatrix, runDir)
	if err != nil {
		return configErrorStatus, err
	}

	finished := time.Now().UTC()
	rawPath := filepath.Join(runDir, "go-bench.txt")
	if err := os.WriteFile(rawPath, []byte(rawOutput), 0o644); err != nil {
		return configErrorStatus, err
	}

	env := parseGoBenchmarkEnvironment(rawOutput)
	results := parseGoBenchmarkResults(rawOutput)
	pgStats := parsePGStatStatements(rawOutput)
	metadata := HarnessMetadata{
		Suite:           args.suite.name,
		StartedAt:       started.Format(time.RFC3339Nano),
		FinishedAt:      finished.Format(time.RFC3339Nano),
		DurationSeconds: roundDurationSeconds(time.Since(startedMonotonic)),
		Command:         metadataCommand,
		Status:          status,
		GitCommit:       gitOutput([]string{"rev-parse", "--short=12", "HEAD"}, "unknown"),
		GitDirty:        gitDirty(),
		GoVersion:       commandOutput([]string{args.goBin, "version"}, "unknown"),
		GOOS:            env["goos"],
		GOARCH:          env["goarch"],
		CPU:             env["cpu"],
		Pkg:             env["pkg"],
	}

	summary := Summary{Metadata: metadata, Results: results, PGStatStatements: pgStats}
	summaryPath := filepath.Join(runDir, "summary.json")
	if err := writeJSON(summaryPath, summary); err != nil {
		return configErrorStatus, err
	}

	markdownPath := filepath.Join(runDir, "summary.md")
	if err := writeMarkdownSummary(markdownPath, summary, filepath.Base(rawPath)); err != nil {
		return configErrorStatus, err
	}

	reportPath := filepath.Join(runDir, "report.html")
	if err := writeHTMLReport(reportPath, summary); err != nil {
		return configErrorStatus, err
	}

	printHeading("Environment")
	printEnvironment(metadata)

	if status != 0 {
		printHeading("Benchmark Failed")
		emitRaw(rawOutput)
		printArtifactFooter(runDir)
		if args.serve {
			if err := serveRunDir(runDir, args.serveHost, args.servePort); err != nil {
				return configErrorStatus, err
			}
		}

		return status, nil
	}

	printHeading("Benchmark Summary")
	printBenchmarkSummary(results)

	compareStatus := 0
	if args.baseline != "" {
		printHeading("Benchstat Comparison")
		compareStatus = runBenchstat(args.benchstat, args.baseline, rawPath, filepath.Join(runDir, "benchstat.txt"))
	}

	printNextChecks(args.suite.name)
	printArtifactFooter(runDir)
	if args.serve {
		if err := serveRunDir(runDir, args.serveHost, args.servePort); err != nil {
			return configErrorStatus, err
		}
	}

	return compareStatus, nil
}

func goBenchmarkCommand(args runArgs) []string {
	return []string{
		args.goBin,
		"test",
		args.suite.pkg,
		"-run",
		"^$",
		"-bench",
		args.bench,
		"-benchtime",
		args.benchtime,
		"-count",
		strconv.Itoa(args.count),
		"-benchmem",
	}
}

func goBenchmarkRunDir(args runArgs, started time.Time) (string, error) {
	runName := args.runName
	if runName == "" {
		runName = fmt.Sprintf("%s-%s", started.Format("20060102T150405Z"), args.suite.name)
	}

	runDir := filepath.Join(args.artifactDir, sanitizePathPart(runName))
	return runDir, os.MkdirAll(runDir, 0o755)
}

func captureGoBenchmarkOutput(args runArgs, command []string, databaseMatrix []string, runDir string) (string, int, []string, error) {
	if len(databaseMatrix) > 0 {
		return runMacroDatabaseMatrix(command, databaseMatrix, runDir)
	}

	env := goBenchmarkEnv(args)
	var pgStatOutput string
	if args.suite.name == "macro" {
		pgStatOutput = filepath.Join(runDir, "pg-stat-statements.txt")
		preparePGStatOutput(pgStatOutput)
		if env == nil {
			env = envMapFromOS()
		}

		env[envPerfPGStatStatementsOutput] = pgStatOutput
	}

	result := runCommand(command, env)
	rawOutput := appendPGStatOutput(result.output, pgStatOutput, "")
	return rawOutput, result.status, command, nil
}

func macroDatabaseMatrix(args runArgs) []string {
	if args.databases == "" {
		return nil
	}

	var databases []string
	for _, database := range strings.Split(args.databases, ",") {
		if trimmed := strings.TrimSpace(database); trimmed != "" {
			databases = append(databases, trimmed)
		}
	}

	return databases
}

func runMacroDatabaseMatrix(command []string, databases []string, runDir string) (string, int, []string, error) {
	var outputs []string
	status := 0

	for _, database := range databases {
		tag, err := macroDatabaseTag(database)
		if err != nil {
			return "", 0, nil, err
		}

		tag = "db_" + sanitizeBenchmarkPart(tag)
		env, err := macroDatabaseEnv(database)
		if err != nil {
			return "", 0, nil, err
		}

		pgStatOutput := filepath.Join(runDir, fmt.Sprintf("pg-stat-statements-%s.txt", tag))
		preparePGStatOutput(pgStatOutput)
		env[envPerfPGStatStatementsOutput] = pgStatOutput

		databaseCommand, err := macroDatabaseCommand(command, database)
		if err != nil {
			return "", 0, nil, err
		}

		result := runCommand(databaseCommand, env)
		if status == 0 {
			status = result.status
		}

		goOutput := tagGoBenchmarkOutput(result.output, tag)
		goOutput = appendPGStatOutput(goOutput, pgStatOutput, tag)
		outputs = append(outputs, fmt.Sprintf("# database: %s\n%s", database, goOutput))
	}

	metadataCommand := append([]string{"database-matrix=" + strings.Join(databases, ",")}, command...)
	return strings.Join(outputs, "\n"), status, metadataCommand, nil
}

func macroDatabaseEnv(database string) (map[string]string, error) {
	env := envMapFromOS()
	driver, err := canonicalMacroDatabaseDriver(database)
	if err != nil {
		return nil, err
	}

	if driver == "pgx_podman" || driver == "pgx_podman_unsafe" {
		delete(env, envPerfDatabaseDSN)
		delete(env, envVectisDatabaseDSN)
		env[envPerfDatabaseDriver] = "pgx"
		env[envVectisDatabaseDriver] = "pgx"

		if driver == "pgx_podman_unsafe" {
			env[envPerfPostgresDurability] = postgresDurabilityUnsafe
		} else {
			env[envPerfPostgresDurability] = postgresDurabilitySafe
		}

		return env, nil
	}

	env[envPerfDatabaseDriver] = driver
	env[envVectisDatabaseDriver] = driver
	if driver == "sqlite3" {
		sqliteDSN := env[envPerfSQLiteDSN]
		if sqliteDSN != "" {
			env[envPerfDatabaseDSN] = sqliteDSN
			env[envVectisDatabaseDSN] = sqliteDSN
		} else {
			delete(env, envPerfDatabaseDSN)
			delete(env, envVectisDatabaseDSN)
		}

		return env, nil
	}

	postgresDSN := env[envPerfPostgresDSN]
	if postgresDSN == "" {
		postgresDSN = env[envPerfDatabaseDSN]
	}

	if postgresDSN == "" {
		return nil, fmt.Errorf("pgx macro database matrix runs require %s or %s", envPerfPostgresDSN, envPerfDatabaseDSN)
	}

	env[envPerfDatabaseDSN] = postgresDSN
	env[envVectisDatabaseDSN] = postgresDSN
	return env, nil
}

func goBenchmarkEnv(args runArgs) map[string]string {
	if args.suite.name != "macro" {
		return nil
	}

	env := envMapFromOS()
	driver := env[envPerfDatabaseDriver]
	if driver == "" {
		driver = "sqlite3"
	}

	env[envVectisDatabaseDriver] = driver
	dsn := env[envPerfDatabaseDSN]
	if dsn != "" {
		env[envVectisDatabaseDSN] = dsn
	} else if driver == "sqlite3" {
		delete(env, envVectisDatabaseDSN)
	}

	return env
}

func macroDatabaseCommand(command []string, database string) ([]string, error) {
	driver, err := canonicalMacroDatabaseDriver(database)
	if err != nil {
		return nil, err
	}

	if driver != "pgx_podman" && driver != "pgx_podman_unsafe" {
		return command, nil
	}

	databaseCommand := []string{command[0], "run", "./scripts/perf-postgres-podman", "--"}
	databaseCommand = append(databaseCommand, command...)
	return databaseCommand, nil
}

func canonicalMacroDatabaseDriver(database string) (string, error) {
	value := strings.ToLower(strings.TrimSpace(database))
	switch value {
	case "sqlite", "sqlite3":
		return "sqlite3", nil
	case "postgres", "postgresql", "pg", "pgx":
		return "pgx", nil
	case "pgx_podman", "postgres_podman", "podman":
		return "pgx_podman", nil
	case "pgx_podman_unsafe", "postgres_podman_unsafe", "podman_unsafe":
		return "pgx_podman_unsafe", nil
	case "pgx_container", "postgres_container", "testcontainers", "testcontainer":
		return "pgx_podman", nil
	default:
		return "", fmt.Errorf("unsupported macro database backend %q; expected sqlite3, pgx, pgx_podman, or pgx_podman_unsafe", database)
	}
}

func macroDatabaseTag(database string) (string, error) {
	return canonicalMacroDatabaseDriver(database)
}

func runCommand(command []string, env map[string]string) commandResult {
	cmd := exec.Command(command[0], command[1:]...) // #nosec G204 -- perf harness intentionally runs configured local benchmark commands.
	cmd.Dir = repoRoot()
	if env != nil {
		cmd.Env = envList(env)
	}

	output, err := cmd.CombinedOutput()
	if err == nil {
		return commandResult{output: string(output), status: 0}
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return commandResult{output: string(output), status: exitErr.ExitCode()}
	}

	return commandResult{
		output: string(output) + fmt.Sprintf("run %s: %v\n", strings.Join(command, " "), err),
		status: 127,
	}
}

func tagGoBenchmarkOutput(output, tag string) string {
	lines := strings.Split(output, "\n")
	for i, line := range lines {
		lines[i] = tagGoBenchmarkLine(line, tag)
	}

	tagged := strings.Join(lines, "\n")
	if !strings.HasSuffix(output, "\n") && strings.HasSuffix(tagged, "\n") {
		tagged = strings.TrimSuffix(tagged, "\n")
	}

	return tagged
}

func tagGoBenchmarkLine(line, tag string) string {
	if !strings.HasPrefix(line, "Benchmark") {
		return line
	}

	fields := strings.SplitN(line, " ", 2)
	if len(fields) != 2 {
		return line
	}

	match := benchmarkLineTagRE.FindStringSubmatch(fields[0])
	if match == nil {
		return line
	}

	return fmt.Sprintf("%s/%s%s %s", match[1], tag, match[2], fields[1])
}

func preparePGStatOutput(path string) {
	if path == "" {
		return
	}

	_ = os.Remove(path)
}

func appendPGStatOutput(output, path, tag string) string {
	if path == "" {
		return output
	}

	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return output
	}

	pgStatOutput := string(data)
	if tag != "" {
		pgStatOutput = tagPGStatOutput(pgStatOutput, tag)
	}

	if output != "" && !strings.HasSuffix(output, "\n") {
		output += "\n"
	}

	return output + pgStatOutput
}

func tagPGStatOutput(output, tag string) string {
	lines := strings.Split(output, "\n")
	for i, line := range lines {
		lines[i] = tagPGStatLine(line, tag)
	}

	tagged := strings.Join(lines, "\n")
	if !strings.HasSuffix(output, "\n") && strings.HasSuffix(tagged, "\n") {
		tagged = strings.TrimSuffix(tagged, "\n")
	}

	return tagged
}

func tagPGStatLine(line, tag string) string {
	const prefix = "# pg_stat_statements "
	if !strings.HasPrefix(line, prefix) {
		return line
	}

	return prefix + "database=" + tag + " " + strings.TrimPrefix(line, prefix)
}

func parseGoBenchmarkEnvironment(output string) map[string]string {
	env := make(map[string]string)
	for _, line := range strings.Split(output, "\n") {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}

		key = strings.TrimSpace(key)
		if key == "goos" || key == "goarch" || key == "pkg" || key == "cpu" {
			env[key] = strings.TrimSpace(value)
		}
	}

	return env
}

func parseGoBenchmarkResults(output string) []BenchmarkResult {
	var results []BenchmarkResult
	for _, line := range strings.Split(output, "\n") {
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		result := BenchmarkResult{
			Name:       benchmarkArchSuffixRE.ReplaceAllString(fields[0], ""),
			Iterations: fields[1],
		}

		for i := 2; i+1 < len(fields); i += 2 {
			result.Metrics = append(result.Metrics, BenchmarkMetric{Value: fields[i], Unit: fields[i+1]})
		}

		results = append(results, result)
	}

	return results
}

func parsePGStatStatements(output string) []PGStatStatement {
	var statements []PGStatStatement
	for _, line := range strings.Split(output, "\n") {
		match := pgStatLineRE.FindStringSubmatch(line)
		if match == nil {
			continue
		}

		iterations, _ := strconv.Atoi(match[3])
		rank, _ := strconv.Atoi(match[4])
		calls, _ := strconv.ParseInt(match[5], 10, 64)
		totalMS, _ := strconv.ParseFloat(match[6], 64)
		meanMS, _ := strconv.ParseFloat(match[7], 64)
		rows, _ := strconv.ParseInt(match[8], 10, 64)
		query := unquotePGStatQuery(match[9])

		statements = append(statements, PGStatStatement{
			Database:   match[1],
			Benchmark:  match[2],
			Iterations: iterations,
			Rank:       rank,
			Calls:      calls,
			TotalMS:    totalMS,
			MeanMS:     meanMS,
			Rows:       rows,
			Query:      query,
		})
	}

	return statements
}

func unquotePGStatQuery(query string) string {
	unquoted, err := strconv.Unquote(`"` + query + `"`)
	if err != nil {
		return query
	}

	return unquoted
}

func printEnvironment(metadata HarnessMetadata) {
	rows := [][2]string{
		{"goos", metadata.GOOS},
		{"goarch", metadata.GOARCH},
		{"pkg", metadata.Pkg},
		{"cpu", metadata.CPU},
		{"go", metadata.GoVersion},
		{"commit", metadata.GitCommit + dirtySuffix(metadata.GitDirty)},
	}

	for _, row := range rows {
		if row[1] != "" {
			emit(fmt.Sprintf("%-10s %s", row[0]+":", row[1]))
		}
	}
}

func printBenchmarkSummary(results []BenchmarkResult) {
	if len(results) == 0 {
		emit("No benchmark rows parsed.")
		return
	}

	for _, result := range results {
		emit("- " + result.Name)
		rows := make([][2]string, 0, len(result.Metrics)+1)
		rows = append(rows, [2]string{"iterations", result.Iterations})
		for _, metric := range result.Metrics {
			rows = append(rows, [2]string{metric.Unit, metric.Value})
		}

		labelWidth, valueWidth := measurementColumnWidths(rows)
		for _, row := range rows {
			label := row[0] + ":"
			emit(fmt.Sprintf("  %-*s %*s", labelWidth, label, valueWidth, row[1]))
		}
	}
}

func measurementColumnWidths(rows [][2]string) (int, int) {
	labelWidth := 0
	valueWidth := 0
	for _, row := range rows {
		labelWidth = max(labelWidth, len(row[0])+1)
		valueWidth = max(valueWidth, len(row[1]))
	}

	return labelWidth, valueWidth
}

func printNextChecks(suite string) {
	printHeading("Remaining Manual Checks")
	switch suite {
	case "queue":
		emit("- Service boundary: rerun queue rows after any UDS/component split to isolate queue transport overhead from macro DB work.")
		emit("- Durability shape: add a WAL/batch-focused queue row before using queue-only throughput as a persistence claim.")
		emit("- End-to-end context: pair queue-only throughput with macro trigger-to-terminal rows when changing dispatch behavior.")
	case "dal":
		emit("- DB matrix: run sqlite3, pgx_podman, and pgx_podman_unsafe before separating query cost from network and durability cost.")
		emit("- Table shape: grow list, repair, and retention table-size scenarios beyond the default seed before making scan-cost claims.")
		emit("- Pool pressure: capture connection wait and pg_stat_statements data alongside DAL rows when a change adds transactions or indexes.")
		emit("- HA and multi-cell: add catalog/reconciler/cell-local workloads before treating single-database DAL numbers as topology capacity.")
	case "job":
		emit("- Action mix: add checkout, shell-output, failure, and cancellation rows when executor changes touch non-result actions.")
		emit("- Log transport: add forwarder/log-service gRPC rows before treating noop or local-store log sinks as production log cost.")
		emit("- End-to-end context: compare executor rows with macro result-action rows when isolating scheduler and DB overhead.")
	case "macro":
		emit("- DB matrix: include sqlite3, pgx_podman, and pgx_podman_unsafe before attributing macro losses to network or durability.")
		emit("- Choreography split: compare SQL, Orchestrator, and OrchestratorGRPC macro rows before claiming hot-state offload wins.")
		emit("- Mixed traffic: add API reads, SSE/log replay clients, and repair/list traffic while trigger-to-terminal load is running.")
		emit("- Topology: add multi-worker deployed, HA, and multi-cell rows before turning local macro throughput into a capacity envelope.")
		emit("- Log pressure: vary log volume and concurrent replay clients beyond the current log-heavy row before changing observability claims.")
	default:
		emit("- Workload mix: add trigger bursts, API reads, log readers, and repair/list scans around the focused benchmark.")
		emit("- Scale shape: vary worker count, database driver, and table size before making capacity claims.")
		emit("- Topology: add HA and multi-cell rows when the change affects routing, catalog, reconciler, or DB ownership.")
	}
}

func printArtifactFooter(runDir string) {
	printHeading("Artifacts")
	emit("- raw benchmark output: " + filepath.Join(runDir, "go-bench.txt"))
	emit("- JSON summary        : " + filepath.Join(runDir, "summary.json"))
	emit("- Markdown summary    : " + filepath.Join(runDir, "summary.md"))
	emit("- HTML report         : " + filepath.Join(runDir, "report.html"))
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}

	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}

func writeMarkdownSummary(path string, summary Summary, rawName string) error {
	var lines []string
	metadata := summary.Metadata
	lines = append(lines,
		"# Vectis Performance Run",
		"",
		fmt.Sprintf("- Suite: `%s`", metadata.Suite),
		fmt.Sprintf("- Started: `%s`", metadata.StartedAt),
		fmt.Sprintf("- Duration: `%gs`", metadata.DurationSeconds),
		fmt.Sprintf("- Status: `%d`", metadata.Status),
		fmt.Sprintf("- Git commit: `%s`%s", metadata.GitCommit, dirtySuffix(metadata.GitDirty)),
		fmt.Sprintf("- Go: `%s`", metadata.GoVersion),
		fmt.Sprintf("- Raw output: `%s`", rawName),
		"- HTML report: `report.html`",
		"",
		"## Results",
		"",
	)

	if len(summary.Results) == 0 {
		lines = append(lines, "No benchmark rows parsed.")
	} else {
		lines = append(lines, "| Benchmark | Iterations | Metrics |", "| --- | ---: | --- |")
		for _, result := range summary.Results {
			var metrics []string
			for _, metric := range result.Metrics {
				metrics = append(metrics, markdownEscape(metric.Value+" "+metric.Unit))
			}

			lines = append(lines, fmt.Sprintf("| `%s` | %s | %s |", markdownEscape(result.Name), result.Iterations, strings.Join(metrics, ", ")))
		}
	}

	finalStats := finalPGStatStatements(summary.PGStatStatements)
	if len(finalStats) > 0 {
		lines = append(lines, "", "## Postgres Statement Hot List", "")
		lines = append(lines, "| Benchmark | Database | Iterations | Rank | Calls | Total ms | Mean ms | Query |")
		lines = append(lines, "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |")

		for _, stat := range finalStats {
			lines = append(lines, fmt.Sprintf(
				"| `%s` | `%s` | %d | %d | %d | %.3f | %.3f | `%s` |",
				markdownEscape(stat.Benchmark),
				markdownEscape(stat.Database),
				stat.Iterations,
				stat.Rank,
				stat.Calls,
				stat.TotalMS,
				stat.MeanMS,
				markdownEscape(stat.Query),
			))
		}
	}

	lines = append(lines, "")
	return os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644)
}

func finalPGStatStatements(statements []PGStatStatement) []PGStatStatement {
	maxIterations := make(map[string]int)
	for _, stat := range statements {
		key := stat.Database + "\x00" + stat.Benchmark
		if stat.Iterations > maxIterations[key] {
			maxIterations[key] = stat.Iterations
		}
	}

	var final []PGStatStatement
	for _, stat := range statements {
		key := stat.Database + "\x00" + stat.Benchmark
		if stat.Iterations == maxIterations[key] {
			final = append(final, stat)
		}
	}

	sort.Slice(final, func(i, j int) bool {
		left := final[i]
		right := final[j]
		if left.Benchmark != right.Benchmark {
			return left.Benchmark < right.Benchmark
		}

		if left.Database != right.Database {
			return left.Database < right.Database
		}

		return left.Rank < right.Rank
	})

	return final
}

func writeHTMLReport(path string, summary Summary) error {
	payload, err := json.Marshal(summary)
	if err != nil {
		return err
	}

	tmpl, err := template.New("report").Parse(reportHTML)
	if err != nil {
		return err
	}

	var rendered bytes.Buffer
	err = tmpl.Execute(&rendered, struct {
		Payload template.JS
	}{
		Payload: template.JS(payload), // #nosec G203 -- payload is produced by json.Marshal from benchmark results for this static report.
	})

	if err != nil {
		return err
	}

	if err := os.WriteFile(path, rendered.Bytes(), 0o644); err != nil {
		return err
	}

	reportDir := filepath.Dir(path)
	if err := os.WriteFile(filepath.Join(reportDir, "report.css"), []byte(reportCSS), 0o644); err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(reportDir, "report.js"), []byte(reportJS), 0o644)
}

func serveRunDir(runDir, host string, port int) error {
	if host == "" {
		host = "127.0.0.1"
	}

	listener, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return fmt.Errorf("start report server: %w", err)
	}

	fileServer := http.FileServer(http.Dir(runDir))
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && isFile(filepath.Join(runDir, "report.html")) {
			http.Redirect(w, r, "/report.html", http.StatusFound)
			return
		}

		fileServer.ServeHTTP(w, r)
	})

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}

		errCh <- nil
	}()

	addr := listener.Addr()
	displayHost := reportServerDisplayHost(host)
	reportURL := "http://" + net.JoinHostPort(displayHost, strconv.Itoa(listenerPort(addr))) + "/report.html"

	printHeading("Report server")
	emit("Serving artifact directory: " + runDir)
	emit("Report URL               : " + reportURL)
	emit("Press Ctrl-C to stop.")

	stopCh := make(chan os.Signal, 1)
	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stopCh)

	select {
	case <-stopCh:
		if err := server.Close(); err != nil {
			return err
		}

		return <-errCh
	case err := <-errCh:
		return err
	}
}

func reportServerDisplayHost(host string) string {
	switch host {
	case "", "0.0.0.0", "::":
		return "127.0.0.1"
	default:
		return host
	}
}

func listenerPort(addr net.Addr) int {
	if tcpAddr, ok := addr.(*net.TCPAddr); ok {
		return tcpAddr.Port
	}

	_, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return 0
	}

	value, err := strconv.Atoi(port)
	if err != nil {
		return 0
	}

	return value
}

func runBenchstat(benchstat, baseline, current, outputPath string) int {
	if !isFile(baseline) {
		fmt.Fprintf(os.Stderr, "baseline benchmark output not found: %s\n", baseline)
		return configErrorStatus
	}

	if !isFile(current) {
		fmt.Fprintf(os.Stderr, "current benchmark output not found: %s\n", current)
		return configErrorStatus
	}

	benchstatPath, err := exec.LookPath(benchstat)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchstat not found: %s\n", benchstat)
		fmt.Fprintln(os.Stderr, "Install golang.org/x/perf/cmd/benchstat or set BENCHSTAT.")
		return 127
	}

	result := runCommand([]string{benchstatPath, baseline, current}, nil)
	emitRaw(result.output)
	if outputPath != "" {
		_ = os.WriteFile(outputPath, []byte(result.output), 0o644)
	}

	return result.status
}

func isFile(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func gitOutput(args []string, fallback string) string {
	return commandOutput(append([]string{"git"}, args...), fallback)
}

func gitDirty() bool {
	return strings.TrimSpace(commandOutput([]string{"git", "status", "--porcelain"}, "")) != ""
}

func commandOutput(command []string, fallback string) string {
	result := runCommand(command, nil)
	if result.status != 0 {
		return fallback
	}

	if trimmed := strings.TrimSpace(result.output); trimmed != "" {
		return trimmed
	}

	return fallback
}

func repoRoot() string {
	root := strings.TrimSpace(commandOutputNoRepo([]string{"git", "rev-parse", "--show-toplevel"}, ""))
	if root != "" {
		return root
	}

	wd, err := os.Getwd()
	if err != nil {
		return "."
	}

	return wd
}

func commandOutputNoRepo(command []string, fallback string) string {
	cmd := exec.Command(command[0], command[1:]...) // #nosec G204 -- perf harness intentionally runs configured local helper commands.
	output, err := cmd.Output()
	if err != nil {
		return fallback
	}

	if trimmed := strings.TrimSpace(string(output)); trimmed != "" {
		return trimmed
	}

	return fallback
}

func envMapFromOS() map[string]string {
	env := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			env[key] = value
		}
	}

	return env
}

func envList(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	out := make([]string, 0, len(keys))
	for _, key := range keys {
		out = append(out, key+"="+env[key])
	}

	return out
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}

	return value
}

func envNonNegativeInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}

	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return fallback
	}

	return value
}

func envBool(name string, fallback bool) bool {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	switch raw {
	case "":
		return fallback
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func sanitizeBenchmarkPart(value string) string {
	return sanitizePartRE.ReplaceAllString(value, "_")
}

func sanitizePathPart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "perf-run"
	}

	return sanitizePathRE.ReplaceAllString(value, "-")
}

func markdownEscape(value string) string {
	value = strings.ReplaceAll(value, "`", "'")
	value = strings.ReplaceAll(value, "|", "\\|")
	return value
}

func dirtySuffix(dirty bool) string {
	if dirty {
		return " dirty"
	}

	return ""
}

func roundDurationSeconds(duration time.Duration) float64 {
	value := duration.Seconds()
	return float64(int(value*1_000_000+0.5)) / 1_000_000
}

func emit(message string) {
	fmt.Println(message)
}

func emitRaw(message string) {
	if message == "" {
		return
	}

	if strings.HasSuffix(message, "\n") {
		fmt.Print(message)
		return
	}

	fmt.Println(message)
}

func printHeading(title string) {
	fmt.Println()
	fmt.Println(title)
	fmt.Println(strings.Repeat("=", len(title)))
	fmt.Println()
}

//go:embed report.html
var reportHTML string

//go:embed report.css
var reportCSS string

//go:embed report.js
var reportJS string
