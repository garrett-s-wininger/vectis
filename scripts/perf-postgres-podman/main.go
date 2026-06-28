package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultPostgresImage = "postgres:18-alpine"
	postgresDatabase     = "vectis"
	postgresPassword     = "vectis"
	postgresUser         = "vectis"

	envDatabaseDriver           = "VECTIS_PERF_DATABASE_DRIVER"
	envDatabaseDSN              = "VECTIS_PERF_DATABASE_DSN"
	envPodman                   = "VECTIS_PERF_PODMAN"
	envPostgresDSNParams        = "VECTIS_PERF_POSTGRES_DSN_PARAMS"
	envPostgresImage            = "VECTIS_PERF_POSTGRES_IMAGE"
	envPostgresProfile          = "VECTIS_PERF_POSTGRES_DURABILITY"
	envPostgresPort             = "VECTIS_PERF_POSTGRES_PORT"
	envPGAutoExplain            = "VECTIS_PERF_PG_AUTO_EXPLAIN"
	envPGAutoExplainMinDuration = "VECTIS_PERF_PG_AUTO_EXPLAIN_MIN_DURATION"
	envPGAutoExplainOutput      = "VECTIS_PERF_PG_AUTO_EXPLAIN_OUTPUT"
	envPGStatStatements         = "VECTIS_PERF_PG_STAT_STATEMENTS"
	envPGWaitSampleInterval     = "VECTIS_PERF_PG_WAIT_SAMPLE_INTERVAL"
	envPGWaitSamplesOutput      = "VECTIS_PERF_PG_WAIT_SAMPLES_OUTPUT"
	envVectisDriver             = "VECTIS_DATABASE_DRIVER"
	envVectisDSN                = "VECTIS_DATABASE_DSN"

	postgresDurabilityUnsafe        = "unsafe"
	defaultPGWaitSampleInterval     = 50 * time.Millisecond
	defaultPGAutoExplainMinDuration = "1ms"
)

func main() {
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}

	if len(args) == 0 {
		_, _ = fmt.Fprintln(os.Stderr, "usage: perf-postgres-podman -- <command> [args...]")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	podman := os.Getenv(envPodman)
	if podman == "" {
		podman = "podman"
	}

	image := os.Getenv(envPostgresImage)
	if image == "" {
		image = defaultPostgresImage
	}

	hostPort, err := postgresHostPort()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "choose postgres host port: %v\n", err)
		os.Exit(1)
	}

	containerName := fmt.Sprintf("vectis-perf-postgres-%d-%d", os.Getpid(), time.Now().UnixNano())
	containerID, err := startPostgres(ctx, podman, image, containerName, hostPort)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "start podman postgres: %v\n", err)
		os.Exit(1)
	}
	defer removeContainer(podman, containerID)

	if err := waitForPostgres(ctx, podman, containerID); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "wait for podman postgres: %v\n", err)
		os.Exit(1)
	}

	dsn := fmt.Sprintf(
		"postgres://%s:%s@127.0.0.1:%s/%s?sslmode=disable",
		postgresUser,
		postgresPassword,
		hostPort,
		postgresDatabase,
	)
	dsn, err = postgresDSNWithExtraParams(dsn)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "build postgres dsn: %v\n", err)
		os.Exit(1)
	}

	waitSampler, err := startPostgresWaitSampler(context.Background(), dsn)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "start postgres wait sampler: %v\n", err)
		os.Exit(1)
	}

	// #nosec G204 -- this harness intentionally executes the benchmark command supplied after "--".
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Env = append(
		os.Environ(),
		envDatabaseDriver+"=pgx",
		envDatabaseDSN+"="+dsn,
		envVectisDriver+"=pgx",
		envVectisDSN+"="+dsn,
	)

	runErr := cmd.Run()
	if waitSampler != nil {
		waitSampler.stop()
	}

	if err := dumpPostgresAutoExplainLogs(context.Background(), podman, containerID); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "dump postgres auto_explain logs: %v\n", err)
	}

	if runErr != nil {
		exitErr := &exec.ExitError{}
		if errors.As(runErr, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}

		_, _ = fmt.Fprintf(os.Stderr, "run benchmark command: %v\n", runErr)
		os.Exit(1)
	}
}

func postgresHostPort() (string, error) {
	if port := os.Getenv(envPostgresPort); port != "" {
		return port, nil
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return "", fmt.Errorf("unexpected listener address %q", listener.Addr())
	}

	return fmt.Sprintf("%d", addr.Port), nil
}

func postgresDSNWithExtraParams(dsn string) (string, error) {
	raw := strings.TrimSpace(os.Getenv(envPostgresDSNParams))
	if raw == "" {
		return dsn, nil
	}

	parsedDSN, err := url.Parse(dsn)
	if err != nil {
		return "", err
	}

	raw = strings.TrimPrefix(raw, "?")
	params, err := url.ParseQuery(raw)
	if err != nil {
		return "", fmt.Errorf("%s: %w", envPostgresDSNParams, err)
	}

	query := parsedDSN.Query()
	for key, values := range params {
		if len(values) == 0 {
			continue
		}
		query.Del(key)
		for _, value := range values {
			query.Add(key, value)
		}
	}
	parsedDSN.RawQuery = query.Encode()
	return parsedDSN.String(), nil
}

func startPostgres(ctx context.Context, podman, image, containerName, hostPort string) (string, error) {
	args := []string{
		"run",
		"--detach",
		"--rm",
		"--name",
		containerName,
		"-e",
		"POSTGRES_DB=" + postgresDatabase,
		"-e",
		"POSTGRES_PASSWORD=" + postgresPassword,
		"-e",
		"POSTGRES_USER=" + postgresUser,
		"-p",
		"127.0.0.1:" + hostPort + ":5432",
		image,
		"postgres",
	}

	if preload := postgresSharedPreloadLibraries(); preload != "" {
		args = append(args, "-c", "shared_preload_libraries="+preload)
	}

	if postgresPGStatStatementsEnabled() {
		args = append(
			args,
			"-c",
			"pg_stat_statements.track=all",
			"-c",
			"pg_stat_statements.track_planning=on",
		)
	}

	if postgresAutoExplainEnabled() {
		args = append(
			args,
			"-c",
			"auto_explain.log_min_duration="+postgresAutoExplainMinDuration(),
			"-c",
			"auto_explain.log_analyze=on",
			"-c",
			"auto_explain.log_buffers=on",
			"-c",
			"auto_explain.log_wal=on",
			"-c",
			"auto_explain.log_timing=off",
			"-c",
			"auto_explain.log_nested_statements=on",
		)
	}

	if postgresDurabilityProfile() == postgresDurabilityUnsafe {
		args = append(
			args,
			"-c",
			"fsync=off",
			"-c",
			"synchronous_commit=off",
			"-c",
			"full_page_writes=off",
		)
	}

	// #nosec G204 -- podman executable and arguments are explicit perf harness configuration.
	output, err := exec.CommandContext(ctx, podman, args...).CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %v: %w\n%s", podman, args, err, string(output))
	}

	return strings.TrimSpace(string(output)), nil
}

func postgresSharedPreloadLibraries() string {
	var libraries []string
	if postgresPGStatStatementsEnabled() {
		libraries = append(libraries, "pg_stat_statements")
	}

	if postgresAutoExplainEnabled() {
		libraries = append(libraries, "auto_explain")
	}

	return strings.Join(libraries, ",")
}

func postgresPGStatStatementsEnabled() bool {
	if raw := strings.TrimSpace(os.Getenv(envPGStatStatements)); raw != "" {
		return truthy(raw)
	}

	return true
}

func postgresAutoExplainEnabled() bool {
	if raw := strings.TrimSpace(os.Getenv(envPGAutoExplain)); raw != "" {
		return truthy(raw)
	}

	return strings.TrimSpace(os.Getenv(envPGAutoExplainOutput)) != ""
}

func postgresAutoExplainMinDuration() string {
	if value := strings.TrimSpace(os.Getenv(envPGAutoExplainMinDuration)); value != "" {
		return value
	}

	return defaultPGAutoExplainMinDuration
}

func truthy(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func postgresDurabilityProfile() string {
	return strings.ToLower(strings.TrimSpace(os.Getenv(envPostgresProfile)))
}

type postgresWaitSampler struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

func startPostgresWaitSampler(ctx context.Context, dsn string) (*postgresWaitSampler, error) {
	outputPath := strings.TrimSpace(os.Getenv(envPGWaitSamplesOutput))
	if outputPath == "" {
		return nil, nil
	}

	interval := postgresWaitSampleInterval()
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return nil, err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	configurePostgresWaitSampler(db)

	sampleCtx, cancel := context.WithCancel(ctx)
	sampler := &postgresWaitSampler{
		cancel: cancel,
		done:   make(chan struct{}),
	}

	_, _ = fmt.Fprintln(file, strings.Join([]string{
		"sampled_at",
		"pid",
		"state",
		"wait_event_type",
		"wait_event",
		"query",
		"locktype",
		"mode",
		"granted",
		"relation",
		"page",
		"tuple",
		"transactionid",
	}, "\t"))

	go func() {
		defer close(sampler.done)
		defer db.Close()
		defer file.Close()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			samplePostgresWaits(sampleCtx, db, file)

			select {
			case <-sampleCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return sampler, nil
}

func configurePostgresWaitSampler(db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = db.ExecContext(ctx, "SET application_name = 'vectis-perf-wait-sampler'")
	_, _ = db.ExecContext(ctx, "SET auto_explain.log_min_duration = -1")
}

func postgresWaitSampleInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(envPGWaitSampleInterval))
	if raw == "" {
		return defaultPGWaitSampleInterval
	}

	interval, err := time.ParseDuration(raw)
	if err != nil || interval <= 0 {
		return defaultPGWaitSampleInterval
	}

	return interval
}

func samplePostgresWaits(ctx context.Context, db *sql.DB, file *os.File) {
	sampleCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(sampleCtx, `
		SELECT
			now()::text,
			a.pid::text,
			a.state,
			COALESCE(a.wait_event_type, ''),
			COALESCE(a.wait_event, ''),
			left(regexp_replace(a.query, E'[\n\r\t]+', ' ', 'g'), 220),
			COALESCE(l.locktype, ''),
			COALESCE(l.mode, ''),
			COALESCE(l.granted::text, ''),
			COALESCE(l.relation::regclass::text, ''),
			COALESCE(l.page::text, ''),
			COALESCE(l.tuple::text, ''),
			COALESCE(l.transactionid::text, '')
		FROM pg_stat_activity a
		LEFT JOIN pg_locks l
			ON l.pid = a.pid
			AND NOT l.granted
		WHERE a.datname = current_database()
			AND a.pid <> pg_backend_pid()
			AND (
				(a.state = 'active' AND a.wait_event_type IS NOT NULL)
				OR NOT l.granted
			)
		ORDER BY a.pid, l.locktype, l.mode, l.relation::regclass::text
	`)

	if err != nil {
		writePostgresWaitSampleError(file, err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		fields := make([]string, 13)
		scan := make([]any, len(fields))
		for i := range fields {
			scan[i] = &fields[i]
		}

		if err := rows.Scan(scan...); err != nil {
			writePostgresWaitSampleError(file, err)
			return
		}

		for i := range fields {
			fields[i] = sanitizeTSVField(fields[i])
		}

		_, _ = fmt.Fprintln(file, strings.Join(fields, "\t"))
	}

	if err := rows.Err(); err != nil {
		writePostgresWaitSampleError(file, err)
		return
	}
}

func writePostgresWaitSampleError(file *os.File, err error) {
	_, _ = fmt.Fprintf(file, "# sample error: %s\n", sanitizeTSVField(err.Error()))
}

func sanitizeTSVField(value string) string {
	replacer := strings.NewReplacer("\t", " ", "\n", " ", "\r", " ")
	return replacer.Replace(value)
}

func (s *postgresWaitSampler) stop() {
	s.once.Do(func() {
		s.cancel()
		<-s.done
	})
}

func dumpPostgresAutoExplainLogs(ctx context.Context, podman, containerID string) error {
	outputPath := strings.TrimSpace(os.Getenv(envPGAutoExplainOutput))
	if outputPath == "" || !postgresAutoExplainEnabled() {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}

	logCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	output, err := exec.CommandContext(logCtx, podman, "logs", containerID).CombinedOutput()
	writeErr := os.WriteFile(outputPath, output, 0o644)
	if err != nil {
		return err
	}

	return writeErr
}

func waitForPostgres(ctx context.Context, podman, containerID string) error {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var lastOutput string
	for {
		output, err := exec.CommandContext(ctx, podman, "exec", containerID, "pg_isready", "-U", postgresUser, "-d", postgresDatabase).CombinedOutput()
		if err == nil {
			return nil
		}
		lastOutput = string(output)

		select {
		case <-ctx.Done():
			return fmt.Errorf("%w; last pg_isready output: %s", ctx.Err(), lastOutput)
		case <-ticker.C:
		}
	}
}

func removeContainer(podman, containerID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	output, err := exec.CommandContext(ctx, podman, "rm", "-f", "-t", "0", containerID).CombinedOutput()
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "remove podman container %s: %v\n%s", containerID, err, string(output))
	}
}
