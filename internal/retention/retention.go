package retention

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"vectis/internal/database"
)

const (
	DefaultTerminalRunRetention   = 30 * 24 * time.Hour
	DefaultJobDefinitionRetention = 30 * 24 * time.Hour
	DefaultIdempotencyRetention   = 24 * time.Hour
	DefaultAuditLogRetention      = 365 * 24 * time.Hour

	auditEventRetentionCleanup = "retention.cleanup"
	sqlTimeLayout              = "2006-01-02 15:04:05"
)

// Policy controls which durable records are eligible for cleanup. A zero or
// negative duration disables that surface.
type Policy struct {
	TerminalRuns    time.Duration
	JobDefinitions  time.Duration
	IdempotencyKeys time.Duration
	AuditLog        time.Duration
}

type Cutoffs struct {
	TerminalRuns    *time.Time
	JobDefinitions  *time.Time
	IdempotencyKeys *time.Time
	AuditLog        *time.Time
}

type Counts struct {
	TerminalRuns        int64
	RunDispatchEvents   int64
	RunTasks            int64
	TaskAttempts        int64
	RunSegments         int64
	SegmentExecutions   int64
	TaskDispatchIntents int64
	JobDefinitions      int64
	IdempotencyKeys     int64
	AuditLog            int64
}

type Report struct {
	DryRun             bool
	Cutoffs            Cutoffs
	Counts             Counts
	AuditEventInserted bool
}

type FileReport struct {
	RunLogFiles int64
	RunLogBytes int64
}

type SQLCleaner struct {
	db *sql.DB
}

func DefaultPolicy() Policy {
	return Policy{
		TerminalRuns:    DefaultTerminalRunRetention,
		JobDefinitions:  DefaultJobDefinitionRetention,
		IdempotencyKeys: DefaultIdempotencyRetention,
		AuditLog:        DefaultAuditLogRetention,
	}
}

func NewSQLCleaner(db *sql.DB) *SQLCleaner {
	return &SQLCleaner{db: db}
}

func (c *SQLCleaner) Preview(ctx context.Context, policy Policy, now time.Time) (Report, error) {
	report := Report{DryRun: true, Cutoffs: policyCutoffs(policy, now)}
	counts, err := c.counts(ctx, nil, report.Cutoffs)
	if err != nil {
		return Report{}, err
	}
	report.Counts = counts
	return report, nil
}

func (c *SQLCleaner) Apply(ctx context.Context, policy Policy, now time.Time) (Report, error) {
	report := Report{Cutoffs: policyCutoffs(policy, now)}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return Report{}, err
	}
	defer func() { _ = tx.Rollback() }()

	counts, err := c.counts(ctx, tx, report.Cutoffs)
	if err != nil {
		return Report{}, err
	}

	if report.Cutoffs.TerminalRuns != nil {
		if _, err := execRows(ctx, tx, `
			DELETE FROM run_dispatch_events
			WHERE run_id IN (
				SELECT run_id
				FROM job_runs
				WHERE status IN ('succeeded', 'failed', 'aborted', 'cancelled', 'abandoned')
					AND finished_at IS NOT NULL
					AND finished_at < ?
			)
		`, sqlTimeParam(*report.Cutoffs.TerminalRuns)); err != nil {
			return Report{}, fmt.Errorf("delete run dispatch events: %w", err)
		}

		for _, child := range []struct {
			table string
			label string
		}{
			{table: "task_dispatch_intents", label: "task dispatch intents"},
			{table: "segment_executions", label: "segment executions"},
			{table: "run_segments", label: "run segments"},
			{table: "task_attempts", label: "task attempts"},
			{table: "run_tasks", label: "run tasks"},
		} {
			if err := deleteTerminalRunChildren(ctx, tx, child.table, *report.Cutoffs.TerminalRuns); err != nil {
				return Report{}, fmt.Errorf("delete %s: %w", child.label, err)
			}
		}

		deletedRuns, err := execRows(ctx, tx, `
			DELETE FROM job_runs
			WHERE status IN ('succeeded', 'failed', 'aborted', 'cancelled', 'abandoned')
				AND finished_at IS NOT NULL
				AND finished_at < ?
		`, sqlTimeParam(*report.Cutoffs.TerminalRuns))

		if err != nil {
			return Report{}, fmt.Errorf("delete terminal runs: %w", err)
		}

		counts.TerminalRuns = deletedRuns
	}

	if report.Cutoffs.JobDefinitions != nil {
		deleted, err := execRows(ctx, tx, `
			DELETE FROM job_definitions
			WHERE created_at < ?
				AND NOT EXISTS (
					SELECT 1
					FROM job_runs
					WHERE job_runs.job_id = job_definitions.job_id
						AND job_runs.definition_version = job_definitions.version
				)
				AND NOT EXISTS (
					SELECT 1
					FROM stored_jobs
					WHERE stored_jobs.job_id = job_definitions.job_id
				)
		`, sqlTimeParam(*report.Cutoffs.JobDefinitions))
		if err != nil {
			return Report{}, fmt.Errorf("delete orphaned job definitions: %w", err)
		}
		counts.JobDefinitions = deleted
	}

	if report.Cutoffs.IdempotencyKeys != nil {
		deleted, err := execRows(ctx, tx, `DELETE FROM idempotency_keys WHERE updated_at < ?`, sqlTimeParam(*report.Cutoffs.IdempotencyKeys))
		if err != nil {
			return Report{}, fmt.Errorf("delete idempotency keys: %w", err)
		}
		counts.IdempotencyKeys = deleted
	}

	if report.Cutoffs.AuditLog != nil {
		deleted, err := execRows(ctx, tx, `DELETE FROM audit_log WHERE created_at < ?`, sqlTimeParam(*report.Cutoffs.AuditLog))
		if err != nil {
			return Report{}, fmt.Errorf("delete audit log: %w", err)
		}
		counts.AuditLog = deleted
	}

	report.Counts = counts
	if err := insertCleanupAuditEvent(ctx, tx, report); err != nil {
		return Report{}, err
	}
	report.AuditEventInserted = true

	if err := tx.Commit(); err != nil {
		return Report{}, err
	}

	return report, nil
}

func (c *SQLCleaner) TerminalRunIDs(ctx context.Context, retention time.Duration, now time.Time) ([]string, error) {
	if retention <= 0 {
		return nil, nil
	}

	cutoff := now.UTC().Add(-retention)
	rows, err := c.db.QueryContext(ctx, rebind(`
		SELECT run_id
		FROM job_runs
		WHERE status IN ('succeeded', 'failed', 'aborted', 'cancelled', 'abandoned')
			AND finished_at IS NOT NULL
			AND finished_at < ?
		ORDER BY id ASC
	`), sqlTimeParam(cutoff))

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return nil, err
		}
		out = append(out, runID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *SQLCleaner) counts(ctx context.Context, tx *sql.Tx, cutoffs Cutoffs) (Counts, error) {
	var out Counts
	var err error

	if cutoffs.TerminalRuns != nil {
		out.TerminalRuns, err = c.queryCount(ctx, tx, `
			SELECT COUNT(*)
			FROM job_runs
			WHERE status IN ('succeeded', 'failed', 'aborted', 'cancelled', 'abandoned')
				AND finished_at IS NOT NULL
				AND finished_at < ?
		`, sqlTimeParam(*cutoffs.TerminalRuns))

		if err != nil {
			return Counts{}, fmt.Errorf("count terminal runs: %w", err)
		}

		out.RunDispatchEvents, err = c.queryCount(ctx, tx, `
			SELECT COUNT(*)
			FROM run_dispatch_events
			WHERE run_id IN (
				SELECT run_id
				FROM job_runs
				WHERE status IN ('succeeded', 'failed', 'aborted', 'cancelled', 'abandoned')
					AND finished_at IS NOT NULL
					AND finished_at < ?
			)
		`, sqlTimeParam(*cutoffs.TerminalRuns))
		if err != nil {
			return Counts{}, fmt.Errorf("count run dispatch events: %w", err)
		}

		out.RunTasks, err = c.countTerminalRunChildren(ctx, tx, "run_tasks", *cutoffs.TerminalRuns)
		if err != nil {
			return Counts{}, fmt.Errorf("count run tasks: %w", err)
		}

		out.TaskAttempts, err = c.countTerminalRunChildren(ctx, tx, "task_attempts", *cutoffs.TerminalRuns)
		if err != nil {
			return Counts{}, fmt.Errorf("count task attempts: %w", err)
		}

		out.RunSegments, err = c.countTerminalRunChildren(ctx, tx, "run_segments", *cutoffs.TerminalRuns)
		if err != nil {
			return Counts{}, fmt.Errorf("count run segments: %w", err)
		}

		out.SegmentExecutions, err = c.countTerminalRunChildren(ctx, tx, "segment_executions", *cutoffs.TerminalRuns)
		if err != nil {
			return Counts{}, fmt.Errorf("count segment executions: %w", err)
		}

		out.TaskDispatchIntents, err = c.countTerminalRunChildren(ctx, tx, "task_dispatch_intents", *cutoffs.TerminalRuns)
		if err != nil {
			return Counts{}, fmt.Errorf("count task dispatch intents: %w", err)
		}
	}

	if cutoffs.JobDefinitions != nil {
		query := `
			SELECT COUNT(*)
			FROM job_definitions
			WHERE created_at < ?
				AND NOT EXISTS (
					SELECT 1
					FROM job_runs
					WHERE job_runs.job_id = job_definitions.job_id
						AND job_runs.definition_version = job_definitions.version
		`
		args := []any{sqlTimeParam(*cutoffs.JobDefinitions)}
		if cutoffs.TerminalRuns != nil {
			query += `
						AND NOT (
							status IN ('succeeded', 'failed', 'aborted', 'cancelled', 'abandoned')
							AND finished_at IS NOT NULL
							AND finished_at < ?
						)
			`
			args = append(args, sqlTimeParam(*cutoffs.TerminalRuns))
		}
		query += `
				)
				AND NOT EXISTS (
					SELECT 1
					FROM stored_jobs
					WHERE stored_jobs.job_id = job_definitions.job_id
				)
		`

		out.JobDefinitions, err = c.queryCount(ctx, tx, query, args...)
		if err != nil {
			return Counts{}, fmt.Errorf("count orphaned job definitions: %w", err)
		}
	}

	if cutoffs.IdempotencyKeys != nil {
		out.IdempotencyKeys, err = c.queryCount(ctx, tx, `SELECT COUNT(*) FROM idempotency_keys WHERE updated_at < ?`, sqlTimeParam(*cutoffs.IdempotencyKeys))
		if err != nil {
			return Counts{}, fmt.Errorf("count idempotency keys: %w", err)
		}
	}

	if cutoffs.AuditLog != nil {
		out.AuditLog, err = c.queryCount(ctx, tx, `SELECT COUNT(*) FROM audit_log WHERE created_at < ?`, sqlTimeParam(*cutoffs.AuditLog))
		if err != nil {
			return Counts{}, fmt.Errorf("count audit log: %w", err)
		}
	}

	return out, nil
}

func (c *SQLCleaner) countTerminalRunChildren(ctx context.Context, tx *sql.Tx, table string, cutoff time.Time) (int64, error) {
	return c.queryCount(ctx, tx, `
		SELECT COUNT(*)
		FROM `+table+`
		WHERE run_id IN (
			SELECT run_id
			FROM job_runs
			WHERE status IN ('succeeded', 'failed', 'aborted', 'cancelled', 'abandoned')
				AND finished_at IS NOT NULL
				AND finished_at < ?
		)
	`, sqlTimeParam(cutoff))
}

func deleteTerminalRunChildren(ctx context.Context, tx *sql.Tx, table string, cutoff time.Time) error {
	_, err := execRows(ctx, tx, `
		DELETE FROM `+table+`
		WHERE run_id IN (
			SELECT run_id
			FROM job_runs
			WHERE status IN ('succeeded', 'failed', 'aborted', 'cancelled', 'abandoned')
				AND finished_at IS NOT NULL
				AND finished_at < ?
		)
	`, sqlTimeParam(cutoff))
	return err
}

type LocalRunLogCleaner struct {
	Dir string
}

func (c LocalRunLogCleaner) Preview(runIDs []string) (FileReport, error) {
	return c.walk(runIDs, false)
}

func (c LocalRunLogCleaner) Delete(runIDs []string) (FileReport, error) {
	return c.walk(runIDs, true)
}

func RunLogPath(dir, runID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(runID))
	return filepath.Join(dir, encoded+".jsonl")
}

func (c LocalRunLogCleaner) walk(runIDs []string, remove bool) (FileReport, error) {
	if c.Dir == "" || len(runIDs) == 0 {
		return FileReport{}, nil
	}

	if _, err := os.Stat(c.Dir); err != nil {
		if os.IsNotExist(err) {
			return FileReport{}, nil
		}
		return FileReport{}, fmt.Errorf("inspect log storage dir: %w", err)
	}

	var report FileReport
	for _, runID := range runIDs {
		path := RunLogPath(c.Dir, runID)
		st, err := os.Stat(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return FileReport{}, fmt.Errorf("inspect run log %s: %w", path, err)
		}
		if st.IsDir() {
			continue
		}

		report.RunLogFiles++
		report.RunLogBytes += st.Size()
		if remove {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return FileReport{}, fmt.Errorf("remove run log %s: %w", path, err)
			}
		}
	}

	return report, nil
}

func policyCutoffs(policy Policy, now time.Time) Cutoffs {
	now = now.UTC()
	cutoff := func(d time.Duration) *time.Time {
		if d <= 0 {
			return nil
		}
		t := now.Add(-d)
		return &t
	}

	return Cutoffs{
		TerminalRuns:    cutoff(policy.TerminalRuns),
		JobDefinitions:  cutoff(policy.JobDefinitions),
		IdempotencyKeys: cutoff(policy.IdempotencyKeys),
		AuditLog:        cutoff(policy.AuditLog),
	}
}

func (c *SQLCleaner) queryCount(ctx context.Context, tx *sql.Tx, query string, args ...any) (int64, error) {
	var row *sql.Row
	if tx != nil {
		row = tx.QueryRowContext(ctx, rebind(query), args...)
	} else {
		row = c.db.QueryRowContext(ctx, rebind(query), args...)
	}

	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func execRows(ctx context.Context, tx *sql.Tx, query string, args ...any) (int64, error) {
	res, err := tx.ExecContext(ctx, rebind(query), args...)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	return n, nil
}

func insertCleanupAuditEvent(ctx context.Context, tx *sql.Tx, report Report) error {
	metadata, err := json.Marshal(map[string]any{
		"dry_run": report.DryRun,
		"counts": map[string]int64{
			"terminal_runs":         report.Counts.TerminalRuns,
			"run_dispatch_events":   report.Counts.RunDispatchEvents,
			"run_tasks":             report.Counts.RunTasks,
			"task_attempts":         report.Counts.TaskAttempts,
			"run_segments":          report.Counts.RunSegments,
			"segment_executions":    report.Counts.SegmentExecutions,
			"task_dispatch_intents": report.Counts.TaskDispatchIntents,
			"job_definitions":       report.Counts.JobDefinitions,
			"idempotency_keys":      report.Counts.IdempotencyKeys,
			"audit_log":             report.Counts.AuditLog,
		},
		"cutoffs": map[string]string{
			"terminal_runs":    formatCutoff(report.Cutoffs.TerminalRuns),
			"job_definitions":  formatCutoff(report.Cutoffs.JobDefinitions),
			"idempotency_keys": formatCutoff(report.Cutoffs.IdempotencyKeys),
			"audit_log":        formatCutoff(report.Cutoffs.AuditLog),
		},
	})
	if err != nil {
		return fmt.Errorf("marshal cleanup audit metadata: %w", err)
	}

	_, err = tx.ExecContext(ctx, rebind(`
		INSERT INTO audit_log (event_type, actor_id, target_id, metadata, ip_address, correlation_id, created_at)
		VALUES (?, NULL, NULL, ?, NULL, ?, CURRENT_TIMESTAMP)
	`), auditEventRetentionCleanup, metadata, "vectis-cli-retention")
	if err != nil {
		return fmt.Errorf("insert cleanup audit event: %w", err)
	}
	return nil
}

func formatCutoff(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

func sqlTimeParam(t time.Time) any {
	t = t.UTC()
	if os.Getenv(database.EnvDatabaseDriver) == "pgx" {
		return t
	}
	return t.Format(sqlTimeLayout)
}

func rebind(query string) string {
	if os.Getenv(database.EnvDatabaseDriver) != "pgx" {
		return query
	}

	var b strings.Builder
	b.Grow(len(query) + 8)
	argNum := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(argNum))
			argNum++
			continue
		}
		b.WriteByte(query[i])
	}

	return b.String()
}
