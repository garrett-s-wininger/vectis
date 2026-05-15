package main

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"os"
	"time"
	"vectis/internal/database"
	"vectis/internal/retention"
)

func runRetentionCleanup(cmd *cobra.Command, args []string) {
	policy := retention.Policy{
		TerminalRuns:    retentionRunAge,
		JobDefinitions:  retentionDefAge,
		IdempotencyKeys: retentionIdemAge,
		AuditLog:        retentionAuditAge,
	}

	if err := retentionCleanup(cmd.Context(), os.Stdout, policy, retentionDryRun, retentionYes, retentionLogDir); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func retentionCleanup(ctx context.Context, w io.Writer, policy retention.Policy, dryRun, yes bool, logStorageDir string) error {
	if !dryRun && !yes {
		return fmt.Errorf("retention cleanup deletes durable records; pass --dry-run to inspect or --yes to apply")
	}

	dbPath := database.GetDBPath()
	db, err := database.OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := database.WaitForMigrations(db, nil); err != nil {
		return err
	}

	cleaner := retention.NewSQLCleaner(db)
	now := time.Now().UTC()

	var fileReport retention.FileReport
	if logStorageDir != "" {
		runIDs, err := cleaner.TerminalRunIDs(ctx, policy.TerminalRuns, now)
		if err != nil {
			return fmt.Errorf("list terminal run logs: %w", err)
		}

		logCleaner := retention.LocalRunLogCleaner{Dir: logStorageDir}
		if dryRun {
			fileReport, err = logCleaner.Preview(runIDs)
		} else {
			fileReport, err = logCleaner.Delete(runIDs)
		}

		if err != nil {
			return err
		}
	}

	var report retention.Report
	if dryRun {
		report, err = cleaner.Preview(ctx, policy, now)
	} else {
		report, err = cleaner.Apply(ctx, policy, now)
	}

	if err != nil {
		return err
	}

	if outputIsJSON() {
		return writeJSON(w, map[string]any{
			"applied": !dryRun,
			"dry_run": report.DryRun,
			"cutoffs": map[string]string{
				"terminal_runs":    retentionCutoff(report.Cutoffs.TerminalRuns),
				"job_definitions":  retentionCutoff(report.Cutoffs.JobDefinitions),
				"idempotency_keys": retentionCutoff(report.Cutoffs.IdempotencyKeys),
				"audit_log":        retentionCutoff(report.Cutoffs.AuditLog),
			},
			"counts": map[string]int64{
				"terminal_runs":       report.Counts.TerminalRuns,
				"run_dispatch_events": report.Counts.RunDispatchEvents,
				"job_definitions":     report.Counts.JobDefinitions,
				"idempotency_keys":    report.Counts.IdempotencyKeys,
				"audit_log":           report.Counts.AuditLog,
				"run_log_files":       fileReport.RunLogFiles,
				"run_log_bytes":       fileReport.RunLogBytes,
			},
			"audit": map[string]bool{"event_inserted": report.AuditEventInserted},
		})
	}

	printRetentionReport(w, report, fileReport)
	if dryRun {
		fmt.Fprintln(w, "Cleanup not applied.")
		return nil
	}

	fmt.Fprintln(w, "Cleanup applied.")
	return nil
}

func printRetentionReport(w io.Writer, report retention.Report, fileReport retention.FileReport) {
	prefix := "deleted"
	if report.DryRun {
		prefix = "would_delete"
	}

	fmt.Fprintf(w, "dry_run=%t\n", report.DryRun)
	fmt.Fprintf(w, "cutoff.terminal_runs=%s\n", retentionCutoff(report.Cutoffs.TerminalRuns))
	fmt.Fprintf(w, "cutoff.job_definitions=%s\n", retentionCutoff(report.Cutoffs.JobDefinitions))
	fmt.Fprintf(w, "cutoff.idempotency_keys=%s\n", retentionCutoff(report.Cutoffs.IdempotencyKeys))
	fmt.Fprintf(w, "cutoff.audit_log=%s\n", retentionCutoff(report.Cutoffs.AuditLog))
	fmt.Fprintf(w, "%s.terminal_runs=%d\n", prefix, report.Counts.TerminalRuns)
	fmt.Fprintf(w, "%s.run_dispatch_events=%d\n", prefix, report.Counts.RunDispatchEvents)
	fmt.Fprintf(w, "%s.job_definitions=%d\n", prefix, report.Counts.JobDefinitions)
	fmt.Fprintf(w, "%s.idempotency_keys=%d\n", prefix, report.Counts.IdempotencyKeys)
	fmt.Fprintf(w, "%s.audit_log=%d\n", prefix, report.Counts.AuditLog)
	fmt.Fprintf(w, "%s.run_log_files=%d\n", prefix, fileReport.RunLogFiles)
	fmt.Fprintf(w, "%s.run_log_bytes=%d\n", prefix, fileReport.RunLogBytes)
	fmt.Fprintf(w, "audit_event_inserted=%t\n", report.AuditEventInserted)
}

func retentionCutoff(t *time.Time) string {
	if t == nil {
		return "disabled"
	}

	return t.UTC().Format(time.RFC3339)
}

var retentionCmd = &cobra.Command{
	Use:     "retention",
	Short:   "Manage durable data retention",
	GroupID: cliGroupOperations,
	Run:     showCommandHelp,
}

var retentionCleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Prune old terminal runs and related durable records",
	Long: `Prune old terminal run rows, dispatch events, orphaned ephemeral job definitions,
idempotency keys, audit log rows, and optionally local durable run log files.

The command is destructive. Use --dry-run first, then pass --yes to apply.`,
	Args: cobra.NoArgs,
	Run:  runRetentionCleanup,
}
