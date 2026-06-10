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
		ArtifactBlobs:   retentionArtifactAge,
	}

	runCLIError(retentionCleanup(cmd.Context(), os.Stdout, policy, retentionDryRun, retentionYes, retentionLogDir, retentionArtifactDir))
}

func retentionCleanup(ctx context.Context, w io.Writer, policy retention.Policy, dryRun, yes bool, logStorageDir, artifactStorageDir string) error {
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
	var artifactRefs map[string]bool
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

	if dryRun && artifactStorageDir != "" && policy.ArtifactBlobs > 0 {
		artifactRefs, err = cleaner.ReferencedArtifactBlobKeysExcludingTerminalRuns(ctx, policy.TerminalRuns, now)
		if err != nil {
			return fmt.Errorf("list artifact blob references: %w", err)
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

	if artifactStorageDir != "" {
		if !dryRun && policy.ArtifactBlobs > 0 {
			artifactRefs, err = cleaner.ReferencedArtifactBlobKeys(ctx)
			if err != nil {
				return fmt.Errorf("list artifact blob references: %w", err)
			}
		}

		artifactCleaner := retention.LocalArtifactBlobCleaner{
			Dir:                artifactStorageDir,
			Cutoff:             report.Cutoffs.ArtifactBlobs,
			ReferencedBlobKeys: artifactRefs,
		}

		var artifactReport retention.FileReport
		if dryRun {
			artifactReport, err = artifactCleaner.Preview()
		} else {
			artifactReport, err = artifactCleaner.Delete()
		}
		if err != nil {
			return err
		}

		fileReport.ArtifactBlobFiles = artifactReport.ArtifactBlobFiles
		fileReport.ArtifactBlobBytes = artifactReport.ArtifactBlobBytes
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
				"artifact_blobs":   retentionCutoff(report.Cutoffs.ArtifactBlobs),
			},
			"counts": map[string]int64{
				"terminal_runs":         report.Counts.TerminalRuns,
				"run_dispatch_events":   report.Counts.RunDispatchEvents,
				"run_artifacts":         report.Counts.RunArtifacts,
				"run_tasks":             report.Counts.RunTasks,
				"task_attempts":         report.Counts.TaskAttempts,
				"run_segments":          report.Counts.RunSegments,
				"segment_executions":    report.Counts.SegmentExecutions,
				"task_dispatch_intents": report.Counts.TaskDispatchIntents,
				"job_definitions":       report.Counts.JobDefinitions,
				"idempotency_keys":      report.Counts.IdempotencyKeys,
				"audit_log":             report.Counts.AuditLog,
				"run_log_files":         fileReport.RunLogFiles,
				"run_log_bytes":         fileReport.RunLogBytes,
				"artifact_blob_files":   fileReport.ArtifactBlobFiles,
				"artifact_blob_bytes":   fileReport.ArtifactBlobBytes,
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
	fmt.Fprintf(w, "cutoff.artifact_blobs=%s\n", retentionCutoff(report.Cutoffs.ArtifactBlobs))
	fmt.Fprintf(w, "%s.terminal_runs=%d\n", prefix, report.Counts.TerminalRuns)
	fmt.Fprintf(w, "%s.run_dispatch_events=%d\n", prefix, report.Counts.RunDispatchEvents)
	fmt.Fprintf(w, "%s.run_artifacts=%d\n", prefix, report.Counts.RunArtifacts)
	fmt.Fprintf(w, "%s.run_tasks=%d\n", prefix, report.Counts.RunTasks)
	fmt.Fprintf(w, "%s.task_attempts=%d\n", prefix, report.Counts.TaskAttempts)
	fmt.Fprintf(w, "%s.run_segments=%d\n", prefix, report.Counts.RunSegments)
	fmt.Fprintf(w, "%s.segment_executions=%d\n", prefix, report.Counts.SegmentExecutions)
	fmt.Fprintf(w, "%s.task_dispatch_intents=%d\n", prefix, report.Counts.TaskDispatchIntents)
	fmt.Fprintf(w, "%s.job_definitions=%d\n", prefix, report.Counts.JobDefinitions)
	fmt.Fprintf(w, "%s.idempotency_keys=%d\n", prefix, report.Counts.IdempotencyKeys)
	fmt.Fprintf(w, "%s.audit_log=%d\n", prefix, report.Counts.AuditLog)
	fmt.Fprintf(w, "%s.run_log_files=%d\n", prefix, fileReport.RunLogFiles)
	fmt.Fprintf(w, "%s.run_log_bytes=%d\n", prefix, fileReport.RunLogBytes)
	fmt.Fprintf(w, "%s.artifact_blob_files=%d\n", prefix, fileReport.ArtifactBlobFiles)
	fmt.Fprintf(w, "%s.artifact_blob_bytes=%d\n", prefix, fileReport.ArtifactBlobBytes)
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
	Long: `Prune old terminal run rows, artifact manifests, task graph rows, dispatch events,
orphaned ephemeral job definitions, idempotency keys, audit log rows, and optionally
local durable run log files and unreferenced artifact blobs.

The command is destructive. Use --dry-run first, then pass --yes to apply.`,
	Args: cobra.NoArgs,
	Run:  runRetentionCleanup,
}
