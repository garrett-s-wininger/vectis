package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"vectis/internal/database"
	"vectis/internal/retention"
)

type retentionBackupCheckOptions struct {
	ManifestPath string
	ExpectPath   string
	MaxAge       time.Duration
}

type retentionBackupEvidence struct {
	ManifestPath        string `json:"manifest_path"`
	ExpectPath          string `json:"expect_path,omitempty"`
	Verified            bool   `json:"verified"`
	CheckedAt           string `json:"checked_at"`
	ManifestGeneratedAt string `json:"manifest_generated_at,omitempty"`
	ExpectationSource   string `json:"expectation_source,omitempty"`
	MaxAge              string `json:"max_age,omitempty"`
	Age                 string `json:"age,omitempty"`
	Warnings            int    `json:"warnings"`
}

func runRetentionCleanup(cmd *cobra.Command, args []string) {
	policy := retention.Policy{
		TerminalRuns:    retentionRunAge,
		JobDefinitions:  retentionDefAge,
		IdempotencyKeys: retentionIdemAge,
		AuditLog:        retentionAuditAge,
		ArtifactBlobs:   retentionArtifactAge,
	}

	backupOptions := retentionBackupCheckOptions{
		ManifestPath: retentionBackupManifest,
		ExpectPath:   retentionBackupExpect,
		MaxAge:       retentionBackupMaxAge,
	}

	runCLIError(retentionCleanup(cmd.Context(), os.Stdout, policy, retentionDryRun, retentionYes, retentionLogDir, retentionArtifactDir, backupOptions))
}

func retentionCleanup(ctx context.Context, w io.Writer, policy retention.Policy, dryRun, yes bool, logStorageDir, artifactStorageDir string, backupOptions retentionBackupCheckOptions) error {
	if !dryRun && !yes {
		return fmt.Errorf("retention cleanup deletes durable records; pass --dry-run to inspect or --yes to apply")
	}

	now := time.Now().UTC()
	backupEvidence, err := checkRetentionBackupManifest(backupOptions, now)
	if err != nil {
		return err
	}

	dbPath := database.GetDBPath()
	db, err := database.OpenDB(dbPath)
	if err != nil {
		return err
	}
	defer closeIgnoringError(db)

	if err := database.WaitForMigrationsContext(ctx, db, nil); err != nil {
		return err
	}

	cleaner := retention.NewSQLCleaner(db)

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
		payload := map[string]any{
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
				"terminal_runs":       report.Counts.TerminalRuns,
				"run_dispatch_events": report.Counts.RunDispatchEvents,
				"run_artifacts":       report.Counts.RunArtifacts,
				"run_tasks":           report.Counts.RunTasks,
				"task_attempts":       report.Counts.TaskAttempts,
				"run_segments":        report.Counts.RunSegments,
				"segment_executions":  report.Counts.SegmentExecutions,
				"job_definitions":     report.Counts.JobDefinitions,
				"idempotency_keys":    report.Counts.IdempotencyKeys,
				"audit_log":           report.Counts.AuditLog,
				"run_log_files":       fileReport.RunLogFiles,
				"run_log_bytes":       fileReport.RunLogBytes,
				"artifact_blob_files": fileReport.ArtifactBlobFiles,
				"artifact_blob_bytes": fileReport.ArtifactBlobBytes,
			},
			"audit": map[string]bool{"event_inserted": report.AuditEventInserted},
		}
		if backupEvidence != nil {
			payload["backup"] = backupEvidence
		}
		return writeJSON(w, payload)
	}

	printRetentionReport(w, report, fileReport, backupEvidence)
	if dryRun {
		fmt.Fprintln(w, "Cleanup not applied.")
		return nil
	}

	fmt.Fprintln(w, "Cleanup applied.")
	return nil
}

func checkRetentionBackupManifest(options retentionBackupCheckOptions, checkedAt time.Time) (*retentionBackupEvidence, error) {
	options.ManifestPath = strings.TrimSpace(options.ManifestPath)
	options.ExpectPath = strings.TrimSpace(options.ExpectPath)
	if options.ManifestPath == "" {
		if options.ExpectPath != "" || options.MaxAge > 0 {
			return nil, fmt.Errorf("--backup-expect and --backup-max-age require --backup-manifest")
		}

		return nil, nil
	}

	if options.ManifestPath == "-" && options.ExpectPath == "-" {
		return nil, fmt.Errorf("manifest and expected topology cannot both be read from stdin")
	}

	manifest, err := readBackupManifestFile(options.ManifestPath)
	if err != nil {
		return nil, err
	}

	var expected *backupExpectedTopologyInput
	if options.ExpectPath != "" {
		input, err := readBackupExpectedTopologyFile(options.ExpectPath)
		if err != nil {
			return nil, err
		}

		expected = &input
	}

	result := verifyBackupManifest(manifest, expected, checkedAt)
	evidence := &retentionBackupEvidence{
		ManifestPath:        options.ManifestPath,
		ExpectPath:          options.ExpectPath,
		Verified:            result.Status == backupManifestStatusOK,
		CheckedAt:           result.CheckedAt,
		ManifestGeneratedAt: result.ManifestGeneratedAt,
		ExpectationSource:   result.ExpectationSource,
		Warnings:            len(result.Warnings),
	}

	if options.MaxAge > 0 {
		evidence.MaxAge = options.MaxAge.String()
	}

	if result.Status != backupManifestStatusOK {
		return evidence, fmt.Errorf("backup manifest verification failed: %d error(s)", len(result.Errors))
	}

	if options.MaxAge > 0 {
		generatedAt, err := time.Parse(time.RFC3339, manifest.GeneratedAt)
		if err != nil {
			return evidence, fmt.Errorf("backup manifest generated_at is not RFC3339: %w", err)
		}

		age := checkedAt.Sub(generatedAt)
		evidence.Age = age.String()
		if age < 0 {
			return evidence, fmt.Errorf("backup manifest generated_at %s is after check time %s", manifest.GeneratedAt, checkedAt.Format(time.RFC3339))
		}

		if age > options.MaxAge {
			return evidence, fmt.Errorf("backup manifest is stale: generated_at=%s age=%s max_age=%s", manifest.GeneratedAt, age, options.MaxAge)
		}
	}

	return evidence, nil
}

func printRetentionReport(w io.Writer, report retention.Report, fileReport retention.FileReport, backupEvidence *retentionBackupEvidence) {
	prefix := "deleted"
	if report.DryRun {
		prefix = "would_delete"
	}

	fmt.Fprintf(w, "dry_run=%t\n", report.DryRun)
	if backupEvidence != nil {
		fmt.Fprintf(w, "backup_manifest_verified=%t\n", backupEvidence.Verified)
		fmt.Fprintf(w, "backup_manifest_path=%s\n", backupEvidence.ManifestPath)
		fmt.Fprintf(w, "backup_manifest_checked_at=%s\n", backupEvidence.CheckedAt)
		fmt.Fprintf(w, "backup_manifest_generated_at=%s\n", backupEvidence.ManifestGeneratedAt)
		if backupEvidence.ExpectationSource != "" {
			fmt.Fprintf(w, "backup_manifest_expectation_source=%s\n", backupEvidence.ExpectationSource)
		}

		if backupEvidence.MaxAge != "" {
			fmt.Fprintf(w, "backup_manifest_max_age=%s\n", backupEvidence.MaxAge)
			fmt.Fprintf(w, "backup_manifest_age=%s\n", backupEvidence.Age)
		}

		fmt.Fprintf(w, "backup_manifest_warnings=%d\n", backupEvidence.Warnings)
	}

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
unreferenced job definition snapshots, idempotency keys, audit log rows, and optionally
local durable run log files and unreferenced artifact blobs.

The command is destructive. Use --dry-run first, then pass --yes to apply.
Pass --backup-manifest to require backup manifest verification before cleanup.
Use --backup-expect to enforce a topology file and --backup-max-age to require
fresh manifest evidence.`,
	Args: cobra.NoArgs,
	Run:  runRetentionCleanup,
}
