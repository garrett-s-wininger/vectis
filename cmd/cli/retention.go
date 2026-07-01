package main

import (
	"context"
	"database/sql"
	"encoding/json"
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
	ManifestPath        string
	ExpectPath          string
	MaxAge              time.Duration
	StorageReportPaths  []string
	StorageReportMaxAge time.Duration
}

type retentionBackupEvidence struct {
	ManifestPath           string `json:"manifest_path"`
	ExpectPath             string `json:"expect_path,omitempty"`
	Verified               bool   `json:"verified"`
	CheckedAt              string `json:"checked_at"`
	ManifestGeneratedAt    string `json:"manifest_generated_at,omitempty"`
	ExpectationSource      string `json:"expectation_source,omitempty"`
	MaxAge                 string `json:"max_age,omitempty"`
	Age                    string `json:"age,omitempty"`
	Warnings               int    `json:"warnings"`
	StorageReports         int    `json:"storage_reports,omitempty"`
	StorageReportsVerified int    `json:"storage_reports_verified,omitempty"`
	StoragePathsRequired   int    `json:"storage_paths_required,omitempty"`
	StorageReportMaxAge    string `json:"storage_report_max_age,omitempty"`
}

type retentionAuditExportCheckOptions struct {
	ExportPath string
	MaxAge     time.Duration
}

type retentionAuditExportEvidence struct {
	ExportPath     string `json:"export_path"`
	Verified       bool   `json:"verified"`
	CheckedAt      string `json:"checked_at"`
	GeneratedAt    string `json:"generated_at,omitempty"`
	MaxAge         string `json:"max_age,omitempty"`
	Age            string `json:"age,omitempty"`
	Cutoff         string `json:"cutoff,omitempty"`
	RowsEligible   int64  `json:"rows_eligible"`
	RowsExported   int    `json:"rows_exported"`
	PageCount      int    `json:"page_count"`
	EventsSHA256   string `json:"events_sha256,omitempty"`
	OldestEventAt  string `json:"oldest_event_at,omitempty"`
	NewestEventAt  string `json:"newest_event_at,omitempty"`
	MayBeTruncated bool   `json:"may_be_truncated"`
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
		ManifestPath:        retentionBackupManifest,
		ExpectPath:          retentionBackupExpect,
		MaxAge:              retentionBackupMaxAge,
		StorageReportPaths:  retentionBackupStorageReports,
		StorageReportMaxAge: retentionBackupStorageMaxAge,
	}

	auditExportOptions := retentionAuditExportCheckOptions{
		ExportPath: retentionAuditExport,
		MaxAge:     retentionAuditExportMaxAge,
	}

	runCLIError(retentionCleanup(cmd.Context(), os.Stdout, policy, retentionDryRun, retentionYes, retentionLogDir, retentionArtifactDir, backupOptions, auditExportOptions))
}

func retentionCleanup(ctx context.Context, w io.Writer, policy retention.Policy, dryRun, yes bool, logStorageDir, artifactStorageDir string, backupOptions retentionBackupCheckOptions, auditExportOptions retentionAuditExportCheckOptions) error {
	if !dryRun && !yes {
		return fmt.Errorf("retention cleanup deletes durable records; pass --dry-run to inspect or --yes to apply")
	}

	now := time.Now().UTC()
	backupEvidence, err := checkRetentionBackupManifest(backupOptions, now)
	if err != nil {
		return err
	}

	if strings.TrimSpace(auditExportOptions.ExportPath) == "" && auditExportOptions.MaxAge > 0 {
		return fmt.Errorf("--audit-export-max-age requires --audit-export")
	}

	db, err := openRetentionDatabase()
	if err != nil {
		return err
	}
	defer closeIgnoringError(db)

	if err := database.WaitForMigrationsContext(ctx, db, nil); err != nil {
		return err
	}

	cleaner := retention.NewSQLCleaner(db)

	var auditExportEvidence *retentionAuditExportEvidence
	var precheckedReport *retention.Report
	if strings.TrimSpace(auditExportOptions.ExportPath) != "" {
		preview, err := cleaner.Preview(ctx, policy, now)
		if err != nil {
			return err
		}

		precheckedReport = &preview
		auditExportEvidence, err = checkRetentionAuditExport(auditExportOptions, preview.Cutoffs.AuditLog, preview.Counts.AuditLog, now)
		if err != nil {
			return err
		}
	} else if auditExportOptions.MaxAge > 0 {
		return fmt.Errorf("--audit-export-max-age requires --audit-export")
	}

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
		if precheckedReport != nil {
			report = *precheckedReport
		} else {
			report, err = cleaner.Preview(ctx, policy, now)
		}
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
			"held_counts": map[string]int64{
				"terminal_runs":       report.HeldCounts.TerminalRuns,
				"run_dispatch_events": report.HeldCounts.RunDispatchEvents,
				"run_artifacts":       report.HeldCounts.RunArtifacts,
				"run_tasks":           report.HeldCounts.RunTasks,
				"task_attempts":       report.HeldCounts.TaskAttempts,
				"run_segments":        report.HeldCounts.RunSegments,
				"segment_executions":  report.HeldCounts.SegmentExecutions,
			},
			"audit": map[string]bool{"event_inserted": report.AuditEventInserted},
		}

		if backupEvidence != nil {
			payload["backup"] = backupEvidence
		}

		if auditExportEvidence != nil {
			payload["audit_export"] = auditExportEvidence
		}

		return writeJSON(w, payload)
	}

	printRetentionReport(w, report, fileReport, backupEvidence, auditExportEvidence)
	if dryRun {
		fmt.Fprintln(w, "Cleanup not applied.")
		return nil
	}

	fmt.Fprintln(w, "Cleanup applied.")
	return nil
}

func openRetentionDatabase() (*sql.DB, error) {
	dbPath := database.GetDBPath()
	db, err := database.OpenDB(dbPath)
	if err != nil {
		return nil, err
	}

	if err := database.WaitForMigrations(db, nil); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func checkRetentionAuditExport(options retentionAuditExportCheckOptions, cutoff *time.Time, rowsEligible int64, checkedAt time.Time) (*retentionAuditExportEvidence, error) {
	options.ExportPath = strings.TrimSpace(options.ExportPath)
	if options.ExportPath == "" {
		if options.MaxAge > 0 {
			return nil, fmt.Errorf("--audit-export-max-age requires --audit-export")
		}

		return nil, nil
	}

	if options.ExportPath == "-" {
		return nil, fmt.Errorf("--audit-export must be a retained file path")
	}

	raw, err := os.ReadFile(options.ExportPath)
	if err != nil {
		return nil, fmt.Errorf("read audit export: %w", err)
	}

	var export auditExportEvidence
	if err := json.Unmarshal(raw, &export); err != nil {
		return nil, fmt.Errorf("parse audit export: %w", err)
	}

	evidence := &retentionAuditExportEvidence{
		ExportPath:     options.ExportPath,
		CheckedAt:      checkedAt.UTC().Format(time.RFC3339),
		GeneratedAt:    export.GeneratedAt,
		Cutoff:         retentionCutoff(cutoff),
		RowsEligible:   rowsEligible,
		RowsExported:   export.RowCount,
		PageCount:      export.PageCount,
		EventsSHA256:   export.EventsSHA256,
		OldestEventAt:  export.OldestEventAt,
		NewestEventAt:  export.NewestEventAt,
		MayBeTruncated: export.MayBeTruncated,
	}

	if options.MaxAge > 0 {
		evidence.MaxAge = options.MaxAge.String()
	}

	if export.SchemaVersion != auditExportSchemaVersion {
		return evidence, fmt.Errorf("audit export schema_version = %q, want %q", export.SchemaVersion, auditExportSchemaVersion)
	}

	if export.RowCount != len(export.Events) {
		return evidence, fmt.Errorf("audit export row_count=%d does not match events length %d", export.RowCount, len(export.Events))
	}

	if export.MayBeTruncated {
		return evidence, fmt.Errorf("audit export may be truncated")
	}

	eventsSHA256, err := auditEventsSHA256(export.Events)
	if err != nil {
		return evidence, err
	}

	if export.EventsSHA256 != eventsSHA256 {
		return evidence, fmt.Errorf("audit export events_sha256 mismatch")
	}

	generatedAt, err := time.Parse(time.RFC3339, export.GeneratedAt)
	if err != nil {
		return evidence, fmt.Errorf("audit export generated_at is not RFC3339: %w", err)
	}

	age := checkedAt.Sub(generatedAt)
	evidence.Age = age.String()
	if age < 0 {
		return evidence, fmt.Errorf("audit export generated_at %s is after check time %s", export.GeneratedAt, checkedAt.Format(time.RFC3339))
	}

	if options.MaxAge > 0 && age > options.MaxAge {
		return evidence, fmt.Errorf("audit export is stale: generated_at=%s age=%s max_age=%s", export.GeneratedAt, age, options.MaxAge)
	}

	if !auditExportHasFullRetentionRange(export.Filters) {
		return evidence, fmt.Errorf("audit export for retention cleanup must not set event, actor, target, correlation, or since filters")
	}

	if cutoff != nil {
		until := strings.TrimSpace(export.Filters.Until)
		if until != "" {
			untilTime, err := parseAuditExportRetentionTime(until)
			if err != nil {
				return evidence, err
			}

			if untilTime.Before(cutoff.UTC()) {
				return evidence, fmt.Errorf("audit export until %s is before audit cleanup cutoff %s", until, cutoff.UTC().Format(time.RFC3339))
			}
		}
	}

	if rowsEligible > int64(export.RowCount) {
		return evidence, fmt.Errorf("audit export row_count=%d is less than retention-eligible audit rows=%d", export.RowCount, rowsEligible)
	}

	evidence.Verified = true
	return evidence, nil
}

func auditExportHasFullRetentionRange(filters auditExportFilters) bool {
	return strings.TrimSpace(filters.EventType) == "" &&
		filters.ActorID == nil &&
		filters.TargetID == nil &&
		strings.TrimSpace(filters.CorrelationID) == "" &&
		strings.TrimSpace(filters.Since) == ""
}

func parseAuditExportRetentionTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}

	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t.UTC(), nil
	}

	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t.UTC(), nil
	}

	return time.Time{}, fmt.Errorf("audit export until must be RFC3339 or YYYY-MM-DD")
}

func checkRetentionBackupManifest(options retentionBackupCheckOptions, checkedAt time.Time) (*retentionBackupEvidence, error) {
	options.ManifestPath = strings.TrimSpace(options.ManifestPath)
	options.ExpectPath = strings.TrimSpace(options.ExpectPath)
	if options.ManifestPath == "" {
		if options.ExpectPath != "" || options.MaxAge > 0 || len(options.StorageReportPaths) > 0 || options.StorageReportMaxAge > 0 {
			return nil, fmt.Errorf("--backup-expect, --backup-max-age, --backup-storage-report, and --backup-storage-max-age require --backup-manifest")
		}

		return nil, nil
	}

	if options.StorageReportMaxAge > 0 && len(options.StorageReportPaths) == 0 {
		return nil, fmt.Errorf("--backup-storage-max-age requires --backup-storage-report")
	}

	if options.ManifestPath == "-" && (options.ExpectPath == "-" || backupStorageReportPathsContainStdin(options.StorageReportPaths)) {
		return nil, fmt.Errorf("manifest, expected topology, and storage reports cannot share stdin")
	}

	if options.ExpectPath == "-" && backupStorageReportPathsContainStdin(options.StorageReportPaths) {
		return nil, fmt.Errorf("expected topology and storage reports cannot both be read from stdin")
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

	storageReports, err := readBackupStorageReportInputs(options.StorageReportPaths)
	if err != nil {
		return nil, err
	}

	result := verifyBackupManifestWithStorage(manifest, expected, storageReports, options.StorageReportMaxAge, checkedAt)
	evidence := &retentionBackupEvidence{
		ManifestPath:           options.ManifestPath,
		ExpectPath:             options.ExpectPath,
		Verified:               result.Status == backupManifestStatusOK,
		CheckedAt:              result.CheckedAt,
		ManifestGeneratedAt:    result.ManifestGeneratedAt,
		ExpectationSource:      result.ExpectationSource,
		Warnings:               len(result.Warnings),
		StorageReports:         result.Summary.StorageReports,
		StorageReportsVerified: result.Summary.StorageReportsVerified,
		StoragePathsRequired:   result.Summary.StoragePathsRequired,
		StorageReportMaxAge:    result.StorageReportMaxAge,
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

func printRetentionReport(w io.Writer, report retention.Report, fileReport retention.FileReport, backupEvidence *retentionBackupEvidence, auditExportEvidence *retentionAuditExportEvidence) {
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
		if backupEvidence.StorageReports > 0 {
			fmt.Fprintf(w, "backup_storage_reports=%d\n", backupEvidence.StorageReports)
			fmt.Fprintf(w, "backup_storage_reports_verified=%d\n", backupEvidence.StorageReportsVerified)
			fmt.Fprintf(w, "backup_storage_paths_required=%d\n", backupEvidence.StoragePathsRequired)
			if backupEvidence.StorageReportMaxAge != "" {
				fmt.Fprintf(w, "backup_storage_report_max_age=%s\n", backupEvidence.StorageReportMaxAge)
			}
		}
	}
	if auditExportEvidence != nil {
		fmt.Fprintf(w, "audit_export_verified=%t\n", auditExportEvidence.Verified)
		fmt.Fprintf(w, "audit_export_path=%s\n", auditExportEvidence.ExportPath)
		fmt.Fprintf(w, "audit_export_checked_at=%s\n", auditExportEvidence.CheckedAt)
		fmt.Fprintf(w, "audit_export_generated_at=%s\n", auditExportEvidence.GeneratedAt)
		if auditExportEvidence.MaxAge != "" {
			fmt.Fprintf(w, "audit_export_max_age=%s\n", auditExportEvidence.MaxAge)
			fmt.Fprintf(w, "audit_export_age=%s\n", auditExportEvidence.Age)
		}

		if auditExportEvidence.Cutoff != "" {
			fmt.Fprintf(w, "audit_export_cutoff=%s\n", auditExportEvidence.Cutoff)
		}

		fmt.Fprintf(w, "audit_export_rows_eligible=%d\n", auditExportEvidence.RowsEligible)
		fmt.Fprintf(w, "audit_export_rows_exported=%d\n", auditExportEvidence.RowsExported)
		fmt.Fprintf(w, "audit_export_pages=%d\n", auditExportEvidence.PageCount)
		fmt.Fprintf(w, "audit_export_may_be_truncated=%t\n", auditExportEvidence.MayBeTruncated)
		if auditExportEvidence.EventsSHA256 != "" {
			fmt.Fprintf(w, "audit_export_events_sha256=%s\n", auditExportEvidence.EventsSHA256)
		}
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
	fmt.Fprintf(w, "held.terminal_runs=%d\n", report.HeldCounts.TerminalRuns)
	fmt.Fprintf(w, "held.run_dispatch_events=%d\n", report.HeldCounts.RunDispatchEvents)
	fmt.Fprintf(w, "held.run_artifacts=%d\n", report.HeldCounts.RunArtifacts)
	fmt.Fprintf(w, "held.run_tasks=%d\n", report.HeldCounts.RunTasks)
	fmt.Fprintf(w, "held.task_attempts=%d\n", report.HeldCounts.TaskAttempts)
	fmt.Fprintf(w, "held.run_segments=%d\n", report.HeldCounts.RunSegments)
	fmt.Fprintf(w, "held.segment_executions=%d\n", report.HeldCounts.SegmentExecutions)
	fmt.Fprintf(w, "audit_event_inserted=%t\n", report.AuditEventInserted)
}

func runRetentionHoldCreate(cmd *cobra.Command, args []string) {
	expiresAt, err := parseRetentionHoldExpiresAt(retentionHoldExpiresAt)
	if err != nil {
		runCLIError(err)
	}

	db, err := openRetentionDatabase()
	if err != nil {
		runCLIError(err)
	}
	defer db.Close()

	createdBy := strings.TrimSpace(retentionHoldCreatedBy)
	if createdBy == "" {
		createdBy = defaultRetentionHoldActor()
	}

	hold, err := retention.NewSQLHoldStore(db).Create(cmd.Context(), retention.CreateHoldOptions{
		Scope:       retention.HoldScopeRun,
		TargetID:    retentionHoldRunID,
		Reason:      retentionHoldReason,
		Owner:       retentionHoldOwner,
		ExternalRef: retentionHoldExternalRef,
		CreatedBy:   createdBy,
		ExpiresAt:   expiresAt,
	}, time.Now().UTC())
	if err != nil {
		runCLIError(err)
	}

	runCLIError(writeRetentionHold(os.Stdout, hold))
}

func runRetentionHoldList(cmd *cobra.Command, args []string) {
	db, err := openRetentionDatabase()
	if err != nil {
		runCLIError(err)
	}
	defer db.Close()

	holds, err := retention.NewSQLHoldStore(db).List(cmd.Context(), retention.ListHoldOptions{
		Scope:         retention.HoldScopeRun,
		TargetID:      retentionHoldListRunID,
		IncludeClosed: retentionHoldListAll,
	}, time.Now().UTC())
	if err != nil {
		runCLIError(err)
	}

	runCLIError(writeRetentionHoldList(os.Stdout, holds))
}

func runRetentionHoldRelease(cmd *cobra.Command, args []string) {
	db, err := openRetentionDatabase()
	if err != nil {
		runCLIError(err)
	}
	defer db.Close()

	releasedBy := strings.TrimSpace(retentionHoldReleasedBy)
	if releasedBy == "" {
		releasedBy = defaultRetentionHoldActor()
	}

	hold, err := retention.NewSQLHoldStore(db).Release(cmd.Context(), retention.ReleaseHoldOptions{
		HoldID:        args[0],
		ReleasedBy:    releasedBy,
		ReleaseReason: retentionHoldReleaseReason,
	}, time.Now().UTC())
	if err != nil {
		runCLIError(err)
	}

	runCLIError(writeRetentionHold(os.Stdout, hold))
}

func writeRetentionHold(w io.Writer, hold retention.Hold) error {
	if outputIsJSON() {
		return writeJSON(w, hold)
	}

	fmt.Fprintf(w, "hold_id=%s\n", hold.HoldID)
	fmt.Fprintf(w, "scope=%s\n", hold.Scope)
	fmt.Fprintf(w, "target_id=%s\n", hold.TargetID)
	fmt.Fprintf(w, "status=%s\n", hold.Status)
	fmt.Fprintf(w, "owner=%s\n", hold.Owner)
	fmt.Fprintf(w, "reason=%s\n", hold.Reason)
	if hold.ExternalRef != "" {
		fmt.Fprintf(w, "external_ref=%s\n", hold.ExternalRef)
	}

	if hold.CreatedBy != "" {
		fmt.Fprintf(w, "created_by=%s\n", hold.CreatedBy)
	}

	fmt.Fprintf(w, "created_at=%s\n", hold.CreatedAt.UTC().Format(time.RFC3339))
	if hold.ExpiresAt != nil {
		fmt.Fprintf(w, "expires_at=%s\n", hold.ExpiresAt.UTC().Format(time.RFC3339))
	}

	if hold.ReleasedAt != nil {
		fmt.Fprintf(w, "released_by=%s\n", hold.ReleasedBy)
		fmt.Fprintf(w, "release_reason=%s\n", hold.ReleaseReason)
		fmt.Fprintf(w, "released_at=%s\n", hold.ReleasedAt.UTC().Format(time.RFC3339))
	}

	return nil
}

func writeRetentionHoldList(w io.Writer, holds []retention.Hold) error {
	if outputIsJSON() {
		return writeJSON(w, map[string]any{"holds": holds})
	}

	if len(holds) == 0 {
		_, err := fmt.Fprintln(w, "holds=0")
		return err
	}

	for _, hold := range holds {
		parts := []string{
			"hold_id=" + hold.HoldID,
			"scope=" + hold.Scope,
			"target_id=" + hold.TargetID,
			"status=" + hold.Status,
			"owner=" + hold.Owner,
			"reason=" + hold.Reason,
			"created_at=" + hold.CreatedAt.UTC().Format(time.RFC3339),
		}

		if hold.ExternalRef != "" {
			parts = append(parts, "external_ref="+hold.ExternalRef)
		}

		if hold.ExpiresAt != nil {
			parts = append(parts, "expires_at="+hold.ExpiresAt.UTC().Format(time.RFC3339))
		}

		if hold.ReleasedAt != nil {
			parts = append(parts,
				"released_by="+hold.ReleasedBy,
				"release_reason="+hold.ReleaseReason,
				"released_at="+hold.ReleasedAt.UTC().Format(time.RFC3339),
			)
		}

		if _, err := fmt.Fprintln(w, strings.Join(parts, " ")); err != nil {
			return err
		}
	}

	return nil
}

func parseRetentionHoldExpiresAt(value string) (*time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}

	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf("--expires-at must be RFC3339: %w", err)
	}

	t = t.UTC()
	return &t, nil
}

func defaultRetentionHoldActor() string {
	for _, key := range []string{"VECTIS_OPERATOR", "USER", "USERNAME"} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}

	return "vectis-cli"
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
fresh manifest evidence. Use --backup-storage-report to also require byte-level
storage verifier evidence for the manifest's file-backed state.`,
	Args: cobra.NoArgs,
	Run:  runRetentionCleanup,
}

var retentionHoldsCmd = &cobra.Command{
	Use:   "holds",
	Short: "Manage retention compliance holds",
	Long: `Manage run-scoped compliance holds that prevent retention cleanup from
deleting protected run state, task graph rows, dispatch events, artifact
manifests, and matching local run log files while the hold is active.`,
	Args: cobra.NoArgs,
	Run:  showCommandHelp,
}

var retentionHoldCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a run retention hold",
	Long: `Create a run-scoped retention hold. Active holds protect retention-eligible
terminal runs and their related durable state from cleanup until released or
expired.`,
	Args: cobra.NoArgs,
	Run:  runRetentionHoldCreate,
}

var retentionHoldListCmd = &cobra.Command{
	Use:   "list",
	Short: "List retention holds",
	Args:  cobra.NoArgs,
	Run:   runRetentionHoldList,
}

var retentionHoldReleaseCmd = &cobra.Command{
	Use:   "release <hold-id>",
	Short: "Release a retention hold",
	Args:  cobra.ExactArgs(1),
	Run:   runRetentionHoldRelease,
}
