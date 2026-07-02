package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
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

type retentionHoldReviewCheckOptions struct {
	ReviewPath string
	MaxAge     time.Duration
}

type retentionPolicyGateOptions struct {
	RequireBackupManifest bool
	RequireAuditExport    bool
	RequireHoldReview     bool
	WaiverPath            string
}

type retentionCleanupEvidenceManifestFile struct {
	SchemaVersion         string   `json:"schema_version"`
	GeneratedAt           string   `json:"generated_at,omitempty"`
	GeneratedBy           string   `json:"generated_by,omitempty"`
	ExternalRef           string   `json:"external_ref,omitempty"`
	BackupManifest        string   `json:"backup_manifest,omitempty"`
	BackupExpect          string   `json:"backup_expect,omitempty"`
	BackupStorageReports  []string `json:"backup_storage_reports,omitempty"`
	BackupMaxAge          string   `json:"backup_max_age,omitempty"`
	BackupStorageMaxAge   string   `json:"backup_storage_max_age,omitempty"`
	AuditExport           string   `json:"audit_export,omitempty"`
	AuditExportMaxAge     string   `json:"audit_export_max_age,omitempty"`
	HoldReview            string   `json:"hold_review,omitempty"`
	HoldReviewMaxAge      string   `json:"hold_review_max_age,omitempty"`
	RequireBackupManifest *bool    `json:"require_backup_manifest,omitempty"`
	RequireAuditExport    *bool    `json:"require_audit_export,omitempty"`
	RequireHoldReview     *bool    `json:"require_hold_review,omitempty"`
	Waiver                string   `json:"waiver,omitempty"`
}

type retentionCleanupEvidenceManifestEvidence struct {
	ManifestPath          string   `json:"manifest_path"`
	Verified              bool     `json:"verified"`
	CheckedAt             string   `json:"checked_at"`
	SchemaVersion         string   `json:"schema_version"`
	GeneratedAt           string   `json:"generated_at,omitempty"`
	GeneratedBy           string   `json:"generated_by,omitempty"`
	ExternalRef           string   `json:"external_ref,omitempty"`
	BackupManifest        string   `json:"backup_manifest,omitempty"`
	BackupExpect          string   `json:"backup_expect,omitempty"`
	BackupStorageReports  []string `json:"backup_storage_reports,omitempty"`
	BackupMaxAge          string   `json:"backup_max_age,omitempty"`
	BackupStorageMaxAge   string   `json:"backup_storage_max_age,omitempty"`
	AuditExport           string   `json:"audit_export,omitempty"`
	AuditExportMaxAge     string   `json:"audit_export_max_age,omitempty"`
	HoldReview            string   `json:"hold_review,omitempty"`
	HoldReviewMaxAge      string   `json:"hold_review_max_age,omitempty"`
	RequireBackupManifest bool     `json:"require_backup_manifest"`
	RequireAuditExport    bool     `json:"require_audit_export"`
	RequireHoldReview     bool     `json:"require_hold_review"`
	Waiver                string   `json:"waiver,omitempty"`
}

type retentionCleanupEvidenceManifestBuildOptions struct {
	BackupManifest        string
	BackupExpect          string
	BackupStorageReports  []string
	BackupMaxAge          time.Duration
	BackupStorageMaxAge   time.Duration
	AuditExport           string
	AuditExportMaxAge     time.Duration
	HoldReview            string
	HoldReviewMaxAge      time.Duration
	RequireBackupManifest bool
	RequireAuditExport    bool
	RequireHoldReview     bool
	Waiver                string
}

type retentionCleanupEvidenceManifestWriteResult struct {
	Status        string                                        `json:"status"`
	Path          string                                        `json:"path,omitempty"`
	PromotedTo    string                                        `json:"promoted_to,omitempty"`
	Verified      bool                                          `json:"verified,omitempty"`
	SchemaVersion string                                        `json:"schema_version"`
	GeneratedAt   string                                        `json:"generated_at,omitempty"`
	GeneratedBy   string                                        `json:"generated_by,omitempty"`
	ExternalRef   string                                        `json:"external_ref,omitempty"`
	Verification  *retentionCleanupEvidenceManifestVerification `json:"verification,omitempty"`
}

type retentionCleanupEvidenceManifestVerification struct {
	Verified          bool                          `json:"verified"`
	CheckedAt         string                        `json:"checked_at"`
	Backup            *retentionBackupEvidence      `json:"backup,omitempty"`
	AuditExport       *retentionAuditExportEvidence `json:"audit_export,omitempty"`
	HoldReview        *retentionHoldReviewEvidence  `json:"hold_review,omitempty"`
	Waiver            *retentionWaiverEvidence      `json:"waiver,omitempty"`
	AuditRowsEligible int64                         `json:"audit_rows_eligible,omitempty"`
	AuditCutoff       string                        `json:"audit_cutoff,omitempty"`
}

type retentionCleanupFlagChanges struct {
	BackupManifest        bool
	BackupExpect          bool
	BackupStorageReports  bool
	BackupMaxAge          bool
	BackupStorageMaxAge   bool
	AuditExport           bool
	AuditExportMaxAge     bool
	HoldReview            bool
	HoldReviewMaxAge      bool
	RequireBackupManifest bool
	RequireAuditExport    bool
	RequireHoldReview     bool
	Waiver                bool
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

type retentionHoldReviewRecord struct {
	HoldID      string `json:"hold_id"`
	Scope       string `json:"scope"`
	TargetID    string `json:"target_id"`
	Status      string `json:"status"`
	Owner       string `json:"owner"`
	Reason      string `json:"reason"`
	ExternalRef string `json:"external_ref,omitempty"`
	CreatedBy   string `json:"created_by,omitempty"`
	CreatedAt   string `json:"created_at"`
	ExpiresAt   string `json:"expires_at,omitempty"`
}

type retentionHoldReviewFile struct {
	SchemaVersion string                      `json:"schema_version"`
	GeneratedAt   string                      `json:"generated_at"`
	ReviewedBy    string                      `json:"reviewed_by"`
	Reason        string                      `json:"reason"`
	ExternalRef   string                      `json:"external_ref,omitempty"`
	ActiveHolds   int                         `json:"active_holds"`
	HoldsSHA256   string                      `json:"holds_sha256"`
	Holds         []retentionHoldReviewRecord `json:"holds"`
}

type retentionHoldReviewEvidence struct {
	ReviewPath string `json:"review_path,omitempty"`
	Verified   bool   `json:"verified"`
	CheckedAt  string `json:"checked_at,omitempty"`
	retentionHoldReviewFile
	MaxAge string `json:"max_age,omitempty"`
	Age    string `json:"age,omitempty"`
}

type retentionWaiverEvidence struct {
	WaiverPath    string   `json:"waiver_path"`
	Verified      bool     `json:"verified"`
	CheckedAt     string   `json:"checked_at"`
	SchemaVersion string   `json:"schema_version"`
	Waives        []string `json:"waives"`
	Reason        string   `json:"reason"`
	ApprovedBy    string   `json:"approved_by"`
	ExternalRef   string   `json:"external_ref,omitempty"`
	ExpiresAt     string   `json:"expires_at"`
}

type retentionWaiverFile struct {
	SchemaVersion string   `json:"schema_version"`
	Waives        []string `json:"waives"`
	Reason        string   `json:"reason"`
	ApprovedBy    string   `json:"approved_by"`
	ExternalRef   string   `json:"external_ref,omitempty"`
	ExpiresAt     string   `json:"expires_at"`
}

const (
	retentionWaiverSchemaVersion                  = "vectis.retention_waiver.v1"
	retentionHoldReviewSchemaVersion              = "vectis.retention_hold_review.v1"
	retentionCleanupEvidenceManifestSchemaVersion = "vectis.retention_cleanup_evidence.v1"
	retentionWaiverBackup                         = "backup_manifest"
	retentionWaiverAuditExport                    = "audit_export"
	retentionWaiverHoldReview                     = "hold_review"
)

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

	holdReviewOptions := retentionHoldReviewCheckOptions{
		ReviewPath: retentionHoldReview,
		MaxAge:     retentionHoldReviewMaxAge,
	}

	gateOptions := retentionPolicyGateOptions{
		RequireBackupManifest: retentionRequireBackupManifest,
		RequireAuditExport:    retentionRequireAuditExport,
		RequireHoldReview:     retentionRequireHoldReview,
		WaiverPath:            retentionWaiver,
	}

	evidenceManifest, err := applyRetentionCleanupEvidenceManifest(retentionCleanupEvidenceManifest, retentionCleanupFlagChangesFromCommand(cmd), &backupOptions, &auditExportOptions, &holdReviewOptions, &gateOptions, time.Now().UTC())
	if err != nil {
		runCLIError(err)
	}

	runCLIError(retentionCleanup(cmd.Context(), os.Stdout, policy, retentionDryRun, retentionYes, retentionLogDir, retentionArtifactDir, backupOptions, auditExportOptions, holdReviewOptions, gateOptions, evidenceManifest))
}

func runRetentionEvidenceManifest(cmd *cobra.Command, args []string) {
	generatedBy := strings.TrimSpace(retentionEvidenceManifestGeneratedBy)
	if generatedBy == "" {
		generatedBy = defaultRetentionOperator()
	}

	now := time.Now().UTC()
	buildOptions := retentionCleanupEvidenceManifestBuildOptions{
		BackupManifest:        retentionEvidenceBackupManifest,
		BackupExpect:          retentionEvidenceBackupExpect,
		BackupStorageReports:  retentionEvidenceBackupStorageReport,
		BackupMaxAge:          retentionEvidenceBackupMaxAge,
		BackupStorageMaxAge:   retentionEvidenceBackupStorageMaxAge,
		AuditExport:           retentionEvidenceAuditExport,
		AuditExportMaxAge:     retentionEvidenceAuditExportMaxAge,
		HoldReview:            retentionEvidenceHoldReview,
		HoldReviewMaxAge:      retentionEvidenceHoldReviewMaxAge,
		RequireBackupManifest: retentionEvidenceRequireBackup,
		RequireAuditExport:    retentionEvidenceRequireAuditExport,
		RequireHoldReview:     retentionEvidenceRequireHoldReview,
		Waiver:                retentionEvidenceWaiver,
	}

	manifest, err := buildRetentionCleanupEvidenceManifest(now, generatedBy, retentionEvidenceManifestExternalRef, buildOptions)
	if err != nil {
		runCLIError(err)
	}

	var verification *retentionCleanupEvidenceManifestVerification
	if retentionEvidenceManifestVerify {
		verification, err = verifyRetentionCleanupEvidenceManifest(cmd.Context(), retentionEvidenceManifestPolicy(), buildOptions, now)
		if err != nil {
			runCLIError(err)
		}
	}

	runCLIError(writeRetentionCleanupEvidenceManifest(os.Stdout, retentionEvidenceManifestOutput, retentionEvidenceManifestPromote, manifest, verification))
}

func retentionEvidenceManifestPolicy() retention.Policy {
	return retention.Policy{
		AuditLog: retentionEvidenceAuditAge,
	}
}

func verifyRetentionCleanupEvidenceManifest(ctx context.Context, policy retention.Policy, opts retentionCleanupEvidenceManifestBuildOptions, checkedAt time.Time) (*retentionCleanupEvidenceManifestVerification, error) {
	backupOptions := retentionBackupCheckOptions{
		ManifestPath:        opts.BackupManifest,
		ExpectPath:          opts.BackupExpect,
		MaxAge:              opts.BackupMaxAge,
		StorageReportPaths:  opts.BackupStorageReports,
		StorageReportMaxAge: opts.BackupStorageMaxAge,
	}

	auditExportOptions := retentionAuditExportCheckOptions{
		ExportPath: opts.AuditExport,
		MaxAge:     opts.AuditExportMaxAge,
	}

	holdReviewOptions := retentionHoldReviewCheckOptions{
		ReviewPath: opts.HoldReview,
		MaxAge:     opts.HoldReviewMaxAge,
	}

	gateOptions := retentionPolicyGateOptions{
		RequireBackupManifest: opts.RequireBackupManifest,
		RequireAuditExport:    opts.RequireAuditExport,
		RequireHoldReview:     opts.RequireHoldReview,
		WaiverPath:            opts.Waiver,
	}

	verification := &retentionCleanupEvidenceManifestVerification{
		CheckedAt: checkedAt.UTC().Format(time.RFC3339),
	}

	waiverEvidence, err := checkRetentionWaiver(gateOptions.WaiverPath, checkedAt)
	verification.Waiver = waiverEvidence
	if err != nil {
		return verification, err
	}

	backupEvidence, err := checkRetentionBackupManifest(backupOptions, checkedAt)
	verification.Backup = backupEvidence
	if err != nil {
		return verification, err
	}

	if err := enforceRetentionBackupGate(gateOptions, backupEvidence, waiverEvidence); err != nil {
		return verification, err
	}

	if strings.TrimSpace(auditExportOptions.ExportPath) == "" && auditExportOptions.MaxAge > 0 {
		return verification, fmt.Errorf("--audit-export-max-age requires --audit-export")
	}

	if strings.TrimSpace(holdReviewOptions.ReviewPath) == "" && holdReviewOptions.MaxAge > 0 {
		return verification, fmt.Errorf("--hold-review-max-age requires --hold-review")
	}

	needsDB := strings.TrimSpace(auditExportOptions.ExportPath) != "" ||
		gateOptions.RequireAuditExport ||
		strings.TrimSpace(holdReviewOptions.ReviewPath) != ""

	var db *sql.DB
	if needsDB {
		db, err = openRetentionDatabase()
		if err != nil {
			return verification, err
		}
		defer db.Close()
	}

	var holdReviewEvidence *retentionHoldReviewEvidence
	if strings.TrimSpace(holdReviewOptions.ReviewPath) != "" {
		activeHolds, err := retention.NewSQLHoldStore(db).List(ctx, retention.ListHoldOptions{}, checkedAt)
		if err != nil {
			return verification, fmt.Errorf("list active retention holds: %w", err)
		}

		holdReviewEvidence, err = checkRetentionHoldReview(holdReviewOptions, activeHolds, checkedAt)
		verification.HoldReview = holdReviewEvidence
		if err != nil {
			return verification, err
		}
	}

	if err := enforceRetentionHoldReviewGate(gateOptions, holdReviewEvidence, waiverEvidence); err != nil {
		return verification, err
	}

	var auditExportEvidence *retentionAuditExportEvidence
	var report *retention.Report
	if strings.TrimSpace(auditExportOptions.ExportPath) != "" || gateOptions.RequireAuditExport {
		preview, err := retention.NewSQLCleaner(db).Preview(ctx, policy, checkedAt)
		if err != nil {
			return verification, err
		}
		report = &preview
		verification.AuditRowsEligible = preview.Counts.AuditLog
		verification.AuditCutoff = retentionCutoff(preview.Cutoffs.AuditLog)
	}

	if strings.TrimSpace(auditExportOptions.ExportPath) != "" {
		auditExportEvidence, err = checkRetentionAuditExport(auditExportOptions, report.Cutoffs.AuditLog, report.Counts.AuditLog, checkedAt)
		verification.AuditExport = auditExportEvidence
		if err != nil {
			return verification, err
		}
	}

	if err := enforceRetentionAuditExportGate(gateOptions, auditExportEvidence, waiverEvidence, report); err != nil {
		return verification, err
	}

	verification.Verified = true
	return verification, nil
}

func buildRetentionCleanupEvidenceManifest(generatedAt time.Time, generatedBy, externalRef string, opts retentionCleanupEvidenceManifestBuildOptions) (retentionCleanupEvidenceManifestFile, error) {
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}

	backupManifest, err := retentionEvidenceManifestPath("--backup-manifest", opts.BackupManifest)
	if err != nil {
		return retentionCleanupEvidenceManifestFile{}, err
	}

	backupExpect, err := retentionEvidenceManifestPath("--backup-expect", opts.BackupExpect)
	if err != nil {
		return retentionCleanupEvidenceManifestFile{}, err
	}

	backupStorageReports, err := retentionEvidenceManifestPaths("--backup-storage-report", opts.BackupStorageReports)
	if err != nil {
		return retentionCleanupEvidenceManifestFile{}, err
	}

	auditExport, err := retentionEvidenceManifestPath("--audit-export", opts.AuditExport)
	if err != nil {
		return retentionCleanupEvidenceManifestFile{}, err
	}

	holdReview, err := retentionEvidenceManifestPath("--hold-review", opts.HoldReview)
	if err != nil {
		return retentionCleanupEvidenceManifestFile{}, err
	}

	waiver, err := retentionEvidenceManifestPath("--waiver", opts.Waiver)
	if err != nil {
		return retentionCleanupEvidenceManifestFile{}, err
	}

	backupMaxAge, err := retentionEvidenceManifestDuration("--backup-max-age", opts.BackupMaxAge)
	if err != nil {
		return retentionCleanupEvidenceManifestFile{}, err
	}

	backupStorageMaxAge, err := retentionEvidenceManifestDuration("--backup-storage-max-age", opts.BackupStorageMaxAge)
	if err != nil {
		return retentionCleanupEvidenceManifestFile{}, err
	}

	auditExportMaxAge, err := retentionEvidenceManifestDuration("--audit-export-max-age", opts.AuditExportMaxAge)
	if err != nil {
		return retentionCleanupEvidenceManifestFile{}, err
	}

	holdReviewMaxAge, err := retentionEvidenceManifestDuration("--hold-review-max-age", opts.HoldReviewMaxAge)
	if err != nil {
		return retentionCleanupEvidenceManifestFile{}, err
	}

	if backupManifest == "" && backupExpect != "" {
		return retentionCleanupEvidenceManifestFile{}, fmt.Errorf("--backup-expect requires --backup-manifest")
	}

	if backupManifest == "" && len(backupStorageReports) > 0 {
		return retentionCleanupEvidenceManifestFile{}, fmt.Errorf("--backup-storage-report requires --backup-manifest")
	}

	if backupManifest == "" && backupMaxAge != "" {
		return retentionCleanupEvidenceManifestFile{}, fmt.Errorf("--backup-max-age requires --backup-manifest")
	}

	if backupManifest == "" && backupStorageMaxAge != "" {
		return retentionCleanupEvidenceManifestFile{}, fmt.Errorf("--backup-storage-max-age requires --backup-manifest")
	}

	if len(backupStorageReports) == 0 && backupStorageMaxAge != "" {
		return retentionCleanupEvidenceManifestFile{}, fmt.Errorf("--backup-storage-max-age requires --backup-storage-report")
	}

	if auditExport == "" && auditExportMaxAge != "" {
		return retentionCleanupEvidenceManifestFile{}, fmt.Errorf("--audit-export-max-age requires --audit-export")
	}

	if holdReview == "" && holdReviewMaxAge != "" {
		return retentionCleanupEvidenceManifestFile{}, fmt.Errorf("--hold-review-max-age requires --hold-review")
	}

	manifest := retentionCleanupEvidenceManifestFile{
		SchemaVersion:        retentionCleanupEvidenceManifestSchemaVersion,
		GeneratedAt:          generatedAt.UTC().Format(time.RFC3339),
		GeneratedBy:          strings.TrimSpace(generatedBy),
		ExternalRef:          strings.TrimSpace(externalRef),
		BackupManifest:       backupManifest,
		BackupExpect:         backupExpect,
		BackupStorageReports: backupStorageReports,
		BackupMaxAge:         backupMaxAge,
		BackupStorageMaxAge:  backupStorageMaxAge,
		AuditExport:          auditExport,
		AuditExportMaxAge:    auditExportMaxAge,
		HoldReview:           holdReview,
		HoldReviewMaxAge:     holdReviewMaxAge,
		Waiver:               waiver,
	}

	if opts.RequireBackupManifest {
		manifest.RequireBackupManifest = retentionEvidenceBoolPtr(true)
	}

	if opts.RequireAuditExport {
		manifest.RequireAuditExport = retentionEvidenceBoolPtr(true)
	}

	if opts.RequireHoldReview {
		manifest.RequireHoldReview = retentionEvidenceBoolPtr(true)
	}

	return manifest, nil
}

func retentionEvidenceManifestPath(flagName, path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "-" {
		return "", fmt.Errorf("%s must be a retained file path", flagName)
	}

	return path, nil
}

func retentionEvidenceManifestPaths(flagName string, paths []string) ([]string, error) {
	result := make([]string, 0, len(paths))
	for _, path := range paths {
		clean, err := retentionEvidenceManifestPath(flagName, path)
		if err != nil {
			return nil, err
		}

		if clean != "" {
			result = append(result, clean)
		}
	}

	return result, nil
}

func retentionEvidenceManifestDuration(flagName string, duration time.Duration) (string, error) {
	if duration < 0 {
		return "", fmt.Errorf("%s must be >= 0", flagName)
	}

	if duration == 0 {
		return "", nil
	}

	return duration.String(), nil
}

func retentionEvidenceBoolPtr(value bool) *bool {
	return &value
}

func writeRetentionCleanupEvidenceManifest(w io.Writer, outputPath, promotePath string, manifest retentionCleanupEvidenceManifestFile, verification *retentionCleanupEvidenceManifestVerification) error {
	outputPath = strings.TrimSpace(outputPath)
	if outputPath == "" {
		outputPath = "-"
	}

	promotePath = strings.TrimSpace(promotePath)
	if promotePath == "-" {
		return fmt.Errorf("--promote must be a retained file path")
	}

	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode retention cleanup evidence manifest: %w", err)
	}

	payload = append(payload, '\n')
	if outputPath == "-" && promotePath == "" {
		_, err := w.Write(payload)
		return err
	}

	result := retentionCleanupEvidenceManifestWriteResult{
		Status:        "generated",
		Verified:      verification != nil && verification.Verified,
		SchemaVersion: manifest.SchemaVersion,
		GeneratedAt:   manifest.GeneratedAt,
		GeneratedBy:   manifest.GeneratedBy,
		ExternalRef:   manifest.ExternalRef,
		Verification:  verification,
	}

	if outputPath != "-" {
		if err := writeFileAtomic(outputPath, payload, 0o600); err != nil {
			return fmt.Errorf("write retention cleanup evidence manifest: %w", err)
		}

		result.Path = outputPath
	}

	if promotePath != "" {
		if outputPath == "-" || filepath.Clean(outputPath) != filepath.Clean(promotePath) {
			if err := writeFileAtomic(promotePath, payload, 0o600); err != nil {
				return fmt.Errorf("promote retention cleanup evidence manifest: %w", err)
			}
		}

		result.PromotedTo = promotePath
		if result.Path == "" {
			result.Path = promotePath
		}
	}

	return writeRetentionCleanupEvidenceManifestReceipt(w, result)
}

func writeRetentionCleanupEvidenceManifestReceipt(w io.Writer, result retentionCleanupEvidenceManifestWriteResult) error {
	if outputIsJSON() {
		return writeJSON(w, result)
	}

	fmt.Fprintf(w, "evidence_manifest_path=%s\n", result.Path)
	if result.PromotedTo != "" {
		fmt.Fprintf(w, "evidence_manifest_promoted_to=%s\n", result.PromotedTo)
	}

	fmt.Fprintf(w, "evidence_manifest_schema_version=%s\n", result.SchemaVersion)
	fmt.Fprintf(w, "evidence_manifest_generated_at=%s\n", result.GeneratedAt)
	if result.Verification != nil {
		fmt.Fprintf(w, "evidence_manifest_verified=%t\n", result.Verified)
		fmt.Fprintf(w, "evidence_manifest_checked_at=%s\n", result.Verification.CheckedAt)
		if result.Verification.Backup != nil {
			fmt.Fprintf(w, "evidence_manifest_backup_verified=%t\n", result.Verification.Backup.Verified)
		}
		if result.Verification.AuditExport != nil {
			fmt.Fprintf(w, "evidence_manifest_audit_export_verified=%t\n", result.Verification.AuditExport.Verified)
		}
		if result.Verification.HoldReview != nil {
			fmt.Fprintf(w, "evidence_manifest_hold_review_verified=%t\n", result.Verification.HoldReview.Verified)
		}
		if result.Verification.Waiver != nil {
			fmt.Fprintf(w, "evidence_manifest_waiver_verified=%t\n", result.Verification.Waiver.Verified)
		}
	}
	if result.GeneratedBy != "" {
		fmt.Fprintf(w, "evidence_manifest_generated_by=%s\n", result.GeneratedBy)
	}

	if result.ExternalRef != "" {
		fmt.Fprintf(w, "evidence_manifest_external_ref=%s\n", result.ExternalRef)
	}

	return nil
}

func writeFileAtomic(path string, payload []byte, perm fs.FileMode) error {
	path = strings.TrimSpace(path)
	if path == "" || path == "-" {
		return fmt.Errorf("path must be a file path")
	}

	dir := filepath.Dir(path)
	base := filepath.Base(path)
	temp, err := os.CreateTemp(dir, "."+base+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary file: %w", err)
	}

	tempPath := temp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()

	if _, err := temp.Write(payload); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write temporary file: %w", err)
	}

	if err := temp.Chmod(perm); err != nil {
		_ = temp.Close()
		return fmt.Errorf("chmod temporary file: %w", err)
	}

	if err := temp.Close(); err != nil {
		return fmt.Errorf("close temporary file: %w", err)
	}

	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("rename temporary file: %w", err)
	}

	removeTemp = false
	return nil
}

func retentionCleanup(ctx context.Context, w io.Writer, policy retention.Policy, dryRun, yes bool, logStorageDir, artifactStorageDir string, backupOptions retentionBackupCheckOptions, auditExportOptions retentionAuditExportCheckOptions, holdReviewOptions retentionHoldReviewCheckOptions, gateOptions retentionPolicyGateOptions, evidenceManifest *retentionCleanupEvidenceManifestEvidence) error {
	if !dryRun && !yes {
		return fmt.Errorf("retention cleanup deletes durable records; pass --dry-run to inspect or --yes to apply")
	}

	now := time.Now().UTC()
	waiverEvidence, err := checkRetentionWaiver(gateOptions.WaiverPath, now)
	if err != nil {
		return err
	}

	backupEvidence, err := checkRetentionBackupManifest(backupOptions, now)
	if err != nil {
		return err
	}

	if err := enforceRetentionBackupGate(gateOptions, backupEvidence, waiverEvidence); err != nil {
		return err
	}

	if strings.TrimSpace(auditExportOptions.ExportPath) == "" && auditExportOptions.MaxAge > 0 {
		return fmt.Errorf("--audit-export-max-age requires --audit-export")
	}

	if strings.TrimSpace(holdReviewOptions.ReviewPath) == "" && holdReviewOptions.MaxAge > 0 {
		return fmt.Errorf("--hold-review-max-age requires --hold-review")
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
	holdStore := retention.NewSQLHoldStore(db)

	var holdReviewEvidence *retentionHoldReviewEvidence
	if strings.TrimSpace(holdReviewOptions.ReviewPath) != "" {
		activeHolds, err := holdStore.List(ctx, retention.ListHoldOptions{}, now)
		if err != nil {
			return fmt.Errorf("list active retention holds: %w", err)
		}

		holdReviewEvidence, err = checkRetentionHoldReview(holdReviewOptions, activeHolds, now)
		if err != nil {
			return err
		}
	}

	if err := enforceRetentionHoldReviewGate(gateOptions, holdReviewEvidence, waiverEvidence); err != nil {
		return err
	}

	var auditExportEvidence *retentionAuditExportEvidence
	var precheckedReport *retention.Report
	needsAuditPreview := strings.TrimSpace(auditExportOptions.ExportPath) != "" || gateOptions.RequireAuditExport
	if needsAuditPreview {
		preview, err := cleaner.Preview(ctx, policy, now)
		if err != nil {
			return err
		}

		precheckedReport = &preview
	}

	if strings.TrimSpace(auditExportOptions.ExportPath) != "" {
		auditExportEvidence, err = checkRetentionAuditExport(auditExportOptions, precheckedReport.Cutoffs.AuditLog, precheckedReport.Counts.AuditLog, now)
		if err != nil {
			return err
		}
	} else if auditExportOptions.MaxAge > 0 {
		return fmt.Errorf("--audit-export-max-age requires --audit-export")
	}

	if err := enforceRetentionAuditExportGate(gateOptions, auditExportEvidence, waiverEvidence, precheckedReport); err != nil {
		return err
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
				"audit_log":           report.HeldCounts.AuditLog,
			},
			"audit": map[string]bool{"event_inserted": report.AuditEventInserted},
		}

		if backupEvidence != nil {
			payload["backup"] = backupEvidence
		}

		if auditExportEvidence != nil {
			payload["audit_export"] = auditExportEvidence
		}

		if holdReviewEvidence != nil {
			payload["hold_review"] = holdReviewEvidence
		}

		if evidenceManifest != nil {
			payload["evidence_manifest"] = evidenceManifest
		}

		if waiverEvidence != nil {
			payload["waiver"] = waiverEvidence
		}

		return writeJSON(w, payload)
	}

	printRetentionReport(w, report, fileReport, backupEvidence, auditExportEvidence, holdReviewEvidence, evidenceManifest, waiverEvidence)
	if dryRun {
		fmt.Fprintln(w, "Cleanup not applied.")
		return nil
	}

	fmt.Fprintln(w, "Cleanup applied.")
	return nil
}

func retentionCleanupFlagChangesFromCommand(cmd *cobra.Command) retentionCleanupFlagChanges {
	flags := cmd.Flags()
	return retentionCleanupFlagChanges{
		BackupManifest:        flags.Changed("backup-manifest"),
		BackupExpect:          flags.Changed("backup-expect"),
		BackupStorageReports:  flags.Changed("backup-storage-report"),
		BackupMaxAge:          flags.Changed("backup-max-age"),
		BackupStorageMaxAge:   flags.Changed("backup-storage-max-age"),
		AuditExport:           flags.Changed("audit-export"),
		AuditExportMaxAge:     flags.Changed("audit-export-max-age"),
		HoldReview:            flags.Changed("hold-review"),
		HoldReviewMaxAge:      flags.Changed("hold-review-max-age"),
		RequireBackupManifest: flags.Changed("require-backup-manifest"),
		RequireAuditExport:    flags.Changed("require-audit-export"),
		RequireHoldReview:     flags.Changed("require-hold-review"),
		Waiver:                flags.Changed("waiver"),
	}
}

func applyRetentionCleanupEvidenceManifest(path string, changes retentionCleanupFlagChanges, backupOptions *retentionBackupCheckOptions, auditExportOptions *retentionAuditExportCheckOptions, holdReviewOptions *retentionHoldReviewCheckOptions, gateOptions *retentionPolicyGateOptions, checkedAt time.Time) (*retentionCleanupEvidenceManifestEvidence, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}

	if path == "-" {
		return nil, fmt.Errorf("--evidence-manifest must be a retained file path")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read retention cleanup evidence manifest: %w", err)
	}

	var manifest retentionCleanupEvidenceManifestFile
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("parse retention cleanup evidence manifest: %w", err)
	}

	manifest.SchemaVersion = strings.TrimSpace(manifest.SchemaVersion)
	if manifest.SchemaVersion != retentionCleanupEvidenceManifestSchemaVersion {
		return nil, fmt.Errorf("retention cleanup evidence manifest schema_version = %q, want %q", manifest.SchemaVersion, retentionCleanupEvidenceManifestSchemaVersion)
	}

	baseDir := filepath.Dir(path)
	resolvePath := func(fieldName, value string) (string, error) {
		value = strings.TrimSpace(value)
		if value == "" {
			return "", nil
		}

		if value == "-" {
			return "", fmt.Errorf("retention cleanup evidence manifest %s must be a retained file path", fieldName)
		}

		if filepath.IsAbs(value) {
			return filepath.Clean(value), nil
		}

		return filepath.Clean(filepath.Join(baseDir, value)), nil
	}

	parseDuration := func(fieldName, value string) (time.Duration, string, error) {
		value = strings.TrimSpace(value)
		if value == "" {
			return 0, "", nil
		}

		duration, err := time.ParseDuration(value)
		if err != nil {
			return 0, "", fmt.Errorf("retention cleanup evidence manifest %s must be a duration: %w", fieldName, err)
		}

		if duration < 0 {
			return 0, "", fmt.Errorf("retention cleanup evidence manifest %s must be >= 0", fieldName)
		}

		return duration, duration.String(), nil
	}

	backupManifest, err := resolvePath("backup_manifest", manifest.BackupManifest)
	if err != nil {
		return nil, err
	}

	backupExpect, err := resolvePath("backup_expect", manifest.BackupExpect)
	if err != nil {
		return nil, err
	}

	var backupStorageReports []string
	for _, value := range manifest.BackupStorageReports {
		resolved, err := resolvePath("backup_storage_reports", value)
		if err != nil {
			return nil, err
		}

		if resolved != "" {
			backupStorageReports = append(backupStorageReports, resolved)
		}
	}

	backupMaxAge, backupMaxAgeText, err := parseDuration("backup_max_age", manifest.BackupMaxAge)
	if err != nil {
		return nil, err
	}

	backupStorageMaxAge, backupStorageMaxAgeText, err := parseDuration("backup_storage_max_age", manifest.BackupStorageMaxAge)
	if err != nil {
		return nil, err
	}

	auditExport, err := resolvePath("audit_export", manifest.AuditExport)
	if err != nil {
		return nil, err
	}

	auditExportMaxAge, auditExportMaxAgeText, err := parseDuration("audit_export_max_age", manifest.AuditExportMaxAge)
	if err != nil {
		return nil, err
	}

	holdReview, err := resolvePath("hold_review", manifest.HoldReview)
	if err != nil {
		return nil, err
	}

	holdReviewMaxAge, holdReviewMaxAgeText, err := parseDuration("hold_review_max_age", manifest.HoldReviewMaxAge)
	if err != nil {
		return nil, err
	}

	waiver, err := resolvePath("waiver", manifest.Waiver)
	if err != nil {
		return nil, err
	}

	if !changes.BackupManifest && backupManifest != "" {
		backupOptions.ManifestPath = backupManifest
	}

	if !changes.BackupExpect && backupExpect != "" {
		backupOptions.ExpectPath = backupExpect
	}

	if !changes.BackupStorageReports && len(backupStorageReports) > 0 {
		backupOptions.StorageReportPaths = backupStorageReports
	}

	if !changes.BackupMaxAge && backupMaxAgeText != "" {
		backupOptions.MaxAge = backupMaxAge
	}

	if !changes.BackupStorageMaxAge && backupStorageMaxAgeText != "" {
		backupOptions.StorageReportMaxAge = backupStorageMaxAge
	}

	if !changes.AuditExport && auditExport != "" {
		auditExportOptions.ExportPath = auditExport
	}

	if !changes.AuditExportMaxAge && auditExportMaxAgeText != "" {
		auditExportOptions.MaxAge = auditExportMaxAge
	}

	if !changes.HoldReview && holdReview != "" {
		holdReviewOptions.ReviewPath = holdReview
	}

	if !changes.HoldReviewMaxAge && holdReviewMaxAgeText != "" {
		holdReviewOptions.MaxAge = holdReviewMaxAge
	}

	if !changes.Waiver && waiver != "" {
		gateOptions.WaiverPath = waiver
	}

	if !changes.RequireBackupManifest && manifest.RequireBackupManifest != nil && *manifest.RequireBackupManifest {
		gateOptions.RequireBackupManifest = true
	}

	if !changes.RequireAuditExport && manifest.RequireAuditExport != nil && *manifest.RequireAuditExport {
		gateOptions.RequireAuditExport = true
	}

	if !changes.RequireHoldReview && manifest.RequireHoldReview != nil && *manifest.RequireHoldReview {
		gateOptions.RequireHoldReview = true
	}

	return &retentionCleanupEvidenceManifestEvidence{
		ManifestPath:          path,
		Verified:              true,
		CheckedAt:             checkedAt.UTC().Format(time.RFC3339),
		SchemaVersion:         manifest.SchemaVersion,
		GeneratedAt:           strings.TrimSpace(manifest.GeneratedAt),
		GeneratedBy:           strings.TrimSpace(manifest.GeneratedBy),
		ExternalRef:           strings.TrimSpace(manifest.ExternalRef),
		BackupManifest:        backupManifest,
		BackupExpect:          backupExpect,
		BackupStorageReports:  backupStorageReports,
		BackupMaxAge:          backupMaxAgeText,
		BackupStorageMaxAge:   backupStorageMaxAgeText,
		AuditExport:           auditExport,
		AuditExportMaxAge:     auditExportMaxAgeText,
		HoldReview:            holdReview,
		HoldReviewMaxAge:      holdReviewMaxAgeText,
		RequireBackupManifest: manifest.RequireBackupManifest != nil && *manifest.RequireBackupManifest,
		RequireAuditExport:    manifest.RequireAuditExport != nil && *manifest.RequireAuditExport,
		RequireHoldReview:     manifest.RequireHoldReview != nil && *manifest.RequireHoldReview,
		Waiver:                waiver,
	}, nil
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

func buildRetentionHoldReviewFile(holds []retention.Hold, generatedAt time.Time, reviewedBy, reason, externalRef string) (retentionHoldReviewFile, error) {
	reviewedBy = strings.TrimSpace(reviewedBy)
	reason = strings.TrimSpace(reason)
	externalRef = strings.TrimSpace(externalRef)
	if reviewedBy == "" {
		return retentionHoldReviewFile{}, fmt.Errorf("--reviewed-by is required")
	}

	if reason == "" {
		return retentionHoldReviewFile{}, fmt.Errorf("--reason is required")
	}

	records := retentionHoldReviewRecords(holds)
	holdsSHA256, err := retentionHoldReviewRecordsSHA256(records)
	if err != nil {
		return retentionHoldReviewFile{}, err
	}

	return retentionHoldReviewFile{
		SchemaVersion: retentionHoldReviewSchemaVersion,
		GeneratedAt:   generatedAt.UTC().Format(time.RFC3339),
		ReviewedBy:    reviewedBy,
		Reason:        reason,
		ExternalRef:   externalRef,
		ActiveHolds:   len(records),
		HoldsSHA256:   holdsSHA256,
		Holds:         records,
	}, nil
}

func checkRetentionHoldReview(options retentionHoldReviewCheckOptions, currentHolds []retention.Hold, checkedAt time.Time) (*retentionHoldReviewEvidence, error) {
	options.ReviewPath = strings.TrimSpace(options.ReviewPath)
	if options.ReviewPath == "" {
		if options.MaxAge > 0 {
			return nil, fmt.Errorf("--hold-review-max-age requires --hold-review")
		}

		return nil, nil
	}

	if options.ReviewPath == "-" {
		return nil, fmt.Errorf("--hold-review must be a retained file path")
	}

	raw, err := os.ReadFile(options.ReviewPath)
	if err != nil {
		return nil, fmt.Errorf("read retention hold review: %w", err)
	}

	var review retentionHoldReviewFile
	if err := json.Unmarshal(raw, &review); err != nil {
		return nil, fmt.Errorf("parse retention hold review: %w", err)
	}

	evidence := &retentionHoldReviewEvidence{
		ReviewPath: options.ReviewPath,
		CheckedAt:  checkedAt.UTC().Format(time.RFC3339),
		retentionHoldReviewFile: retentionHoldReviewFile{
			SchemaVersion: strings.TrimSpace(review.SchemaVersion),
			GeneratedAt:   strings.TrimSpace(review.GeneratedAt),
			ReviewedBy:    strings.TrimSpace(review.ReviewedBy),
			Reason:        strings.TrimSpace(review.Reason),
			ExternalRef:   strings.TrimSpace(review.ExternalRef),
			ActiveHolds:   review.ActiveHolds,
			HoldsSHA256:   strings.TrimSpace(review.HoldsSHA256),
			Holds:         normalizeRetentionHoldReviewRecords(review.Holds),
		},
	}

	if options.MaxAge > 0 {
		evidence.MaxAge = options.MaxAge.String()
	}

	if evidence.SchemaVersion != retentionHoldReviewSchemaVersion {
		return evidence, fmt.Errorf("retention hold review schema_version = %q, want %q", evidence.SchemaVersion, retentionHoldReviewSchemaVersion)
	}

	if evidence.GeneratedAt == "" {
		return evidence, fmt.Errorf("retention hold review generated_at is required")
	}

	generatedAt, err := time.Parse(time.RFC3339, evidence.GeneratedAt)
	if err != nil {
		return evidence, fmt.Errorf("retention hold review generated_at is not RFC3339: %w", err)
	}

	if evidence.ReviewedBy == "" {
		return evidence, fmt.Errorf("retention hold review reviewed_by is required")
	}

	if evidence.Reason == "" {
		return evidence, fmt.Errorf("retention hold review reason is required")
	}

	if evidence.ActiveHolds != len(evidence.Holds) {
		return evidence, fmt.Errorf("retention hold review active_holds=%d does not match holds length %d", evidence.ActiveHolds, len(evidence.Holds))
	}

	holdsSHA256, err := retentionHoldReviewRecordsSHA256(evidence.Holds)
	if err != nil {
		return evidence, err
	}
	if evidence.HoldsSHA256 != holdsSHA256 {
		return evidence, fmt.Errorf("retention hold review holds_sha256 mismatch")
	}

	currentRecords := retentionHoldReviewRecords(currentHolds)
	currentSHA256, err := retentionHoldReviewRecordsSHA256(currentRecords)
	if err != nil {
		return evidence, err
	}

	if evidence.ActiveHolds != len(currentRecords) || evidence.HoldsSHA256 != currentSHA256 {
		return evidence, fmt.Errorf("retention hold review does not match current active holds")
	}

	age := checkedAt.Sub(generatedAt)
	evidence.Age = age.String()
	if age < 0 {
		return evidence, fmt.Errorf("retention hold review generated_at %s is after check time %s", evidence.GeneratedAt, checkedAt.Format(time.RFC3339))
	}

	if options.MaxAge > 0 && age > options.MaxAge {
		return evidence, fmt.Errorf("retention hold review is stale: generated_at=%s age=%s max_age=%s", evidence.GeneratedAt, age, options.MaxAge)
	}

	evidence.Verified = true
	return evidence, nil
}

func retentionHoldReviewRecords(holds []retention.Hold) []retentionHoldReviewRecord {
	records := make([]retentionHoldReviewRecord, 0, len(holds))
	for _, hold := range holds {
		record := retentionHoldReviewRecord{
			HoldID:      hold.HoldID,
			Scope:       hold.Scope,
			TargetID:    hold.TargetID,
			Status:      hold.Status,
			Owner:       hold.Owner,
			Reason:      hold.Reason,
			ExternalRef: hold.ExternalRef,
			CreatedBy:   hold.CreatedBy,
			CreatedAt:   hold.CreatedAt.UTC().Format(time.RFC3339),
		}

		if hold.ExpiresAt != nil {
			record.ExpiresAt = hold.ExpiresAt.UTC().Format(time.RFC3339)
		}

		records = append(records, record)
	}

	return normalizeRetentionHoldReviewRecords(records)
}

func normalizeRetentionHoldReviewRecords(records []retentionHoldReviewRecord) []retentionHoldReviewRecord {
	out := append([]retentionHoldReviewRecord(nil), records...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Scope != out[j].Scope {
			return out[i].Scope < out[j].Scope
		}
		if out[i].TargetID != out[j].TargetID {
			return out[i].TargetID < out[j].TargetID
		}
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}
		return out[i].HoldID < out[j].HoldID
	})

	return out
}

func retentionHoldReviewRecordsSHA256(records []retentionHoldReviewRecord) (string, error) {
	payload, err := json.Marshal(normalizeRetentionHoldReviewRecords(records))
	if err != nil {
		return "", fmt.Errorf("encode retention hold review inventory: %w", err)
	}

	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}

func checkRetentionWaiver(path string, checkedAt time.Time) (*retentionWaiverEvidence, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}

	if path == "-" {
		return nil, fmt.Errorf("--waiver must be a retained file path")
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read retention waiver: %w", err)
	}

	var waiver retentionWaiverFile
	if err := json.Unmarshal(raw, &waiver); err != nil {
		return nil, fmt.Errorf("parse retention waiver: %w", err)
	}

	evidence := &retentionWaiverEvidence{
		WaiverPath:    path,
		CheckedAt:     checkedAt.UTC().Format(time.RFC3339),
		SchemaVersion: waiver.SchemaVersion,
		Waives:        normalizeRetentionWaiverNames(waiver.Waives),
		Reason:        strings.TrimSpace(waiver.Reason),
		ApprovedBy:    strings.TrimSpace(waiver.ApprovedBy),
		ExternalRef:   strings.TrimSpace(waiver.ExternalRef),
		ExpiresAt:     strings.TrimSpace(waiver.ExpiresAt),
	}

	if evidence.SchemaVersion != retentionWaiverSchemaVersion {
		return evidence, fmt.Errorf("retention waiver schema_version = %q, want %q", evidence.SchemaVersion, retentionWaiverSchemaVersion)
	}

	if len(evidence.Waives) == 0 {
		return evidence, fmt.Errorf("retention waiver must list at least one waived gate")
	}

	seen := map[string]bool{}
	for _, name := range evidence.Waives {
		switch name {
		case retentionWaiverBackup, retentionWaiverAuditExport, retentionWaiverHoldReview:
		default:
			return evidence, fmt.Errorf("retention waiver references unknown gate %q", name)
		}

		if seen[name] {
			return evidence, fmt.Errorf("retention waiver references duplicate gate %q", name)
		}

		seen[name] = true
	}

	if evidence.Reason == "" {
		return evidence, fmt.Errorf("retention waiver reason is required")
	}

	if evidence.ApprovedBy == "" {
		return evidence, fmt.Errorf("retention waiver approved_by is required")
	}

	if evidence.ExpiresAt == "" {
		return evidence, fmt.Errorf("retention waiver expires_at is required")
	}

	expiresAt, err := time.Parse(time.RFC3339, evidence.ExpiresAt)
	if err != nil {
		return evidence, fmt.Errorf("retention waiver expires_at must be RFC3339: %w", err)
	}

	if !expiresAt.After(checkedAt.UTC()) {
		return evidence, fmt.Errorf("retention waiver expired at %s", evidence.ExpiresAt)
	}

	evidence.Verified = true
	return evidence, nil
}

func normalizeRetentionWaiverNames(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			out = append(out, value)
		}
	}

	return out
}

func enforceRetentionBackupGate(options retentionPolicyGateOptions, backupEvidence *retentionBackupEvidence, waiverEvidence *retentionWaiverEvidence) error {
	if !options.RequireBackupManifest {
		return nil
	}

	if backupEvidence != nil && backupEvidence.Verified {
		return nil
	}

	if retentionWaiverAllows(waiverEvidence, retentionWaiverBackup) {
		return nil
	}

	return fmt.Errorf("--require-backup-manifest requires --backup-manifest or a retention waiver for %s", retentionWaiverBackup)
}

func enforceRetentionAuditExportGate(options retentionPolicyGateOptions, auditExportEvidence *retentionAuditExportEvidence, waiverEvidence *retentionWaiverEvidence, report *retention.Report) error {
	if !options.RequireAuditExport {
		return nil
	}

	if report == nil || report.Cutoffs.AuditLog == nil || report.Counts.AuditLog == 0 {
		return nil
	}

	if auditExportEvidence != nil && auditExportEvidence.Verified {
		return nil
	}

	if retentionWaiverAllows(waiverEvidence, retentionWaiverAuditExport) {
		return nil
	}

	return fmt.Errorf("--require-audit-export requires --audit-export or a retention waiver for %s before deleting %d audit row(s)", retentionWaiverAuditExport, report.Counts.AuditLog)
}

func enforceRetentionHoldReviewGate(options retentionPolicyGateOptions, holdReviewEvidence *retentionHoldReviewEvidence, waiverEvidence *retentionWaiverEvidence) error {
	if !options.RequireHoldReview {
		return nil
	}

	if holdReviewEvidence != nil && holdReviewEvidence.Verified {
		return nil
	}

	if retentionWaiverAllows(waiverEvidence, retentionWaiverHoldReview) {
		return nil
	}

	return fmt.Errorf("--require-hold-review requires --hold-review or a retention waiver for %s", retentionWaiverHoldReview)
}

func retentionWaiverAllows(evidence *retentionWaiverEvidence, gate string) bool {
	if evidence == nil || !evidence.Verified {
		return false
	}

	for _, waived := range evidence.Waives {
		if waived == gate {
			return true
		}
	}

	return false
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

func printRetentionReport(w io.Writer, report retention.Report, fileReport retention.FileReport, backupEvidence *retentionBackupEvidence, auditExportEvidence *retentionAuditExportEvidence, holdReviewEvidence *retentionHoldReviewEvidence, evidenceManifest *retentionCleanupEvidenceManifestEvidence, waiverEvidence *retentionWaiverEvidence) {
	prefix := "deleted"
	if report.DryRun {
		prefix = "would_delete"
	}

	fmt.Fprintf(w, "dry_run=%t\n", report.DryRun)
	if evidenceManifest != nil {
		fmt.Fprintf(w, "evidence_manifest_verified=%t\n", evidenceManifest.Verified)
		fmt.Fprintf(w, "evidence_manifest_path=%s\n", evidenceManifest.ManifestPath)
		fmt.Fprintf(w, "evidence_manifest_checked_at=%s\n", evidenceManifest.CheckedAt)
		if evidenceManifest.GeneratedAt != "" {
			fmt.Fprintf(w, "evidence_manifest_generated_at=%s\n", evidenceManifest.GeneratedAt)
		}

		if evidenceManifest.GeneratedBy != "" {
			fmt.Fprintf(w, "evidence_manifest_generated_by=%s\n", evidenceManifest.GeneratedBy)
		}

		if evidenceManifest.ExternalRef != "" {
			fmt.Fprintf(w, "evidence_manifest_external_ref=%s\n", evidenceManifest.ExternalRef)
		}

		if evidenceManifest.BackupManifest != "" {
			fmt.Fprintf(w, "evidence_manifest_backup_manifest=%s\n", evidenceManifest.BackupManifest)
		}

		if evidenceManifest.BackupExpect != "" {
			fmt.Fprintf(w, "evidence_manifest_backup_expect=%s\n", evidenceManifest.BackupExpect)
		}

		if len(evidenceManifest.BackupStorageReports) > 0 {
			fmt.Fprintf(w, "evidence_manifest_backup_storage_reports=%d\n", len(evidenceManifest.BackupStorageReports))
		}

		if evidenceManifest.BackupMaxAge != "" {
			fmt.Fprintf(w, "evidence_manifest_backup_max_age=%s\n", evidenceManifest.BackupMaxAge)
		}

		if evidenceManifest.BackupStorageMaxAge != "" {
			fmt.Fprintf(w, "evidence_manifest_backup_storage_max_age=%s\n", evidenceManifest.BackupStorageMaxAge)
		}

		if evidenceManifest.AuditExport != "" {
			fmt.Fprintf(w, "evidence_manifest_audit_export=%s\n", evidenceManifest.AuditExport)
		}

		if evidenceManifest.AuditExportMaxAge != "" {
			fmt.Fprintf(w, "evidence_manifest_audit_export_max_age=%s\n", evidenceManifest.AuditExportMaxAge)
		}

		if evidenceManifest.HoldReview != "" {
			fmt.Fprintf(w, "evidence_manifest_hold_review=%s\n", evidenceManifest.HoldReview)
		}

		if evidenceManifest.HoldReviewMaxAge != "" {
			fmt.Fprintf(w, "evidence_manifest_hold_review_max_age=%s\n", evidenceManifest.HoldReviewMaxAge)
		}

		if evidenceManifest.Waiver != "" {
			fmt.Fprintf(w, "evidence_manifest_waiver=%s\n", evidenceManifest.Waiver)
		}

		fmt.Fprintf(w, "evidence_manifest_require_backup_manifest=%t\n", evidenceManifest.RequireBackupManifest)
		fmt.Fprintf(w, "evidence_manifest_require_audit_export=%t\n", evidenceManifest.RequireAuditExport)
		fmt.Fprintf(w, "evidence_manifest_require_hold_review=%t\n", evidenceManifest.RequireHoldReview)
	}

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
	if holdReviewEvidence != nil {
		fmt.Fprintf(w, "hold_review_verified=%t\n", holdReviewEvidence.Verified)
		fmt.Fprintf(w, "hold_review_path=%s\n", holdReviewEvidence.ReviewPath)
		fmt.Fprintf(w, "hold_review_checked_at=%s\n", holdReviewEvidence.CheckedAt)
		fmt.Fprintf(w, "hold_review_generated_at=%s\n", holdReviewEvidence.GeneratedAt)
		fmt.Fprintf(w, "hold_review_reviewed_by=%s\n", holdReviewEvidence.ReviewedBy)
		fmt.Fprintf(w, "hold_review_reason=%s\n", holdReviewEvidence.Reason)
		if holdReviewEvidence.ExternalRef != "" {
			fmt.Fprintf(w, "hold_review_external_ref=%s\n", holdReviewEvidence.ExternalRef)
		}

		fmt.Fprintf(w, "hold_review_active_holds=%d\n", holdReviewEvidence.ActiveHolds)
		fmt.Fprintf(w, "hold_review_holds_sha256=%s\n", holdReviewEvidence.HoldsSHA256)
		if holdReviewEvidence.MaxAge != "" {
			fmt.Fprintf(w, "hold_review_max_age=%s\n", holdReviewEvidence.MaxAge)
			fmt.Fprintf(w, "hold_review_age=%s\n", holdReviewEvidence.Age)
		}
	}
	if waiverEvidence != nil {
		fmt.Fprintf(w, "retention_waiver_verified=%t\n", waiverEvidence.Verified)
		fmt.Fprintf(w, "retention_waiver_path=%s\n", waiverEvidence.WaiverPath)
		fmt.Fprintf(w, "retention_waiver_checked_at=%s\n", waiverEvidence.CheckedAt)
		fmt.Fprintf(w, "retention_waiver_waives=%s\n", strings.Join(waiverEvidence.Waives, ","))
		fmt.Fprintf(w, "retention_waiver_reason=%s\n", waiverEvidence.Reason)
		fmt.Fprintf(w, "retention_waiver_approved_by=%s\n", waiverEvidence.ApprovedBy)
		if waiverEvidence.ExternalRef != "" {
			fmt.Fprintf(w, "retention_waiver_external_ref=%s\n", waiverEvidence.ExternalRef)
		}
		fmt.Fprintf(w, "retention_waiver_expires_at=%s\n", waiverEvidence.ExpiresAt)
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
	fmt.Fprintf(w, "held.audit_log=%d\n", report.HeldCounts.AuditLog)
	fmt.Fprintf(w, "audit_event_inserted=%t\n", report.AuditEventInserted)
}

func runRetentionHoldCreate(cmd *cobra.Command, args []string) {
	expiresAt, err := parseRetentionHoldExpiresAt(retentionHoldExpiresAt)
	if err != nil {
		runCLIError(err)
	}

	scope, targetID, err := retentionHoldCreateScopeAndTarget(retentionHoldRunID, retentionHoldAuditSince, retentionHoldAuditUntil)
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
		Scope:       scope,
		TargetID:    targetID,
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
	scope, targetID, err := retentionHoldListScopeAndTarget(retentionHoldListScope, retentionHoldListRunID)
	if err != nil {
		runCLIError(err)
	}

	db, err := openRetentionDatabase()
	if err != nil {
		runCLIError(err)
	}
	defer db.Close()

	holds, err := retention.NewSQLHoldStore(db).List(cmd.Context(), retention.ListHoldOptions{
		Scope:         scope,
		TargetID:      targetID,
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

func runRetentionHoldReview(cmd *cobra.Command, args []string) {
	db, err := openRetentionDatabase()
	if err != nil {
		runCLIError(err)
	}
	defer db.Close()

	reviewedBy := strings.TrimSpace(retentionHoldReviewReviewedBy)
	if reviewedBy == "" {
		reviewedBy = defaultRetentionHoldActor()
	}

	now := time.Now().UTC()
	holds, err := retention.NewSQLHoldStore(db).List(cmd.Context(), retention.ListHoldOptions{}, now)
	if err != nil {
		runCLIError(err)
	}

	review, err := buildRetentionHoldReviewFile(holds, now, reviewedBy, retentionHoldReviewReason, retentionHoldReviewExternalRef)
	if err != nil {
		runCLIError(err)
	}

	runCLIError(writeRetentionHoldReview(os.Stdout, retentionHoldReviewOutput, review))
}

func retentionHoldCreateScopeAndTarget(runID, auditSince, auditUntil string) (scope, targetID string, err error) {
	runID = strings.TrimSpace(runID)
	auditSince = strings.TrimSpace(auditSince)
	auditUntil = strings.TrimSpace(auditUntil)

	if runID != "" {
		if auditSince != "" || auditUntil != "" {
			return "", "", fmt.Errorf("retention hold target must be either --run or --audit-since/--audit-until, not both")
		}

		return retention.HoldScopeRun, runID, nil
	}

	if auditSince == "" && auditUntil == "" {
		return "", "", fmt.Errorf("retention holds create requires --run or --audit-since/--audit-until")
	}

	if auditSince == "" || auditUntil == "" {
		return "", "", fmt.Errorf("audit range holds require both --audit-since and --audit-until")
	}

	since, err := parseRetentionHoldAuditTime("--audit-since", auditSince)
	if err != nil {
		return "", "", err
	}

	until, err := parseRetentionHoldAuditTime("--audit-until", auditUntil)
	if err != nil {
		return "", "", err
	}

	targetID, err = retention.AuditRangeHoldTarget(since, until)
	if err != nil {
		return "", "", err
	}

	return retention.HoldScopeAuditRange, targetID, nil
}

func retentionHoldListScopeAndTarget(scope, runID string) (string, string, error) {
	scope = strings.TrimSpace(scope)
	runID = strings.TrimSpace(runID)

	if runID != "" {
		if scope != "" && scope != retention.HoldScopeRun {
			return "", "", fmt.Errorf("--run can only be used with --scope %s", retention.HoldScopeRun)
		}

		return retention.HoldScopeRun, runID, nil
	}

	switch scope {
	case "", retention.HoldScopeRun, retention.HoldScopeAuditRange:
		return scope, "", nil
	default:
		return "", "", fmt.Errorf("--scope must be %s or %s", retention.HoldScopeRun, retention.HoldScopeAuditRange)
	}
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

func writeRetentionHoldReview(w io.Writer, outputPath string, review retentionHoldReviewFile) error {
	outputPath = strings.TrimSpace(outputPath)
	if outputPath == "" || outputPath == "-" {
		return writeJSON(w, review)
	}

	payload, err := json.MarshalIndent(review, "", "  ")
	if err != nil {
		return fmt.Errorf("encode retention hold review: %w", err)
	}

	payload = append(payload, '\n')
	if err := os.WriteFile(outputPath, payload, 0o600); err != nil {
		return fmt.Errorf("write retention hold review: %w", err)
	}

	if outputIsJSON() {
		return writeJSON(w, map[string]any{
			"status":       "reviewed",
			"path":         outputPath,
			"active_holds": review.ActiveHolds,
			"holds_sha256": review.HoldsSHA256,
			"generated_at": review.GeneratedAt,
			"reviewed_by":  review.ReviewedBy,
			"external_ref": review.ExternalRef,
		})
	}

	fmt.Fprintf(w, "hold_review_path=%s\n", outputPath)
	fmt.Fprintf(w, "hold_review_generated_at=%s\n", review.GeneratedAt)
	fmt.Fprintf(w, "hold_review_reviewed_by=%s\n", review.ReviewedBy)
	fmt.Fprintf(w, "hold_review_active_holds=%d\n", review.ActiveHolds)
	fmt.Fprintf(w, "hold_review_holds_sha256=%s\n", review.HoldsSHA256)
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

func parseRetentionHoldAuditTime(flagName, value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, fmt.Errorf("%s is required", flagName)
	}

	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return t.UTC(), nil
	}

	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t.UTC(), nil
	}

	return time.Time{}, fmt.Errorf("%s must be RFC3339 or YYYY-MM-DD", flagName)
}

func defaultRetentionHoldActor() string {
	return defaultRetentionOperator()
}

func defaultRetentionOperator() string {
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
storage verifier evidence for the manifest's file-backed state. Use
--hold-review to require retained active-hold review evidence.`,
	Args: cobra.NoArgs,
	Run:  runRetentionCleanup,
}

var retentionEvidenceCmd = &cobra.Command{
	Use:   "evidence",
	Short: "Generate retained retention evidence",
	Long: `Generate retained evidence files used by retention cleanup policy gates.
These files let scheduled cleanup jobs consume backup, audit, hold-review, and
waiver paths without bespoke wrapper scripts.`,
	Args: cobra.NoArgs,
	Run:  showCommandHelp,
}

var retentionEvidenceManifestCmd = &cobra.Command{
	Use:   "manifest",
	Short: "Generate a retention cleanup evidence manifest",
	Long: `Generate a retained cleanup evidence manifest for retention cleanup.
The manifest names backup, audit export, active hold review, waiver, freshness,
and required-gate evidence. Use --promote to atomically update the stable path
that recurring cleanup jobs consume with --evidence-manifest.`,
	Args: cobra.NoArgs,
	Run:  runRetentionEvidenceManifest,
}

var retentionHoldsCmd = &cobra.Command{
	Use:   "holds",
	Short: "Manage retention compliance holds",
	Long: `Manage compliance holds that prevent retention cleanup from deleting
protected run evidence or audit_log ranges while the hold is active.`,
	Args: cobra.NoArgs,
	Run:  showCommandHelp,
}

var retentionHoldCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a retention hold",
	Long: `Create a retention hold. Use --run to protect a terminal run and related
durable state, or --audit-since and --audit-until to protect an audit_log time
range until the hold is released or expired.`,
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

var retentionHoldReviewCmd = &cobra.Command{
	Use:   "review",
	Short: "Generate active hold review evidence",
	Long: `Generate a retained evidence envelope for the current active retention
hold inventory. Cleanup can verify this file with --hold-review and require it
with --require-hold-review.`,
	Args: cobra.NoArgs,
	Run:  runRetentionHoldReview,
}
