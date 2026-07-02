package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"vectis/internal/retention"
	"vectis/internal/storageverify"
)

func TestRetentionCleanupRequiresBackupManifestForBackupExpect(t *testing.T) {
	err := retentionCleanup(context.Background(), io.Discard, retention.Policy{}, true, false, "", "", retentionBackupCheckOptions{ExpectPath: "expected-topology.json"}, retentionAuditExportCheckOptions{}, retentionHoldReviewCheckOptions{}, retentionPolicyGateOptions{})
	if err == nil {
		t.Fatalf("retention cleanup succeeded unexpectedly")
	}
	if !strings.Contains(err.Error(), "--backup-manifest") {
		t.Fatalf("retention cleanup error = %v, want --backup-manifest", err)
	}
}

func TestRetentionCleanupRequiresBackupManifestForBackupStorageReport(t *testing.T) {
	err := retentionCleanup(context.Background(), io.Discard, retention.Policy{}, true, false, "", "", retentionBackupCheckOptions{StorageReportPaths: []string{"queue.report.json"}}, retentionAuditExportCheckOptions{}, retentionHoldReviewCheckOptions{}, retentionPolicyGateOptions{})
	if err == nil {
		t.Fatalf("retention cleanup succeeded unexpectedly")
	}
	if !strings.Contains(err.Error(), "--backup-manifest") {
		t.Fatalf("retention cleanup error = %v, want --backup-manifest", err)
	}
}

func TestRetentionCleanupRequiresBackupManifestWhenPolicyRequires(t *testing.T) {
	err := retentionCleanup(context.Background(), io.Discard, retention.Policy{}, true, false, "", "", retentionBackupCheckOptions{}, retentionAuditExportCheckOptions{}, retentionHoldReviewCheckOptions{}, retentionPolicyGateOptions{RequireBackupManifest: true})
	if err == nil {
		t.Fatalf("retention cleanup succeeded unexpectedly")
	}
	if !strings.Contains(err.Error(), "--require-backup-manifest") {
		t.Fatalf("retention cleanup error = %v, want --require-backup-manifest", err)
	}
}

func TestRetentionCleanupRequiresAuditExportForAuditExportMaxAge(t *testing.T) {
	err := retentionCleanup(context.Background(), io.Discard, retention.Policy{}, true, false, "", "", retentionBackupCheckOptions{}, retentionAuditExportCheckOptions{MaxAge: time.Hour}, retentionHoldReviewCheckOptions{}, retentionPolicyGateOptions{})
	if err == nil {
		t.Fatalf("retention cleanup succeeded unexpectedly")
	}
	if !strings.Contains(err.Error(), "--audit-export") {
		t.Fatalf("retention cleanup error = %v, want --audit-export", err)
	}
}

func TestCheckRetentionWaiverAcceptsFreshKnownGates(t *testing.T) {
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	root := t.TempDir()
	waiverPath := writeBackupJSONFile(t, root, "retention-waiver.json", retentionWaiverFile{
		SchemaVersion: retentionWaiverSchemaVersion,
		Waives:        []string{" Backup_Manifest ", "audit_export", "hold_review"},
		Reason:        "emergency storage pressure while export service is down",
		ApprovedBy:    "security-oncall",
		ExternalRef:   "INC-1234",
		ExpiresAt:     now.Add(time.Hour).Format(time.RFC3339),
	})

	evidence, err := checkRetentionWaiver(waiverPath, now)
	if err != nil {
		t.Fatalf("check retention waiver: %v", err)
	}
	if evidence == nil || !evidence.Verified {
		t.Fatalf("waiver evidence = %+v, want verified", evidence)
	}
	if evidence.WaiverPath != waiverPath || evidence.ExternalRef != "INC-1234" {
		t.Fatalf("waiver evidence = %+v", evidence)
	}
	if len(evidence.Waives) != 3 || evidence.Waives[0] != retentionWaiverBackup || evidence.Waives[1] != retentionWaiverAuditExport || evidence.Waives[2] != retentionWaiverHoldReview {
		t.Fatalf("waived gates = %#v", evidence.Waives)
	}
}

func TestRetentionCleanupRequiresHoldReviewForHoldReviewMaxAge(t *testing.T) {
	err := retentionCleanup(context.Background(), io.Discard, retention.Policy{}, true, false, "", "", retentionBackupCheckOptions{}, retentionAuditExportCheckOptions{}, retentionHoldReviewCheckOptions{MaxAge: time.Hour}, retentionPolicyGateOptions{})
	if err == nil {
		t.Fatalf("retention cleanup succeeded unexpectedly")
	}
	if !strings.Contains(err.Error(), "--hold-review") {
		t.Fatalf("retention cleanup error = %v, want --hold-review", err)
	}
}

func TestCheckRetentionWaiverRejectsInvalidEvidence(t *testing.T) {
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name   string
		waiver retentionWaiverFile
		want   string
	}{
		{
			name: "unknown gate",
			waiver: retentionWaiverFile{
				SchemaVersion: retentionWaiverSchemaVersion,
				Waives:        []string{"database"},
				Reason:        "maintenance exception",
				ApprovedBy:    "security",
				ExpiresAt:     now.Add(time.Hour).Format(time.RFC3339),
			},
			want: "unknown gate",
		},
		{
			name: "expired",
			waiver: retentionWaiverFile{
				SchemaVersion: retentionWaiverSchemaVersion,
				Waives:        []string{retentionWaiverBackup},
				Reason:        "maintenance exception",
				ApprovedBy:    "security",
				ExpiresAt:     now.Add(-time.Minute).Format(time.RFC3339),
			},
			want: "expired",
		},
		{
			name: "missing reason",
			waiver: retentionWaiverFile{
				SchemaVersion: retentionWaiverSchemaVersion,
				Waives:        []string{retentionWaiverBackup},
				ApprovedBy:    "security",
				ExpiresAt:     now.Add(time.Hour).Format(time.RFC3339),
			},
			want: "reason",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			waiverPath := writeBackupJSONFile(t, root, "retention-waiver.json", tt.waiver)

			evidence, err := checkRetentionWaiver(waiverPath, now)
			if err == nil {
				t.Fatalf("check retention waiver succeeded unexpectedly with evidence %+v", evidence)
			}
			if evidence == nil || evidence.Verified {
				t.Fatalf("waiver evidence = %+v, want unverified evidence", evidence)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("waiver error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestEnforceRetentionAuditExportGateRequiresEvidenceWhenRowsEligible(t *testing.T) {
	cutoff := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	report := &retention.Report{
		Cutoffs: retention.Cutoffs{AuditLog: &cutoff},
		Counts:  retention.Counts{AuditLog: 2},
	}

	err := enforceRetentionAuditExportGate(retentionPolicyGateOptions{RequireAuditExport: true}, nil, nil, report)
	if err == nil {
		t.Fatalf("audit export gate succeeded unexpectedly")
	}
	if !strings.Contains(err.Error(), "--require-audit-export") {
		t.Fatalf("audit export gate error = %v, want --require-audit-export", err)
	}
}

func TestEnforceRetentionBackupGateAllowsWaiver(t *testing.T) {
	err := enforceRetentionBackupGate(retentionPolicyGateOptions{RequireBackupManifest: true}, nil, &retentionWaiverEvidence{
		Verified: true,
		Waives:   []string{retentionWaiverBackup},
	})
	if err != nil {
		t.Fatalf("backup gate with waiver: %v", err)
	}
}

func TestEnforceRetentionAuditExportGateSkipsWhenNoRowsEligible(t *testing.T) {
	cutoff := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	report := &retention.Report{
		Cutoffs: retention.Cutoffs{AuditLog: &cutoff},
		Counts:  retention.Counts{AuditLog: 0},
	}

	err := enforceRetentionAuditExportGate(retentionPolicyGateOptions{RequireAuditExport: true}, nil, nil, report)
	if err != nil {
		t.Fatalf("audit export gate with no eligible rows: %v", err)
	}
}

func TestEnforceRetentionAuditExportGateAllowsWaiver(t *testing.T) {
	cutoff := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	report := &retention.Report{
		Cutoffs: retention.Cutoffs{AuditLog: &cutoff},
		Counts:  retention.Counts{AuditLog: 2},
	}

	err := enforceRetentionAuditExportGate(retentionPolicyGateOptions{RequireAuditExport: true}, nil, &retentionWaiverEvidence{
		Verified: true,
		Waives:   []string{retentionWaiverAuditExport},
	}, report)
	if err != nil {
		t.Fatalf("audit export gate with waiver: %v", err)
	}
}

func TestEnforceRetentionHoldReviewGateRequiresEvidence(t *testing.T) {
	err := enforceRetentionHoldReviewGate(retentionPolicyGateOptions{RequireHoldReview: true}, nil, nil)
	if err == nil {
		t.Fatalf("hold review gate succeeded unexpectedly")
	}
	if !strings.Contains(err.Error(), "--require-hold-review") {
		t.Fatalf("hold review gate error = %v, want --require-hold-review", err)
	}
}

func TestEnforceRetentionHoldReviewGateAllowsWaiver(t *testing.T) {
	err := enforceRetentionHoldReviewGate(retentionPolicyGateOptions{RequireHoldReview: true}, nil, &retentionWaiverEvidence{
		Verified: true,
		Waives:   []string{retentionWaiverHoldReview},
	})
	if err != nil {
		t.Fatalf("hold review gate with waiver: %v", err)
	}
}

func TestBuildRetentionHoldReviewFileHashesActiveHolds(t *testing.T) {
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	holds := []retention.Hold{
		retentionHoldForTest("hold-run", retention.HoldScopeRun, "run-123", now.Add(-2*time.Hour)),
		retentionHoldForTest("hold-audit", retention.HoldScopeAuditRange, "2026-06-01T00:00:00Z..2026-07-01T00:00:00Z", now.Add(-time.Hour)),
	}

	review, err := buildRetentionHoldReviewFile(holds, now, " compliance-oncall ", " weekly review ", " GRC-123 ")
	if err != nil {
		t.Fatalf("build hold review: %v", err)
	}
	if review.SchemaVersion != retentionHoldReviewSchemaVersion {
		t.Fatalf("schema version = %q", review.SchemaVersion)
	}
	if review.ActiveHolds != 2 || len(review.Holds) != 2 {
		t.Fatalf("hold counts = active %d records %d", review.ActiveHolds, len(review.Holds))
	}
	if review.ReviewedBy != "compliance-oncall" || review.Reason != "weekly review" || review.ExternalRef != "GRC-123" {
		t.Fatalf("review metadata = %+v", review)
	}

	holdsSHA256, err := retentionHoldReviewRecordsSHA256(review.Holds)
	if err != nil {
		t.Fatalf("hold review hash: %v", err)
	}
	if review.HoldsSHA256 != holdsSHA256 {
		t.Fatalf("holds_sha256 = %q, want %q", review.HoldsSHA256, holdsSHA256)
	}
}

func TestCheckRetentionHoldReviewAcceptsFreshMatchingInventory(t *testing.T) {
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	holds := []retention.Hold{
		retentionHoldForTest("hold-run", retention.HoldScopeRun, "run-123", now.Add(-2*time.Hour)),
		retentionHoldForTest("hold-audit", retention.HoldScopeAuditRange, "2026-06-01T00:00:00Z..2026-07-01T00:00:00Z", now.Add(-time.Hour)),
	}
	review, err := buildRetentionHoldReviewFile(holds, now.Add(-30*time.Minute), "compliance", "weekly review", "GRC-123")
	if err != nil {
		t.Fatalf("build hold review: %v", err)
	}

	root := t.TempDir()
	reviewPath := writeBackupJSONFile(t, root, "hold-review.json", review)
	evidence, err := checkRetentionHoldReview(retentionHoldReviewCheckOptions{ReviewPath: reviewPath, MaxAge: time.Hour}, holds, now)
	if err != nil {
		t.Fatalf("check hold review: %v", err)
	}
	if evidence == nil || !evidence.Verified {
		t.Fatalf("hold review evidence = %+v, want verified", evidence)
	}
	if evidence.Age != "30m0s" || evidence.MaxAge != "1h0m0s" {
		t.Fatalf("freshness evidence = age %q max %q", evidence.Age, evidence.MaxAge)
	}
	if evidence.ActiveHolds != 2 || evidence.HoldsSHA256 != review.HoldsSHA256 {
		t.Fatalf("hold review evidence = %+v", evidence)
	}
}

func TestCheckRetentionHoldReviewRejectsChangedInventory(t *testing.T) {
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	holds := []retention.Hold{
		retentionHoldForTest("hold-run", retention.HoldScopeRun, "run-123", now.Add(-2*time.Hour)),
	}
	review, err := buildRetentionHoldReviewFile(holds, now.Add(-30*time.Minute), "compliance", "weekly review", "")
	if err != nil {
		t.Fatalf("build hold review: %v", err)
	}

	root := t.TempDir()
	reviewPath := writeBackupJSONFile(t, root, "hold-review.json", review)
	currentHolds := append([]retention.Hold(nil), holds...)
	currentHolds = append(currentHolds, retentionHoldForTest("hold-new", retention.HoldScopeRun, "run-456", now.Add(-time.Minute)))

	evidence, err := checkRetentionHoldReview(retentionHoldReviewCheckOptions{ReviewPath: reviewPath}, currentHolds, now)
	if err == nil {
		t.Fatalf("hold review check succeeded unexpectedly")
	}
	if evidence == nil || evidence.Verified {
		t.Fatalf("hold review evidence = %+v, want unverified", evidence)
	}
	if !strings.Contains(err.Error(), "current active holds") {
		t.Fatalf("hold review error = %v, want current active holds", err)
	}
}

func TestCheckRetentionHoldReviewRejectsStaleReview(t *testing.T) {
	now := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	holds := []retention.Hold{
		retentionHoldForTest("hold-run", retention.HoldScopeRun, "run-123", now.Add(-3*time.Hour)),
	}
	review, err := buildRetentionHoldReviewFile(holds, now.Add(-2*time.Hour), "compliance", "weekly review", "")
	if err != nil {
		t.Fatalf("build hold review: %v", err)
	}

	root := t.TempDir()
	reviewPath := writeBackupJSONFile(t, root, "hold-review.json", review)
	evidence, err := checkRetentionHoldReview(retentionHoldReviewCheckOptions{ReviewPath: reviewPath, MaxAge: time.Hour}, holds, now)
	if err == nil {
		t.Fatalf("hold review check succeeded unexpectedly")
	}
	if evidence == nil || evidence.Age != "2h0m0s" {
		t.Fatalf("hold review evidence = %+v, want 2h age", evidence)
	}
	if !strings.Contains(err.Error(), "stale") {
		t.Fatalf("hold review error = %v, want stale", err)
	}
}

func TestPrintRetentionReportIncludesHoldReviewEvidence(t *testing.T) {
	var buf bytes.Buffer
	printRetentionReport(&buf, retention.Report{DryRun: true}, retention.FileReport{}, nil, nil, &retentionHoldReviewEvidence{
		ReviewPath: "hold-review.json",
		Verified:   true,
		CheckedAt:  "2026-07-02T12:00:00Z",
		retentionHoldReviewFile: retentionHoldReviewFile{
			GeneratedAt: "2026-07-02T11:30:00Z",
			ReviewedBy:  "compliance",
			Reason:      "weekly review",
			ExternalRef: "GRC-123",
			ActiveHolds: 2,
			HoldsSHA256: "abc123",
		},
		MaxAge: "1h0m0s",
		Age:    "30m0s",
	}, nil)

	out := buf.String()
	for _, want := range []string{
		"hold_review_verified=true",
		"hold_review_path=hold-review.json",
		"hold_review_reviewed_by=compliance",
		"hold_review_external_ref=GRC-123",
		"hold_review_active_holds=2",
		"hold_review_holds_sha256=abc123",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("retention report missing %q in:\n%s", want, out)
		}
	}
}

func TestRetentionHoldReviewEvidenceJSONIncludesReviewFields(t *testing.T) {
	raw, err := json.Marshal(retentionHoldReviewEvidence{
		ReviewPath: "hold-review.json",
		Verified:   true,
		CheckedAt:  "2026-07-02T12:00:00Z",
		retentionHoldReviewFile: retentionHoldReviewFile{
			SchemaVersion: retentionHoldReviewSchemaVersion,
			GeneratedAt:   "2026-07-02T11:30:00Z",
			ReviewedBy:    "compliance",
			Reason:        "weekly review",
			ActiveHolds:   1,
			HoldsSHA256:   "abc123",
		},
	})
	if err != nil {
		t.Fatalf("marshal hold review evidence: %v", err)
	}

	for _, want := range []string{
		`"review_path":"hold-review.json"`,
		`"schema_version":"vectis.retention_hold_review.v1"`,
		`"generated_at":"2026-07-02T11:30:00Z"`,
		`"reviewed_by":"compliance"`,
		`"holds_sha256":"abc123"`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("hold review evidence JSON missing %s in %s", want, raw)
		}
	}
}

func TestPrintRetentionReportIncludesWaiverEvidence(t *testing.T) {
	var buf bytes.Buffer
	printRetentionReport(&buf, retention.Report{DryRun: true}, retention.FileReport{}, nil, nil, nil, &retentionWaiverEvidence{
		WaiverPath:  "retention-waiver.json",
		Verified:    true,
		CheckedAt:   "2026-07-02T12:00:00Z",
		Waives:      []string{retentionWaiverAuditExport},
		Reason:      "approved exception",
		ApprovedBy:  "security",
		ExternalRef: "INC-1234",
		ExpiresAt:   "2026-07-03T12:00:00Z",
	})

	out := buf.String()
	for _, want := range []string{
		"retention_waiver_verified=true",
		"retention_waiver_path=retention-waiver.json",
		"retention_waiver_waives=audit_export",
		"retention_waiver_external_ref=INC-1234",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("retention report missing %q in:\n%s", want, out)
		}
	}
}

func retentionHoldForTest(holdID, scope, targetID string, createdAt time.Time) retention.Hold {
	return retention.Hold{
		HoldID:    holdID,
		Scope:     scope,
		TargetID:  targetID,
		Status:    retention.HoldStatusActive,
		Owner:     "compliance",
		Reason:    "preserve evidence",
		CreatedBy: "security-oncall",
		CreatedAt: createdAt,
	}
}

func TestRetentionHoldCreateScopeAndTarget(t *testing.T) {
	scope, targetID, err := retentionHoldCreateScopeAndTarget("run-123", "", "")
	if err != nil {
		t.Fatalf("run target: %v", err)
	}
	if scope != retention.HoldScopeRun || targetID != "run-123" {
		t.Fatalf("run target = %q %q", scope, targetID)
	}

	scope, targetID, err = retentionHoldCreateScopeAndTarget("", "2026-07-01T00:00:00Z", "2026-07-02")
	if err != nil {
		t.Fatalf("audit range target: %v", err)
	}
	if scope != retention.HoldScopeAuditRange {
		t.Fatalf("audit range scope = %q", scope)
	}
	since, until, err := retention.ParseAuditRangeHoldTarget(targetID)
	if err != nil {
		t.Fatalf("parse audit range target: %v", err)
	}
	if !since.Equal(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)) ||
		!until.Equal(time.Date(2026, 7, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("audit range bounds = %s %s", since, until)
	}
}

func TestRetentionHoldCreateScopeAndTargetRejectsAmbiguousTargets(t *testing.T) {
	tests := []struct {
		name       string
		runID      string
		auditSince string
		auditUntil string
		want       string
	}{
		{name: "missing", want: "--run"},
		{name: "mixed", runID: "run-123", auditSince: "2026-07-01", auditUntil: "2026-07-02", want: "not both"},
		{name: "partial", auditSince: "2026-07-01", want: "both"},
		{name: "reversed", auditSince: "2026-07-02", auditUntil: "2026-07-01", want: "after since"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := retentionHoldCreateScopeAndTarget(tt.runID, tt.auditSince, tt.auditUntil)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestCheckRetentionAuditExportAcceptsFreshFullRange(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-365 * 24 * time.Hour)
	events := []auditEventResult{
		{ID: 2, EventType: "token.deleted", CreatedAt: cutoff.Add(-time.Hour).Format(time.RFC3339)},
		{ID: 1, EventType: "token.created", CreatedAt: cutoff.Add(-2 * time.Hour).Format(time.RFC3339)},
	}

	root := t.TempDir()
	exportPath := writeBackupJSONFile(t, root, "audit-export.json", auditExportEvidenceForTest(t, now.Add(-30*time.Minute), cutoff.Format(time.RFC3339), 10, events))

	evidence, err := checkRetentionAuditExport(retentionAuditExportCheckOptions{ExportPath: exportPath, MaxAge: time.Hour}, &cutoff, 2, now)
	if err != nil {
		t.Fatalf("audit export check: %v", err)
	}
	if evidence == nil || !evidence.Verified {
		t.Fatalf("audit export evidence = %+v, want verified", evidence)
	}
	if evidence.Age != "30m0s" || evidence.MaxAge != "1h0m0s" {
		t.Fatalf("freshness evidence = age %q max %q", evidence.Age, evidence.MaxAge)
	}
	if evidence.RowsEligible != 2 || evidence.RowsExported != 2 {
		t.Fatalf("row evidence = eligible %d exported %d", evidence.RowsEligible, evidence.RowsExported)
	}
}

func TestCheckRetentionAuditExportRejectsTruncatedExport(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-365 * 24 * time.Hour)
	events := []auditEventResult{{ID: 1, EventType: "token.created", CreatedAt: cutoff.Add(-time.Hour).Format(time.RFC3339)}}
	export := auditExportEvidenceForTest(t, now.Add(-30*time.Minute), cutoff.Format(time.RFC3339), 1, events)
	export.MayBeTruncated = true

	root := t.TempDir()
	exportPath := writeBackupJSONFile(t, root, "audit-export.json", export)

	evidence, err := checkRetentionAuditExport(retentionAuditExportCheckOptions{ExportPath: exportPath}, &cutoff, 1, now)
	if err == nil {
		t.Fatalf("audit export check succeeded unexpectedly")
	}
	if evidence == nil || evidence.Verified {
		t.Fatalf("audit export evidence = %+v, want unverified", evidence)
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Fatalf("audit export error = %v, want truncated", err)
	}
}

func TestCheckRetentionAuditExportRejectsFilteredExport(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-365 * 24 * time.Hour)
	events := []auditEventResult{{ID: 1, EventType: "token.created", CreatedAt: cutoff.Add(-time.Hour).Format(time.RFC3339)}}
	export := auditExportEvidenceForTest(t, now.Add(-30*time.Minute), cutoff.Format(time.RFC3339), 10, events)
	export.Filters.EventType = "token.created"

	root := t.TempDir()
	exportPath := writeBackupJSONFile(t, root, "audit-export.json", export)

	evidence, err := checkRetentionAuditExport(retentionAuditExportCheckOptions{ExportPath: exportPath}, &cutoff, 1, now)
	if err == nil {
		t.Fatalf("audit export check succeeded unexpectedly")
	}
	if evidence == nil || evidence.Verified {
		t.Fatalf("audit export evidence = %+v, want unverified", evidence)
	}
	if !strings.Contains(err.Error(), "must not set") {
		t.Fatalf("audit export error = %v, want filter rejection", err)
	}
}

func TestCheckRetentionAuditExportRejectsUndercount(t *testing.T) {
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	cutoff := now.Add(-365 * 24 * time.Hour)
	events := []auditEventResult{{ID: 1, EventType: "token.created", CreatedAt: cutoff.Add(-time.Hour).Format(time.RFC3339)}}

	root := t.TempDir()
	exportPath := writeBackupJSONFile(t, root, "audit-export.json", auditExportEvidenceForTest(t, now.Add(-30*time.Minute), cutoff.Format(time.RFC3339), 10, events))

	evidence, err := checkRetentionAuditExport(retentionAuditExportCheckOptions{ExportPath: exportPath}, &cutoff, 2, now)
	if err == nil {
		t.Fatalf("audit export check succeeded unexpectedly")
	}
	if evidence == nil || evidence.Verified {
		t.Fatalf("audit export evidence = %+v, want unverified", evidence)
	}
	if !strings.Contains(err.Error(), "less than retention-eligible") {
		t.Fatalf("audit export error = %v, want undercount", err)
	}
}

func TestCheckRetentionBackupManifestRejectsVerificationFailures(t *testing.T) {
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-30 * time.Minute).Format(time.RFC3339))
	manifest.RequiredPaths[1].Exists = false
	manifest.RequiredPaths[1].Readable = false

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "backup-manifest.json", manifest)

	evidence, err := checkRetentionBackupManifest(retentionBackupCheckOptions{ManifestPath: manifestPath}, now)
	if err == nil {
		t.Fatalf("backup manifest check succeeded unexpectedly")
	}
	if evidence == nil || evidence.Verified {
		t.Fatalf("backup manifest evidence = %+v, want unverified evidence", evidence)
	}
	if !strings.Contains(err.Error(), "verification failed") {
		t.Fatalf("backup manifest check error = %v, want verification failure", err)
	}
}

func TestCheckRetentionBackupManifestRejectsStaleManifest(t *testing.T) {
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-2 * time.Hour).Format(time.RFC3339))

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "backup-manifest.json", manifest)

	evidence, err := checkRetentionBackupManifest(retentionBackupCheckOptions{ManifestPath: manifestPath, MaxAge: time.Hour}, now)
	if err == nil {
		t.Fatalf("backup manifest check succeeded unexpectedly")
	}
	if evidence == nil || evidence.Age != "2h0m0s" {
		t.Fatalf("backup manifest evidence = %+v, want 2h age", evidence)
	}
	if !strings.Contains(err.Error(), "stale") {
		t.Fatalf("backup manifest check error = %v, want stale", err)
	}
}

func TestCheckRetentionBackupManifestAcceptsFreshExpectedTopology(t *testing.T) {
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-30 * time.Minute).Format(time.RFC3339))
	expected := backupExpectedTopology{
		SchemaVersion:    backupExpectedTopologySchemaVersion,
		InventorySources: []string{"host-a.inventory.json"},
		DatabaseRoles: []backupExpectedDatabaseRole{
			{InventorySource: "host-a.inventory.json", Role: "default", Driver: "sqlite3"},
		},
		Paths: []backupExpectedPath{
			{InventorySource: "host-a.inventory.json", Category: "local_state", ID: "queue.persistence", Path: "/var/lib/vectis/queue"},
		},
		RequireCategories: []string{"secret_stores", "tls_files", "config_paths"},
	}

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "backup-manifest.json", manifest)
	expectPath := writeBackupJSONFile(t, root, "expected-topology.json", expected)

	evidence, err := checkRetentionBackupManifest(retentionBackupCheckOptions{ManifestPath: manifestPath, ExpectPath: expectPath, MaxAge: 24 * time.Hour}, now)
	if err != nil {
		t.Fatalf("backup manifest check: %v", err)
	}
	if evidence == nil || !evidence.Verified {
		t.Fatalf("backup manifest evidence = %+v, want verified evidence", evidence)
	}
	if evidence.ExpectationSource != expectPath {
		t.Fatalf("expectation source = %q, want %q", evidence.ExpectationSource, expectPath)
	}
	if evidence.Age != "30m0s" || evidence.MaxAge != "24h0m0s" {
		t.Fatalf("freshness evidence = age %q max %q", evidence.Age, evidence.MaxAge)
	}
}

func TestCheckRetentionBackupManifestAcceptsFreshStorageReports(t *testing.T) {
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-30 * time.Minute).Format(time.RFC3339))

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "backup-manifest.json", manifest)
	reportPaths := []string{
		writeBackupJSONFile(t, root, "queue.report.json", backupStorageReportForTest(storageverify.SurfaceQueue, "/var/lib/vectis/queue", now.Add(-5*time.Minute))),
		writeBackupJSONFile(t, root, "logs.report.json", backupStorageReportForTest(storageverify.SurfaceLogs, "/var/lib/vectis/log", now.Add(-5*time.Minute))),
		writeBackupJSONFile(t, root, "artifact.report.json", backupStorageReportForTest(storageverify.SurfaceArtifact, "/var/lib/vectis/artifact", now.Add(-5*time.Minute))),
	}

	evidence, err := checkRetentionBackupManifest(retentionBackupCheckOptions{ManifestPath: manifestPath, StorageReportPaths: reportPaths, StorageReportMaxAge: time.Hour}, now)
	if err != nil {
		t.Fatalf("backup manifest check with storage reports: %v", err)
	}
	if evidence == nil || !evidence.Verified {
		t.Fatalf("backup manifest evidence = %+v, want verified evidence", evidence)
	}
	if evidence.StorageReports != 3 || evidence.StorageReportsVerified != 3 || evidence.StoragePathsRequired != 3 {
		t.Fatalf("storage evidence = %+v", evidence)
	}
	if evidence.StorageReportMaxAge != "1h0m0s" {
		t.Fatalf("storage max age = %q, want 1h0m0s", evidence.StorageReportMaxAge)
	}
}

func TestCheckRetentionBackupManifestRejectsStaleStorageReport(t *testing.T) {
	now := time.Date(2026, 6, 28, 16, 0, 0, 0, time.UTC)
	manifest := retentionBackupManifestForTest(now.Add(-30 * time.Minute).Format(time.RFC3339))

	root := t.TempDir()
	manifestPath := writeBackupJSONFile(t, root, "backup-manifest.json", manifest)
	reportPaths := []string{
		writeBackupJSONFile(t, root, "queue.report.json", backupStorageReportForTest(storageverify.SurfaceQueue, "/var/lib/vectis/queue", now.Add(-2*time.Hour))),
		writeBackupJSONFile(t, root, "logs.report.json", backupStorageReportForTest(storageverify.SurfaceLogs, "/var/lib/vectis/log", now.Add(-5*time.Minute))),
		writeBackupJSONFile(t, root, "artifact.report.json", backupStorageReportForTest(storageverify.SurfaceArtifact, "/var/lib/vectis/artifact", now.Add(-5*time.Minute))),
	}

	evidence, err := checkRetentionBackupManifest(retentionBackupCheckOptions{ManifestPath: manifestPath, StorageReportPaths: reportPaths, StorageReportMaxAge: time.Hour}, now)
	if err == nil {
		t.Fatalf("backup manifest check succeeded unexpectedly")
	}
	if evidence == nil || evidence.Verified {
		t.Fatalf("backup manifest evidence = %+v, want unverified evidence", evidence)
	}
	if !strings.Contains(err.Error(), "verification failed") {
		t.Fatalf("backup manifest check error = %v, want verification failure", err)
	}
}

func retentionBackupManifestForTest(generatedAt string) backupManifest {
	version := 42
	dirty := false
	return backupManifest{
		SchemaVersion: backupManifestSchemaVersion,
		GeneratedAt:   generatedAt,
		Inventories: []backupManifestInventory{
			{Source: "host-a.inventory.json", GeneratedAt: generatedAt, Version: "test", DatabaseDriver: "sqlite3"},
		},
		DatabaseRoles: []backupManifestDatabaseRole{
			{
				InventorySource: "host-a.inventory.json",
				Role:            "default",
				Driver:          "sqlite3",
				DSN:             "sqlite.db",
				DSNSource:       "VECTIS_DATABASE_DSN",
				LocalPath:       "sqlite.db",
				Schema:          backupSchemaInventory{Inspectable: true, CurrentVersion: &version, Dirty: &dirty},
			},
		},
		RequiredPaths: []backupManifestPath{
			{InventorySource: "host-a.inventory.json", Category: "database", ID: "database.default", Kind: "file", Path: "sqlite.db", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "local_state", ID: "queue.persistence", Kind: "directory", Path: "/var/lib/vectis/queue", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "local_state", ID: "log.storage", Kind: "directory", Path: "/var/lib/vectis/log", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "local_state", ID: "artifact.storage", Kind: "directory", Path: "/var/lib/vectis/artifact", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "secret_stores", ID: "secrets.encryptedfs.root", Kind: "directory", Path: "/var/lib/vectis/secrets", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "tls_files", ID: "grpc.cert_file", Kind: "file", Path: "/etc/vectis/tls/grpc.crt", Enabled: true, Exists: true, Readable: true},
			{InventorySource: "host-a.inventory.json", Category: "config_paths", ID: "deploy.config_dir", Kind: "directory", Path: "/etc/vectis/deploy", Enabled: true, Exists: true, Readable: true},
		},
	}
}

func auditExportEvidenceForTest(t *testing.T, generatedAt time.Time, until string, limit int, events []auditEventResult) auditExportEvidence {
	t.Helper()

	eventsSHA256, err := auditEventsSHA256(events)
	if err != nil {
		t.Fatalf("audit events sha256: %v", err)
	}

	evidence := auditExportEvidence{
		SchemaVersion:  auditExportSchemaVersion,
		GeneratedAt:    generatedAt.UTC().Format(time.RFC3339),
		Filters:        auditExportFilters{Until: until},
		Limit:          limit,
		PageCount:      1,
		RowCount:       len(events),
		MayBeTruncated: limit > 0 && len(events) >= limit,
		EventsSHA256:   eventsSHA256,
		Events:         events,
	}
	for _, event := range events {
		if evidence.NewestEventAt == "" || event.CreatedAt > evidence.NewestEventAt {
			evidence.NewestEventAt = event.CreatedAt
		}
		if evidence.OldestEventAt == "" || event.CreatedAt < evidence.OldestEventAt {
			evidence.OldestEventAt = event.CreatedAt
		}
	}

	return evidence
}
