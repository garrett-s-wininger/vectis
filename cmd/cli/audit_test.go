package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAuditExportWritesEvidenceFile(t *testing.T) {
	requests := 0
	setupTestAPIClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet {
			t.Errorf("method=%s", r.Method)
		}

		if r.URL.Path != "/api/v1/audit/events" {
			t.Errorf("path=%s", r.URL.Path)
		}

		if got := r.URL.Query().Get("until"); got != "2026-07-01T00:00:00Z" {
			t.Errorf("until=%q", got)
		}

		if got := r.URL.Query().Get("limit"); got != "1" {
			t.Errorf("limit=%q", got)
		}

		switch cursor := r.URL.Query().Get("cursor"); cursor {
		case "":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"limit":       1,
				"next_cursor": "cursor-2",
				"events": []map[string]any{
					{
						"id":             42,
						"event_type":     "retention.cleanup",
						"metadata":       map[string]any{"dry_run": false},
						"correlation_id": "corr-1",
						"created_at":     "2026-06-29T12:00:00Z",
					},
				},
			})
		case "cursor-2":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"limit": 1,
				"events": []map[string]any{
					{
						"id":             41,
						"event_type":     "token.created",
						"correlation_id": "corr-2",
						"created_at":     "2026-06-28T12:00:00Z",
					},
				},
			})
		default:
			t.Errorf("unexpected cursor=%q", cursor)
		}
	})

	outputPath := filepath.Join(t.TempDir(), "audit-export.json")
	var stdout bytes.Buffer
	err := auditExport(&stdout, auditExportOptions{
		auditListOptions: auditListOptions{
			Until: "2026-07-01T00:00:00Z",
			Limit: 1,
		},
		OutputPath:  outputPath,
		GeneratedAt: time.Date(2026, 7, 1, 1, 0, 0, 0, time.UTC),
	})

	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 {
		t.Fatalf("requests=%d, want 2", requests)
	}

	if !strings.Contains(stdout.String(), "audit_export_path="+outputPath) {
		t.Fatalf("stdout did not mention output path:\n%s", stdout.String())
	}

	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read export: %v", err)
	}

	var evidence auditExportEvidence
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatalf("decode export: %v", err)
	}

	if evidence.SchemaVersion != auditExportSchemaVersion {
		t.Fatalf("schema_version=%q", evidence.SchemaVersion)
	}

	if evidence.GeneratedAt != "2026-07-01T01:00:00Z" {
		t.Fatalf("generated_at=%q", evidence.GeneratedAt)
	}

	if evidence.RowCount != 2 || evidence.PageCount != 2 || evidence.MayBeTruncated {
		t.Fatalf("row/truncation evidence = count %d truncated %t", evidence.RowCount, evidence.MayBeTruncated)
	}

	if evidence.Filters.Until != "2026-07-01T00:00:00Z" {
		t.Fatalf("until filter=%q", evidence.Filters.Until)
	}

	if evidence.EventsSHA256 == "" {
		t.Fatal("events_sha256 is empty")
	}

	if evidence.NewestEventAt != "2026-06-29T12:00:00Z" || evidence.OldestEventAt != "2026-06-28T12:00:00Z" {
		t.Fatalf("event bounds newest=%q oldest=%q", evidence.NewestEventAt, evidence.OldestEventAt)
	}
}
