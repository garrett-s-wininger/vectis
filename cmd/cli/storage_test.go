package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"vectis/internal/storageverify"
)

func TestWriteStorageVerificationJSONFailsOnIntegrityError(t *testing.T) {
	withOutputFormat(t, outputJSON)

	dir := t.TempDir()
	root := filepath.Join(dir, "blobs", "sha256")
	badDigest := strings.Repeat("0", 64)
	path := filepath.Join(root, "00", "00", badDigest+".blob")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir blob dir: %v", err)
	}

	if err := os.WriteFile(path, []byte("not zero"), 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}

	var buf bytes.Buffer
	err := writeStorageVerification(context.Background(), &buf, storageverify.SurfaceArtifact, dir)
	if err == nil {
		t.Fatal("expected integrity error")
	}

	var report storageverify.Report
	if decodeErr := json.Unmarshal(buf.Bytes(), &report); decodeErr != nil {
		t.Fatalf("decode report: %v\n%s", decodeErr, buf.String())
	}

	if report.Status != storageverify.StatusFailed {
		t.Fatalf("status = %s, want failed", report.Status)
	}

	if len(report.Errors) != 1 || report.Errors[0].ID != "artifact.digest_mismatch" {
		t.Fatalf("errors = %+v, want digest mismatch", report.Errors)
	}
}

func TestWriteStorageVerificationTextSummarizesOK(t *testing.T) {
	withOutputFormat(t, outputText)

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "blobs", "sha256"), 0o755); err != nil {
		t.Fatalf("mkdir artifact root: %v", err)
	}

	var buf bytes.Buffer
	if err := writeStorageVerification(context.Background(), &buf, storageverify.SurfaceArtifact, dir); err != nil {
		t.Fatalf("write verification: %v", err)
	}

	out := buf.String()
	for _, want := range []string{
		"status=ok",
		"surface=artifact",
		"checked_files=0",
		"errors=0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("output missing %q:\n%s", want, out)
		}
	}
}
