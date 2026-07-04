package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	api "vectis/api/gen/go"
	"vectis/internal/artifact"
	"vectis/internal/dal"
	"vectis/internal/database"
	"vectis/internal/logserver"
	"vectis/internal/storageverify"
)

func TestLocalRestoreDrillVerifiesRestoredSQLiteAndFileStores(t *testing.T) {
	withOutputFormat(t, outputJSON)
	t.Setenv(database.EnvDatabaseDriver, "sqlite3")

	ctx := context.Background()
	root := t.TempDir()
	source := localRestoreDrillPathsForRoot(filepath.Join(root, "source"))
	backup := localRestoreDrillPathsForRoot(filepath.Join(root, "backup"))
	restored := localRestoreDrillPathsForRoot(filepath.Join(root, "restored"))

	seed := seedLocalRestoreDrillState(t, ctx, source)

	copyLocalRestoreDrillTree(t, source.Root, backup.Root)
	copyLocalRestoreDrillTree(t, backup.Root, restored.Root)

	if err := database.Migrate(restored.DatabasePath); err != nil {
		t.Fatalf("migrate restored database: %v", err)
	}

	assertLocalRestoreDrillDatabase(t, ctx, restored.DatabasePath, seed.JobID, seed.RunID)
	assertLocalRestoreDrillLogs(t, restored.LogDir, seed.RunID)
	assertLocalRestoreDrillArtifact(t, ctx, restored.ArtifactDir, seed.Artifact)

	t.Setenv(database.EnvDatabaseDSN, restored.DatabasePath)
	t.Setenv("VECTIS_QUEUE_INSTANCE_ID", "restore-queue")
	t.Setenv("VECTIS_QUEUE_PERSISTENCE_DIR", restored.QueueDir)
	t.Setenv("VECTIS_LOG_INSTANCE_ID", "restore-log")
	t.Setenv("VECTIS_LOG_STORAGE_DIR", restored.LogDir)
	t.Setenv("VECTIS_ARTIFACT_INSTANCE_ID", "restore-artifact")
	t.Setenv("VECTIS_ARTIFACT_STORAGE_DIR", restored.ArtifactDir)
	t.Setenv("VECTIS_LOG_FORWARDER_SPOOL_DIR", restored.LogForwarderSpoolDir)
	t.Setenv("VECTIS_SOURCE_CHECKOUT_ROOT", restored.SourceCheckoutRoot)
	t.Setenv("TMPDIR", restored.TempDir)
	t.Setenv("TMP", restored.TempDir)
	t.Setenv("TEMP", restored.TempDir)

	evidenceDir := filepath.Join(root, "evidence")
	if err := os.MkdirAll(evidenceDir, 0o755); err != nil {
		t.Fatalf("mkdir evidence dir: %v", err)
	}

	manifestGeneratedAt := time.Date(2026, 6, 28, 15, 0, 0, 0, time.UTC)
	var inventoryBuf bytes.Buffer
	if err := writeBackupInventory(&inventoryBuf, manifestGeneratedAt); err != nil {
		t.Fatalf("write restored inventory: %v", err)
	}

	inventoryPath := filepath.Join(evidenceDir, "restored.inventory.json")
	if err := os.WriteFile(inventoryPath, inventoryBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write restored inventory file: %v", err)
	}

	var manifestBuf bytes.Buffer
	if err := writeBackupManifest(&manifestBuf, []string{inventoryPath}, manifestGeneratedAt); err != nil {
		t.Fatalf("write restored manifest: %v", err)
	}

	manifestPath := filepath.Join(evidenceDir, "restored.backup-manifest.json")
	if err := os.WriteFile(manifestPath, manifestBuf.Bytes(), 0o600); err != nil {
		t.Fatalf("write restored manifest file: %v", err)
	}

	reportInputs := []struct {
		name    string
		surface string
		dir     string
	}{
		{name: "queue.storage-report.json", surface: storageverify.SurfaceQueue, dir: restored.QueueDir},
		{name: "logs.storage-report.json", surface: storageverify.SurfaceLogs, dir: restored.LogDir},
		{name: "artifact.storage-report.json", surface: storageverify.SurfaceArtifact, dir: restored.ArtifactDir},
		{name: "log-forwarder-spool.storage-report.json", surface: storageverify.SurfaceLogForwarderSpool, dir: restored.LogForwarderSpoolDir},
		{name: "worker-log-spool.storage-report.json", surface: storageverify.SurfaceWorkerLogSpool, dir: restored.WorkerLogSpoolDir},
	}

	reportPaths := make([]string, 0, len(reportInputs))
	for _, input := range reportInputs {
		report, err := storageverify.Verify(ctx, input.surface, input.dir)
		if err != nil {
			t.Fatalf("verify restored %s storage: %v", input.surface, err)
		}

		if report.Status != storageverify.StatusOK {
			t.Fatalf("restored %s storage status = %s, errors = %+v", input.surface, report.Status, report.Errors)
		}

		reportPaths = append(reportPaths, writeBackupJSONFile(t, evidenceDir, input.name, report))
	}

	checkedAt := time.Now().UTC()
	var verificationBuf bytes.Buffer
	if err := writeBackupManifestVerification(&verificationBuf, manifestPath, "", reportPaths, time.Hour, checkedAt); err != nil {
		t.Fatalf("verify restored manifest with storage reports: %v\n%s", err, verificationBuf.String())
	}

	var verification backupManifestVerification
	if err := json.Unmarshal(verificationBuf.Bytes(), &verification); err != nil {
		t.Fatalf("decode restored verification: %v\n%s", err, verificationBuf.String())
	}

	if verification.Status != backupManifestStatusOK {
		t.Fatalf("verification status = %s, errors = %+v", verification.Status, verification.Errors)
	}

	if verification.Summary.StorageReports != len(reportInputs) {
		t.Fatalf("storage reports = %d, want %d", verification.Summary.StorageReports, len(reportInputs))
	}

	if verification.Summary.StorageReportsVerified != len(reportInputs) {
		t.Fatalf("verified storage reports = %d, want %d", verification.Summary.StorageReportsVerified, len(reportInputs))
	}

	wantMatched := map[string]bool{
		"queue.persistence":        false,
		"log.storage":              false,
		"artifact.storage":         false,
		"log_forwarder.spool":      false,
		"worker.pending_log_spool": false,
	}

	for _, evidence := range verification.StorageReports {
		for _, id := range evidence.MatchedPathIDs {
			if _, ok := wantMatched[id]; ok {
				wantMatched[id] = true
			}
		}
	}

	for id, matched := range wantMatched {
		if !matched {
			t.Fatalf("storage report did not match manifest path %s; evidence = %+v", id, verification.StorageReports)
		}
	}
}

type localRestoreDrillPaths struct {
	Root                 string
	DatabasePath         string
	QueueDir             string
	LogDir               string
	ArtifactDir          string
	LogForwarderSpoolDir string
	SourceCheckoutRoot   string
	TempDir              string
	WorkerLogSpoolDir    string
}

type localRestoreDrillSeed struct {
	JobID    string
	RunID    string
	Artifact artifact.BlobDescriptor
}

type localRestoreDrillQueueWALRecord struct {
	Index uint64 `json:"index"`
	Type  string `json:"type"`
	Job   []byte `json:"job,omitempty"`
}

func localRestoreDrillPathsForRoot(root string) localRestoreDrillPaths {
	tempDir := filepath.Join(root, "tmp")
	return localRestoreDrillPaths{
		Root:                 root,
		DatabasePath:         filepath.Join(root, "db", "vectis.sqlite3"),
		QueueDir:             filepath.Join(root, "queue"),
		LogDir:               filepath.Join(root, "logs"),
		ArtifactDir:          filepath.Join(root, "artifact"),
		LogForwarderSpoolDir: filepath.Join(root, "log-forwarder-spool"),
		SourceCheckoutRoot:   filepath.Join(root, "sources"),
		TempDir:              tempDir,
		WorkerLogSpoolDir:    filepath.Join(tempDir, "vectis-log-spool", "pending"),
	}
}

func seedLocalRestoreDrillState(t *testing.T, ctx context.Context, paths localRestoreDrillPaths) localRestoreDrillSeed {
	t.Helper()

	for _, dir := range []string{
		paths.QueueDir,
		paths.LogDir,
		paths.ArtifactDir,
		paths.LogForwarderSpoolDir,
		paths.SourceCheckoutRoot,
		paths.WorkerLogSpoolDir,
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", dir, err)
		}
	}

	if err := database.Migrate(paths.DatabasePath); err != nil {
		t.Fatalf("migrate source database: %v", err)
	}

	db, err := database.OpenDB(paths.DatabasePath)
	if err != nil {
		t.Fatalf("open source database: %v", err)
	}

	repos := dal.NewSQLRepositories(db)
	jobID := "restore-drill-job"
	definition := `{"id":"restore-drill-job","root":{"id":"root","uses":"builtins/script","with":{"script":"echo restore-drill"}}}`
	runID, _, err := repos.CreateDefinitionAndRun(ctx, jobID, definition, nil)
	if closeErr := db.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("close source database: %w", closeErr)
	}

	if err != nil {
		t.Fatalf("seed source database: %v", err)
	}

	seedLocalRestoreDrillQueue(t, paths.QueueDir, jobID, runID)
	seedLocalRestoreDrillLogs(t, paths.LogDir, runID)
	desc := seedLocalRestoreDrillArtifact(t, ctx, paths.ArtifactDir)

	return localRestoreDrillSeed{
		JobID:    jobID,
		RunID:    runID,
		Artifact: desc,
	}
}

func seedLocalRestoreDrillQueue(t *testing.T, dir, jobID, runID string) {
	t.Helper()

	payload, err := proto.Marshal(&api.JobRequest{
		Job: &api.Job{
			Id:    proto.String(jobID),
			RunId: proto.String(runID),
			Root: &api.Node{
				Id:   proto.String("root"),
				Uses: proto.String("builtins/script"),
				With: map[string]string{"script": "echo restore-drill"},
			},
		},
	})

	if err != nil {
		t.Fatalf("marshal queue payload: %v", err)
	}

	line, err := json.Marshal(localRestoreDrillQueueWALRecord{Index: 1, Type: "enqueue", Job: payload})
	if err != nil {
		t.Fatalf("marshal queue wal: %v", err)
	}

	path := filepath.Join(dir, "queue.wal.000001")
	if err := os.WriteFile(path, append(line, '\n'), 0o644); err != nil {
		t.Fatalf("write queue wal: %v", err)
	}
}

func seedLocalRestoreDrillLogs(t *testing.T, dir, runID string) {
	t.Helper()

	store, err := logserver.NewLocalRunLogStore(dir)
	if err != nil {
		t.Fatalf("open source log store: %v", err)
	}

	err = store.AppendBatch(runID, []logserver.LogEntry{
		{Timestamp: time.Unix(1_800_000_000, 0).UTC(), Stream: api.Stream_STREAM_STDOUT, Sequence: 1, Data: []byte("restore drill log\n")},
		{Timestamp: time.Unix(1_800_000_001, 0).UTC(), Stream: api.Stream_STREAM_CONTROL, Sequence: 2, Data: []byte("completed\n"), Completed: api.RunOutcome_RUN_OUTCOME_SUCCESS},
	})

	if closeErr := store.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("close source log store: %w", closeErr)
	}

	if err != nil {
		t.Fatalf("seed source log store: %v", err)
	}
}

func seedLocalRestoreDrillArtifact(t *testing.T, ctx context.Context, dir string) artifact.BlobDescriptor {
	t.Helper()

	store, err := artifact.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("open source artifact store: %v", err)
	}

	desc, err := store.Put(ctx, strings.NewReader("restore drill artifact\n"), artifact.PutOptions{})
	if closeErr := store.Close(); err == nil && closeErr != nil {
		err = fmt.Errorf("close source artifact store: %w", closeErr)
	}

	if err != nil {
		t.Fatalf("seed source artifact store: %v", err)
	}

	return desc
}

func assertLocalRestoreDrillDatabase(t *testing.T, ctx context.Context, dbPath, jobID, runID string) {
	t.Helper()

	db, err := database.OpenDB(dbPath)
	if err != nil {
		t.Fatalf("open restored database: %v", err)
	}
	defer db.Close()

	if err := database.WaitForMigrations(db, nil); err != nil {
		t.Fatalf("wait for restored migrations: %v", err)
	}

	rec, err := dal.NewSQLRepositories(db).Runs().GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get restored run: %v", err)
	}

	if rec.JobID != jobID || rec.Status != dal.RunStatusQueued {
		t.Fatalf("restored run = %+v, want job %s status %s", rec, jobID, dal.RunStatusQueued)
	}
}

func assertLocalRestoreDrillLogs(t *testing.T, dir, runID string) {
	t.Helper()

	store, err := logserver.NewLocalRunLogStore(dir)
	if err != nil {
		t.Fatalf("open restored log store: %v", err)
	}
	defer store.Close()

	entries, err := store.List(runID)
	if err != nil {
		t.Fatalf("list restored logs: %v", err)
	}

	if len(entries) != 2 || string(entries[0].Data) != "restore drill log\n" || entries[1].Completed != api.RunOutcome_RUN_OUTCOME_SUCCESS {
		t.Fatalf("restored log entries = %+v", entries)
	}
}

func assertLocalRestoreDrillArtifact(t *testing.T, ctx context.Context, dir string, want artifact.BlobDescriptor) {
	t.Helper()

	store, err := artifact.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("open restored artifact store: %v", err)
	}
	defer store.Close()

	got, err := store.Stat(ctx, want.Key)
	if err != nil {
		t.Fatalf("stat restored artifact: %v", err)
	}

	if got.Digest != want.Digest || got.Size != want.Size {
		t.Fatalf("restored artifact = %+v, want %+v", got, want)
	}
}

func copyLocalRestoreDrillTree(t *testing.T, src, dst string) {
	t.Helper()

	err := filepath.WalkDir(src, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		target := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}

		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}

		if !info.Mode().IsRegular() {
			return nil
		}

		return copyLocalRestoreDrillFile(path, target, info.Mode().Perm())
	})

	if err != nil {
		t.Fatalf("copy %s to %s: %v", src, dst, err)
	}
}

func copyLocalRestoreDrillFile(src, dst string, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}

	return closeErr
}
