package storageverify

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/artifact"
	"vectis/internal/logforwarder"
	"vectis/internal/logrecord"
	"vectis/internal/logserver"

	"google.golang.org/protobuf/proto"
)

func TestVerifyArtifactsDetectsDigestMismatch(t *testing.T) {
	dir := t.TempDir()
	store, err := artifact.NewLocalStore(dir)
	if err != nil {
		t.Fatalf("new artifact store: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close artifact store: %v", err)
	}

	digest := sha256Hex("expected")
	path := filepath.Join(dir, "blobs", "sha256", digest[:2], digest[2:4], digest+".blob")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir blob dir: %v", err)
	}

	if err := os.WriteFile(path, []byte("actual"), 0o644); err != nil {
		t.Fatalf("write blob: %v", err)
	}

	report, err := VerifyArtifacts(context.Background(), dir)
	if err != nil {
		t.Fatalf("verify artifacts: %v", err)
	}

	if report.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", report.Status)
	}

	if !hasFinding(report.Errors, "artifact.digest_mismatch") {
		t.Fatalf("errors = %+v, want digest mismatch", report.Errors)
	}
}

func TestVerifyRunLogsAcceptsProductionFormatAndRejectsTruncation(t *testing.T) {
	dir := t.TempDir()
	store, err := logserver.NewLocalRunLogStore(dir)
	if err != nil {
		t.Fatalf("new run log store: %v", err)
	}

	if err := store.Append("run-1", logserver.LogEntry{
		Timestamp: time.Unix(1_710_000_000, 123).UTC(),
		Stream:    api.Stream_STREAM_STDOUT,
		Sequence:  1,
		Data:      []byte("hello"),
	}); err != nil {
		t.Fatalf("append log entry: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close run log store: %v", err)
	}

	report, err := VerifyRunLogs(context.Background(), dir)
	if err != nil {
		t.Fatalf("verify run logs: %v", err)
	}

	if report.Status != StatusOK || report.Records != 1 {
		t.Fatalf("report = %+v, want ok with one record", report)
	}

	logPath := filepath.Join(dir, base64RunLogName("run-1"))
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log path: %v", err)
	}

	if err := os.Truncate(logPath, info.Size()-1); err != nil {
		t.Fatalf("truncate log path: %v", err)
	}

	report, err = VerifyRunLogs(context.Background(), dir)
	if err != nil {
		t.Fatalf("verify truncated run logs: %v", err)
	}

	if report.Status != StatusFailed || !hasFinding(report.Errors, "logs.record_corrupt") {
		t.Fatalf("report = %+v, want corrupt record", report)
	}
}

func TestVerifyQueueDetectsCorruptWALPayload(t *testing.T) {
	dir := t.TempDir()
	payload, err := proto.Marshal(&api.JobRequest{Job: &api.Job{Id: proto.String("job-1")}})
	if err != nil {
		t.Fatalf("marshal job request: %v", err)
	}

	valid, err := json.Marshal(queueWALRecord{Index: 1, Type: "enqueue", Job: payload})
	if err != nil {
		t.Fatalf("marshal wal: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "queue.wal.000001"), append(valid, '\n'), 0o644); err != nil {
		t.Fatalf("write wal: %v", err)
	}

	report, err := VerifyQueue(context.Background(), dir)
	if err != nil {
		t.Fatalf("verify queue: %v", err)
	}

	if report.Status != StatusOK || report.Records != 1 {
		t.Fatalf("report = %+v, want ok with one record", report)
	}

	corrupt, err := json.Marshal(queueWALRecord{Index: 2, Type: "enqueue", Job: []byte{0xff}})
	if err != nil {
		t.Fatalf("marshal corrupt wal: %v", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "queue.wal.000002"), append(corrupt, '\n'), 0o644); err != nil {
		t.Fatalf("write corrupt wal: %v", err)
	}

	report, err = VerifyQueue(context.Background(), dir)
	if err != nil {
		t.Fatalf("verify corrupt queue: %v", err)
	}

	if report.Status != StatusFailed || !hasFinding(report.Errors, "queue.wal_record_corrupt") {
		t.Fatalf("report = %+v, want corrupt wal record", report)
	}
}

func TestVerifyLogForwarderSpoolDetectsCRCError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "batch.spool")
	writer, err := logforwarder.NewSpoolWriter(path, 10)
	if err != nil {
		t.Fatalf("new spool writer: %v", err)
	}

	if err := writer.Append(&api.LogChunk{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("hello")}); err != nil {
		t.Fatalf("append spool chunk: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("close spool writer: %v", err)
	}

	report, err := VerifyLogForwarderSpool(context.Background(), dir)
	if err != nil {
		t.Fatalf("verify spool: %v", err)
	}

	if report.Status != StatusOK || report.Records != 1 || report.Batches != 1 {
		t.Fatalf("report = %+v, want one batch and one record", report)
	}

	flipLastByte(t, path)
	report, err = VerifyLogForwarderSpool(context.Background(), dir)
	if err != nil {
		t.Fatalf("verify corrupt spool: %v", err)
	}

	if report.Status != StatusFailed || !hasFinding(report.Errors, "spool.batch_corrupt") {
		t.Fatalf("report = %+v, want corrupt batch", report)
	}
}

func TestVerifyWorkerLogSpoolDetectsBadFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pending", "run-1.spool")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir pending: %v", err)
	}

	payload, err := proto.Marshal(&api.LogChunk{RunId: proto.String("run-1"), Sequence: proto.Int64(1), Data: []byte("hello")})
	if err != nil {
		t.Fatalf("marshal chunk: %v", err)
	}

	record, err := logrecord.Append(nil, payload)
	if err != nil {
		t.Fatalf("frame chunk: %v", err)
	}

	if err := os.WriteFile(path, record, 0o600); err != nil {
		t.Fatalf("write worker spool: %v", err)
	}

	report, err := VerifyWorkerLogSpool(context.Background(), dir)
	if err != nil {
		t.Fatalf("verify worker spool: %v", err)
	}

	if report.Status != StatusOK || report.Records != 1 {
		t.Fatalf("report = %+v, want one record", report)
	}

	binary.LittleEndian.PutUint32(record[len(record)-logrecord.LengthSize:], 42)
	if err := os.WriteFile(path, record, 0o600); err != nil {
		t.Fatalf("write corrupt worker spool: %v", err)
	}

	report, err = VerifyWorkerLogSpool(context.Background(), dir)
	if err != nil {
		t.Fatalf("verify corrupt worker spool: %v", err)
	}

	if report.Status != StatusFailed || !hasFinding(report.Errors, "worker_spool.record_corrupt") {
		t.Fatalf("report = %+v, want corrupt record", report)
	}
}

func hasFinding(findings []Finding, id string) bool {
	for _, finding := range findings {
		if finding.ID == id {
			return true
		}
	}

	return false
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func base64RunLogName(runID string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(runID)) + ".vlog"
}

func flipLastByte(t *testing.T, path string) {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	if len(data) == 0 {
		t.Fatalf("%s is empty", path)
	}

	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
