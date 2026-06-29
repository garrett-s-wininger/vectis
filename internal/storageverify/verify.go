package storageverify

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/logforwarder"
	"vectis/internal/logrecord"

	"google.golang.org/protobuf/proto"
)

const (
	StatusOK     = "ok"
	StatusFailed = "failed"

	SurfaceArtifact          = "artifact"
	SurfaceLogs              = "logs"
	SurfaceQueue             = "queue"
	SurfaceLogForwarderSpool = "log-forwarder-spool"
	SurfaceWorkerLogSpool    = "worker-log-spool"

	artifactHashSHA256     = "sha256"
	artifactBlobFileSuffix = ".blob"
	logFileSuffix          = ".vlog"
	logStorageLockFileName = "log.lock"

	queueSnapshotFileName = "queue.snapshot"
	queueWALSegmentPrefix = "queue.wal."
	queueLockFileName     = "queue.lock"

	logEntryRecordLengthSize = 4
	logEntryRecordHeaderSize = 32

	defaultWorkerSpoolMaxPayload = 10 * 1024 * 1024
)

type Report struct {
	Surface      string    `json:"surface"`
	Path         string    `json:"path"`
	Status       string    `json:"status"`
	CheckedFiles int64     `json:"checked_files"`
	CheckedBytes int64     `json:"checked_bytes"`
	Records      int64     `json:"records"`
	Batches      int64     `json:"batches,omitempty"`
	CheckedAt    time.Time `json:"checked_at"`
	Errors       []Finding `json:"errors,omitempty"`
	Warnings     []Finding `json:"warnings,omitempty"`
}

type Finding struct {
	ID      string `json:"id"`
	Path    string `json:"path,omitempty"`
	Message string `json:"message"`
}

type queueSnapshot struct {
	LastAppliedIndex uint64                  `json:"last_applied_index"`
	Jobs             [][]byte                `json:"jobs"`
	Inflight         []queueInflightSnapshot `json:"inflight"`
	DeadLetter       []queueDeadLetterItem   `json:"dead_letter,omitempty"`
	JobAttempts      map[string]int          `json:"job_attempts,omitempty"`
}

type queueInflightSnapshot struct {
	DeliveryID    string `json:"delivery_id"`
	Job           []byte `json:"job"`
	LeaseUntilUTC int64  `json:"lease_until_utc"`
	AttemptCount  int    `json:"attempt_count"`
}

type queueDeadLetterItem struct {
	DeliveryID   string `json:"delivery_id"`
	Job          []byte `json:"job"`
	AttemptCount int    `json:"attempt_count"`
}

type queueWALRecord struct {
	Index         uint64 `json:"index"`
	Type          string `json:"type"`
	Job           []byte `json:"job,omitempty"`
	DeliveryID    string `json:"delivery_id,omitempty"`
	LeaseUntilUTC int64  `json:"lease_until_utc,omitempty"`
	AttemptCount  int    `json:"attempt_count,omitempty"`
}

func Verify(ctx context.Context, surface, dir string) (Report, error) {
	switch surface {
	case SurfaceArtifact:
		return VerifyArtifacts(ctx, dir)
	case SurfaceLogs:
		return VerifyRunLogs(ctx, dir)
	case SurfaceQueue:
		return VerifyQueue(ctx, dir)
	case SurfaceLogForwarderSpool:
		return VerifyLogForwarderSpool(ctx, dir)
	case SurfaceWorkerLogSpool:
		return VerifyWorkerLogSpool(ctx, dir)
	default:
		return Report{}, fmt.Errorf("unknown storage verification surface %q", surface)
	}
}

func VerifyArtifacts(ctx context.Context, dir string) (Report, error) {
	report, ok := newReport(SurfaceArtifact, dir)
	if !ok {
		return report, nil
	}

	root := filepath.Join(report.Path, "blobs", artifactHashSHA256)
	if !report.requireDir(root, "artifact.blobs_missing") {
		return report.finish(), nil
	}

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			report.addError("artifact.walk_error", path, err.Error())
			return nil
		}

		if err := ctxErr(ctx); err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		if !entry.Type().IsRegular() {
			report.addWarning("artifact.non_regular_file", path, "non-regular file under artifact blob directory was skipped")
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			report.addError("artifact.stat_failed", path, err.Error())
			return nil
		}

		report.CheckedFiles++
		report.CheckedBytes += info.Size()

		digest, err := artifactDigestFromPath(root, path)
		if err != nil {
			report.addError("artifact.invalid_path", path, err.Error())
			return nil
		}

		got, err := hashFile(ctx, path, sha256.New())
		if err != nil {
			report.addError("artifact.read_failed", path, err.Error())
			return nil
		}

		if got != digest {
			report.addError("artifact.digest_mismatch", path, fmt.Sprintf("content digest sha256:%s does not match path digest sha256:%s", got, digest))
			return nil
		}

		report.Records++
		return nil
	})

	if err != nil {
		return report, err
	}

	return report.finish(), nil
}

func VerifyRunLogs(ctx context.Context, dir string) (Report, error) {
	report, ok := newReport(SurfaceLogs, dir)
	if !ok {
		return report, nil
	}

	if !report.requireDir(report.Path, "logs.path_missing") {
		return report.finish(), nil
	}

	err := filepath.WalkDir(report.Path, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			report.addError("logs.walk_error", path, err.Error())
			return nil
		}

		if err := ctxErr(ctx); err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		name := entry.Name()
		if name == logStorageLockFileName {
			return nil
		}

		if !strings.HasSuffix(name, logFileSuffix) {
			report.addWarning("logs.unrecognized_file", path, "file is not a durable run log and was skipped")
			return nil
		}

		if !entry.Type().IsRegular() {
			report.addError("logs.non_regular_file", path, "run log path is not a regular file")
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			report.addError("logs.stat_failed", path, err.Error())
			return nil
		}

		report.CheckedFiles++
		report.CheckedBytes += info.Size()

		if _, err := runIDFromLogFileName(name); err != nil {
			report.addError("logs.invalid_name", path, err.Error())
			return nil
		}

		records, err := verifyRunLogFile(ctx, path)
		if err != nil {
			report.addError("logs.record_corrupt", path, err.Error())
			return nil
		}

		report.Records += records
		return nil
	})

	if err != nil {
		return report, err
	}

	return report.finish(), nil
}

func VerifyQueue(ctx context.Context, dir string) (Report, error) {
	report, ok := newReport(SurfaceQueue, dir)
	if !ok {
		return report, nil
	}

	if !report.requireDir(report.Path, "queue.path_missing") {
		return report.finish(), nil
	}

	entries, err := os.ReadDir(report.Path)
	if err != nil {
		report.addError("queue.read_dir_failed", report.Path, err.Error())
		return report.finish(), nil
	}

	for _, entry := range entries {
		if err := ctxErr(ctx); err != nil {
			return report, err
		}

		if entry.IsDir() {
			continue
		}

		path := filepath.Join(report.Path, entry.Name())
		if entry.Name() == queueLockFileName {
			continue
		}

		if entry.Name() == queueSnapshotFileName {
			verifyQueueSnapshot(ctx, path, &report)
			continue
		}

		if strings.HasPrefix(entry.Name(), queueWALSegmentPrefix) {
			if _, err := queueWALSegmentSequence(entry.Name()); err != nil {
				report.addError("queue.wal_invalid_name", path, err.Error())
				continue
			}

			verifyQueueWAL(ctx, path, &report)
			continue
		}

		if entry.Name() == queueSnapshotFileName+".tmp" {
			report.addWarning("queue.snapshot_temp", path, "snapshot temp file was present; verify a quiesced backup if this persists")
			continue
		}

		report.addWarning("queue.unrecognized_file", path, "file is not queue persistence state and was skipped")
	}

	return report.finish(), nil
}

func VerifyLogForwarderSpool(ctx context.Context, dir string) (Report, error) {
	report, ok := newReport(SurfaceLogForwarderSpool, dir)
	if !ok {
		return report, nil
	}

	if !report.requireDir(report.Path, "spool.path_missing") {
		return report.finish(), nil
	}

	err := filepath.WalkDir(report.Path, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			report.addError("spool.walk_error", path, err.Error())
			return nil
		}

		if err := ctxErr(ctx); err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		if strings.HasSuffix(entry.Name(), ".spool.quarantine") {
			report.addError("spool.quarantined_file", path, "quarantined spool file is not replayable")
			return nil
		}

		if strings.HasSuffix(entry.Name(), ".spool.tmp") {
			report.addWarning("spool.temp_file", path, "temporary spool file was present; verify a quiesced backup if this persists")
			return nil
		}

		if !strings.HasSuffix(entry.Name(), ".spool") {
			report.addWarning("spool.unrecognized_file", path, "file is not a log-forwarder spool and was skipped")
			return nil
		}

		verifyLogForwarderSpoolFile(path, &report)
		return nil
	})

	if err != nil {
		return report, err
	}

	return report.finish(), nil
}

func VerifyWorkerLogSpool(ctx context.Context, dir string) (Report, error) {
	report, ok := newReport(SurfaceWorkerLogSpool, dir)
	if !ok {
		return report, nil
	}

	if !report.requireDir(report.Path, "worker_spool.path_missing") {
		return report.finish(), nil
	}

	err := filepath.WalkDir(report.Path, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			report.addError("worker_spool.walk_error", path, err.Error())
			return nil
		}

		if err := ctxErr(ctx); err != nil {
			return err
		}

		if entry.IsDir() {
			return nil
		}

		if strings.HasSuffix(entry.Name(), ".spool.quarantine") {
			report.addError("worker_spool.quarantined_file", path, "quarantined pending log spool is not replayable")
			return nil
		}

		if !strings.HasSuffix(entry.Name(), ".spool") {
			report.addWarning("worker_spool.unrecognized_file", path, "file is not a worker pending log spool and was skipped")
			return nil
		}

		verifyWorkerLogSpoolFile(ctx, path, &report)
		return nil
	})

	if err != nil {
		return report, err
	}

	return report.finish(), nil
}

func newReport(surface, dir string) (Report, bool) {
	dir = strings.TrimSpace(dir)
	report := Report{
		Surface:   surface,
		Path:      dir,
		Status:    StatusOK,
		CheckedAt: time.Now().UTC(),
	}

	if dir == "" {
		report.addError("path.required", "", "storage directory is required")
		return report.finish(), false
	}

	return report, true
}

func (r *Report) requireDir(path, id string) bool {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			r.addError(id, path, "directory does not exist")
		} else {
			r.addError("path.stat_failed", path, err.Error())
		}

		return false
	}

	if !info.IsDir() {
		r.addError("path.not_directory", path, "path is not a directory")
		return false
	}

	return true
}

func (r *Report) finish() Report {
	if len(r.Errors) > 0 {
		r.Status = StatusFailed
	} else {
		r.Status = StatusOK
	}

	return *r
}

func (r *Report) addError(id, path, message string) {
	r.Errors = append(r.Errors, Finding{ID: id, Path: path, Message: message})
}

func (r *Report) addWarning(id, path, message string) {
	r.Warnings = append(r.Warnings, Finding{ID: id, Path: path, Message: message})
}

func artifactDigestFromPath(root, path string) (string, error) {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return "", err
	}

	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) != 3 {
		return "", fmt.Errorf("expected blobs/sha256/<first2>/<next2>/<digest>.blob layout")
	}

	if len(parts[0]) != 2 || len(parts[1]) != 2 {
		return "", fmt.Errorf("artifact digest prefix directories must be two hex characters")
	}

	name := parts[2]
	if !strings.HasSuffix(name, artifactBlobFileSuffix) {
		return "", fmt.Errorf("artifact blob filename must end with %s", artifactBlobFileSuffix)
	}

	digest := strings.TrimSuffix(name, artifactBlobFileSuffix)
	if len(digest) != sha256.Size*2 {
		return "", fmt.Errorf("artifact digest must be %d hex characters", sha256.Size*2)
	}

	if _, err := hex.DecodeString(digest); err != nil {
		return "", fmt.Errorf("artifact digest is not valid hex: %w", err)
	}

	if digest != strings.ToLower(digest) {
		return "", fmt.Errorf("artifact digest must be lowercase hex")
	}

	if parts[0] != digest[:2] || parts[1] != digest[2:4] {
		return "", fmt.Errorf("artifact prefix directories do not match digest")
	}

	return digest, nil
}

func hashFile(ctx context.Context, path string, h hash.Hash) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	buf := make([]byte, 128*1024)
	for {
		if err := ctxErr(ctx); err != nil {
			return "", err
		}

		n, err := f.Read(buf)
		if n > 0 {
			if _, writeErr := h.Write(buf[:n]); writeErr != nil {
				return "", writeErr
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return "", err
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

func runIDFromLogFileName(name string) (string, error) {
	encoded := strings.TrimSuffix(name, logFileSuffix)
	if encoded == "" || encoded == name {
		return "", fmt.Errorf("run log filename must end with %s", logFileSuffix)
	}

	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", fmt.Errorf("run log filename is not base64url without padding: %w", err)
	}

	if len(decoded) == 0 {
		return "", fmt.Errorf("run log filename decodes to an empty run id")
	}

	return string(decoded), nil
}

func verifyRunLogFile(ctx context.Context, path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var records int64
	for {
		if err := ctxErr(ctx); err != nil {
			return records, err
		}

		var lengthBuf [logEntryRecordLengthSize]byte
		if _, err := io.ReadFull(f, lengthBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return records, nil
			}

			return records, err
		}

		bodyLen := binary.LittleEndian.Uint32(lengthBuf[:])
		if bodyLen < logEntryRecordHeaderSize {
			return records, fmt.Errorf("record body length %d is smaller than header length %d", bodyLen, logEntryRecordHeaderSize)
		}

		if uint64(bodyLen) > uint64(int(^uint(0)>>1)) {
			return records, fmt.Errorf("record body length %d exceeds addressable buffer size", bodyLen)
		}

		body := make([]byte, int(bodyLen))
		if _, err := io.ReadFull(f, body); err != nil {
			return records, err
		}

		var suffix [logEntryRecordLengthSize]byte
		if _, err := io.ReadFull(f, suffix[:]); err != nil {
			return records, err
		}

		if got := binary.LittleEndian.Uint32(suffix[:]); got != bodyLen {
			return records, fmt.Errorf("record length suffix %d does not match prefix %d", got, bodyLen)
		}

		if err := verifyRunLogRecordBody(body); err != nil {
			return records, err
		}

		records++
	}
}

func verifyRunLogRecordBody(body []byte) error {
	if len(body) < logEntryRecordHeaderSize {
		return fmt.Errorf("record body length %d is smaller than header length %d", len(body), logEntryRecordHeaderSize)
	}

	dataLen := binary.LittleEndian.Uint32(body[28:32])
	if dataLen != uint32(len(body)-logEntryRecordHeaderSize) {
		return fmt.Errorf("record data length %d does not match body payload length %d", dataLen, len(body)-logEntryRecordHeaderSize)
	}

	nsec := binary.LittleEndian.Uint32(body[8:12])
	if nsec > uint32(time.Second-time.Nanosecond) {
		return fmt.Errorf("record timestamp nanosecond value %d is invalid", nsec)
	}

	return nil
}

func verifyQueueSnapshot(ctx context.Context, path string, report *Report) {
	info, err := os.Stat(path)
	if err != nil {
		report.addError("queue.snapshot_stat_failed", path, err.Error())
		return
	}

	report.CheckedFiles++
	report.CheckedBytes += info.Size()

	f, err := os.Open(path)
	if err != nil {
		report.addError("queue.snapshot_open_failed", path, err.Error())
		return
	}
	defer f.Close()

	var snap queueSnapshot
	if err := json.NewDecoder(f).Decode(&snap); err != nil {
		report.addError("queue.snapshot_corrupt", path, err.Error())
		return
	}

	for i, payload := range snap.Jobs {
		if err := ctxErr(ctx); err != nil {
			report.addError("queue.snapshot_interrupted", path, err.Error())
			return
		}

		if err := verifyQueueJobPayload(payload); err != nil {
			report.addError("queue.snapshot_job_corrupt", path, fmt.Sprintf("jobs[%d]: %v", i, err))
		}
	}

	for i, row := range snap.Inflight {
		if strings.TrimSpace(row.DeliveryID) == "" {
			report.addError("queue.snapshot_delivery_id_missing", path, fmt.Sprintf("inflight[%d] delivery_id is required", i))
		}

		if err := verifyQueueJobPayload(row.Job); err != nil {
			report.addError("queue.snapshot_job_corrupt", path, fmt.Sprintf("inflight[%d]: %v", i, err))
		}
	}

	for i, row := range snap.DeadLetter {
		if strings.TrimSpace(row.DeliveryID) == "" {
			report.addError("queue.snapshot_delivery_id_missing", path, fmt.Sprintf("dead_letter[%d] delivery_id is required", i))
		}

		if err := verifyQueueJobPayload(row.Job); err != nil {
			report.addError("queue.snapshot_job_corrupt", path, fmt.Sprintf("dead_letter[%d]: %v", i, err))
		}
	}

	report.Records++
}

func verifyQueueWAL(ctx context.Context, path string, report *Report) {
	info, err := os.Stat(path)
	if err != nil {
		report.addError("queue.wal_stat_failed", path, err.Error())
		return
	}

	report.CheckedFiles++
	report.CheckedBytes += info.Size()

	f, err := os.Open(path)
	if err != nil {
		report.addError("queue.wal_open_failed", path, err.Error())
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	lineNumber := 0
	for scanner.Scan() {
		if err := ctxErr(ctx); err != nil {
			report.addError("queue.wal_interrupted", path, err.Error())
			return
		}

		lineNumber++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var rec queueWALRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			report.addError("queue.wal_record_corrupt", path, fmt.Sprintf("line %d: %v", lineNumber, err))
			continue
		}

		if err := verifyQueueWALRecord(rec); err != nil {
			report.addError("queue.wal_record_corrupt", path, fmt.Sprintf("line %d: %v", lineNumber, err))
			continue
		}

		report.Records++
	}

	if err := scanner.Err(); err != nil {
		report.addError("queue.wal_scan_failed", path, err.Error())
	}
}

func verifyQueueWALRecord(rec queueWALRecord) error {
	if rec.Index == 0 {
		return fmt.Errorf("record index is required")
	}

	switch rec.Type {
	case "enqueue", "deliver", "requeue_expired", "drop_expired", "dlq", "dlq_requeue":
		return verifyQueueJobPayload(rec.Job)
	case "ack":
		if strings.TrimSpace(rec.DeliveryID) == "" {
			return fmt.Errorf("ack delivery_id is required")
		}

		return nil
	default:
		return fmt.Errorf("unknown wal record type %q", rec.Type)
	}
}

func verifyQueueJobPayload(payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("job payload is required")
	}

	var req api.JobRequest
	if err := proto.Unmarshal(payload, &req); err != nil {
		return fmt.Errorf("unmarshal job request: %w", err)
	}

	return nil
}

func queueWALSegmentSequence(name string) (int, error) {
	if !strings.HasPrefix(name, queueWALSegmentPrefix) {
		return 0, fmt.Errorf("invalid wal segment name %q", name)
	}

	part := strings.TrimPrefix(name, queueWALSegmentPrefix)
	if part == "" {
		return 0, fmt.Errorf("wal segment sequence is required")
	}

	seq, err := strconv.Atoi(part)
	if err != nil {
		return 0, fmt.Errorf("parse wal segment sequence %q: %w", name, err)
	}

	if seq <= 0 {
		return 0, fmt.Errorf("wal segment sequence must be positive")
	}

	return seq, nil
}

func verifyLogForwarderSpoolFile(path string, report *Report) {
	info, err := os.Stat(path)
	if err != nil {
		report.addError("spool.stat_failed", path, err.Error())
		return
	}

	report.CheckedFiles++
	report.CheckedBytes += info.Size()

	reader, err := logforwarder.NewSpoolReader(path)
	if err != nil {
		report.addError("spool.open_failed", path, err.Error())
		return
	}
	defer reader.Close()

	for {
		chunks, err := reader.ReadBatch()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			report.addError("spool.batch_corrupt", path, err.Error())
			return
		}

		report.Batches++
		report.Records += int64(len(chunks))
	}
}

func verifyWorkerLogSpoolFile(ctx context.Context, path string, report *Report) {
	info, err := os.Stat(path)
	if err != nil {
		report.addError("worker_spool.stat_failed", path, err.Error())
		return
	}

	report.CheckedFiles++
	report.CheckedBytes += info.Size()

	f, err := os.Open(path)
	if err != nil {
		report.addError("worker_spool.open_failed", path, err.Error())
		return
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	for {
		if err := ctxErr(ctx); err != nil {
			report.addError("worker_spool.interrupted", path, err.Error())
			return
		}

		payload, _, err := logrecord.ReadWithMax(reader, defaultWorkerSpoolMaxPayload)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return
			}

			report.addError("worker_spool.record_corrupt", path, err.Error())
			return
		}

		var chunk api.LogChunk
		if err := proto.Unmarshal(payload, &chunk); err != nil {
			report.addError("worker_spool.chunk_corrupt", path, fmt.Sprintf("unmarshal chunk: %v", err))
			return
		}

		if strings.TrimSpace(chunk.GetRunId()) == "" {
			report.addError("worker_spool.run_id_missing", path, "spooled log chunk is missing run_id")
			return
		}

		report.Records++
	}
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}

	return ctx.Err()
}
