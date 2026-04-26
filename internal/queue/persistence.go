package queue

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"

	"google.golang.org/protobuf/proto"
)

const (
	snapshotFileName          = "queue.snapshot"
	walSegmentPrefix          = "queue.wal."
	defaultSegmentMaxBytes    = 4 * 1024 * 1024
	defaultRetainTailSegments = 2
)

type walRecordType string

const (
	walRecordEnqueue    walRecordType = "enqueue"
	walRecordDeliver    walRecordType = "deliver"
	walRecordAck        walRecordType = "ack"
	walRecordRequeue    walRecordType = "requeue_expired"
	walRecordDLQ        walRecordType = "dlq"
	walRecordDLQRequeue walRecordType = "dlq_requeue"
)

type walRecord struct {
	Index         uint64        `json:"index"`
	Type          walRecordType `json:"type"`
	Job           []byte        `json:"job,omitempty"`
	DeliveryID    string        `json:"delivery_id,omitempty"`
	LeaseUntilUTC int64         `json:"lease_until_utc,omitempty"`
	AttemptCount  int           `json:"attempt_count,omitempty"`
}

type inflightSnapshot struct {
	DeliveryID    string `json:"delivery_id"`
	Job           []byte `json:"job"`
	LeaseUntilUTC int64  `json:"lease_until_utc"`
	AttemptCount  int    `json:"attempt_count"`
}

type deadLetterSnapshot struct {
	DeliveryID   string `json:"delivery_id"`
	Job          []byte `json:"job"`
	AttemptCount int    `json:"attempt_count"`
}

type queueSnapshot struct {
	LastAppliedIndex uint64               `json:"last_applied_index"`
	Jobs             [][]byte             `json:"jobs"`
	Inflight         []inflightSnapshot   `json:"inflight"`
	DeadLetter       []deadLetterSnapshot `json:"dead_letter,omitempty"`
	JobAttempts      map[string]int       `json:"job_attempts,omitempty"`
}

type inflightDelivery struct {
	Job          *api.Job
	LeaseUntil   time.Time
	AttemptCount int
}

type persistenceStore struct {
	dir                string
	snapshotPath       string
	nextIndex          uint64
	snapshotEvery      int
	opSinceSnapshot    int
	segmentMaxBytes    int64
	retainTailSegments int
	log                interfaces.Logger
}

type queueState struct {
	jobs             []*api.Job
	inflight         map[string]inflightDelivery
	deadLetter       []deadLetterItem
	jobAttempts      map[string]int
	lastAppliedIndex uint64
}

type snapshotState struct {
	pending     []*api.Job
	inflight    map[string]inflightDelivery
	deadLetter  []deadLetterItem
	jobAttempts map[string]int
}

func newPersistenceStore(dir string, snapshotEvery int, segmentMaxBytes int64, retainTailSegments int, log interfaces.Logger) (*persistenceStore, *queueState, error) {
	if dir == "" {
		return nil, &queueState{inflight: make(map[string]inflightDelivery), deadLetter: make([]deadLetterItem, 0), jobAttempts: make(map[string]int)}, nil
	}

	if snapshotEvery <= 0 {
		snapshotEvery = 128
	}
	if segmentMaxBytes <= 0 {
		segmentMaxBytes = defaultSegmentMaxBytes
	}

	if retainTailSegments <= 0 {
		retainTailSegments = defaultRetainTailSegments
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create queue persistence dir: %w", err)
	}

	p := &persistenceStore{
		dir:                dir,
		snapshotPath:       filepath.Join(dir, snapshotFileName),
		nextIndex:          1,
		snapshotEvery:      snapshotEvery,
		segmentMaxBytes:    segmentMaxBytes,
		retainTailSegments: retainTailSegments,
		log:                log,
	}

	state, err := p.loadState()
	if err != nil {
		return nil, nil, err
	}

	return p, state, nil
}

func (p *persistenceStore) loadState() (*queueState, error) {
	state := &queueState{inflight: make(map[string]inflightDelivery)}

	snap, err := p.loadSnapshot()
	if err != nil {
		return nil, err
	}

	if snap != nil {
		jobs, err := decodeJobs(snap.Jobs)
		if err != nil {
			return nil, fmt.Errorf("decode snapshot jobs: %w", err)
		}

		state.jobs = jobs
		inflight, err := decodeInflight(snap.Inflight)
		if err != nil {
			return nil, fmt.Errorf("decode snapshot inflight: %w", err)
		}

		state.inflight = inflight
		state.deadLetter, err = decodeDeadLetter(snap.DeadLetter)
		if err != nil {
			return nil, fmt.Errorf("decode snapshot dead letter: %w", err)
		}

		if snap.JobAttempts != nil {
			state.jobAttempts = snap.JobAttempts
		}

		state.lastAppliedIndex = snap.LastAppliedIndex
		p.nextIndex = snap.LastAppliedIndex + 1
	}

	if err := p.replayWAL(state); err != nil {
		return nil, err
	}

	return state, nil
}

func (p *persistenceStore) loadSnapshot() (*queueSnapshot, error) {
	f, err := os.Open(p.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open snapshot: %w", err)
	}
	defer f.Close()

	var snap queueSnapshot
	if err := json.NewDecoder(f).Decode(&snap); err != nil {
		return nil, fmt.Errorf("decode snapshot: %w", err)
	}

	return &snap, nil
}

func (p *persistenceStore) replayWAL(state *queueState) error {
	segments, err := p.listWALSegments()
	if err != nil {
		return err
	}

	for _, seg := range segments {
		f, err := os.Open(seg)
		if err != nil {
			return fmt.Errorf("open wal segment %s: %w", seg, err)
		}

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 4096), 1024*1024)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}

			var rec walRecord
			if err := json.Unmarshal(line, &rec); err != nil {
				if p.log != nil {
					p.log.Error("Skipping corrupted WAL record in %s: %v", seg, err)
				}
				continue
			}

			if rec.Index <= state.lastAppliedIndex {
				continue
			}

			if err := applyRecord(state, rec); err != nil {
				_ = f.Close()
				return err
			}

			state.lastAppliedIndex = rec.Index
			if rec.Index >= p.nextIndex {
				p.nextIndex = rec.Index + 1
			}
		}

		if err := scanner.Err(); err != nil {
			_ = f.Close()
			return fmt.Errorf("scan wal segment %s: %w", seg, err)
		}

		if err := f.Close(); err != nil {
			return fmt.Errorf("close wal segment %s: %w", seg, err)
		}
	}

	return nil
}

func (p *persistenceStore) appendEnqueue(job *api.Job, state snapshotState) error {
	payload, err := proto.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal enqueue job: %w", err)
	}

	return p.appendRecord(walRecord{Type: walRecordEnqueue, Job: payload}, state)
}

func (p *persistenceStore) appendDeliver(deliveryID string, leaseUntil time.Time, attemptCount int, state snapshotState) error {
	return p.appendRecord(walRecord{Type: walRecordDeliver, DeliveryID: deliveryID, LeaseUntilUTC: leaseUntil.UTC().Unix(), AttemptCount: attemptCount}, state)
}

func (p *persistenceStore) appendDLQ(deliveryID string, job *api.Job, attemptCount int, state snapshotState) error {
	payload, err := proto.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal dlq job: %w", err)
	}

	return p.appendRecord(walRecord{Type: walRecordDLQ, DeliveryID: deliveryID, Job: payload, AttemptCount: attemptCount}, state)
}

func (p *persistenceStore) appendDLQRequeue(deliveryID string, job *api.Job, state snapshotState) error {
	payload, err := proto.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal dlq requeue job: %w", err)
	}

	return p.appendRecord(walRecord{Type: walRecordDLQRequeue, DeliveryID: deliveryID, Job: payload}, state)
}

func (p *persistenceStore) appendAck(deliveryID string, state snapshotState) error {
	return p.appendRecord(walRecord{Type: walRecordAck, DeliveryID: deliveryID}, state)
}

func (p *persistenceStore) appendRequeueExpired(deliveryID string, job *api.Job, state snapshotState) error {
	payload, err := proto.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal requeue job: %w", err)
	}

	return p.appendRecord(walRecord{Type: walRecordRequeue, DeliveryID: deliveryID, Job: payload}, state)
}

func (p *persistenceStore) appendRecord(rec walRecord, state snapshotState) error {
	rec.Index = p.nextIndex
	p.nextIndex++

	segPath, segSeq, err := p.ensureActiveSegment()
	if err != nil {
		return err
	}

	f, err := os.OpenFile(segPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open wal segment for append %s: %w", segPath, err)
	}

	enc, err := json.Marshal(rec)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("encode wal record: %w", err)
	}

	if _, err := f.Write(append(enc, '\n')); err != nil {
		_ = f.Close()
		return fmt.Errorf("write wal record: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync wal segment: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close wal segment: %w", err)
	}

	if err := p.rotateIfNeeded(segPath, segSeq); err != nil {
		return err
	}

	p.opSinceSnapshot++
	if p.opSinceSnapshot >= p.snapshotEvery {
		if err := p.writeSnapshotAndCompact(rec.Index, state); err != nil {
			return err
		}

		p.opSinceSnapshot = 0
	}

	return nil
}

func (p *persistenceStore) writeSnapshotAndCompact(lastApplied uint64, state snapshotState) error {
	jobs, err := encodeJobs(state.pending)
	if err != nil {
		return fmt.Errorf("encode pending queue for snapshot: %w", err)
	}

	inflight, err := encodeInflight(state.inflight)
	if err != nil {
		return fmt.Errorf("encode inflight queue for snapshot: %w", err)
	}

	deadLetter, err := encodeDeadLetter(state.deadLetter)
	if err != nil {
		return fmt.Errorf("encode dead letter queue for snapshot: %w", err)
	}

	snap := queueSnapshot{LastAppliedIndex: lastApplied, Jobs: jobs, Inflight: inflight, DeadLetter: deadLetter, JobAttempts: state.jobAttempts}
	tmp := p.snapshotPath + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open snapshot temp file: %w", err)
	}

	enc := json.NewEncoder(f)
	if err := enc.Encode(snap); err != nil {
		_ = f.Close()
		return fmt.Errorf("encode snapshot: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync snapshot: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}

	if err := os.Rename(tmp, p.snapshotPath); err != nil {
		return fmt.Errorf("promote snapshot: %w", err)
	}

	if err := p.compactSegments(lastApplied); err != nil {
		return err
	}

	return nil
}

func (p *persistenceStore) compactSegments(lastApplied uint64) error {
	segments, err := p.listWALSegments()
	if err != nil {
		return err
	}

	if len(segments) <= p.retainTailSegments {
		return nil
	}

	candidates := segments[:len(segments)-p.retainTailSegments]
	for _, seg := range candidates {
		_, maxIdx, count, err := p.segmentBounds(seg)
		if err != nil {
			return err
		}

		if count == 0 || maxIdx <= lastApplied {
			if err := os.Remove(seg); err != nil {
				return fmt.Errorf("remove compacted wal segment %s: %w", seg, err)
			}
		}
	}

	return nil
}

func (p *persistenceStore) ensureActiveSegment() (string, int, error) {
	segments, err := p.listWALSegments()
	if err != nil {
		return "", 0, err
	}

	if len(segments) == 0 {
		path := p.segmentPath(1)
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			return "", 0, fmt.Errorf("create wal segment %s: %w", path, err)
		}

		return path, 1, nil
	}

	last := segments[len(segments)-1]
	seq, err := p.segmentSeq(last)
	if err != nil {
		return "", 0, err
	}

	return last, seq, nil
}

func (p *persistenceStore) rotateIfNeeded(path string, seq int) error {
	st, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat wal segment %s: %w", path, err)
	}

	if st.Size() < p.segmentMaxBytes {
		return nil
	}

	next := p.segmentPath(seq + 1)
	if _, err := os.Stat(next); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat next wal segment %s: %w", next, err)
	}

	if err := os.WriteFile(next, nil, 0o644); err != nil {
		return fmt.Errorf("create next wal segment %s: %w", next, err)
	}

	return nil
}

func (p *persistenceStore) listWALSegments() ([]string, error) {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, fmt.Errorf("read wal dir: %w", err)
	}

	segments := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !strings.HasPrefix(name, walSegmentPrefix) {
			continue
		}

		path := filepath.Join(p.dir, name)
		if _, err := p.segmentSeq(path); err != nil {
			continue
		}

		segments = append(segments, path)
	}

	sort.Slice(segments, func(i, j int) bool {
		si, _ := p.segmentSeq(segments[i])
		sj, _ := p.segmentSeq(segments[j])
		return si < sj
	})

	return segments, nil
}

func (p *persistenceStore) segmentSeq(path string) (int, error) {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, walSegmentPrefix) {
		return 0, fmt.Errorf("invalid wal segment name %q", base)
	}

	part := strings.TrimPrefix(base, walSegmentPrefix)
	seq, err := strconv.Atoi(part)
	if err != nil {
		return 0, fmt.Errorf("parse wal segment sequence %q: %w", base, err)
	}

	return seq, nil
}

func (p *persistenceStore) segmentPath(seq int) string {
	return filepath.Join(p.dir, fmt.Sprintf("%s%06d", walSegmentPrefix, seq))
}

func (p *persistenceStore) segmentBounds(path string) (uint64, uint64, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("open wal segment for bounds %s: %w", path, err)
	}
	defer f.Close()

	var minIdx uint64
	var maxIdx uint64
	count := 0

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return 0, 0, 0, fmt.Errorf("decode wal record in %s: %w", path, err)
		}

		if count == 0 {
			minIdx = rec.Index
			maxIdx = rec.Index
		} else {
			if rec.Index < minIdx {
				minIdx = rec.Index
			}

			if rec.Index > maxIdx {
				maxIdx = rec.Index
			}
		}

		count++
	}

	if err := scanner.Err(); err != nil {
		return 0, 0, 0, fmt.Errorf("scan wal segment for bounds %s: %w", path, err)
	}

	return minIdx, maxIdx, count, nil
}

func encodeJobs(jobs []*api.Job) ([][]byte, error) {
	encoded := make([][]byte, 0, len(jobs))
	for _, job := range jobs {
		payload, err := proto.Marshal(job)
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, payload)
	}

	return encoded, nil
}

func decodeJobs(records [][]byte) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0, len(records))
	for _, payload := range records {
		var job api.Job
		if err := proto.Unmarshal(payload, &job); err != nil {
			return nil, err
		}
		jobs = append(jobs, &job)
	}

	return jobs, nil
}

func encodeInflight(inflight map[string]inflightDelivery) ([]inflightSnapshot, error) {
	out := make([]inflightSnapshot, 0, len(inflight))
	for deliveryID, item := range inflight {
		payload, err := proto.Marshal(item.Job)
		if err != nil {
			return nil, err
		}

		out = append(out, inflightSnapshot{
			DeliveryID:    deliveryID,
			Job:           payload,
			LeaseUntilUTC: item.LeaseUntil.UTC().Unix(),
			AttemptCount:  item.AttemptCount,
		})
	}

	return out, nil
}

func decodeInflight(rows []inflightSnapshot) (map[string]inflightDelivery, error) {
	out := make(map[string]inflightDelivery, len(rows))
	for _, row := range rows {
		var job api.Job
		if err := proto.Unmarshal(row.Job, &job); err != nil {
			return nil, err
		}
		out[row.DeliveryID] = inflightDelivery{
			Job:          &job,
			LeaseUntil:   time.Unix(row.LeaseUntilUTC, 0).UTC(),
			AttemptCount: row.AttemptCount,
		}
	}

	return out, nil
}

func encodeDeadLetter(items []deadLetterItem) ([]deadLetterSnapshot, error) {
	out := make([]deadLetterSnapshot, 0, len(items))
	for _, item := range items {
		payload, err := proto.Marshal(item.job)
		if err != nil {
			return nil, err
		}

		out = append(out, deadLetterSnapshot{
			DeliveryID:   item.deliveryID,
			Job:          payload,
			AttemptCount: item.attemptCount,
		})
	}

	return out, nil
}

func decodeDeadLetter(rows []deadLetterSnapshot) ([]deadLetterItem, error) {
	out := make([]deadLetterItem, 0, len(rows))
	for _, row := range rows {
		var job api.Job
		if err := proto.Unmarshal(row.Job, &job); err != nil {
			return nil, err
		}
		out = append(out, deadLetterItem{
			deliveryID:   row.DeliveryID,
			job:          &job,
			attemptCount: row.AttemptCount,
		})
	}

	return out, nil
}

func applyRecord(state *queueState, rec walRecord) error {
	switch rec.Type {
	case walRecordEnqueue:
		var job api.Job
		if err := proto.Unmarshal(rec.Job, &job); err != nil {
			return fmt.Errorf("unmarshal enqueue payload: %w", err)
		}

		state.jobs = append(state.jobs, &job)
		return nil
	case walRecordDeliver:
		if len(state.jobs) == 0 {
			return fmt.Errorf("replay deliver on empty pending queue")
		}

		job := state.jobs[0]
		state.jobs = state.jobs[1:]
		state.inflight[rec.DeliveryID] = inflightDelivery{
			Job:          job,
			LeaseUntil:   time.Unix(rec.LeaseUntilUTC, 0).UTC(),
			AttemptCount: rec.AttemptCount,
		}

		if state.jobAttempts == nil {
			state.jobAttempts = make(map[string]int)
		}

		state.jobAttempts[job.GetId()] = rec.AttemptCount
		return nil
	case walRecordAck:
		if item, ok := state.inflight[rec.DeliveryID]; ok {
			delete(state.jobAttempts, item.Job.GetId())
		}

		delete(state.inflight, rec.DeliveryID)
		return nil
	case walRecordRequeue:
		var job api.Job
		if err := proto.Unmarshal(rec.Job, &job); err != nil {
			return fmt.Errorf("unmarshal requeue payload: %w", err)
		}

		delete(state.inflight, rec.DeliveryID)
		state.jobs = append(state.jobs, &job)
		if state.jobAttempts == nil {
			state.jobAttempts = make(map[string]int)
		}

		state.jobAttempts[job.GetId()]++
		return nil
	case walRecordDLQ:
		var job api.Job
		if err := proto.Unmarshal(rec.Job, &job); err != nil {
			return fmt.Errorf("unmarshal dlq payload: %w", err)
		}

		delete(state.inflight, rec.DeliveryID)
		if state.jobAttempts == nil {
			state.jobAttempts = make(map[string]int)
		}

		state.jobAttempts[job.GetId()] = rec.AttemptCount
		state.deadLetter = append(state.deadLetter, deadLetterItem{
			deliveryID:   rec.DeliveryID,
			job:          &job,
			attemptCount: rec.AttemptCount,
		})
		return nil
	case walRecordDLQRequeue:
		var job api.Job
		if err := proto.Unmarshal(rec.Job, &job); err != nil {
			return fmt.Errorf("unmarshal dlq requeue payload: %w", err)
		}

		state.jobs = append(state.jobs, &job)
		if len(state.deadLetter) > 0 {
			filtered := make([]deadLetterItem, 0, len(state.deadLetter))
			for _, item := range state.deadLetter {
				if item.deliveryID == rec.DeliveryID {
					continue
				}
				filtered = append(filtered, item)
			}
			state.deadLetter = filtered
		}

		if state.jobAttempts == nil {
			state.jobAttempts = make(map[string]int)
		}

		delete(state.jobAttempts, job.GetId())
		return nil
	default:
		return fmt.Errorf("unknown wal record type: %q", rec.Type)
	}
}
