package queue

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	api "vectis/api/gen/go"

	"google.golang.org/protobuf/proto"
)

const (
	walFileName      = "queue.wal"
	snapshotFileName = "queue.snapshot"
)

type walRecordType string

const (
	walRecordEnqueue walRecordType = "enqueue"
	walRecordDeliver walRecordType = "deliver"
	walRecordAck     walRecordType = "ack"
	walRecordRequeue walRecordType = "requeue_expired"
)

type walRecord struct {
	Index         uint64        `json:"index"`
	Type          walRecordType `json:"type"`
	Job           []byte        `json:"job,omitempty"`
	DeliveryID    string        `json:"delivery_id,omitempty"`
	LeaseUntilUTC int64         `json:"lease_until_utc,omitempty"`
}

type inflightSnapshot struct {
	DeliveryID    string `json:"delivery_id"`
	Job           []byte `json:"job"`
	LeaseUntilUTC int64  `json:"lease_until_utc"`
}

type queueSnapshot struct {
	LastAppliedIndex uint64             `json:"last_applied_index"`
	Jobs             [][]byte           `json:"jobs"`
	Inflight         []inflightSnapshot `json:"inflight"`
}

type inflightDelivery struct {
	Job        *api.Job
	LeaseUntil time.Time
}

type persistenceStore struct {
	walPath         string
	snapshotPath    string
	nextIndex       uint64
	snapshotEvery   int
	opSinceSnapshot int
}

type queueState struct {
	jobs             []*api.Job
	inflight         map[string]inflightDelivery
	lastAppliedIndex uint64
}

type snapshotState struct {
	pending  []*api.Job
	inflight map[string]inflightDelivery
}

func newPersistenceStore(dir string, snapshotEvery int) (*persistenceStore, *queueState, error) {
	if dir == "" {
		return nil, &queueState{inflight: make(map[string]inflightDelivery)}, nil
	}

	if snapshotEvery <= 0 {
		snapshotEvery = 128
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create queue persistence dir: %w", err)
	}

	p := &persistenceStore{
		walPath:       filepath.Join(dir, walFileName),
		snapshotPath:  filepath.Join(dir, snapshotFileName),
		nextIndex:     1,
		snapshotEvery: snapshotEvery,
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
	f, err := os.Open(p.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open wal: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var rec walRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("decode wal record: %w", err)
		}

		if rec.Index <= state.lastAppliedIndex {
			continue
		}

		if err := applyRecord(state, rec); err != nil {
			return err
		}

		state.lastAppliedIndex = rec.Index
		if rec.Index >= p.nextIndex {
			p.nextIndex = rec.Index + 1
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan wal: %w", err)
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

func (p *persistenceStore) appendDeliver(deliveryID string, leaseUntil time.Time, state snapshotState) error {
	return p.appendRecord(walRecord{Type: walRecordDeliver, DeliveryID: deliveryID, LeaseUntilUTC: leaseUntil.UTC().Unix()}, state)
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

	f, err := os.OpenFile(p.walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open wal for append: %w", err)
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
		return fmt.Errorf("sync wal: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close wal: %w", err)
	}

	p.opSinceSnapshot++
	if p.opSinceSnapshot >= p.snapshotEvery {
		if err := p.writeSnapshotAndTruncate(rec.Index, state); err != nil {
			return err
		}
		p.opSinceSnapshot = 0
	}

	return nil
}

func (p *persistenceStore) writeSnapshotAndTruncate(lastApplied uint64, state snapshotState) error {
	jobs, err := encodeJobs(state.pending)
	if err != nil {
		return fmt.Errorf("encode pending queue for snapshot: %w", err)
	}

	inflight, err := encodeInflight(state.inflight)
	if err != nil {
		return fmt.Errorf("encode inflight queue for snapshot: %w", err)
	}

	snap := queueSnapshot{LastAppliedIndex: lastApplied, Jobs: jobs, Inflight: inflight}
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

	if err := os.WriteFile(p.walPath, nil, 0o644); err != nil {
		return fmt.Errorf("truncate wal: %w", err)
	}

	return nil
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
		out[row.DeliveryID] = inflightDelivery{Job: &job, LeaseUntil: time.Unix(row.LeaseUntilUTC, 0).UTC()}
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
			Job:        job,
			LeaseUntil: time.Unix(rec.LeaseUntilUTC, 0).UTC(),
		}

		return nil
	case walRecordAck:
		delete(state.inflight, rec.DeliveryID)
		return nil
	case walRecordRequeue:
		var job api.Job
		if err := proto.Unmarshal(rec.Job, &job); err != nil {
			return fmt.Errorf("unmarshal requeue payload: %w", err)
		}

		delete(state.inflight, rec.DeliveryID)
		state.jobs = append(state.jobs, &job)
		return nil
	default:
		return fmt.Errorf("unknown wal record type: %q", rec.Type)
	}
}
