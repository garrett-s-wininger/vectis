package queue

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
)

func TestQueueProperty_PersistedOperationTracesPreservePendingFIFO(t *testing.T) {
	skipQueuePropertyInShort(t)

	var lastErr error
	var lastTrace []byte
	prop := func(raw []byte) bool {
		trace := boundedPropertyTrace(raw, 64)
		err := checkPersistedQueueTrace(trace)
		if err != nil {
			lastErr = err
			lastTrace = append([]byte(nil), trace...)
			return false
		}

		return true
	}

	if err := quick.Check(prop, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatalf("queue persistence property failed: %v\ntrace=%v\nreason=%v", err, lastTrace, lastErr)
	}
}

func TestQueueProperty_TryDequeueReturnsEarliestEligibleIsolation(t *testing.T) {
	skipQueuePropertyInShort(t)

	var lastErr error
	var lastTrace []byte
	prop := func(raw []byte) bool {
		trace := boundedPropertyTrace(raw, 48)
		err := checkIsolationEligibilityTrace(trace)
		if err != nil {
			lastErr = err
			lastTrace = append([]byte(nil), trace...)
			return false
		}

		return true
	}

	if err := quick.Check(prop, &quick.Config{MaxCount: 100}); err != nil {
		t.Fatalf("queue isolation property failed: %v\ntrace=%v\nreason=%v", err, lastTrace, lastErr)
	}
}

func skipQueuePropertyInShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("queue property tests run under mage testProperty")
	}
}

func boundedPropertyTrace(raw []byte, maxLen int) []byte {
	if len(raw) <= maxLen {
		return raw
	}

	return raw[:maxLen]
}

func checkPersistedQueueTrace(trace []byte) error {
	ctx := context.Background()
	dir, cleanup, err := propertyTempDir()
	if err != nil {
		return err
	}
	defer cleanup()

	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  3,
		WALSegmentMax:  256,
		WALRetainTail:  2,
		DeliveryTTL:    time.Hour,
	}, nil)

	if err != nil {
		return fmt.Errorf("new queue: %w", err)
	}

	defer func() {
		_ = closeQueueServiceForProperty(svc)
	}()

	model := queueTraceModel{inflight: make(map[string]string)}
	for step, op := range trace {
		switch op % 4 {
		case 0:
			if err := model.enqueue(ctx, svc); err != nil {
				return fmt.Errorf("step %d enqueue: %w", step, err)
			}
		case 1:
			if err := model.tryDequeue(ctx, svc); err != nil {
				return fmt.Errorf("step %d trydequeue: %w", step, err)
			}
		case 2:
			if err := model.ack(ctx, svc, op); err != nil {
				return fmt.Errorf("step %d ack: %w", step, err)
			}
		case 3:
			if err := closeQueueServiceForProperty(svc); err != nil {
				return fmt.Errorf("step %d close before restart: %w", step, err)
			}
			svc, err = NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
				PersistenceDir: dir,
				SnapshotEvery:  3,
				WALSegmentMax:  256,
				WALRetainTail:  2,
				DeliveryTTL:    time.Hour,
			}, nil)
			if err != nil {
				return fmt.Errorf("step %d restart: %w", step, err)
			}
		}
	}

	if err := closeQueueServiceForProperty(svc); err != nil {
		return fmt.Errorf("close before final restart: %w", err)
	}

	svc, err = NewQueueServiceWithOptions(noopLogger{}, QueueOptions{
		PersistenceDir: dir,
		SnapshotEvery:  3,
		WALSegmentMax:  256,
		WALRetainTail:  2,
		DeliveryTTL:    time.Hour,
	}, nil)

	if err != nil {
		return fmt.Errorf("final restart: %w", err)
	}

	for i, want := range model.pending {
		got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
		if err != nil {
			return fmt.Errorf("drain %d: %w", i, err)
		}
		if got == nil {
			return fmt.Errorf("drain %d: got empty queue, want %s", i, want)
		}
		if got.GetJob().GetId() != want {
			return fmt.Errorf("drain %d: got %s, want %s", i, got.GetJob().GetId(), want)
		}
	}

	got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		return fmt.Errorf("final empty trydequeue: %w", err)
	}
	if got != nil {
		return fmt.Errorf("queue had extra pending job %s after model drained", got.GetJob().GetId())
	}

	return nil
}

func propertyTempDir() (string, func(), error) {
	dir, err := os.MkdirTemp("", "vectis-queue-property-*")
	if err != nil {
		return "", func() {}, fmt.Errorf("create temp dir: %w", err)
	}

	return dir, func() {
		_ = os.RemoveAll(dir)
	}, nil
}

func closeQueueServiceForProperty(svc api.QueueServiceServer) error {
	closer, ok := svc.(queueServiceCloser)
	if !ok {
		return nil
	}

	return closer.Close()
}

type queueTraceModel struct {
	nextID     int
	pending    []string
	inflight   map[string]string
	deliveries []string
}

func (m *queueTraceModel) enqueue(ctx context.Context, svc api.QueueServiceServer) error {
	jobID := fmt.Sprintf("job-%03d", m.nextID)
	m.nextID++
	req, err := newQueueTestRequest(&api.JobRequest{Job: &api.Job{Id: &jobID}}, 0)
	if err != nil {
		return err
	}

	if _, err := svc.Enqueue(ctx, req); err != nil {
		return err
	}

	m.pending = append(m.pending, jobID)
	return nil
}

func (m *queueTraceModel) tryDequeue(ctx context.Context, svc api.QueueServiceServer) error {
	got, err := svc.TryDequeue(ctx, &api.DequeueRequest{})
	if err != nil {
		return err
	}

	if len(m.pending) == 0 {
		if got != nil {
			return fmt.Errorf("got %s from empty model", got.GetJob().GetId())
		}
		return nil
	}

	want := m.pending[0]
	if got == nil {
		return fmt.Errorf("got empty queue, want %s", want)
	}

	if got.GetJob().GetId() != want {
		return fmt.Errorf("got %s, want %s", got.GetJob().GetId(), want)
	}

	deliveryID := got.GetJob().GetDeliveryId()
	if deliveryID == "" {
		return fmt.Errorf("delivery for %s had empty delivery id", want)
	}

	if _, ok := m.inflight[deliveryID]; ok {
		return fmt.Errorf("duplicate delivery id %s", deliveryID)
	}

	m.pending = m.pending[1:]
	m.inflight[deliveryID] = want
	m.deliveries = append(m.deliveries, deliveryID)
	return nil
}

func (m *queueTraceModel) ack(ctx context.Context, svc api.QueueServiceServer, op byte) error {
	if len(m.deliveries) == 0 {
		missing := "missing-delivery"
		_, err := svc.Ack(ctx, &api.AckRequest{DeliveryId: &missing})
		return err
	}

	idx := int(op) % len(m.deliveries)
	deliveryID := m.deliveries[idx]
	if _, err := svc.Ack(ctx, &api.AckRequest{DeliveryId: &deliveryID}); err != nil {
		return err
	}

	delete(m.inflight, deliveryID)
	m.deliveries = append(m.deliveries[:idx], m.deliveries[idx+1:]...)
	return nil
}

func checkIsolationEligibilityTrace(trace []byte) error {
	ctx := context.Background()
	svc, err := NewQueueServiceWithOptions(noopLogger{}, QueueOptions{}, nil)
	if err != nil {
		return fmt.Errorf("new queue: %w", err)
	}

	supported := supportedIsolationForTrace(trace)
	expected := make([]string, 0)
	for i, raw := range trace {
		jobID := fmt.Sprintf("job-%03d", i)
		req, err := propertyIsolationJob(jobID, raw)
		if err != nil {
			return err
		}

		if _, err := svc.Enqueue(ctx, req); err != nil {
			return fmt.Errorf("enqueue %s: %w", jobID, err)
		}

		if propertyJobEligible(req, supported) {
			expected = append(expected, jobID)
		}
	}

	actual := make([]string, 0, len(expected))
	for {
		got, err := svc.TryDequeue(ctx, &api.DequeueRequest{SupportedIsolation: supported})
		if err != nil {
			return fmt.Errorf("trydequeue: %w", err)
		}

		if got == nil {
			break
		}

		actual = append(actual, got.GetJob().GetId())
	}

	if !reflect.DeepEqual(actual, expected) {
		return fmt.Errorf("eligible order got %v, want %v for supported %v", actual, expected, supported)
	}

	return nil
}

func supportedIsolationForTrace(trace []byte) []string {
	if len(trace) == 0 {
		return nil
	}

	switch trace[0] % 4 {
	case 0:
		return nil
	case 1:
		return []string{action.IsolationHost}
	case 2:
		return []string{action.IsolationVM}
	default:
		return []string{action.IsolationHost, action.IsolationVM}
	}
}

func propertyIsolationJob(jobID string, raw byte) (*api.JobRequest, error) {
	root := queueTestNode("root", "builtins/script")
	switch raw % 3 {
	case 1:
		host := action.IsolationHost
		root.Isolation = &host
	case 2:
		vm := action.IsolationVM
		root.Isolation = &vm
	}

	return newQueueTestRequest(&api.JobRequest{Job: &api.Job{Id: &jobID, Root: root}}, 0)
}

func propertyJobEligible(req *api.JobRequest, supported []string) bool {
	if len(supported) == 0 {
		return true
	}

	required := jobRequestIsolationRequirementMask(req)
	supportedMask := uint64(0)
	for _, isolation := range supported {
		supportedMask |= isolationRequirementMask(isolation)
	}

	return required&^supportedMask == 0
}
