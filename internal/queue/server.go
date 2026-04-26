package queue

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/observability"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

type QueueOptions struct {
	PersistenceDir     string
	SnapshotEvery      int
	DeliveryTTL        time.Duration
	WALSegmentMax      int64
	WALRetainTail      int
	MaxRequeueAttempts int
}

type queueServer struct {
	api.UnimplementedQueueServiceServer
	jobs               []*api.Job
	head               int
	size               int
	inflight           map[string]inflightDelivery
	deadLetter         []deadLetterItem
	deliveryTTL        time.Duration
	maxRequeueAttempts int
	mu                 sync.Mutex
	notify             chan struct{}
	log                interfaces.Logger
	persistence        *persistenceStore
	metrics            *observability.QueueMetrics
}

type deadLetterItem struct {
	deliveryID   string
	job          *api.Job
	attemptCount int
}

const (
	initialQueueCapacity = 1024
	defaultDeliveryTTL   = 2 * time.Minute
)

func NewQueueService(logger interfaces.Logger) api.QueueServiceServer {
	s, err := newQueueServer(logger, QueueOptions{}, nil)
	if err != nil {
		panic(err)
	}
	return s
}

func NewQueueServiceWithOptions(logger interfaces.Logger, opts QueueOptions, metrics *observability.QueueMetrics) (api.QueueServiceServer, error) {
	return newQueueServer(logger, opts, metrics)
}

func newQueueServer(logger interfaces.Logger, opts QueueOptions, metrics *observability.QueueMetrics) (*queueServer, error) {
	ttl := opts.DeliveryTTL
	if ttl <= 0 {
		ttl = defaultDeliveryTTL
	}

	maxAttempts := opts.MaxRequeueAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	s := &queueServer{
		jobs:               make([]*api.Job, initialQueueCapacity),
		head:               0,
		size:               0,
		inflight:           make(map[string]inflightDelivery),
		deadLetter:         make([]deadLetterItem, 0),
		deliveryTTL:        ttl,
		maxRequeueAttempts: maxAttempts,
		notify:             make(chan struct{}, 1),
		log:                logger,
		metrics:            metrics,
	}

	store, state, err := newPersistenceStore(opts.PersistenceDir, opts.SnapshotEvery, opts.WALSegmentMax, opts.WALRetainTail)
	if err != nil {
		return nil, err
	}
	s.persistence = store

	if len(state.jobs) > 0 {
		s.loadPending(state.jobs)
		s.log.Info("Restored %d pending job(s) from queue persistence", len(state.jobs))
	}

	if len(state.inflight) > 0 {
		s.inflight = state.inflight
		s.log.Info("Restored %d in-flight delivery(s) from queue persistence", len(state.inflight))
	}

	return s, nil
}

func (s *queueServer) Enqueue(ctx context.Context, req *api.Job) (*api.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requeueExpiredLocked(time.Now().UTC()); err != nil {
		return nil, err
	}

	if s.persistence != nil {
		if err := s.persistence.appendEnqueue(req, s.snapshotAfterEnqueueLocked(req)); err != nil {
			return nil, fmt.Errorf("persist enqueue: %w", err)
		}
	}

	if s.size == len(s.jobs) {
		s.grow()
	}

	tail := (s.head + s.size) % len(s.jobs)
	s.jobs[tail] = req
	s.size++
	s.log.Info("Enqueued job: %s", req.GetId())
	if s.metrics != nil {
		s.metrics.RecordEnqueued(ctx)
	}

	select {
	case s.notify <- struct{}{}:
	default:
	}

	return &api.Empty{}, nil
}

func (s *queueServer) Dequeue(ctx context.Context, _ *api.Empty) (*api.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.size == 0 {
		if err := s.requeueExpiredLocked(time.Now().UTC()); err != nil {
			return nil, err
		}

		if s.size > 0 {
			break
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		s.mu.Unlock()
		select {
		case <-s.notify:
		case <-ctx.Done():
			s.mu.Lock()
			return nil, ctx.Err()
		}
		s.mu.Lock()
	}

	if s.size == 0 {
		return nil, nil
	}

	job := s.jobs[s.head]
	deliveryID := uuid.NewString()
	leaseUntil := time.Now().UTC().Add(s.deliveryTTL)

	s.inflight[deliveryID] = inflightDelivery{Job: job, LeaseUntil: leaseUntil, AttemptCount: 0}

	s.inflight[deliveryID] = inflightDelivery{Job: job, LeaseUntil: leaseUntil, AttemptCount: 0}

	if s.persistence != nil {
		if err := s.persistence.appendDeliver(deliveryID, leaseUntil, s.snapshotAfterDeliverLocked(deliveryID, job, leaseUntil, 0)); err != nil {
			return nil, fmt.Errorf("persist trydequeue delivery: %w", err)
		}
	}

	s.jobs[s.head] = nil
	s.head = (s.head + 1) % len(s.jobs)
	s.size--

	job.DeliveryId = &deliveryID
	s.log.Info("Dequeued job: %s (delivery %s)", job.GetId(), deliveryID)
	if s.metrics != nil {
		s.metrics.RecordDequeued(ctx)
	}
	return job, nil
}

func (s *queueServer) TryDequeue(ctx context.Context, _ *api.Empty) (*api.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requeueExpiredLocked(time.Now().UTC()); err != nil {
		return nil, err
	}

	if s.size == 0 {
		return nil, nil
	}

	job := s.jobs[s.head]
	deliveryID := uuid.NewString()
	leaseUntil := time.Now().UTC().Add(s.deliveryTTL)

	s.inflight[deliveryID] = inflightDelivery{Job: job, LeaseUntil: leaseUntil, AttemptCount: 0}

	if s.persistence != nil {
		if err := s.persistence.appendDeliver(deliveryID, leaseUntil, s.snapshotAfterDeliverLocked(deliveryID, job, leaseUntil, 0)); err != nil {
			return nil, fmt.Errorf("persist trydequeue delivery: %w", err)
		}
	}

	s.jobs[s.head] = nil
	s.head = (s.head + 1) % len(s.jobs)
	s.size--

	job.DeliveryId = &deliveryID
	s.log.Info("TryDequeue returned job: %s (delivery %s)", job.GetId(), deliveryID)
	if s.metrics != nil {
		s.metrics.RecordDequeued(ctx)
	}
	return job, nil
}

func (s *queueServer) Ack(ctx context.Context, req *api.AckRequest) (*api.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requeueExpiredLocked(time.Now().UTC()); err != nil {
		return nil, err
	}

	deliveryID := req.GetDeliveryId()
	if deliveryID == "" {
		return nil, fmt.Errorf("delivery_id is required")
	}

	if _, ok := s.inflight[deliveryID]; !ok {
		return &api.Empty{}, nil
	}

	if s.persistence != nil {
		if err := s.persistence.appendAck(deliveryID, s.snapshotAfterAckLocked(deliveryID)); err != nil {
			return nil, fmt.Errorf("persist ack: %w", err)
		}
	}

	delete(s.inflight, deliveryID)
	return &api.Empty{}, nil
}

func (s *queueServer) pendingJobsLocked() []*api.Job {
	out := make([]*api.Job, 0, s.size)
	for i := 0; i < s.size; i++ {
		out = append(out, s.jobs[(s.head+i)%len(s.jobs)])
	}
	return out
}

func (s *queueServer) snapshotAfterEnqueueLocked(job *api.Job) snapshotState {
	pending := s.pendingJobsLocked()
	pending = append(pending, job)
	return snapshotState{pending: pending, inflight: s.copyInflightLocked()}
}

func (s *queueServer) snapshotAfterDeliverLocked(deliveryID string, job *api.Job, leaseUntil time.Time, attemptCount int) snapshotState {
	pending := make([]*api.Job, 0, s.size-1)
	for i := 1; i < s.size; i++ {
		pending = append(pending, s.jobs[(s.head+i)%len(s.jobs)])
	}

	inflight := s.copyInflightLocked()
	inflight[deliveryID] = inflightDelivery{Job: job, LeaseUntil: leaseUntil, AttemptCount: attemptCount}
	return snapshotState{pending: pending, inflight: inflight}
}

func (s *queueServer) snapshotAfterAckLocked(deliveryID string) snapshotState {
	inflight := s.copyInflightLocked()
	delete(inflight, deliveryID)

	return snapshotState{pending: s.pendingJobsLocked(), inflight: inflight}
}

func (s *queueServer) copyInflightLocked() map[string]inflightDelivery {
	out := make(map[string]inflightDelivery, len(s.inflight))
	maps.Copy(out, s.inflight)

	return out
}

func (s *queueServer) snapshotAfterExpiredRequeueLocked(deliveryID string, item inflightDelivery) snapshotState {
	pending := s.pendingJobsLocked()
	pending = append(pending, item.Job)

	inflight := s.copyInflightLocked()
	delete(inflight, deliveryID)
	return snapshotState{pending: pending, inflight: inflight}
}

func (s *queueServer) requeueExpiredLocked(now time.Time) error {
	for deliveryID, item := range s.inflight {
		if item.LeaseUntil.After(now) {
			continue
		}

		item.AttemptCount++
		if item.AttemptCount > s.maxRequeueAttempts {
			// Move to dead letter queue.
			s.deadLetter = append(s.deadLetter, deadLetterItem{
				deliveryID:   deliveryID,
				job:          item.Job,
				attemptCount: item.AttemptCount,
			})
			delete(s.inflight, deliveryID)
			s.log.Error("Delivery %s for job %s exceeded max requeue attempts (%d); moved to DLQ",
				deliveryID, item.Job.GetId(), s.maxRequeueAttempts)
			if s.metrics != nil {
				s.metrics.RecordDLQMoved(context.Background())
			}
			continue
		}

		if s.persistence != nil {
			if err := s.persistence.appendRequeueExpired(deliveryID, item.Job, s.snapshotAfterExpiredRequeueLocked(deliveryID, item)); err != nil {
				return fmt.Errorf("persist expired requeue %s: %w", deliveryID, err)
			}
		}

		if s.size == len(s.jobs) {
			s.grow()
		}

		tail := (s.head + s.size) % len(s.jobs)
		s.jobs[tail] = item.Job
		s.size++
		delete(s.inflight, deliveryID)
		s.log.Warn("Re-queued expired delivery %s for job %s (attempt %d)", deliveryID, item.Job.GetId(), item.AttemptCount)

		select {
		case s.notify <- struct{}{}:
		default:
		}
	}

	return nil
}

func (s *queueServer) loadPending(jobs []*api.Job) {
	if len(jobs) == 0 {
		return
	}

	capHint := initialQueueCapacity
	for capHint < len(jobs) {
		capHint *= 2
	}

	s.jobs = make([]*api.Job, capHint)
	s.head = 0
	s.size = len(jobs)
	copy(s.jobs, jobs)

	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *queueServer) grow() {
	newCap := len(s.jobs) * 2
	if newCap == 0 {
		newCap = 1
	}

	next := make([]*api.Job, newCap)
	for i := 0; i < s.size; i++ {
		next[i] = s.jobs[(s.head+i)%len(s.jobs)]
	}

	s.jobs = next
	s.head = 0
}

func (s *queueServer) ListDeadLetter(ctx context.Context, _ *api.Empty) (*api.ListDeadLetterResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	items := make([]*api.DeadLetterItem, 0, len(s.deadLetter))
	for _, item := range s.deadLetter {
		ac := int32(item.attemptCount)
		items = append(items, &api.DeadLetterItem{
			DeliveryId:   &item.deliveryID,
			Job:          item.job,
			AttemptCount: &ac,
		})
	}

	return &api.ListDeadLetterResponse{Items: items}, nil
}

func (s *queueServer) RequeueDeadLetter(ctx context.Context, req *api.RequeueDeadLetterRequest) (*api.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	deliveryID := req.GetDeliveryId()
	for i, item := range s.deadLetter {
		if item.deliveryID == deliveryID {
			if s.size == len(s.jobs) {
				s.grow()
			}

			tail := (s.head + s.size) % len(s.jobs)
			s.jobs[tail] = item.job
			s.size++
			s.deadLetter = append(s.deadLetter[:i], s.deadLetter[i+1:]...)
			s.log.Info("Requeued dead letter delivery %s for job %s", deliveryID, item.job.GetId())
			if s.metrics != nil {
				s.metrics.RecordDLQRequeued(ctx)
			}

			select {
			case s.notify <- struct{}{}:
			default:
			}

			return &api.Empty{}, nil
		}
	}

	return nil, fmt.Errorf("dead letter delivery %s not found", deliveryID)
}

func RegisterQueueService(s grpc.ServiceRegistrar, logger interfaces.Logger, opts QueueOptions, metrics *observability.QueueMetrics) api.QueueServiceServer {
	qs, err := newQueueServer(logger, opts, metrics)
	if err != nil {
		logger.Fatal("Failed to initialize queue: %v", err)
	}

	hs := health.NewServer()
	healthgrpc.RegisterHealthServer(s, hs)
	hs.SetServingStatus("queue", healthpb.HealthCheckResponse_SERVING)

	api.RegisterQueueServiceServer(s, qs)
	return qs
}
