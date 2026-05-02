package queue

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces"
	"vectis/internal/observability"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	jobs               []*api.JobRequest
	head               int
	size               int
	inflight           map[string]inflightDelivery
	deadLetter         []deadLetterItem
	jobAttempts        map[string]int // keyed by job ID, tracks delivery attempts across requeues
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
	jobRequest   *api.JobRequest
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
		jobs:               make([]*api.JobRequest, initialQueueCapacity),
		head:               0,
		size:               0,
		inflight:           make(map[string]inflightDelivery),
		deadLetter:         make([]deadLetterItem, 0),
		jobAttempts:        make(map[string]int),
		deliveryTTL:        ttl,
		maxRequeueAttempts: maxAttempts,
		notify:             make(chan struct{}, 1),
		log:                logger,
		metrics:            metrics,
	}

	store, state, err := newPersistenceStore(opts.PersistenceDir, opts.SnapshotEvery, opts.WALSegmentMax, opts.WALRetainTail, logger)
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

	if len(state.deadLetter) > 0 {
		s.deadLetter = state.deadLetter
		s.log.Info("Restored %d dead letter item(s) from queue persistence", len(state.deadLetter))
	}

	if len(state.jobAttempts) > 0 {
		s.jobAttempts = state.jobAttempts
	}

	return s, nil
}

func (s *queueServer) Enqueue(ctx context.Context, req *api.JobRequest) (*api.Empty, error) {
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
	s.log.Info("Enqueued job: %s", req.GetJob().GetId())
	if s.metrics != nil {
		s.metrics.RecordEnqueued(ctx)
	}

	// Mark when enqueue has been accepted so queue wait starts after enqueue.
	if req.Metadata == nil {
		req.Metadata = map[string]string{}
	}
	req.Metadata[observability.JobEnqueueAcceptedUnixNanoKey] = strconv.FormatInt(time.Now().UnixNano(), 10)

	// Reinject trace context from the queue server span so dequeue handoff can
	// be parented after enqueue in the same waterfall.
	observability.InjectJobTraceContext(ctx, req)

	select {
	case s.notify <- struct{}{}:
	default:
	}

	return &api.Empty{}, nil
}

func (s *queueServer) Dequeue(ctx context.Context, _ *api.Empty) (*api.JobRequest, error) {
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

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	jobReq := s.jobs[s.head]
	job := jobReq.GetJob()
	deliveryID := uuid.NewString()
	leaseUntil := time.Now().UTC().Add(s.deliveryTTL)
	attemptCount := s.jobAttempts[job.GetId()]

	if s.persistence != nil {
		if err := s.persistence.appendDeliver(deliveryID, leaseUntil, attemptCount, s.snapshotAfterDeliverLocked(deliveryID, jobReq, leaseUntil, attemptCount)); err != nil {
			return nil, fmt.Errorf("persist dequeue delivery: %w", err)
		}
	}

	s.jobs[s.head] = nil
	s.head = (s.head + 1) % len(s.jobs)
	s.size--
	s.inflight[deliveryID] = inflightDelivery{JobRequest: jobReq, LeaseUntil: leaseUntil, AttemptCount: attemptCount}

	job.DeliveryId = &deliveryID
	s.annotateDequeueHandoff(jobReq, job.GetId(), job.GetRunId(), deliveryID, attemptCount, s.size, s.deliveryTTL)
	s.log.Info("Dequeued job: %s (delivery %s)", job.GetId(), deliveryID)
	if s.metrics != nil {
		s.metrics.RecordDequeued(ctx)
	}

	return jobReq, nil
}

func (s *queueServer) TryDequeue(ctx context.Context, _ *api.Empty) (*api.JobRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requeueExpiredLocked(time.Now().UTC()); err != nil {
		return nil, err
	}

	if s.size == 0 {
		return nil, nil
	}

	jobReq := s.jobs[s.head]
	job := jobReq.GetJob()
	deliveryID := uuid.NewString()
	leaseUntil := time.Now().UTC().Add(s.deliveryTTL)
	attemptCount := s.jobAttempts[job.GetId()]

	if s.persistence != nil {
		if err := s.persistence.appendDeliver(deliveryID, leaseUntil, attemptCount, s.snapshotAfterDeliverLocked(deliveryID, jobReq, leaseUntil, attemptCount)); err != nil {
			return nil, fmt.Errorf("persist trydequeue delivery: %w", err)
		}
	}

	s.jobs[s.head] = nil
	s.head = (s.head + 1) % len(s.jobs)
	s.size--
	s.inflight[deliveryID] = inflightDelivery{JobRequest: jobReq, LeaseUntil: leaseUntil, AttemptCount: attemptCount}

	job.DeliveryId = &deliveryID
	s.annotateDequeueHandoff(jobReq, job.GetId(), job.GetRunId(), deliveryID, attemptCount, s.size, s.deliveryTTL)
	s.log.Info("TryDequeue returned job: %s (delivery %s)", job.GetId(), deliveryID)
	if s.metrics != nil {
		s.metrics.RecordDequeued(ctx)
	}

	return jobReq, nil
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

	if item, ok := s.inflight[deliveryID]; ok {
		delete(s.jobAttempts, item.JobRequest.GetJob().GetId())
	}

	if s.persistence != nil {
		if err := s.persistence.appendAck(deliveryID, s.snapshotAfterAckLocked(deliveryID)); err != nil {
			return nil, fmt.Errorf("persist ack: %w", err)
		}
	}

	delete(s.inflight, deliveryID)
	return &api.Empty{}, nil
}

func (s *queueServer) pendingJobsLocked() []*api.JobRequest {
	out := make([]*api.JobRequest, 0, s.size)
	for i := 0; i < s.size; i++ {
		out = append(out, s.jobs[(s.head+i)%len(s.jobs)])
	}
	return out
}

func (s *queueServer) snapshotAfterEnqueueLocked(jobReq *api.JobRequest) snapshotState {
	pending := s.pendingJobsLocked()
	pending = append(pending, jobReq)
	return snapshotState{pending: pending, inflight: s.copyInflightLocked(), deadLetter: s.copyDeadLetterLocked(), jobAttempts: s.copyJobAttemptsLocked()}
}

func (s *queueServer) snapshotAfterDeliverLocked(deliveryID string, jobReq *api.JobRequest, leaseUntil time.Time, attemptCount int) snapshotState {
	capHint := max(s.size-1, 0)

	pending := make([]*api.JobRequest, 0, capHint)
	for i := 1; i < s.size; i++ {
		pending = append(pending, s.jobs[(s.head+i)%len(s.jobs)])
	}

	inflight := s.copyInflightLocked()
	inflight[deliveryID] = inflightDelivery{JobRequest: jobReq, LeaseUntil: leaseUntil, AttemptCount: attemptCount}
	return snapshotState{pending: pending, inflight: inflight, deadLetter: s.copyDeadLetterLocked(), jobAttempts: s.copyJobAttemptsLocked()}
}

func (s *queueServer) snapshotAfterAckLocked(deliveryID string) snapshotState {
	inflight := s.copyInflightLocked()
	delete(inflight, deliveryID)

	return snapshotState{pending: s.pendingJobsLocked(), inflight: inflight, deadLetter: s.copyDeadLetterLocked(), jobAttempts: s.copyJobAttemptsLocked()}
}

func (s *queueServer) copyInflightLocked() map[string]inflightDelivery {
	out := make(map[string]inflightDelivery, len(s.inflight))
	maps.Copy(out, s.inflight)

	return out
}

func (s *queueServer) annotateDequeueHandoff(
	jobReq *api.JobRequest,
	jobID, runID, deliveryID string,
	attemptCount int,
	pendingDepth int,
	deliveryTTL time.Duration,
) {
	if jobReq == nil {
		return
	}

	dequeueAt := time.Now()
	ctx := observability.ExtractJobTraceContext(context.Background(), jobReq)
	startTime := dequeueAt
	if raw := jobReq.GetMetadata()[observability.JobEnqueueAcceptedUnixNanoKey]; raw != "" {
		if ns, err := strconv.ParseInt(raw, 10, 64); err == nil && ns > 0 {
			startTime = time.Unix(0, ns)
		}
	} else if raw := jobReq.GetMetadata()[observability.JobEnqueuedAtUnixNanoKey]; raw != "" {
		if ns, err := strconv.ParseInt(raw, 10, 64); err == nil && ns > 0 {
			startTime = time.Unix(0, ns)
		}
	}

	handoffCtx, span := observability.Tracer("vectis/queue").Start(
		ctx,
		"queue.handoff.wait",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(startTime),
	)

	span.SetAttributes(observability.JobRunAttrs(jobID, runID)...)
	span.SetAttributes(
		attribute.String("queue.delivery.id", deliveryID),
		attribute.String("queue.phase", "handoff"),
		attribute.Float64("queue.wait.ms", float64(dequeueAt.Sub(startTime))/float64(time.Millisecond)),
		attribute.Int("queue.pending.depth", pendingDepth),
		attribute.Int("queue.attempt.count", attemptCount),
		attribute.Float64("queue.delivery.ttl.ms", float64(deliveryTTL)/float64(time.Millisecond)),
	)

	observability.InjectJobTraceContext(handoffCtx, jobReq)
	span.End(trace.WithTimestamp(dequeueAt))
}

func (s *queueServer) copyDeadLetterLocked() []deadLetterItem {
	out := make([]deadLetterItem, len(s.deadLetter))
	copy(out, s.deadLetter)
	return out
}

func (s *queueServer) copyJobAttemptsLocked() map[string]int {
	out := make(map[string]int, len(s.jobAttempts))
	maps.Copy(out, s.jobAttempts)

	return out
}

func (s *queueServer) snapshotAfterExpiredRequeueLocked(deliveryID string, item inflightDelivery) snapshotState {
	pending := s.pendingJobsLocked()
	pending = append(pending, item.JobRequest)

	inflight := s.copyInflightLocked()
	delete(inflight, deliveryID)
	return snapshotState{pending: pending, inflight: inflight, deadLetter: s.copyDeadLetterLocked(), jobAttempts: s.copyJobAttemptsLocked()}
}

func (s *queueServer) snapshotAfterDLQLocked(deliveryID string, item inflightDelivery) snapshotState {
	pending := s.pendingJobsLocked()

	inflight := s.copyInflightLocked()
	delete(inflight, deliveryID)
	return snapshotState{pending: pending, inflight: inflight, deadLetter: s.copyDeadLetterLocked(), jobAttempts: s.copyJobAttemptsLocked()}
}

func (s *queueServer) snapshotAfterDLQRequeueLocked(deliveryID string, item deadLetterItem) snapshotState {
	pending := s.pendingJobsLocked()
	pending = append(pending, item.jobRequest)

	deadLetter := make([]deadLetterItem, 0, len(s.deadLetter))
	for _, dl := range s.deadLetter {
		if dl.deliveryID != deliveryID {
			deadLetter = append(deadLetter, dl)
		}
	}

	return snapshotState{pending: pending, inflight: s.copyInflightLocked(), deadLetter: deadLetter, jobAttempts: s.copyJobAttemptsLocked()}
}

func (s *queueServer) requeueExpiredLocked(now time.Time) error {
	for deliveryID, item := range s.inflight {
		if item.LeaseUntil.After(now) {
			continue
		}

		jobID := item.JobRequest.GetJob().GetId()
		s.jobAttempts[jobID]++
		attemptCount := s.jobAttempts[jobID]

		if attemptCount > s.maxRequeueAttempts {
			// Move to dead letter queue.
			s.deadLetter = append(s.deadLetter, deadLetterItem{
				deliveryID:   deliveryID,
				jobRequest:   item.JobRequest,
				attemptCount: attemptCount,
			})
			delete(s.inflight, deliveryID)
			delete(s.jobAttempts, jobID)
			s.log.Error("Delivery %s for job %s exceeded max requeue attempts (%d); moved to DLQ",
				deliveryID, jobID, s.maxRequeueAttempts)

			if s.metrics != nil {
				s.metrics.RecordDLQMoved(context.Background())
			}

			if s.persistence != nil {
				if err := s.persistence.appendDLQ(deliveryID, item.JobRequest, attemptCount, s.snapshotAfterDLQLocked(deliveryID, item)); err != nil {
					return fmt.Errorf("persist dlq move %s: %w", deliveryID, err)
				}
			}

			continue
		}

		if s.persistence != nil {
			if err := s.persistence.appendRequeueExpired(deliveryID, item.JobRequest, s.snapshotAfterExpiredRequeueLocked(deliveryID, item)); err != nil {
				return fmt.Errorf("persist expired requeue %s: %w", deliveryID, err)
			}
		}

		if s.size == len(s.jobs) {
			s.grow()
		}

		tail := (s.head + s.size) % len(s.jobs)
		s.jobs[tail] = item.JobRequest
		s.size++
		delete(s.inflight, deliveryID)
		s.log.Warn("Re-queued expired delivery %s for job %s (attempt %d)", deliveryID, jobID, attemptCount)
		if s.metrics != nil {
			s.metrics.RecordExpiredRequeued(context.Background())
		}

		select {
		case s.notify <- struct{}{}:
		default:
		}
	}

	return nil
}

func (s *queueServer) loadPending(jobs []*api.JobRequest) {
	if len(jobs) == 0 {
		return
	}

	capHint := initialQueueCapacity
	for capHint < len(jobs) {
		capHint *= 2
	}

	s.jobs = make([]*api.JobRequest, capHint)
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

	next := make([]*api.JobRequest, newCap)
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
			JobRequest:   item.jobRequest,
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
			s.jobs[tail] = item.jobRequest
			s.size++
			s.deadLetter = append(s.deadLetter[:i], s.deadLetter[i+1:]...)

			delete(s.jobAttempts, item.jobRequest.GetJob().GetId())
			s.log.Info("Requeued dead letter delivery %s for job %s", deliveryID, item.jobRequest.GetJob().GetId())

			if s.persistence != nil {
				if err := s.persistence.appendDLQRequeue(deliveryID, item.jobRequest, s.snapshotAfterDLQRequeueLocked(deliveryID, item)); err != nil {
					return nil, fmt.Errorf("persist dlq requeue %s: %w", deliveryID, err)
				}
			}

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
