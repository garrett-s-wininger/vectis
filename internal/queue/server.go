package queue

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/action"
	"vectis/internal/cell"
	"vectis/internal/dispatchmeta"
	"vectis/internal/faultinject"
	"vectis/internal/interfaces"
	"vectis/internal/observability"
	"vectis/internal/queueid"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type QueueOptions struct {
	PersistenceDir     string
	SnapshotEvery      int
	DeliveryTTL        time.Duration
	WALSegmentMax      int64
	WALRetainTail      int
	MaxRequeueAttempts int
	InstanceID         string
	Faults             faultinject.Hook
}

type queueServer struct {
	api.UnimplementedQueueServiceServer
	jobs                      []*api.JobRequest
	pendingSeqs               []uint64
	head                      int
	size                      int
	nextPendingSeq            uint64
	pendingSlots              map[uint64]int
	pendingRequirementBySeq   map[uint64]uint64
	pendingRequirementBuckets map[uint64][]uint64
	inflight                  map[string]inflightDelivery
	deadLetter                []deadLetterItem
	jobAttempts               map[string]int // keyed by job ID, tracks delivery attempts across requeues
	deliveryTTL               time.Duration
	maxRequeueAttempts        int
	instanceID                string
	deliveryPrefix            string
	deliverySeq               uint64
	mu                        sync.Mutex
	notify                    chan struct{}
	log                       interfaces.Logger
	persistence               *persistenceStore
	metrics                   *observability.QueueMetrics
	faults                    faultinject.Hook
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

const (
	isolationRequirementHost uint64 = 1 << iota
	isolationRequirementVM
	isolationRequirementUnsupported
)

const (
	FaultPointEnqueuePersist     faultinject.Point = "queue.enqueue.persist"
	FaultPointDeliverPersist     faultinject.Point = "queue.deliver.persist"
	FaultPointAckPersist         faultinject.Point = "queue.ack.persist"
	FaultPointExpiredRequeue     faultinject.Point = "queue.expired.requeue.persist"
	FaultPointExpiredDrop        faultinject.Point = "queue.expired.drop.persist"
	FaultPointDeadLetter         faultinject.Point = "queue.deadletter.persist"
	FaultPointDeadLetterRequeue  faultinject.Point = "queue.deadletter.requeue.persist"
	FaultPointPendingExpiredDrop faultinject.Point = "queue.pending.expired.drop.persist"
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

func (s *queueServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.persistence == nil {
		return nil
	}

	err := s.persistence.Close()
	s.persistence = nil
	return err
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
		jobs:                      make([]*api.JobRequest, initialQueueCapacity),
		pendingSeqs:               make([]uint64, initialQueueCapacity),
		head:                      0,
		size:                      0,
		nextPendingSeq:            1,
		pendingSlots:              make(map[uint64]int),
		pendingRequirementBySeq:   make(map[uint64]uint64),
		pendingRequirementBuckets: make(map[uint64][]uint64),
		inflight:                  make(map[string]inflightDelivery),
		deadLetter:                make([]deadLetterItem, 0),
		jobAttempts:               make(map[string]int),
		deliveryTTL:               ttl,
		maxRequeueAttempts:        maxAttempts,
		instanceID:                opts.InstanceID,
		deliveryPrefix:            uuid.NewString(),
		notify:                    make(chan struct{}, 1),
		log:                       logger,
		metrics:                   metrics,
		faults:                    opts.Faults,
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

	now := time.Now().UTC()
	if err := s.requeueExpiredLocked(now); err != nil {
		return nil, err
	}

	if err := s.dropExpiredPendingLocked(now); err != nil {
		return nil, err
	}

	if err := validateEnqueueHandoff(req); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid execution handoff metadata: %v", err)
	}

	queuedReq := cloneJobRequestForEnqueue(ctx, req, now)
	if s.persistence != nil {
		if err := s.beforeFault(ctx, FaultPointEnqueuePersist); err != nil {
			return nil, err
		}

		if err := s.persistence.appendEnqueue(queuedReq, s.snapshotAfterEnqueueLocked(queuedReq)); err != nil {
			return nil, fmt.Errorf("persist enqueue: %w", err)
		}
	}

	s.appendPendingLocked(queuedReq)
	s.log.Info("Enqueued job: %s", queuedReq.GetJob().GetId())
	if s.metrics != nil {
		s.metrics.RecordEnqueued(ctx)
	}

	select {
	case s.notify <- struct{}{}:
	default:
	}

	return &api.Empty{}, nil
}

func validateEnqueueHandoff(req *api.JobRequest) error {
	if req == nil {
		return fmt.Errorf("job request is required")
	}

	if _, ok, err := cell.ExecutionEnvelopeFromRequest(req); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("execution envelope metadata is required")
	}

	return nil
}

func (s *queueServer) Dequeue(ctx context.Context, req *api.DequeueRequest) (*api.JobRequest, error) {
	return s.dequeueWithRequest(ctx, req, true, "dequeue")
}

func (s *queueServer) TryDequeue(ctx context.Context, req *api.DequeueRequest) (*api.JobRequest, error) {
	return s.dequeueWithRequest(ctx, req, false, "trydequeue")
}

func (s *queueServer) dequeueWithRequest(ctx context.Context, req *api.DequeueRequest, wait bool, persistOp string) (*api.JobRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	supportedMask, filtered, err := supportedIsolationRequestMask(req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid dequeue supported isolation: %v", err)
	}

	for {
		now := time.Now().UTC()
		if err := s.requeueExpiredLocked(now); err != nil {
			return nil, err
		}

		if err := s.dropExpiredPendingLocked(now); err != nil {
			return nil, err
		}

		if wait {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}

		if offset := s.firstEligiblePendingOffsetLocked(supportedMask, filtered); offset >= 0 {
			return s.deliverPendingOffsetLocked(ctx, offset, persistOp)
		}

		if !wait {
			return nil, nil
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
}

func (s *queueServer) deliverPendingOffsetLocked(ctx context.Context, offset int, persistOp string) (*api.JobRequest, error) {
	pendingReq := s.jobs[(s.head+offset)%len(s.jobs)]
	deliveryID := s.newDeliveryID()
	jobReq := cloneJobRequestWithDeliveryID(pendingReq, deliveryID)
	job := jobReq.GetJob()
	leaseUntil := time.Now().UTC().Add(s.deliveryTTL)
	attemptCount := s.jobAttempts[job.GetId()]

	if s.persistence != nil {
		if err := s.beforeFault(ctx, FaultPointDeliverPersist); err != nil {
			return nil, err
		}

		if err := s.persistence.appendDeliver(deliveryID, jobReq, leaseUntil, attemptCount, s.snapshotAfterDeliverLocked(deliveryID, offset, jobReq, leaseUntil, attemptCount)); err != nil {
			return nil, fmt.Errorf("persist %s delivery: %w", persistOp, err)
		}
	}

	s.removePendingOffsetLocked(offset)
	s.inflight[deliveryID] = inflightDelivery{JobRequest: jobReq, LeaseUntil: leaseUntil, AttemptCount: attemptCount}

	s.annotateDequeueHandoff(jobReq, job.GetId(), job.GetRunId(), deliveryID, attemptCount, s.size, s.deliveryTTL)
	if persistOp == "trydequeue" {
		s.log.Info("TryDequeue returned job: %s (delivery %s)", job.GetId(), deliveryID)
	} else {
		s.log.Info("Dequeued job: %s (delivery %s)", job.GetId(), deliveryID)
	}
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

	item := s.inflight[deliveryID]

	if s.persistence != nil {
		if err := s.beforeFault(ctx, FaultPointAckPersist); err != nil {
			return nil, err
		}

		if err := s.persistence.appendAck(deliveryID, s.snapshotAfterAckLocked(deliveryID)); err != nil {
			return nil, fmt.Errorf("persist ack: %w", err)
		}
	}

	delete(s.jobAttempts, item.JobRequest.GetJob().GetId())
	delete(s.inflight, deliveryID)
	return &api.Empty{}, nil
}

func (s *queueServer) beforeFault(ctx context.Context, point faultinject.Point) error {
	if s.faults == nil {
		return nil
	}

	if err := s.faults.Before(ctx, point); err != nil {
		return fmt.Errorf("%s: %w", point, err)
	}

	return nil
}

func (s *queueServer) newDeliveryID() string {
	s.deliverySeq++
	return queueid.Encode(s.instanceID, s.deliveryPrefix+"-"+strconv.FormatUint(s.deliverySeq, 10))
}

func (s *queueServer) dropExpiredPendingLocked(now time.Time) error {
	for offset := 0; offset < s.size; {
		jobReq := s.jobs[(s.head+offset)%len(s.jobs)]
		if !dispatchmeta.IsExpired(jobReq, now) {
			offset++
			continue
		}

		jobID := jobReq.GetJob().GetId()
		if s.persistence != nil {
			if err := s.beforeFault(context.Background(), FaultPointPendingExpiredDrop); err != nil {
				return err
			}
			if err := s.persistence.appendDropExpired("", jobReq, s.snapshotAfterPendingOffsetDropLocked(offset, jobReq)); err != nil {
				return fmt.Errorf("persist expired pending drop for job %s: %w", jobID, err)
			}
		}

		s.removePendingOffsetLocked(offset)
		delete(s.jobAttempts, jobID)

		s.log.Warn("Pending job %s expired after dispatch start deadline; dropped", jobID)
		if s.metrics != nil {
			s.metrics.RecordExpiredDropped(context.Background())
		}
	}

	return nil
}

func (s *queueServer) pendingJobsLocked() []*api.JobRequest {
	out := make([]*api.JobRequest, 0, s.size)
	for i := 0; i < s.size; i++ {
		out = append(out, s.jobs[(s.head+i)%len(s.jobs)])
	}
	return out
}

func (s *queueServer) firstEligiblePendingOffsetLocked(supportedMask uint64, filtered bool) int {
	if s.size == 0 {
		return -1
	}

	if !filtered {
		return 0
	}

	var bestSeq uint64
	bestSlot := -1

	for requiredMask := range s.pendingRequirementBuckets {
		if requiredMask&^supportedMask != 0 {
			continue
		}

		seq, ok := s.firstActivePendingSeqForRequirementLocked(requiredMask)
		if !ok {
			continue
		}

		if bestSeq == 0 || seq < bestSeq {
			bestSeq = seq
			bestSlot = s.pendingSlots[seq]
		}
	}

	if bestSlot >= 0 {
		return s.pendingOffsetForSlotLocked(bestSlot)
	}

	return -1
}

func (s *queueServer) removePendingOffsetLocked(offset int) {
	if offset == 0 {
		s.removePendingHeadLocked()
		return
	}

	slot := (s.head + offset) % len(s.jobs)
	seq := s.pendingSeqs[slot]
	s.unindexPendingSeqLocked(seq)

	for i := offset; i < s.size-1; i++ {
		current := (s.head + i) % len(s.jobs)
		next := (s.head + i + 1) % len(s.jobs)
		s.jobs[current] = s.jobs[next]
		s.pendingSeqs[current] = s.pendingSeqs[next]
		if shiftedSeq := s.pendingSeqs[current]; shiftedSeq != 0 {
			s.pendingSlots[shiftedSeq] = current
		}
	}

	tail := (s.head + s.size - 1) % len(s.jobs)
	s.jobs[tail] = nil
	s.pendingSeqs[tail] = 0
	s.size--

	if s.size == 0 {
		s.head = 0
	}
}

func (s *queueServer) snapshotAfterEnqueueLocked(jobReq *api.JobRequest) snapshotState {
	pending := s.pendingJobsLocked()
	pending = append(pending, jobReq)
	return snapshotState{pending: pending, inflight: s.copyInflightLocked(), deadLetter: s.copyDeadLetterLocked(), jobAttempts: s.copyJobAttemptsLocked()}
}

func (s *queueServer) snapshotAfterDeliverLocked(deliveryID string, deliveredOffset int, jobReq *api.JobRequest, leaseUntil time.Time, attemptCount int) snapshotState {
	capHint := max(s.size-1, 0)

	pending := make([]*api.JobRequest, 0, capHint)
	for i := 0; i < s.size; i++ {
		if i == deliveredOffset {
			continue
		}

		pending = append(pending, s.jobs[(s.head+i)%len(s.jobs)])
	}

	inflight := s.copyInflightLocked()
	inflight[deliveryID] = inflightDelivery{JobRequest: jobReq, LeaseUntil: leaseUntil, AttemptCount: attemptCount}
	return snapshotState{pending: pending, inflight: inflight, deadLetter: s.copyDeadLetterLocked(), jobAttempts: s.copyJobAttemptsLocked()}
}

func (s *queueServer) snapshotAfterAckLocked(deliveryID string) snapshotState {
	inflight := s.copyInflightLocked()
	jobAttempts := s.copyJobAttemptsLocked()
	if item, ok := inflight[deliveryID]; ok && item.JobRequest != nil && item.JobRequest.GetJob() != nil {
		delete(jobAttempts, item.JobRequest.GetJob().GetId())
	}

	delete(inflight, deliveryID)
	return snapshotState{pending: s.pendingJobsLocked(), inflight: inflight, deadLetter: s.copyDeadLetterLocked(), jobAttempts: jobAttempts}
}

func (s *queueServer) snapshotAfterPendingOffsetDropLocked(dropOffset int, jobReq *api.JobRequest) snapshotState {
	capHint := max(s.size-1, 0)

	pending := make([]*api.JobRequest, 0, capHint)
	for i := 0; i < s.size; i++ {
		if i == dropOffset {
			continue
		}

		pending = append(pending, s.jobs[(s.head+i)%len(s.jobs)])
	}

	jobAttempts := s.copyJobAttemptsLocked()
	if jobReq != nil && jobReq.GetJob() != nil {
		delete(jobAttempts, jobReq.GetJob().GetId())
	}

	return snapshotState{pending: pending, inflight: s.copyInflightLocked(), deadLetter: s.copyDeadLetterLocked(), jobAttempts: jobAttempts}
}

func (s *queueServer) copyInflightLocked() map[string]inflightDelivery {
	out := make(map[string]inflightDelivery, len(s.inflight))
	maps.Copy(out, s.inflight)

	return out
}

func supportedIsolationRequestMask(req *api.DequeueRequest) (uint64, bool, error) {
	if req == nil {
		return 0, false, nil
	}

	levels := req.GetSupportedIsolation()
	if len(levels) == 0 {
		return 0, false, nil
	}

	levels, err := action.NormalizeSupportedIsolationLevels(levels)
	if err != nil {
		return 0, false, err
	}

	var mask uint64
	for _, level := range levels {
		mask |= isolationRequirementMask(level)
	}

	return mask, true, nil
}

func jobRequestIsolationRequirementMask(req *api.JobRequest) uint64 {
	job := req.GetJob()
	if job == nil {
		return 0
	}

	defaultIsolation := action.NormalizeIsolation(job.GetDefaultIsolation())
	if taskNode, ok := taskNodeFromRequest(req, job); ok {
		return nodeSelfIsolationRequirementMask(taskNode, defaultIsolation)
	}

	return nodeTreeIsolationRequirementMask(job.GetRoot(), defaultIsolation)
}

func taskNodeFromRequest(req *api.JobRequest, job *api.Job) (*api.Node, bool) {
	taskKey := strings.TrimSpace(req.GetMetadata()[cell.ExecutionTaskKeyMetadataKey])
	if taskKey == "" {
		return nil, false
	}

	if taskKey == "root" {
		return job.GetRoot(), true
	}

	if node := findNodeByID(job.GetRoot(), taskKey); node != nil {
		return node, true
	}

	return nil, false
}

func findNodeByID(node *api.Node, id string) *api.Node {
	if node == nil {
		return nil
	}

	if strings.TrimSpace(node.GetId()) == id {
		return node
	}

	for _, child := range node.GetSteps() {
		if found := findNodeByID(child, id); found != nil {
			return found
		}
	}

	return nil
}

func nodeTreeIsolationRequirementMask(node *api.Node, inherited string) uint64 {
	if node == nil {
		return 0
	}

	effective := effectiveNodeIsolation(node, inherited)
	mask := isolationRequirementMask(effective)
	for _, child := range node.GetSteps() {
		mask |= nodeTreeIsolationRequirementMask(child, effective)
	}

	return mask
}

func nodeSelfIsolationRequirementMask(node *api.Node, inherited string) uint64 {
	return isolationRequirementMask(effectiveNodeIsolation(node, inherited))
}

func effectiveNodeIsolation(node *api.Node, inherited string) string {
	effective := inherited
	if node != nil {
		if requested := action.NormalizeIsolation(node.GetIsolation()); requested != "" {
			effective = requested
		}
	}

	return effective
}

func isolationRequirementMask(isolation string) uint64 {
	switch action.NormalizeIsolation(isolation) {
	case "":
		return 0
	case action.IsolationHost:
		return isolationRequirementHost
	case action.IsolationVM:
		return isolationRequirementVM
	default:
		return isolationRequirementUnsupported
	}
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

func (s *queueServer) snapshotAfterExpiredRequeueLocked(deliveryID string, item inflightDelivery, attemptCount int) snapshotState {
	pending := s.pendingJobsLocked()
	pending = append(pending, item.JobRequest)

	inflight := s.copyInflightLocked()
	delete(inflight, deliveryID)
	jobAttempts := s.copyJobAttemptsLocked()

	if item.JobRequest != nil && item.JobRequest.GetJob() != nil {
		jobAttempts[item.JobRequest.GetJob().GetId()] = attemptCount
	}

	return snapshotState{pending: pending, inflight: inflight, deadLetter: s.copyDeadLetterLocked(), jobAttempts: jobAttempts}
}

func (s *queueServer) snapshotAfterExpiredDropLocked(deliveryID string, item inflightDelivery) snapshotState {
	inflight := s.copyInflightLocked()
	delete(inflight, deliveryID)

	jobAttempts := s.copyJobAttemptsLocked()
	if item.JobRequest != nil && item.JobRequest.GetJob() != nil {
		delete(jobAttempts, item.JobRequest.GetJob().GetId())
	}

	return snapshotState{pending: s.pendingJobsLocked(), inflight: inflight, deadLetter: s.copyDeadLetterLocked(), jobAttempts: jobAttempts}
}

func (s *queueServer) snapshotAfterDLQLocked(deliveryID string, item inflightDelivery, attemptCount int) snapshotState {
	pending := s.pendingJobsLocked()

	inflight := s.copyInflightLocked()
	delete(inflight, deliveryID)
	deadLetter := s.copyDeadLetterLocked()
	deadLetter = append(deadLetter, deadLetterItem{
		deliveryID:   deliveryID,
		jobRequest:   item.JobRequest,
		attemptCount: attemptCount,
	})
	jobAttempts := s.copyJobAttemptsLocked()
	if item.JobRequest != nil && item.JobRequest.GetJob() != nil {
		delete(jobAttempts, item.JobRequest.GetJob().GetId())
	}
	return snapshotState{pending: pending, inflight: inflight, deadLetter: deadLetter, jobAttempts: jobAttempts}
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

	jobAttempts := s.copyJobAttemptsLocked()
	if item.jobRequest != nil && item.jobRequest.GetJob() != nil {
		delete(jobAttempts, item.jobRequest.GetJob().GetId())
	}
	return snapshotState{pending: pending, inflight: s.copyInflightLocked(), deadLetter: deadLetter, jobAttempts: jobAttempts}
}

func (s *queueServer) requeueExpiredLocked(now time.Time) error {
	for deliveryID, item := range s.inflight {
		if item.LeaseUntil.After(now) {
			continue
		}

		jobID := item.JobRequest.GetJob().GetId()
		if dispatchmeta.IsExpired(item.JobRequest, now) {
			if s.persistence != nil {
				if err := s.beforeFault(context.Background(), FaultPointExpiredDrop); err != nil {
					return err
				}

				if err := s.persistence.appendDropExpired(deliveryID, item.JobRequest, s.snapshotAfterExpiredDropLocked(deliveryID, item)); err != nil {
					return fmt.Errorf("persist expired delivery drop %s: %w", deliveryID, err)
				}
			}

			delete(s.inflight, deliveryID)
			delete(s.jobAttempts, jobID)

			s.log.Warn("Delivery %s for job %s expired after dispatch start deadline; dropped", deliveryID, jobID)
			if s.metrics != nil {
				s.metrics.RecordExpiredDropped(context.Background())
			}

			continue
		}

		attemptCount := s.jobAttempts[jobID] + 1

		if attemptCount > s.maxRequeueAttempts {
			if s.persistence != nil {
				if err := s.beforeFault(context.Background(), FaultPointDeadLetter); err != nil {
					return err
				}

				if err := s.persistence.appendDLQ(deliveryID, item.JobRequest, attemptCount, s.snapshotAfterDLQLocked(deliveryID, item, attemptCount)); err != nil {
					return fmt.Errorf("persist dlq move %s: %w", deliveryID, err)
				}
			}

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

			continue
		}

		if s.persistence != nil {
			if err := s.beforeFault(context.Background(), FaultPointExpiredRequeue); err != nil {
				return err
			}

			if err := s.persistence.appendRequeueExpired(deliveryID, item.JobRequest, s.snapshotAfterExpiredRequeueLocked(deliveryID, item, attemptCount)); err != nil {
				return fmt.Errorf("persist expired requeue %s: %w", deliveryID, err)
			}
		}

		s.jobAttempts[jobID] = attemptCount
		s.appendPendingLocked(item.JobRequest)
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
	s.pendingSeqs = make([]uint64, capHint)
	s.head = 0
	s.size = 0
	s.pendingSlots = make(map[uint64]int, len(jobs))
	s.pendingRequirementBySeq = make(map[uint64]uint64, len(jobs))
	s.pendingRequirementBuckets = make(map[uint64][]uint64)
	for _, jobReq := range jobs {
		s.appendPendingLocked(jobReq)
	}

	select {
	case s.notify <- struct{}{}:
	default:
	}
}

func (s *queueServer) appendPendingLocked(req *api.JobRequest) {
	if s.size == len(s.jobs) {
		s.grow()
	}

	tail := (s.head + s.size) % len(s.jobs)
	seq := s.nextPendingSeq
	s.nextPendingSeq++

	s.jobs[tail] = req
	s.pendingSeqs[tail] = seq
	s.pendingSlots[seq] = tail

	requirementMask := jobRequestIsolationRequirementMask(req)
	s.pendingRequirementBySeq[seq] = requirementMask
	s.pendingRequirementBuckets[requirementMask] = append(s.pendingRequirementBuckets[requirementMask], seq)
	s.size++
}

func (s *queueServer) removePendingHeadLocked() {
	if s.size == 0 {
		return
	}

	seq := s.pendingSeqs[s.head]
	s.unindexPendingSeqLocked(seq)
	s.jobs[s.head] = nil
	s.pendingSeqs[s.head] = 0
	s.head = (s.head + 1) % len(s.jobs)
	s.size--
	if s.size == 0 {
		s.head = 0
	}
}

func (s *queueServer) unindexPendingSeqLocked(seq uint64) {
	if seq == 0 {
		return
	}

	delete(s.pendingSlots, seq)
	requirementMask, ok := s.pendingRequirementBySeq[seq]
	if !ok {
		return
	}

	delete(s.pendingRequirementBySeq, seq)
	s.dropInactiveRequirementBucketHeadLocked(requirementMask)
}

func (s *queueServer) firstActivePendingSeqForRequirementLocked(requirementMask uint64) (uint64, bool) {
	s.dropInactiveRequirementBucketHeadLocked(requirementMask)
	seqs := s.pendingRequirementBuckets[requirementMask]
	if len(seqs) == 0 {
		return 0, false
	}

	return seqs[0], true
}

func (s *queueServer) dropInactiveRequirementBucketHeadLocked(requirementMask uint64) {
	seqs := s.pendingRequirementBuckets[requirementMask]
	for len(seqs) > 0 {
		if _, ok := s.pendingSlots[seqs[0]]; ok {
			break
		}

		seqs = seqs[1:]
	}

	if len(seqs) == 0 {
		delete(s.pendingRequirementBuckets, requirementMask)
		return
	}

	s.pendingRequirementBuckets[requirementMask] = seqs
}

func (s *queueServer) pendingOffsetForSlotLocked(slot int) int {
	if slot < 0 || len(s.jobs) == 0 {
		return -1
	}

	if slot >= s.head {
		return slot - s.head
	}

	return len(s.jobs) - s.head + slot
}

func (s *queueServer) grow() {
	newCap := len(s.jobs) * 2
	if newCap == 0 {
		newCap = 1
	}

	next := make([]*api.JobRequest, newCap)
	nextSeqs := make([]uint64, newCap)
	for i := 0; i < s.size; i++ {
		slot := (s.head + i) % len(s.jobs)
		next[i] = s.jobs[slot]
		nextSeqs[i] = s.pendingSeqs[slot]
		if nextSeqs[i] != 0 {
			s.pendingSlots[nextSeqs[i]] = i
		}
	}

	s.jobs = next
	s.pendingSeqs = nextSeqs
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
			if s.persistence != nil {
				if err := s.beforeFault(ctx, FaultPointDeadLetterRequeue); err != nil {
					return nil, err
				}
				if err := s.persistence.appendDLQRequeue(deliveryID, item.jobRequest, s.snapshotAfterDLQRequeueLocked(deliveryID, item)); err != nil {
					return nil, fmt.Errorf("persist dlq requeue %s: %w", deliveryID, err)
				}
			}

			s.appendPendingLocked(item.jobRequest)
			s.deadLetter = append(s.deadLetter[:i], s.deadLetter[i+1:]...)

			delete(s.jobAttempts, item.jobRequest.GetJob().GetId())
			s.log.Info("Requeued dead letter delivery %s for job %s", deliveryID, item.jobRequest.GetJob().GetId())

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
	healthpb.RegisterHealthServer(s, hs)
	hs.SetServingStatus("queue", healthpb.HealthCheckResponse_SERVING)

	api.RegisterQueueServiceServer(s, qs)
	return qs
}
