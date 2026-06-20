package queueclient

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/queueid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeQueueServiceClient struct {
	enqueues           int
	enqueueErrs        []error
	acks               []string
	tryErrs            []error
	tryJobs            []*api.JobRequest
	tryDequeueRequests []*api.DequeueRequest
}

func (f *fakeQueueServiceClient) Enqueue(context.Context, *api.JobRequest, ...grpc.CallOption) (*api.Empty, error) {
	f.enqueues++
	if len(f.enqueueErrs) > 0 {
		err := f.enqueueErrs[0]
		f.enqueueErrs = f.enqueueErrs[1:]
		if err != nil {
			return nil, err
		}
	}

	return &api.Empty{}, nil
}

func (f *fakeQueueServiceClient) Dequeue(context.Context, *api.DequeueRequest, ...grpc.CallOption) (*api.JobRequest, error) {
	if len(f.tryJobs) == 0 {
		return nil, nil
	}

	job := f.tryJobs[0]
	f.tryJobs = f.tryJobs[1:]
	return job, nil
}

func (f *fakeQueueServiceClient) TryDequeue(_ context.Context, req *api.DequeueRequest, _ ...grpc.CallOption) (*api.JobRequest, error) {
	f.tryDequeueRequests = append(f.tryDequeueRequests, req)
	return f.tryDequeue()
}

func (f *fakeQueueServiceClient) tryDequeue() (*api.JobRequest, error) {
	if len(f.tryErrs) > 0 {
		err := f.tryErrs[0]
		f.tryErrs = f.tryErrs[1:]
		if err != nil {
			return nil, err
		}
	}

	if len(f.tryJobs) == 0 {
		return nil, nil
	}

	job := f.tryJobs[0]
	f.tryJobs = f.tryJobs[1:]
	return job, nil
}

func (f *fakeQueueServiceClient) Ack(_ context.Context, req *api.AckRequest, _ ...grpc.CallOption) (*api.Empty, error) {
	f.acks = append(f.acks, req.GetDeliveryId())
	return &api.Empty{}, nil
}

func (f *fakeQueueServiceClient) ListDeadLetter(context.Context, *api.Empty, ...grpc.CallOption) (*api.ListDeadLetterResponse, error) {
	return &api.ListDeadLetterResponse{}, nil
}

func (f *fakeQueueServiceClient) RequeueDeadLetter(context.Context, *api.RequeueDeadLetterRequest, ...grpc.CallOption) (*api.Empty, error) {
	return &api.Empty{}, nil
}

func TestQueuePoolChooseEndpointPowerOfTwoUsesLowerInflight(t *testing.T) {
	a := &fakeQueueServiceClient{}
	b := &fakeQueueServiceClient{}
	p := &queuePool{
		endpoints: map[string]*queuePoolEndpoint{
			"a": {id: "a", client: a},
			"b": {id: "b", client: b},
		},
		activeIDs: []string{"a", "b"},
	}
	p.endpoints["b"].inflight.Store(10)

	ep, err := p.chooseEndpoint()
	if err != nil {
		t.Fatalf("choose endpoint: %v", err)
	}

	if ep.id != "a" {
		t.Fatalf("expected lower-inflight endpoint a, got %s", ep.id)
	}
}

func TestQueuePoolEnqueueRetriesTransientFailureOnAlternateShard(t *testing.T) {
	a := &fakeQueueServiceClient{}
	b := &fakeQueueServiceClient{
		enqueueErrs: []error{status.Error(codes.Unavailable, "queue shard unavailable")},
	}

	p := newFakeQueuePool(map[string]*fakeQueueServiceClient{
		"a": a,
		"b": b,
	})

	jobID := "job-transient-enqueue"
	if _, err := p.enqueue(context.Background(), &api.JobRequest{Job: &api.Job{Id: &jobID}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if b.enqueues != 1 {
		t.Fatalf("expected first enqueue attempt on shard b, got %d", b.enqueues)
	}

	if a.enqueues != 1 {
		t.Fatalf("expected retry on shard a, got %d", a.enqueues)
	}
}

func TestQueuePoolEnqueueDoesNotRetryNonTransientFailure(t *testing.T) {
	a := &fakeQueueServiceClient{}
	b := &fakeQueueServiceClient{
		enqueueErrs: []error{status.Error(codes.InvalidArgument, "bad job")},
	}
	p := newFakeQueuePool(map[string]*fakeQueueServiceClient{
		"a": a,
		"b": b,
	})

	jobID := "job-invalid-enqueue"
	if _, err := p.enqueue(context.Background(), &api.JobRequest{Job: &api.Job{Id: &jobID}}); err == nil {
		t.Fatal("expected enqueue error")
	}

	if b.enqueues != 1 {
		t.Fatalf("expected one enqueue attempt on shard b, got %d", b.enqueues)
	}

	if a.enqueues != 0 {
		t.Fatalf("expected non-transient failure not to retry shard a, got %d attempts", a.enqueues)
	}
}

func TestQueuePoolTryDequeueScansPastTransientShard(t *testing.T) {
	jobID := "job-survivor-dequeue"
	a := &fakeQueueServiceClient{
		tryJobs: []*api.JobRequest{{Job: &api.Job{Id: &jobID}}},
	}

	b := &fakeQueueServiceClient{
		tryErrs: []error{status.Error(codes.Unavailable, "queue shard unavailable")},
	}

	p := newFakeQueuePool(map[string]*fakeQueueServiceClient{
		"a": a,
		"b": b,
	})

	got, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("try dequeue: %v", err)
	}

	if got.GetJob().GetId() != jobID {
		t.Fatalf("expected job from survivor shard, got %+v", got.GetJob())
	}
}

func TestQueuePoolTryDequeueSendsSupportedIsolation(t *testing.T) {
	jobID := "job-filtered-dequeue"
	a := &fakeQueueServiceClient{
		tryJobs: []*api.JobRequest{{Job: &api.Job{Id: &jobID}}},
	}

	p := newFakeQueuePool(map[string]*fakeQueueServiceClient{"a": a})
	if err := p.setDequeueSupportedIsolation([]string{" host ", "vm", "host"}); err != nil {
		t.Fatalf("set supported isolation: %v", err)
	}

	got, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("try dequeue: %v", err)
	}

	if got.GetJob().GetId() != jobID {
		t.Fatalf("expected job %s, got %+v", jobID, got.GetJob())
	}

	if len(a.tryDequeueRequests) != 1 {
		t.Fatalf("expected one dequeue request, got %d", len(a.tryDequeueRequests))
	}

	if got := a.tryDequeueRequests[0].GetSupportedIsolation(); strings.Join(got, ",") != "host,vm" {
		t.Fatalf("supported isolation request = %v, want host,vm", got)
	}
}

func TestQueuePoolRejectsInvalidSupportedIsolation(t *testing.T) {
	p := newFakeQueuePool(map[string]*fakeQueueServiceClient{"a": {}})
	if err := p.setDequeueSupportedIsolation([]string{"container"}); err == nil {
		t.Fatal("expected invalid supported isolation error")
	}
}

func TestQueuePoolTryDequeueReturnsErrorWhenAllShardsUnavailable(t *testing.T) {
	a := &fakeQueueServiceClient{
		tryErrs: []error{status.Error(codes.Unavailable, "queue shard a unavailable")},
	}

	b := &fakeQueueServiceClient{
		tryErrs: []error{status.Error(codes.Unavailable, "queue shard b unavailable")},
	}

	p := newFakeQueuePool(map[string]*fakeQueueServiceClient{
		"a": a,
		"b": b,
	})

	if _, err := p.tryDequeue(context.Background()); err == nil {
		t.Fatal("expected error when all queue shards are unavailable")
	}
}

func TestQueuePoolTryDequeueConnectionLimitWalksOneShardAtATime(t *testing.T) {
	jobID := "job-on-second-shard"
	p, dialed, closed := newLazyFakeQueuePool(map[string]*fakeQueueServiceClient{
		"a": {},
		"b": {tryJobs: []*api.JobRequest{{Job: &api.Job{Id: &jobID}}}},
	})

	p.dequeueCounter.Store(^uint64(0))
	got, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("first try dequeue: %v", err)
	}

	if got != nil {
		t.Fatalf("first try dequeue got job %+v, want nil", got.GetJob())
	}

	if strings.Join(*dialed, ",") != "a" {
		t.Fatalf("dialed endpoints after first try = %v, want [a]", *dialed)
	}

	if len(*closed) != 0 {
		t.Fatalf("closed endpoints after first try = %v, want none", *closed)
	}

	got, err = p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("second try dequeue: %v", err)
	}

	if got.GetJob().GetId() != jobID {
		t.Fatalf("second try dequeue got %+v, want job %s", got.GetJob(), jobID)
	}

	if strings.Join(*dialed, ",") != "a,b" {
		t.Fatalf("dialed endpoints after second try = %v, want [a b]", *dialed)
	}

	if strings.Join(*closed, ",") != "a" {
		t.Fatalf("closed endpoints after second try = %v, want [a]", *closed)
	}
}

func TestQueuePoolTryDequeueConnectionLimitSticksToSuccessfulShard(t *testing.T) {
	job := func(id string) *api.JobRequest {
		return &api.JobRequest{Job: &api.Job{Id: &id}}
	}

	p, dialed, closed := newLazyFakeQueuePool(map[string]*fakeQueueServiceClient{
		"a": {tryJobs: []*api.JobRequest{job("a-1"), job("a-2")}},
		"b": {tryJobs: []*api.JobRequest{job("b-1")}},
	})

	p.dequeueCounter.Store(^uint64(0))
	first, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("first try dequeue: %v", err)
	}
	if first.GetJob().GetId() != "a-1" {
		t.Fatalf("first try dequeue got %+v, want a-1", first.GetJob())
	}

	second, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("second try dequeue: %v", err)
	}
	if second.GetJob().GetId() != "a-2" {
		t.Fatalf("second try dequeue got %+v, want a-2", second.GetJob())
	}

	if strings.Join(*dialed, ",") != "a" {
		t.Fatalf("dialed endpoints = %v, want [a]", *dialed)
	}
	if len(*closed) != 0 {
		t.Fatalf("closed endpoints = %v, want none", *closed)
	}
}

func TestQueuePoolTryDequeueConnectionLimitSamplesNewShardAfterStickyBudget(t *testing.T) {
	job := func(id string) *api.JobRequest {
		return &api.JobRequest{Job: &api.Job{Id: &id}}
	}

	aJobs := make([]*api.JobRequest, 0, defaultDequeueStickySuccessBudget+2)
	for i := 0; i < defaultDequeueStickySuccessBudget+2; i++ {
		aJobs = append(aJobs, job("a"))
	}

	clients := map[string]*fakeQueueServiceClient{"a": {tryJobs: aJobs}}
	p, dialed, closed := newLazyFakeQueuePool(clients)

	p.dequeueCounter.Store(^uint64(0))
	first, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("initial single-shard dequeue: %v", err)
	}
	if first.GetJob().GetId()[0] != 'a' {
		t.Fatalf("initial single-shard dequeue got %+v, want shard a", first.GetJob())
	}

	clients["b"] = &fakeQueueServiceClient{tryJobs: []*api.JobRequest{job("b-1")}}
	p.mu.Lock()
	ep := &queuePoolEndpoint{id: "b", address: "b"}
	p.endpoints["b"] = ep
	p.activeIDs = append(p.activeIDs, "b")
	p.active = append(p.active, ep)
	p.mu.Unlock()

	for i := 0; i < defaultDequeueStickySuccessBudget; i++ {
		got, err := p.tryDequeue(context.Background())
		if err != nil {
			t.Fatalf("sticky dequeue %d: %v", i, err)
		}
		if got.GetJob().GetId()[0] != 'a' {
			t.Fatalf("sticky dequeue %d got %+v, want shard a", i, got.GetJob())
		}
	}

	got, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("post-budget dequeue: %v", err)
	}
	if got.GetJob().GetId() != "b-1" {
		t.Fatalf("post-budget dequeue got %+v, want b-1", got.GetJob())
	}

	if strings.Join(*dialed, ",") != "a,b" {
		t.Fatalf("dialed endpoints = %v, want [a b]", *dialed)
	}
	if strings.Join(*closed, ",") != "a" {
		t.Fatalf("closed endpoints = %v, want [a]", *closed)
	}
}

func TestQueuePoolTryDequeueConnectionLimitHonorsConfiguredStickyBudget(t *testing.T) {
	job := func(id string) *api.JobRequest {
		return &api.JobRequest{Job: &api.Job{Id: &id}}
	}

	p, _, _ := newLazyFakeQueuePool(map[string]*fakeQueueServiceClient{
		"a": {tryJobs: []*api.JobRequest{job("a-1"), job("a-2"), job("a-3")}},
		"b": {tryJobs: []*api.JobRequest{job("b-1")}},
	})
	p.opts.DequeueStickySuccessBudget = 2

	p.dequeueCounter.Store(^uint64(0))
	first, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("first try dequeue: %v", err)
	}
	if first.GetJob().GetId() != "a-1" {
		t.Fatalf("first try dequeue got %+v, want a-1", first.GetJob())
	}

	second, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("second try dequeue: %v", err)
	}
	if second.GetJob().GetId() != "a-2" {
		t.Fatalf("second try dequeue got %+v, want a-2", second.GetJob())
	}

	third, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("third try dequeue: %v", err)
	}
	if third.GetJob().GetId() != "b-1" {
		t.Fatalf("third try dequeue got %+v, want b-1", third.GetJob())
	}
}

func TestQueuePoolTryDequeueConnectionLimitRotatesAfterStickyShardEmpty(t *testing.T) {
	job := func(id string) *api.JobRequest {
		return &api.JobRequest{Job: &api.Job{Id: &id}}
	}

	p, dialed, closed := newLazyFakeQueuePool(map[string]*fakeQueueServiceClient{
		"a": {tryJobs: []*api.JobRequest{job("a-1")}},
		"b": {tryJobs: []*api.JobRequest{job("b-1")}},
	})

	p.dequeueCounter.Store(^uint64(0))
	first, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("first try dequeue: %v", err)
	}
	if first.GetJob().GetId() != "a-1" {
		t.Fatalf("first try dequeue got %+v, want a-1", first.GetJob())
	}

	empty, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("second try dequeue: %v", err)
	}
	if empty != nil {
		t.Fatalf("second try dequeue got job %+v, want nil", empty.GetJob())
	}

	third, err := p.tryDequeue(context.Background())
	if err != nil {
		t.Fatalf("third try dequeue: %v", err)
	}
	if third.GetJob().GetId() != "b-1" {
		t.Fatalf("third try dequeue got %+v, want b-1", third.GetJob())
	}

	if strings.Join(*dialed, ",") != "a,b" {
		t.Fatalf("dialed endpoints = %v, want [a b]", *dialed)
	}
	if strings.Join(*closed, ",") != "a" {
		t.Fatalf("closed endpoints = %v, want [a]", *closed)
	}
}

func TestQueuePoolAckRoutesToDeliveryShard(t *testing.T) {
	a := &fakeQueueServiceClient{}
	b := &fakeQueueServiceClient{}
	p := &queuePool{
		endpoints: map[string]*queuePoolEndpoint{
			"a": {id: "a", client: a},
			"b": {id: "b", client: b},
		},
		activeIDs: []string{"a", "b"},
	}

	deliveryID := queueid.Encode("b", "delivery-1")
	if err := p.ack(context.Background(), deliveryID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if len(a.acks) != 0 {
		t.Fatalf("expected no ack on shard a, got %+v", a.acks)
	}

	if len(b.acks) != 1 || b.acks[0] != deliveryID {
		t.Fatalf("expected ack on shard b, got %+v", b.acks)
	}
}

func TestQueuePoolAckUsesPinnedEndpointForShardedDeliveryID(t *testing.T) {
	pinned := &fakeQueueServiceClient{}
	p := &queuePool{
		opts: QueuePoolOptions{PinnedAddress: "localhost:8081"},
		endpoints: map[string]*queuePoolEndpoint{
			"pinned": {id: "pinned", address: "localhost:8081", client: pinned},
		},
		activeIDs: []string{"pinned"},
	}

	deliveryID := queueid.Encode("queue-1", "delivery-1")
	if err := p.ack(context.Background(), deliveryID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if len(pinned.acks) != 1 || pinned.acks[0] != deliveryID {
		t.Fatalf("expected ack on pinned endpoint, got %+v", pinned.acks)
	}
}

func TestProportionalLowerBoundJitter(t *testing.T) {
	delay := time.Second

	if got := proportionalLowerBoundJitter(delay, 0, 0); got != delay {
		t.Fatalf("zero ratio should not jitter: got %v", got)
	}

	if got := proportionalLowerBoundJitter(delay, 0.2, 0); got != 800*time.Millisecond {
		t.Fatalf("sample at lower bound: got %v", got)
	}

	if got := proportionalLowerBoundJitter(delay, 0.2, 1); got != delay {
		t.Fatalf("sample at upper bound: got %v", got)
	}

	if got := proportionalLowerBoundJitter(delay, 0.2, -1); got != 800*time.Millisecond {
		t.Fatalf("sample below range should clamp low: got %v", got)
	}

	if got := proportionalLowerBoundJitter(delay, 0.2, 2); got != delay {
		t.Fatalf("sample above range should clamp high: got %v", got)
	}

	if got := proportionalLowerBoundJitter(delay, 2, 0); got != 0 {
		t.Fatalf("ratio above one should clamp to full lower-bound jitter: got %v", got)
	}
}

func TestQueuePoolLegacyAckBroadcasts(t *testing.T) {
	a := &fakeQueueServiceClient{}
	b := &fakeQueueServiceClient{}
	p := &queuePool{
		endpoints: map[string]*queuePoolEndpoint{
			"a": {id: "a", client: a},
			"b": {id: "b", client: b},
		},
		activeIDs: []string{"a", "b"},
	}

	if err := p.ack(context.Background(), "legacy-delivery"); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if len(a.acks) != 1 || len(b.acks) != 1 {
		t.Fatalf("expected legacy ack to broadcast, got a=%+v b=%+v", a.acks, b.acks)
	}
}

func newFakeQueuePool(clients map[string]*fakeQueueServiceClient) *queuePool {
	p := &queuePool{
		logger:    mocks.NopLogger{},
		endpoints: make(map[string]*queuePoolEndpoint, len(clients)),
	}
	_ = p.setDequeueSupportedIsolation(nil)

	ids := make([]string, 0, len(clients))
	for id := range clients {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		client := clients[id]
		ep := &queuePoolEndpoint{id: id, address: id, client: client}
		p.endpoints[id] = ep
		p.activeIDs = append(p.activeIDs, id)
		p.active = append(p.active, ep)
	}

	p.dial = func(_ context.Context, id, address string) (*queuePoolEndpoint, error) {
		client := clients[id]
		if client == nil {
			return nil, status.Error(codes.Unavailable, "queue shard unavailable")
		}

		return &queuePoolEndpoint{id: id, address: address, client: client}, nil
	}

	return p
}

func newLazyFakeQueuePool(clients map[string]*fakeQueueServiceClient) (*queuePool, *[]string, *[]string) {
	p := &queuePool{
		logger: mocks.NopLogger{},
		opts: QueuePoolOptions{
			DequeueConnectionLimit:     1,
			DequeueStickySuccessBudget: defaultDequeueStickySuccessBudget,
		},
		endpoints: make(map[string]*queuePoolEndpoint, len(clients)),
	}

	p.setDequeueSupportedIsolation(nil)
	ids := make([]string, 0, len(clients))
	for id := range clients {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	for _, id := range ids {
		ep := &queuePoolEndpoint{id: id, address: id}
		p.endpoints[id] = ep
		p.activeIDs = append(p.activeIDs, id)
		p.active = append(p.active, ep)
	}

	dialed := make([]string, 0, len(clients))
	closed := make([]string, 0, len(clients))
	p.dial = func(_ context.Context, id, address string) (*queuePoolEndpoint, error) {
		client := clients[id]
		if client == nil {
			return nil, status.Error(codes.Unavailable, "queue shard unavailable")
		}

		dialed = append(dialed, id)
		return &queuePoolEndpoint{
			id:      id,
			address: address,
			client:  client,
			cleanup: func() {
				closed = append(closed, id)
			},
		}, nil
	}

	return p, &dialed, &closed
}
