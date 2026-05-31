package queueclient

import (
	"context"
	"sort"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/interfaces/mocks"
	"vectis/internal/queueid"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeQueueServiceClient struct {
	enqueues    int
	enqueueErrs []error
	acks        []string
	tryErrs     []error
	tryJobs     []*api.JobRequest
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

func (f *fakeQueueServiceClient) Dequeue(context.Context, *api.Empty, ...grpc.CallOption) (*api.JobRequest, error) {
	if len(f.tryJobs) == 0 {
		return nil, nil
	}

	job := f.tryJobs[0]
	f.tryJobs = f.tryJobs[1:]
	return job, nil
}

func (f *fakeQueueServiceClient) TryDequeue(context.Context, *api.Empty, ...grpc.CallOption) (*api.JobRequest, error) {
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
