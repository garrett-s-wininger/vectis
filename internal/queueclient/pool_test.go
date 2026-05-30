package queueclient

import (
	"context"
	"testing"

	api "vectis/api/gen/go"
	"vectis/internal/queueid"

	"google.golang.org/grpc"
)

type fakeQueueServiceClient struct {
	enqueues int
	acks     []string
	tryJobs  []*api.JobRequest
}

func (f *fakeQueueServiceClient) Enqueue(context.Context, *api.JobRequest, ...grpc.CallOption) (*api.Empty, error) {
	f.enqueues++
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
