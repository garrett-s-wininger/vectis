package builtins

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"vectis/internal/action"
	"vectis/internal/interfaces"
	"vectis/internal/taskgraph"
)

type flakyNode struct {
	count      atomic.Int32
	failUntil  int32
	successOut map[string]any
}

func (n *flakyNode) Type() string { return "test/flaky" }

func (n *flakyNode) ValidateWith(map[string]string) []action.FieldError { return nil }

func (n *flakyNode) Execute(context.Context, *action.ExecutionState, map[string]any, action.Ports) action.Result {
	attempt := n.count.Add(1)
	if attempt <= n.failUntil {
		return action.NewFailureResult(errors.New("not yet"))
	}

	return action.NewSuccessResult(n.successOut)
}

type cancelAwareNode struct {
	started atomic.Int32
}

func (n *cancelAwareNode) Type() string { return "test/cancel-aware" }

func (n *cancelAwareNode) ValidateWith(map[string]string) []action.FieldError { return nil }

func (n *cancelAwareNode) Execute(ctx context.Context, _ *action.ExecutionState, _ map[string]any, _ action.Ports) action.Result {
	n.started.Add(1)
	<-ctx.Done()
	return action.NewFailureResult(ctx.Err())
}

func TestRetryNodeExecuteRetriesUntilSuccess(t *testing.T) {
	child := &flakyNode{failUntil: 1, successOut: map[string]any{"value": "ok"}}
	state := policyTestState(testResolver{"test/flaky": child})

	result := (&RetryNode{}).Execute(context.Background(), state, map[string]any{"attempts": "3"}, action.Ports{
		taskgraph.BodyPort: {ifTestNode("flaky", "test/flaky")},
	})

	if result.Status != action.StatusSuccess {
		t.Fatalf("status: got %s err=%v, want success", result.Status, result.Error)
	}

	if got := child.count.Load(); got != 2 {
		t.Fatalf("attempts: got %d, want 2", got)
	}

	if got := result.Outputs["value"]; got != "ok" {
		t.Fatalf("outputs: got %+v, want value=ok", result.Outputs)
	}
}

func TestRetryNodeExecuteFailsAfterAttempts(t *testing.T) {
	child := &flakyNode{failUntil: 3}
	state := policyTestState(testResolver{"test/flaky": child})

	result := (&RetryNode{}).Execute(context.Background(), state, map[string]any{"attempts": "2"}, action.Ports{
		taskgraph.BodyPort: {ifTestNode("flaky", "test/flaky")},
	})

	if result.Status != action.StatusFailure {
		t.Fatalf("status: got %s, want failure", result.Status)
	}

	if got := child.count.Load(); got != 2 {
		t.Fatalf("attempts: got %d, want 2", got)
	}
}

func TestTimeoutNodeExecuteTimesOut(t *testing.T) {
	child := &cancelAwareNode{}
	state := policyTestState(testResolver{"test/cancel-aware": child})

	result := (&TimeoutNode{}).Execute(context.Background(), state, map[string]any{"duration": "10ms"}, action.Ports{
		taskgraph.BodyPort: {ifTestNode("slow", "test/cancel-aware")},
	})

	if result.Status != action.StatusFailure {
		t.Fatalf("status: got %s, want failure", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), "timeout exceeded after") {
		t.Fatalf("error: got %v, want timeout", result.Error)
	}

	if got := child.started.Load(); got != 1 {
		t.Fatalf("started: got %d, want 1", got)
	}
}

func TestTimeoutNodeExecuteReturnsBodySuccess(t *testing.T) {
	child := &countedNode{count: &atomic.Int32{}, result: action.NewSuccessResult(map[string]any{"value": "ok"})}
	state := policyTestState(testResolver{"test/counted": child})

	result := (&TimeoutNode{}).Execute(context.Background(), state, map[string]any{"duration": time.Second.String()}, action.Ports{
		taskgraph.BodyPort: {ifTestNode("quick", "test/counted")},
	})

	if result.Status != action.StatusSuccess {
		t.Fatalf("status: got %s err=%v, want success", result.Status, result.Error)
	}

	if got := result.Outputs["value"]; got != "ok" {
		t.Fatalf("outputs: got %+v, want value=ok", result.Outputs)
	}
}

func TestFinallyNodeExecuteAlwaysRunsAfterBodyFailure(t *testing.T) {
	bodyErr := errors.New("body failed")
	bodyCount := &atomic.Int32{}
	alwaysCount := &atomic.Int32{}
	state := policyTestState(testResolver{
		"test/body":   &countedNode{count: bodyCount, result: action.NewFailureResult(bodyErr)},
		"test/always": &countedNode{count: alwaysCount, result: action.NewSuccessResult(nil)},
	})

	result := (&FinallyNode{}).Execute(context.Background(), state, nil, action.Ports{
		taskgraph.BodyPort:   {ifTestNode("body", "test/body")},
		taskgraph.AlwaysPort: {ifTestNode("always", "test/always")},
	})

	if result.Status != action.StatusFailure {
		t.Fatalf("status: got %s, want failure", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), bodyErr.Error()) {
		t.Fatalf("error: got %v, want body failure", result.Error)
	}

	if got := bodyCount.Load(); got != 1 {
		t.Fatalf("body executions: got %d, want 1", got)
	}

	if got := alwaysCount.Load(); got != 1 {
		t.Fatalf("always executions: got %d, want 1", got)
	}
}

func TestFinallyNodeExecuteReturnsAlwaysFailureAfterBodySuccess(t *testing.T) {
	alwaysErr := errors.New("cleanup failed")
	state := policyTestState(testResolver{
		"test/body":   &countedNode{count: &atomic.Int32{}, result: action.NewSuccessResult(nil)},
		"test/always": &countedNode{count: &atomic.Int32{}, result: action.NewFailureResult(alwaysErr)},
	})

	result := (&FinallyNode{}).Execute(context.Background(), state, nil, action.Ports{
		taskgraph.BodyPort:   {ifTestNode("body", "test/body")},
		taskgraph.AlwaysPort: {ifTestNode("always", "test/always")},
	})

	if result.Status != action.StatusFailure {
		t.Fatalf("status: got %s, want failure", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), alwaysErr.Error()) {
		t.Fatalf("error: got %v, want always failure", result.Error)
	}
}

func policyTestState(resolver action.Resolver) *action.ExecutionState {
	return &action.ExecutionState{
		JobID:    "policy-test",
		Logger:   interfaces.NewLogger("policy-test"),
		Resolver: resolver,
	}
}
