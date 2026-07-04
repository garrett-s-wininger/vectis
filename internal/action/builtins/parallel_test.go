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

type testResolver map[string]action.Node

func (r testResolver) Resolve(uses string) (action.Node, error) {
	n, ok := r[uses]
	if !ok {
		return nil, errors.New("unknown test action")
	}

	return n, nil
}

type barrierNode struct {
	started chan struct{}
	release chan struct{}
}

func (n *barrierNode) Type() string { return "test/barrier" }

func (n *barrierNode) ValidateWith(map[string]string) []action.FieldError { return nil }

func (n *barrierNode) Execute(ctx context.Context, _ *action.ExecutionState, _ map[string]any, _ action.Ports) action.Result {
	select {
	case n.started <- struct{}{}:
	case <-ctx.Done():
		return action.NewFailureResult(ctx.Err())
	}

	select {
	case <-n.release:
		return action.NewSuccessResult(nil)
	case <-ctx.Done():
		return action.NewFailureResult(ctx.Err())
	}
}

type countedNode struct {
	count  *atomic.Int32
	result action.Result
}

func (n *countedNode) Type() string { return "test/counted" }

func (n *countedNode) ValidateWith(map[string]string) []action.FieldError { return nil }

func (n *countedNode) Execute(context.Context, *action.ExecutionState, map[string]any, action.Ports) action.Result {
	n.count.Add(1)
	return n.result
}

func TestParallelNodeExecuteRunsChildrenConcurrently(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	child := &barrierNode{started: started, release: release}
	state := &action.ExecutionState{
		JobID:    "parallel-test",
		Logger:   interfaces.NewLogger("parallel-test"),
		Resolver: testResolver{"test/barrier": child},
	}
	uses := "test/barrier"
	ports := action.Ports{taskgraph.BranchesPort: {{Uses: &uses}, {Uses: &uses}}}

	done := make(chan action.Result, 1)
	go func() {
		done <- (&ParallelNode{}).Execute(context.Background(), state, nil, ports)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("parallel children did not both start before release")
		}
	}

	close(release)
	select {
	case result := <-done:
		if result.Status != action.StatusSuccess {
			t.Fatalf("parallel result: got %s err=%v, want success", result.Status, result.Error)
		}
	case <-time.After(time.Second):
		t.Fatal("parallel execution did not finish")
	}
}

func TestParallelNodeExecuteReturnsFirstFailure(t *testing.T) {
	var count atomic.Int32
	wantErr := errors.New("branch failed")
	child := &countedNode{count: &count, result: action.NewFailureResult(wantErr)}
	state := &action.ExecutionState{
		JobID:    "parallel-failure-test",
		Logger:   interfaces.NewLogger("parallel-failure-test"),
		Resolver: testResolver{"test/counted": child},
	}

	uses := "test/counted"
	ports := action.Ports{taskgraph.BranchesPort: {{Uses: &uses}, {Uses: &uses}}}

	result := (&ParallelNode{}).Execute(context.Background(), state, nil, ports)
	if result.Status != action.StatusFailure {
		t.Fatalf("parallel result: got %s, want failure", result.Status)
	}

	if result.Error == nil || !strings.Contains(result.Error.Error(), wantErr.Error()) {
		t.Fatalf("parallel error: got %v, want %v", result.Error, wantErr)
	}

	if got := count.Load(); got != 2 {
		t.Fatalf("executed children: got %d, want 2", got)
	}
}

func TestParallelNodeType(t *testing.T) {
	if got := (&ParallelNode{}).Type(); got != "builtins/parallel" {
		t.Fatalf("type: got %q, want builtins/parallel", got)
	}
}
